# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import datetime
from unittest.mock import ANY, MagicMock, call

import pytest
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)
from sqlalchemy import func, select
from sqlalchemy.orm import joinedload


@pytest.fixture
def node_event():
    """Fixture that returns a sample NodeEvent for testing"""
    return NodeEvent.create(
        node="test-node-1",
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        job_id="123",
        job_attempt_index=0,
        job_anomaly_id=uuid.uuid4(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.SYSTEM,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status=FactStatus.OK,
    )


@pytest.fixture
def global_event():
    """Fixture that returns a sample GlobalEvent for testing"""
    return GlobalEvent.create(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        job_id="123",
        job_attempt_index=0,
        job_anomaly_id=uuid.uuid4(),
        error_attribution=ErrorAttribution.APPLICATION,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
        fact_status=FactStatus.OK,
    )


@pytest.fixture
def gpu_error():
    """Fixture that returns a sample GPU Error for testing"""
    return JobExecutionError.create(
        code=1,
        message="GPU error message",
        description="Detailed error information",
        signature="GPU-test-signature",
    )


@pytest.fixture
def network_error():
    """Fixture that returns a sample network Error for testing"""
    return JobExecutionError.create(
        code=2,
        message="Network error message",
        description="Detailed error information",
        signature="NETWORK-test-signature",
    )


@pytest.fixture
def session():
    """Fixture that returns a sample session for testing"""
    return MagicMock()


@pytest.fixture
def connection(session):
    """Fixture that returns a sample MySQLConnection for testing"""
    ret = MagicMock(spec=MySQLConnection)
    ret.session = MagicMock()
    ret.session.return_value.__enter__.return_value = session
    return ret


def test_node_storage_init(connection):
    """Test AttributionStorage initialization sets up a connection"""
    storage = AttributionStorage(connection)
    assert storage.connection == connection


def test_bootstrap_with_schema_upgrade(connection):
    """Test bootstrap forwards to the connection bootstrap and ready functions"""
    connection.bootstrap.return_value = True
    connection.ready.return_value = True
    storage = AttributionStorage(connection)

    result = storage.bootstrap(schema_upgrade=True)
    assert result is True
    connection.bootstrap.assert_called_with(schema_upgrade=True)
    connection.ready.assert_called_once()


def test_bootstrap_without_schema_upgrade(connection):
    """Test bootstrap forwards to the connection bootstrap and ready functions"""
    connection.bootstrap.return_value = True
    connection.ready.return_value = True
    storage = AttributionStorage(connection)

    result = storage.bootstrap(schema_upgrade=False)
    assert result is True
    connection.bootstrap.assert_called_with(schema_upgrade=False)
    connection.ready.assert_called_once()


def test_ready(connection):
    """Test ready() returns True when database connection succeeds"""
    connection.ready.return_value = True
    storage = AttributionStorage(connection)
    assert storage.connection == connection

    result = storage.ready()
    assert result is True
    connection.ready.assert_called_once()


def test_store_attribution(connection, session, node_event, gpu_error, network_error):
    """Test store_attribution() creates new record when no existing error"""
    storage = AttributionStorage(connection)
    session.query.return_value = MagicMock()
    session.query.return_value.filter.return_value = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    storage.store_attribution(gpu_error, [node_event])
    session.query.return_value.filter.return_value.first.return_value = None
    storage.store_attribution(network_error, [node_event, node_event])

    session.query.return_value.filter.return_value.first.return_value = gpu_error
    storage.store_attribution(gpu_error, [node_event])
    session.query.return_value.filter.return_value.first.return_value = network_error
    storage.store_attribution(network_error, [node_event, node_event])

    node_event.error_id = ANY
    session.add.assert_has_calls(
        [
            call(gpu_error),
            call(node_event),
            call(network_error),
            call(node_event),
            call(node_event),
            call(node_event),
            call(node_event),
            call(node_event),
        ]
    )


def test_store_attribution_with_global_event(connection, session, global_event, gpu_error):
    """Test store_attribution() creates new record with GlobalEvent"""
    storage = AttributionStorage(connection)
    session.query.return_value = MagicMock()
    session.query.return_value.filter.return_value = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    storage.store_attribution(gpu_error, [global_event])

    global_event.error_id = ANY
    session.add.assert_has_calls(
        [
            call(gpu_error),
            call(global_event),
        ]
    )


def test_store_attribution_with_mixed_events(connection, session, node_event, global_event, gpu_error):
    """Test store_attribution() creates records with mixed NodeEvent and GlobalEvent"""
    storage = AttributionStorage(connection)
    session.query.return_value = MagicMock()
    session.query.return_value.filter.return_value = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    storage.store_attribution(gpu_error, [node_event, global_event])

    node_event.error_id = ANY
    global_event.error_id = ANY
    session.add.assert_has_calls(
        [
            call(gpu_error),
            call(node_event),
            call(global_event),
        ]
    )


def test_read_node_attributions_for_job(connection, session):
    """Test read_node_attributions_for_job() queries the database correctly

    This test verifies that:
    1. The correct SQL query is constructed with the job_id filter
    2. The job_attempt_index filter is added when specified and >= 0
    3. Results are ordered by job_attempt_index and timestampdescending
    4. The session is used correctly to execute the query
    """
    storage = AttributionStorage(connection)

    job_id = "123"
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    node_event_1 = NodeEvent.create(node="test-node-1", job_id=job_id, job_attempt_index=0, timestamp=timestamp)
    node_event_2 = NodeEvent.create(node="test-node-2", job_id=job_id, job_attempt_index=1, timestamp=timestamp)
    node_event_3 = NodeEvent.create(node="test-node-3", job_id=job_id, job_attempt_index=2, timestamp=timestamp)

    # Test without job_attempt_index
    session.scalars.return_value.all.return_value = [
        node_event_3,
        node_event_2,
        node_event_1,
    ]
    ret = storage.read_node_attributions_for_job(job_id)
    assert ret == [node_event_3, node_event_2, node_event_1]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.job_id == job_id)
        .order_by(NodeEvent.job_attempt_index.desc(), NodeEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index >= 0
    job_attempt_index = 0
    session.scalars.return_value.all.return_value = [node_event_1]
    ret = storage.read_node_attributions_for_job(job_id, job_attempt_index)
    assert ret == [node_event_1]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.job_id == job_id)
        .where(NodeEvent.job_attempt_index == job_attempt_index)
        .order_by(NodeEvent.job_attempt_index.desc(), NodeEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index == -1
    job_attempt_index = -1
    session.scalars.return_value.all.return_value = [
        node_event_3,
        node_event_3,
        node_event_2,
        node_event_1,
    ]
    ret = storage.read_node_attributions_for_job(job_id, job_attempt_index)
    assert ret == [node_event_3, node_event_3]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.job_id == job_id)
        .order_by(NodeEvent.job_attempt_index.desc(), NodeEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index < -1
    job_attempt_index = -2
    with pytest.raises(ValueError):
        storage.read_node_attributions_for_job(job_id, job_attempt_index)


def test_read_attributions(connection, session):
    """Test read_attributions() queries the database correctly

    This test verifies that:
    1. The correct SQL query is constructed with the node names and job ids filters
    2. The correct SQL query is constructed with the node names filter
    3. The correct SQL query is constructed with the job ids filter
    4. The correct SQL query is constructed with the timestamp range filters
    5. The correct SQL query is constructed with the limit and offset parameters
    6. The correct SQL query is constructed with the anomaly_uuid filter
    7. The session is used correctly to execute the query
    """
    storage = AttributionStorage(connection)

    # Test with default limit/offset
    node_names = ["test-node-1", "test-node-2"]
    job_ids = ["123", "456"]
    start_timestamp = datetime(2024, 1, 1)
    end_timestamp = datetime(2024, 1, 2)
    limit = 10
    offset = 5

    # Verify the query construction with all filters
    storage.read_attributions(
        node_names=node_names, job_ids=job_ids, start_timestamp=start_timestamp, end_timestamp=end_timestamp
    )
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = (
        select(func.count(NodeEvent.id))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
    )
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with node names filter
    storage.read_attributions(node_names=node_names, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = (
        select(func.count(NodeEvent.id))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
    )
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with job ids filter
    storage.read_attributions(job_ids=job_ids, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = (
        select(func.count(NodeEvent.id))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
    )
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with custom limit/offset
    storage.read_attributions(
        node_names=node_names,
        job_ids=job_ids,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        limit=limit,
        offset=offset,
    )
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
        .limit(limit)
        .offset(offset)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = (
        select(func.count(NodeEvent.id))
        .where(NodeEvent.node.in_(node_names))
        .where(NodeEvent.job_id.in_(job_ids))
        .where(NodeEvent.timestamp >= start_timestamp)
        .where(NodeEvent.timestamp < end_timestamp)
    )
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with start_timestamp only
    storage.read_attributions(start_timestamp=start_timestamp)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.timestamp >= start_timestamp)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(NodeEvent.id)).where(NodeEvent.timestamp >= start_timestamp)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with end_timestamp only
    storage.read_attributions(end_timestamp=end_timestamp)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.timestamp < end_timestamp)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(NodeEvent.id)).where(NodeEvent.timestamp < end_timestamp)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction without job id and timestamps
    storage.read_attributions(node_names=node_names)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.node.in_(node_names))
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(NodeEvent.id)).where(NodeEvent.node.in_(node_names))
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Verify the query construction with anomaly_uuid filter
    anomaly_uuid = uuid.uuid4()
    storage.read_attributions(anomaly_uuid=anomaly_uuid)
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(NodeEvent)
        .options(joinedload(NodeEvent.job_execution_error))
        .where(NodeEvent.job_anomaly_id == anomaly_uuid)
        .limit(50)
        .offset(0)
        .order_by(NodeEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(NodeEvent.id)).where(NodeEvent.job_anomaly_id == anomaly_uuid)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)


def test_read_global_attributions(connection, session):
    """Test read_global_attributions() queries the database correctly

    This test verifies that:
    1. The correct SQL query is constructed with job_ids filter
    2. Timestamp range, anomaly_uuid, limit/offset filters work correctly
    3. Results ordered by timestamp DESC only (no job_attempt_index)
    4. The session is used correctly to execute the query
    """
    storage = AttributionStorage(connection)

    job_id = "123"
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    global_event_0 = GlobalEvent.create(job_id=job_id, job_attempt_index=0, timestamp=timestamp)
    global_event_1 = GlobalEvent.create(job_id=job_id, job_attempt_index=1, timestamp=timestamp)
    global_event_2 = GlobalEvent.create(job_id=job_id, job_attempt_index=2, timestamp=timestamp)

    # Test with job_ids filter
    session.scalars.return_value.first.return_value = 3
    session.scalars.return_value.all.return_value = [
        global_event_2,
        global_event_1,
        global_event_0,
    ]
    ret, count = storage.read_global_attributions(job_ids=[job_id])
    assert ret == [global_event_2, global_event_1, global_event_0]
    assert count == 3
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_id.in_([job_id]))
        .limit(50)
        .offset(0)
        .order_by(GlobalEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(GlobalEvent.id)).where(GlobalEvent.job_id.in_([job_id]))
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Test with all filters combined
    job_ids = ["123", "456"]
    start_timestamp = datetime(2024, 1, 1)
    end_timestamp = datetime(2024, 1, 2)
    limit = 10
    offset = 5
    session.scalars.return_value.first.return_value = 2
    session.scalars.return_value.all.return_value = [global_event_1]
    ret, count = storage.read_global_attributions(
        job_ids=job_ids,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        limit=limit,
        offset=offset,
    )
    assert ret == [global_event_1]
    assert count == 2
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_id.in_(job_ids))
        .where(GlobalEvent.timestamp >= start_timestamp)
        .where(GlobalEvent.timestamp < end_timestamp)
        .limit(limit)
        .offset(offset)
        .order_by(GlobalEvent.timestamp.desc())
    )
    expected_select_count_query = (
        select(func.count(GlobalEvent.id))
        .where(GlobalEvent.job_id.in_(job_ids))
        .where(GlobalEvent.timestamp >= start_timestamp)
        .where(GlobalEvent.timestamp < end_timestamp)
    )
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Test with anomaly_uuid filter
    anomaly_uuid = uuid.uuid4()
    session.scalars.return_value.first.return_value = 1
    session.scalars.return_value.all.return_value = [global_event_0]
    ret, count = storage.read_global_attributions(anomaly_uuid=anomaly_uuid)
    assert ret == [global_event_0]
    assert count == 1
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_anomaly_id == anomaly_uuid)
        .limit(50)
        .offset(0)
        .order_by(GlobalEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(GlobalEvent.id)).where(GlobalEvent.job_anomaly_id == anomaly_uuid)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Test with start_timestamp only
    start_timestamp = datetime(2024, 1, 1)
    session.scalars.return_value.first.return_value = 2
    session.scalars.return_value.all.return_value = [global_event_1, global_event_0]
    ret, count = storage.read_global_attributions(start_timestamp=start_timestamp)
    assert ret == [global_event_1, global_event_0]
    assert count == 2
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.timestamp >= start_timestamp)
        .limit(50)
        .offset(0)
        .order_by(GlobalEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(GlobalEvent.id)).where(GlobalEvent.timestamp >= start_timestamp)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)

    # Test with end_timestamp only
    end_timestamp = datetime(2024, 1, 2)
    session.scalars.return_value.first.return_value = 1
    session.scalars.return_value.all.return_value = [global_event_0]
    ret, count = storage.read_global_attributions(end_timestamp=end_timestamp)
    assert ret == [global_event_0]
    assert count == 1
    select_query = session.scalars.call_args_list[-1].args[0]
    select_count_query = session.scalars.call_args_list[-2].args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.timestamp < end_timestamp)
        .limit(50)
        .offset(0)
        .order_by(GlobalEvent.timestamp.desc())
    )
    expected_select_count_query = select(func.count(GlobalEvent.id)).where(GlobalEvent.timestamp < end_timestamp)
    assert str(select_query) == str(expected_query)
    assert str(select_count_query) == str(expected_select_count_query)


def test_read_global_attributions_for_job(connection, session):
    """Test read_global_attributions_for_job() queries the database correctly

    This test verifies that:
    1. The correct SQL query is constructed with the job_id filter
    2. The job_attempt_index filter is added when specified and >= 0
    3. Results are ordered by job_attempt_index and timestamp descending
    4. The session is used correctly to execute the query
    """
    storage = AttributionStorage(connection)

    job_id = "123"
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    global_event_0 = GlobalEvent.create(job_id=job_id, job_attempt_index=0, timestamp=timestamp)
    global_event_1 = GlobalEvent.create(job_id=job_id, job_attempt_index=1, timestamp=timestamp)
    global_event_2 = GlobalEvent.create(job_id=job_id, job_attempt_index=2, timestamp=timestamp)

    # Test without job_attempt_index
    session.scalars.return_value.all.return_value = [
        global_event_2,
        global_event_1,
        global_event_0,
    ]
    ret = storage.read_global_attributions_for_job(job_id)
    assert ret == [global_event_2, global_event_1, global_event_0]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_id == job_id)
        .order_by(GlobalEvent.job_attempt_index.desc(), GlobalEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index >= 0
    job_attempt_index = 0
    session.scalars.return_value.all.return_value = [global_event_0]
    ret = storage.read_global_attributions_for_job(job_id, job_attempt_index)
    assert ret == [global_event_0]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_id == job_id)
        .where(GlobalEvent.job_attempt_index == job_attempt_index)
        .order_by(GlobalEvent.job_attempt_index.desc(), GlobalEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index == -1
    job_attempt_index = -1
    session.scalars.return_value.all.return_value = [
        global_event_2,
        global_event_2,
        global_event_1,
        global_event_0,
    ]
    ret = storage.read_global_attributions_for_job(job_id, job_attempt_index)
    assert ret == [global_event_2, global_event_2]
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(GlobalEvent)
        .options(joinedload(GlobalEvent.job_execution_error))
        .where(GlobalEvent.job_id == job_id)
        .order_by(GlobalEvent.job_attempt_index.desc(), GlobalEvent.timestamp.desc())
    )
    assert str(select_query) == str(expected_query)

    # Test with job_attempt_index < -1
    job_attempt_index = -2
    with pytest.raises(ValueError):
        storage.read_global_attributions_for_job(job_id, job_attempt_index)
