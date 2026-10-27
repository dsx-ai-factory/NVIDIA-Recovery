# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    AdviceState,
    JobTimeFilter,
    JobTimeFilterTarget,
)
from recovery_libs.storage.base import RecoveryStorageException, RecoveryStorageInputValidationError
from recovery_libs.storage.dataclass import (
    AdviceReconciliationStatusRecord,
    AdviceRecord,
    ApplicationIntentionRecord,
    JobEvent,
    SlurmJobExecutionRecord,
    SlurmJobFinalStates,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import (
    MAX_BATCH_READ_SIZE,
    UPSERT_CONFLICT_MAX_RETRIES,
    MySQLStorage,
)
from sqlalchemy import Integer, cast, desc, func, inspect, select, tuple_, update
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import (
    Session,
    aliased,
    contains_eager,
    joinedload,
    make_transient_to_detached,
    sessionmaker,
)
from test_utils import create_example_slurm_job_record


@pytest.fixture
def connection():
    return MagicMock(spec=MySQLConnection)


@pytest.fixture
def session():
    return MagicMock()


@pytest.fixture
def mysql_storage(connection, session):
    connection.session = MagicMock(spec=sessionmaker)
    connection.session.return_value.__enter__.return_value = session
    connection.write_session = connection.session
    connection.write_pool_enabled = True
    return MySQLStorage(connection=connection)


def _transaction_method_names(session) -> list[str]:
    """Return ordered storage calls that define each retry transaction boundary."""

    return [
        method_call[0] for method_call in session.method_calls if method_call[0] in {"scalars", "commit", "rollback"}
    ]


def test_bootstrap_with_schema_upgrade(connection, mysql_storage):
    connection.bootstrap.return_value = True
    connection.ready.return_value = True
    result = mysql_storage.bootstrap(schema_upgrade=True)
    assert result is True
    connection.bootstrap.assert_called_with(schema_upgrade=True)
    connection.ready.assert_called_once()


def test_bootstrap_without_schema_upgrade(connection, mysql_storage):
    connection.bootstrap.return_value = True
    connection.ready.return_value = True
    result = mysql_storage.bootstrap(schema_upgrade=False)
    assert result is True
    connection.bootstrap.assert_called_with(schema_upgrade=False)
    connection.ready.assert_called_once()


def test_create_application_intention_persists_first_request(mysql_storage, session):
    session.scalars.return_value.first.return_value = None
    intention = ApplicationIntentionRecord(
        job_id="job-42",
        desired_state="CANCELLED",
        reason="training converged",
        created_at=datetime.now(UTC),
    )
    intention.id = 1

    stored, created = mysql_storage.create_or_get_application_intention(intention)

    assert stored is intention
    assert created is True
    session.add.assert_called_once_with(intention)
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(intention)
    session.expunge.assert_called_once_with(intention)


def test_create_application_intention_returns_existing_absorbing_request(mysql_storage, session):
    existing = ApplicationIntentionRecord(
        job_id="job-42",
        desired_state="CANCELLED",
        reason="first reason",
        created_at=datetime.now(UTC),
    )
    existing.id = 1
    session.scalars.return_value.first.return_value = existing
    duplicate = ApplicationIntentionRecord(
        job_id="job-42",
        desired_state="CANCELLED",
        reason="later reason",
        created_at=datetime.now(UTC),
    )

    stored, created = mysql_storage.create_or_get_application_intention(duplicate)

    assert stored is existing
    assert created is False
    assert stored.reason == "first reason"
    session.add.assert_not_called()
    session.commit.assert_not_called()


def test_bootstrap_returns_false_when_schema_is_not_at_head(connection, mysql_storage):
    connection.bootstrap.return_value = False

    assert mysql_storage.bootstrap(schema_upgrade=False) is False

    connection.ready.assert_not_called()


def test_storage_ready(connection, mysql_storage):
    connection.ready = MagicMock(return_value=True)
    assert mysql_storage.ready()
    connection.ready.assert_called_once()


def test_storage_not_ready(connection, mysql_storage):
    connection.ready = MagicMock(return_value=False)
    assert not mysql_storage.ready()
    connection.ready.assert_called_once()


def test_get_job_latest_attempt_index(session, mysql_storage):
    # Case where actual value is returned.
    session.scalars.return_value.first.return_value = 3
    assert mysql_storage.get_job_latest_attempt_index("1000") == 3
    session.scalars.assert_called_once()

    # Case where no value matching record is found.
    session.scalars.return_value.first.return_value = None
    assert mysql_storage.get_job_latest_attempt_index("1000") == -1
    assert session.scalars.call_count == 2


def test_read_job_record_with_job_id(session, mysql_storage):
    job_id = "10000"
    mysql_storage.read_job_record(job_id=job_id)
    select_query = session.scalars.call_args.args[0]
    expected_query = (
        select(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .order_by(desc(SlurmJobExecutionRecord.job_attempt_index))
        .limit(1)
        .options(joinedload(SlurmJobExecutionRecord.events))
    )
    assert str(select_query) == str(expected_query)


def test_read_job_record_with_job_id_and_attempt_index(session, mysql_storage):
    job_id = "10000"
    job_attempt_index = 0
    mysql_storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)
    select_query = session.scalars.call_args.args[0]
    expected_query = select_query = (
        select(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
        .options(joinedload(SlurmJobExecutionRecord.events))
    )
    assert str(select_query) == str(expected_query)


def test_get_allocated_nodes_batch_success(session, mysql_storage):
    """Test successful batch read with all jobs found."""
    job_runs = [("12345", 0), ("12346", 0), ("12347", 1)]

    # Mock database results
    mock_results = [
        ("12345", 0, ["node1", "node2"]),
        ("12346", 0, ["node3", "node4"]),
        ("12347", 1, ["node5"]),
    ]
    session.execute.return_value.all.return_value = mock_results

    result = mysql_storage.get_allocated_nodes_batch(job_runs)

    # Verify query structure
    select_query = session.execute.call_args.args[0]
    expected_query = select(
        SlurmJobExecutionRecord.job_id,
        SlurmJobExecutionRecord.job_attempt_index,
        SlurmJobExecutionRecord.allocated_node_list,
    ).where(
        tuple_(
            SlurmJobExecutionRecord.job_id,
            SlurmJobExecutionRecord.job_attempt_index,
        ).in_(job_runs)
    )
    assert str(select_query) == str(expected_query)

    # Verify results
    assert result == {
        "12345/0": {"node1", "node2"},
        "12346/0": {"node3", "node4"},
        "12347/1": {"node5"},
    }


def test_get_allocated_nodes_batch_with_missing_jobs(session, mysql_storage):
    """Test batch read with some jobs not found."""
    job_runs = [("12345", 0), ("12346", 0), ("99999", 0)]

    # Only 2 out of 3 jobs found
    mock_results = [
        ("12345", 0, ["node1", "node2"]),
        ("12346", 0, ["node3"]),
    ]
    session.execute.return_value.all.return_value = mock_results

    result = mysql_storage.get_allocated_nodes_batch(job_runs)

    # Verify all jobs present in result (missing one has empty set)
    assert result == {
        "12345/0": {"node1", "node2"},
        "12346/0": {"node3"},
        "99999/0": set(),  # Missing job should have empty set
    }


def test_get_allocated_nodes_batch_with_null_node_list(session, mysql_storage):
    """Test batch read with jobs that have null allocated_node_list."""
    job_runs = [("12345", 0), ("12346", 0)]

    # Job 12346 has null node list
    mock_results = [
        ("12345", 0, ["node1", "node2"]),
        ("12346", 0, None),
    ]
    session.execute.return_value.all.return_value = mock_results

    result = mysql_storage.get_allocated_nodes_batch(job_runs)

    # Verify null node list becomes empty set
    assert result == {
        "12345/0": {"node1", "node2"},
        "12346/0": set(),
    }


def test_get_allocated_nodes_batch_empty_input(session, mysql_storage):
    """Test batch read with empty job list."""
    result = mysql_storage.get_allocated_nodes_batch([])

    # Should return empty dict without querying database
    assert result == {}
    session.execute.assert_not_called()


def test_get_allocated_nodes_batch_exceeds_max_size(mysql_storage):
    """Test batch read with size exceeding maximum."""
    # Create batch exceeding limit
    job_runs = [(str(i), 0) for i in range(MAX_BATCH_READ_SIZE + 1)]

    with pytest.raises(RecoveryStorageInputValidationError) as exc_info:
        mysql_storage.get_allocated_nodes_batch(job_runs)

    assert "exceeds maximum" in str(exc_info.value)
    assert str(MAX_BATCH_READ_SIZE) in str(exc_info.value)


def test_get_allocated_nodes_batch_invalid_input_types(mysql_storage):
    """Test batch read with invalid input types."""
    invalid_inputs = [
        [("12345", "0")],  # str instead of int
        [(123, 0)],  # int instead of str for job_id
        [("12345", -1)],  # negative attempt_index
        [("", 0)],  # empty job_id
        [(None, 0)],  # None job_id
    ]

    for invalid_input in invalid_inputs:
        with pytest.raises(RecoveryStorageInputValidationError) as exc_info:
            mysql_storage.get_allocated_nodes_batch(invalid_input)
        assert "Invalid job_runs format" in str(exc_info.value)


def test_get_allocated_nodes_batch_with_duplicates(session, mysql_storage):
    """Test batch read deduplicates input."""
    job_runs = [("12345", 0), ("12346", 0), ("12345", 0), ("12346", 0)]  # duplicates

    mock_results = [
        ("12345", 0, ["node1"]),
        ("12346", 0, ["node2"]),
    ]
    session.execute.return_value.all.return_value = mock_results

    result = mysql_storage.get_allocated_nodes_batch(job_runs)

    select_query = session.execute.call_args.args[0]
    assert list(select_query.compile().params.values()) == [[("12345", 0), ("12346", 0)]]
    assert result == {
        "12345/0": {"node1"},
        "12346/0": {"node2"},
    }


def test_get_allocated_nodes_batch_filters_none_nodes(session, mysql_storage):
    """Test batch read filters out None values from node list."""
    job_runs = [("12345", 0)]

    # Node list contains None and empty string
    mock_results = [
        ("12345", 0, ["node1", None, "node2", "", "node3"]),
    ]
    session.execute.return_value.all.return_value = mock_results

    result = mysql_storage.get_allocated_nodes_batch(job_runs)

    # Should filter out None and empty strings
    assert result == {
        "12345/0": {"node1", "node2", "node3"},
    }


@pytest.mark.parametrize("workflow_id_match", ["prefix", "fuzzy", "exact"])
def test_read_workflows_groups_and_counts_by_canonical_workflow_id(session, mysql_storage, workflow_id_match):
    """Workflow membership is independent of job_name metadata.

    Grouping by workflow_id combines different job names in one workflow and
    keeps equal job names in different workflows separate.
    """
    workflow_id = "test_workflow"
    session.execute.return_value.all.return_value = []

    mysql_storage.read_workflows(
        limit=7,
        offset=3,
        workflow_id=workflow_id,
        workflow_id_match=workflow_id_match,
    )

    session.execute.assert_called_once()
    session.scalars.assert_called_once()
    select_query = session.execute.call_args.args[0]
    select_count_query = session.scalars.call_args.args[0]

    workflow_start_time = func.min(SlurmJobExecutionRecord.start_time).label("workflow_start_time")
    workflow_end_time = func.max(SlurmJobExecutionRecord.end_time).label("workflow_end_time")
    expected_select_query_base = select(
        SlurmJobExecutionRecord.workflow_id,
        workflow_start_time,
        workflow_end_time,
    ).group_by(SlurmJobExecutionRecord.workflow_id)
    if workflow_id_match == "prefix":
        expected_select_query_base = expected_select_query_base.where(
            SlurmJobExecutionRecord.workflow_id.istartswith(workflow_id, autoescape=True)
        )
    elif workflow_id_match == "fuzzy":
        expected_select_query_base = expected_select_query_base.where(
            SlurmJobExecutionRecord.workflow_id.icontains(workflow_id, autoescape=True)
        )
    else:
        expected_select_query_base = expected_select_query_base.where(
            SlurmJobExecutionRecord.workflow_id == workflow_id
        )

    expected_select_query = (
        expected_select_query_base.order_by(workflow_start_time, SlurmJobExecutionRecord.workflow_id).limit(7).offset(3)
    )
    expected_base_subquery = expected_select_query_base.subquery()
    expected_select_count_query = select(func.count(expected_base_subquery.c.workflow_id)).select_from(
        expected_base_subquery
    )

    assert str(select_query) == str(expected_select_query)
    assert str(select_count_query) == str(expected_select_count_query)
    assert "job_execution_records.job_name" not in str(select_query)
    assert "job_execution_records.job_name" not in str(select_count_query)


@pytest.mark.parametrize("workflow_id_match", ["prefix", "fuzzy"])
@pytest.mark.parametrize(
    ("workflow_id", "escaped_workflow_id"),
    [
        ("%", "/%"),
        ("_", "/_"),
        ("/", "//"),
        ("\\", "\\"),
    ],
)
def test_read_workflows_autoescapes_literal_search_characters(
    session,
    mysql_storage,
    workflow_id_match,
    workflow_id,
    escaped_workflow_id,
):
    session.execute.return_value.all.return_value = []

    mysql_storage.read_workflows(
        workflow_id=workflow_id,
        workflow_id_match=workflow_id_match,
    )

    select_query = session.execute.call_args.args[0]
    select_count_query = session.scalars.call_args.args[0]

    for query in (select_query, select_count_query):
        compiled_query = query.compile(dialect=mysql.dialect())
        assert "ESCAPE '/'" in str(compiled_query)
        assert escaped_workflow_id in compiled_query.params.values()


def test_read_workflows_maps_distinct_canonical_workflow_ids_verbatim(session, mysql_storage):
    workflow_id_column = SlurmJobExecutionRecord.__table__.c.workflow_id
    assert workflow_id_column.type.collation == "utf8mb4_0900_bin"

    workflow_ids = [
        "Case",
        "case",
        "resume",
        "résume",
        "trailing",
        "trailing ",
        "é",
        "e\u0301",
    ]
    start_time = datetime(2026, 7, 6, 10, tzinfo=UTC)
    end_time = start_time + timedelta(minutes=30)
    session.execute.return_value.all.return_value = [
        SimpleNamespace(
            workflow_id=workflow_id,
            workflow_start_time=start_time,
            workflow_end_time=end_time,
        )
        for workflow_id in workflow_ids
    ]
    session.scalars.return_value.first.return_value = len(workflow_ids)

    records, count = mysql_storage.read_workflows()

    assert [record.id for record in records] == workflow_ids
    assert all(record.start_time == start_time for record in records)
    assert all(record.end_time == end_time for record in records)
    assert count == len(workflow_ids)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_with_filters(session, mysql_storage, with_table_join):
    job_id = "10000"
    job_name = "test_job"
    state = "PENDING"
    user = "test_user"

    mysql_storage.read_job_records(
        complete_data=True,
        job_id=job_id,
        job_name=job_name,
        user=user,
        states=[state],
        join_job_events_table=with_table_join,
    )

    # scalars() is called for the select_count query, and execute() is called in the actual select query.
    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    if with_table_join:
        select_query_subquery = (
            select(SlurmJobExecutionRecord)
            .where(SlurmJobExecutionRecord.job_id == job_id)
            .where(SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%"))
            .where(SlurmJobExecutionRecord.user == user)
            .where(SlurmJobExecutionRecord.state == state)
            .limit(50)
            .offset(0)
            .subquery()
        )

        table_alias = aliased(SlurmJobExecutionRecord, select_query_subquery)
        expected_select_query = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )
    else:
        expected_select_query = (
            select(SlurmJobExecutionRecord)
            .where(SlurmJobExecutionRecord.job_id == job_id)
            .where(SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%"))
            .where(SlurmJobExecutionRecord.user == user)
            .where(SlurmJobExecutionRecord.state == state)
            .limit(50)
            .offset(0)
        )

    expected_select_count_query = (
        select(func.count(SlurmJobExecutionRecord.id))
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .where(SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%"))
        .where(SlurmJobExecutionRecord.user == user)
        .where(SlurmJobExecutionRecord.state == state)
    )

    assert str(select_query) == str(expected_select_query)
    assert str(select_count_query) == str(expected_select_count_query)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_with_time_filters(session, mysql_storage, with_table_join):
    states = [s.value for s in SlurmJobFinalStates]
    end_time = datetime(year=2024, month=11, day=21, hour=9, tzinfo=UTC)
    start_time = end_time - timedelta(hours=1)
    time_filters = [
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.END_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.LAST_UPATED_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, from_time=start_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.END_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.LAST_UPATED_TIME, from_time=start_time, to_time=end_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, from_time=start_time),
        JobTimeFilter(job_time_target=JobTimeFilterTarget.SUBMIT_TIME, to_time=end_time),
    ]

    expected_base_subquery = (
        select(SlurmJobExecutionRecord).where(SlurmJobExecutionRecord.state.in_(states)).limit(50).offset(0)
    )

    expected_subqueries = [
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time).where(
            SlurmJobExecutionRecord.submit_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.start_time >= start_time).where(
            SlurmJobExecutionRecord.start_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.end_time >= start_time).where(
            SlurmJobExecutionRecord.end_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.last_updated_at >= start_time).where(
            SlurmJobExecutionRecord.last_updated_at < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time < end_time),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time).where(
            SlurmJobExecutionRecord.submit_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.start_time >= start_time).where(
            SlurmJobExecutionRecord.start_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.end_time >= start_time).where(
            SlurmJobExecutionRecord.end_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.last_updated_at >= start_time).where(
            SlurmJobExecutionRecord.last_updated_at < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time < end_time),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time).where(
            SlurmJobExecutionRecord.submit_time < end_time
        ),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time >= start_time),
        expected_base_subquery.where(SlurmJobExecutionRecord.submit_time < end_time),
    ]

    if with_table_join:
        expected_subqueries = [subquery.subquery() for subquery in expected_subqueries]

    for time_filter, subquery in zip(time_filters, expected_subqueries):
        table_alias = aliased(SlurmJobExecutionRecord, subquery)

        if with_table_join:
            expected_query = (
                select(table_alias)
                .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
                .options(contains_eager(table_alias.events))
            )
        else:
            expected_query = subquery

        mysql_storage.read_job_records(
            complete_data=True, states=states, time_filter=time_filter, join_job_events_table=with_table_join
        )
        select_query = session.execute.call_args.args[0]
        assert str(select_query) == str(expected_query)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_with_sort_by(session, mysql_storage, with_table_join):
    job_id = "12345"
    mysql_storage.read_job_records(
        job_id=job_id,
        sort_by=[("job_attempt_index", "desc"), ("start_time", "desc")],
        join_job_events_table=with_table_join,
    )

    session.execute.assert_called_once()
    select_query = session.execute.call_args.args[0]

    expected_base_query = (
        select(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .order_by(SlurmJobExecutionRecord.job_attempt_index.desc())
        .order_by(SlurmJobExecutionRecord.start_time.desc())
        .limit(50)
        .offset(0)
    )

    if with_table_join:
        select_query_subquery = expected_base_query.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, select_query_subquery)
        expected_query = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
            .order_by(
                table_alias.job_attempt_index.desc(),
            )
            .order_by(table_alias.start_time.desc())
        )
    else:
        expected_query = expected_base_query

    assert str(select_query) == str(expected_query)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_with_sort_by_job_id_and_table_join(session, mysql_storage, with_table_join):
    job_name = "test_job"
    mysql_storage.read_job_records(
        job_name=job_name,
        sort_by=[("job_id", "asc"), ("job_attempt_index", "asc")],
        join_job_events_table=with_table_join,
    )

    session.execute.assert_called_once()
    select_query = session.execute.call_args.args[0]

    expected_base_query = (
        select(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%"))
        .order_by(cast(SlurmJobExecutionRecord.job_id, Integer).asc())
        .order_by(SlurmJobExecutionRecord.job_attempt_index.asc())
        .limit(50)
        .offset(0)
    )

    if with_table_join:
        expected_subquery = expected_base_query.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_subquery)

        expected_query = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
            .order_by(cast(table_alias.job_id, Integer).asc())
            .order_by(table_alias.job_attempt_index.asc())
        )
    else:
        expected_query = expected_base_query

    assert str(select_query) == str(expected_query)


@pytest.mark.parametrize("job_name_match", ["prefix", "exact", "fuzzy"])
@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_filter_by_job_name_match(session, mysql_storage, job_name_match, with_table_join):
    """Exact match uses job_name == value; prefix match uses job_name.like(value%); fuzzy uses job_name.ilike(%value%)."""
    job_name = "workflow"
    mysql_storage.read_job_records(
        job_name=job_name,
        job_name_match=job_name_match,
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    if job_name_match == "prefix":
        expected_job_name_condition = SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%")
    elif job_name_match == "fuzzy":
        expected_job_name_condition = SlurmJobExecutionRecord.job_name.ilike(f"%{job_name}%")
    else:
        expected_job_name_condition = SlurmJobExecutionRecord.job_name == job_name

    expected_select = select(SlurmJobExecutionRecord).where(expected_job_name_condition).limit(50).offset(0)
    expected_count = select(func.count(SlurmJobExecutionRecord.id)).where(expected_job_name_condition)

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_filters_exact_workflow_id_independently_of_job_name(session, mysql_storage, with_table_join):
    workflow_id = " Case/%_\\路 "
    job_name = "shared-metadata"

    mysql_storage.read_job_records(
        workflow_id=workflow_id,
        job_name=job_name,
        job_name_match="fuzzy",
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_job_name_condition = SlurmJobExecutionRecord.job_name.ilike(f"%{job_name}%")
    expected_select = (
        select(SlurmJobExecutionRecord)
        .where(expected_job_name_condition)
        .where(SlurmJobExecutionRecord.workflow_id == workflow_id)
        .limit(50)
        .offset(0)
    )
    expected_count = (
        select(func.count(SlurmJobExecutionRecord.id))
        .where(expected_job_name_condition)
        .where(SlurmJobExecutionRecord.workflow_id == workflow_id)
    )

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_filter_by_job_group_id(session, mysql_storage, with_table_join):
    """When job_group_id is provided, filter records by job_group_id == value."""
    job_group_id = "99999"
    mysql_storage.read_job_records(
        job_group_id=job_group_id,
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_select = (
        select(SlurmJobExecutionRecord).where(SlurmJobExecutionRecord.job_group_id == job_group_id).limit(50).offset(0)
    )
    expected_count = select(func.count(SlurmJobExecutionRecord.id)).where(
        SlurmJobExecutionRecord.job_group_id == job_group_id
    )

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_exclude_job_group_attempts(session, mysql_storage, with_table_join):
    """When include_job_group_attempts=False and no job_group_id, filter for job_group_id IS NULL."""
    mysql_storage.read_job_records(
        include_job_group_attempts=False,
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_select = (
        select(SlurmJobExecutionRecord).where(SlurmJobExecutionRecord.job_group_id.is_(None)).limit(50).offset(0)
    )
    expected_count = select(func.count(SlurmJobExecutionRecord.id)).where(
        SlurmJobExecutionRecord.job_group_id.is_(None)
    )

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_job_group_id_ignores_include_flag(session, mysql_storage, with_table_join):
    """When job_group_id is provided, include_job_group_attempts=False is ignored."""
    job_group_id = "99999"
    mysql_storage.read_job_records(
        job_group_id=job_group_id,
        include_job_group_attempts=False,
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    # Should filter by job_group_id == value, NOT job_group_id IS NULL
    expected_select = (
        select(SlurmJobExecutionRecord).where(SlurmJobExecutionRecord.job_group_id == job_group_id).limit(50).offset(0)
    )
    expected_count = select(func.count(SlurmJobExecutionRecord.id)).where(
        SlurmJobExecutionRecord.job_group_id == job_group_id
    )

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_include_job_group_attempts_default_true(session, mysql_storage, with_table_join):
    """When include_job_group_attempts=True (default) and no job_group_id, no job_group_id filter is applied."""
    mysql_storage.read_job_records(
        join_job_events_table=with_table_join,
    )

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_select = select(SlurmJobExecutionRecord).limit(50).offset(0)
    expected_count = select(func.count(SlurmJobExecutionRecord.id))

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


@pytest.mark.parametrize("is_part_of_group", [True, False])
@pytest.mark.parametrize("with_table_join", [True, False])
def test_read_job_records_heimdall_job_id_with_exclude_job_group_attempts(
    session, mysql_storage, is_part_of_group, with_table_join
):
    """When heimdall_job_id is provided with include_job_group_attempts=False, the query filters by both
    heimdall_job_id and job_group_id IS NULL. If the job is part of a group, no results are returned;
    otherwise the target job attempt is returned."""
    heimdall_job_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    now = datetime.now(UTC)

    if is_part_of_group:
        # Job is part of a group — DB returns nothing because job_group_id IS NOT NULL fails the IS NULL filter
        session.scalars.return_value.first.return_value = 0
        session.execute.return_value.scalars.return_value.unique.return_value.all.return_value = []
    else:
        # Job is standalone — DB returns the matching record
        job_record = create_example_slurm_job_record(
            job_id="10000",
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=0,
        )
        session.scalars.return_value.first.return_value = 1
        session.execute.return_value.scalars.return_value.unique.return_value.all.return_value = [job_record]

    records, count = mysql_storage.read_job_records(
        heimdall_job_id=heimdall_job_id,
        include_job_group_attempts=False,
        join_job_events_table=with_table_join,
    )

    # Verify query structure — both heimdall_job_id and job_group_id IS NULL conditions must be present
    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_select = (
        select(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.heimdall_job_id == heimdall_job_id)
        .where(SlurmJobExecutionRecord.job_group_id.is_(None))
        .limit(50)
        .offset(0)
    )
    expected_count = (
        select(func.count(SlurmJobExecutionRecord.id))
        .where(SlurmJobExecutionRecord.heimdall_job_id == heimdall_job_id)
        .where(SlurmJobExecutionRecord.job_group_id.is_(None))
    )

    if with_table_join:
        expected_select = expected_select.subquery()
        table_alias = aliased(SlurmJobExecutionRecord, expected_select)
        expected_select = (
            select(table_alias)
            .outerjoin(JobEvent, JobEvent.job_record_id == table_alias.id)
            .options(contains_eager(table_alias.events))
        )

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)

    # Verify return values match expected behavior
    if is_part_of_group:
        assert records == []
        assert count == 0
    else:
        assert len(records) == 1
        assert count == 1


def _build_expected_job_group_query(filters=None):
    """Helper to build expected job group base query with optional filters applied."""
    unique_job_count = func.count(func.distinct(SlurmJobExecutionRecord.job_id)).label("unique_job_count")
    total_job_attempt_count = func.count(SlurmJobExecutionRecord.id).label("total_job_attempt_count")

    base = select(SlurmJobExecutionRecord.job_group_id, unique_job_count, total_job_attempt_count).where(
        SlurmJobExecutionRecord.job_group_id.is_not(None)
    )
    for condition in filters or []:
        base = base.where(condition)
    base = base.group_by(SlurmJobExecutionRecord.job_group_id)

    # Build count query from the same base subquery (avoids alias mismatch)
    base_subquery = base.subquery()
    count_query = select(func.count(base_subquery.c.job_group_id)).select_from(base_subquery)

    return base, count_query, unique_job_count, total_job_attempt_count


def test_read_job_groups_no_filters(session, mysql_storage):
    """When no filters are provided, query groups all records with non-null job_group_id."""
    session.scalars.return_value.first.return_value = 0
    session.execute.return_value.all.return_value = []

    mysql_storage.read_job_groups()

    session.scalars.assert_called_once()
    session.execute.assert_called_once()
    select_count_query = session.scalars.call_args.args[0]
    select_query = session.execute.call_args.args[0]

    expected_base, expected_count, _, _ = _build_expected_job_group_query()
    expected_select = expected_base.limit(50).offset(0)

    assert str(select_query) == str(expected_select)
    assert str(select_count_query) == str(expected_count)


def test_read_job_groups_with_job_group_id_filter(session, mysql_storage):
    """When job_group_id is provided, filter by that specific group."""
    session.scalars.return_value.first.return_value = 0
    session.execute.return_value.all.return_value = []

    mysql_storage.read_job_groups(job_group_id="99999")

    select_query = session.execute.call_args.args[0]

    expected_base, _, _, _ = _build_expected_job_group_query(filters=[SlurmJobExecutionRecord.job_group_id == "99999"])
    expected_select = expected_base.limit(50).offset(0)

    assert str(select_query) == str(expected_select)


def test_read_job_groups_with_user_filter(session, mysql_storage):
    """User filter is applied to the base query before grouping."""
    session.scalars.return_value.first.return_value = 0
    session.execute.return_value.all.return_value = []

    mysql_storage.read_job_groups(user="test_user")

    select_query = session.execute.call_args.args[0]

    expected_base, _, _, _ = _build_expected_job_group_query(filters=[SlurmJobExecutionRecord.user == "test_user"])
    expected = expected_base.limit(50).offset(0)

    assert str(select_query) == str(expected)


def test_read_job_groups_with_sorting(session, mysql_storage):
    """Sorting by total_job_attempt_count desc is applied to the query."""
    session.scalars.return_value.first.return_value = 0
    session.execute.return_value.all.return_value = []

    mysql_storage.read_job_groups(sort_by=[("total_job_attempt_count", "desc")])

    select_query = session.execute.call_args.args[0]

    expected_base, _, _, total_job_attempt_count = _build_expected_job_group_query()
    expected = expected_base.order_by(total_job_attempt_count.desc()).limit(50).offset(0)

    assert str(select_query) == str(expected)


def test_read_job_groups_with_sorting_by_job_group_id(session, mysql_storage):
    """Sorting by job_group_id casts to integer, matching the job_id sorting pattern."""
    session.scalars.return_value.first.return_value = 0
    session.execute.return_value.all.return_value = []

    mysql_storage.read_job_groups(sort_by=[("job_group_id", "asc")])

    select_query = session.execute.call_args.args[0]

    expected_base, _, _, _ = _build_expected_job_group_query()
    expected = expected_base.order_by(cast(SlurmJobExecutionRecord.job_group_id, Integer).asc()).limit(50).offset(0)

    assert str(select_query) == str(expected)


def test_read_job_groups_invalid_sort_column(session, mysql_storage):
    """Sorting by an invalid column raises RecoveryStorageInputValidationError."""
    with pytest.raises(RecoveryStorageInputValidationError, match="Invalid sort_by expression"):
        mysql_storage.read_job_groups(sort_by=[("invalid_column", "asc")])


def test_read_job_groups_returns_results(session, mysql_storage):
    """Verify read_job_groups returns correctly structured dicts from DB rows."""
    from unittest.mock import MagicMock

    mock_row = MagicMock()
    mock_row.job_group_id = "99999"
    mock_row.unique_job_count = 3
    mock_row.total_job_attempt_count = 7

    session.scalars.return_value.first.return_value = 1
    session.execute.return_value.all.return_value = [mock_row]

    records, count = mysql_storage.read_job_groups()

    assert count == 1
    assert len(records) == 1
    assert records[0] == {
        "job_group_id": "99999",
        "unique_job_count": 3,
        "total_job_attempt_count": 7,
    }


def test_create_job_record(mysql_storage, session):
    job_record = create_example_slurm_job_record(
        job_id="10000",
        submit_time=datetime.now(UTC) - timedelta(minutes=10),
        last_updated_at=datetime.now(UTC),
        attempt_index=0,
    )

    mysql_storage.create_job_record(job_record)

    session.add.assert_called_once_with(job_record)
    session.commit.assert_called_once()
    assert job_record.heimdall_job_id == generate_heimdall_job_id("10000", 0)


@pytest.mark.parametrize("job_id, job_group_id", [("10000", None), ("10000_0", "10000")])
def test_update_job_record(mysql_storage, session, job_id, job_group_id):
    job_attempt_index = 0
    job_record = create_example_slurm_job_record(
        job_id=job_id,
        attempt_index=job_attempt_index,
        job_group_id=job_group_id,
        submit_time=datetime.now(UTC) - timedelta(minutes=10),
        last_updated_at=datetime.now(UTC),
        allocated_node_list_length=2,
        log_file_paths=[],
    )
    job_record.failed_node_list = ["node_3"]
    session.scalars.return_value.first.return_value = job_record

    mysql_storage.update_job_record(**asdict(job_record))

    session.execute.assert_called_once()
    update_query = session.execute.call_args.args[0]
    update_values = asdict(job_record)

    # Remove all fields that's either None or an empty collection.
    emptry_fields = [
        "start_time",
        "end_time",
        "anomalies",
        "checkpoints",
        "exclusion_node_list",
        "sticky_exclusion_node_list",
        "events",
        "id",
        "log_file_paths",
        "scheduler_metadata",
        "config_validation_failure_types",
        "worker_cluster_name",
        "heimdall_job_id",  # Heimdall job ID (None by default)
    ]

    if not job_group_id:
        emptry_fields.append("job_group_id")

    for field_name in emptry_fields:
        update_values.pop(field_name)

    # job_id and job_attempt_index are recognized as positional arguments.
    update_values.pop("job_id")
    update_values.pop("job_attempt_index")
    update_values.pop("attribution_request_emitted")  # claim-only, never written via update_job_record

    expected_query = (
        update(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
        .values(update_values)
    )

    assert str(update_query) == str(expected_query)


def test_update_job_record_with_shorter_node_list(mysql_storage, session):
    job_id = "10000"
    job_attempt_index = 0
    submit_time = datetime.now(UTC) - timedelta(minutes=10)
    known_job_record = create_example_slurm_job_record(
        job_id=job_id,
        attempt_index=job_attempt_index,
        submit_time=submit_time,
        last_updated_at=datetime.now(UTC),
        allocated_node_list_length=5,
    )
    session.scalars.return_value.first.return_value = known_job_record

    updated_job_record = create_example_slurm_job_record(
        job_id=job_id,
        attempt_index=job_attempt_index,
        submit_time=submit_time,
        last_updated_at=datetime.now(UTC),
        # allocated_node_list in the update is shorter.
        allocated_node_list_length=2,
    )

    mysql_storage.update_job_record(**asdict(updated_job_record))

    # scalars() reads the current record before applying the update.
    session.scalars.assert_called_once()

    session.execute.assert_called_once()
    update_query = session.execute.call_args.args[0]

    update_values = asdict(updated_job_record)
    # job_id and job_attempt_index are recognized as positional arguments.
    update_values.pop("job_id")
    update_values.pop("job_attempt_index")

    # Remove all fields that's either None or an empty collection.
    for field_name in [
        "job_group_id",
        "attribution_request_emitted",  # claim-only, never written via update_job_record
        "start_time",
        "end_time",
        "anomalies",
        "checkpoints",
        "failed_node_list",
        "exclusion_node_list",
        "sticky_exclusion_node_list",
        "config_validation_failure_types",
        "events",
        "id",
        "log_file_paths",
        "scheduler_metadata",
        "worker_cluster_name",
        "heimdall_job_id",  # Heimdall job ID (None by default)
    ]:
        update_values.pop(field_name)

    # Both allocated_node_list and node_count should be removed from the final update values that database receives.
    update_values.pop("allocated_node_list")
    update_values.pop("node_count")

    expected_query = (
        update(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == job_id)
        .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
        .values(update_values)
    )

    assert str(update_query) == str(expected_query)


def test_update_job_record_returns_transition_snapshots_captured_under_lock(connection):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="locked-update",
        attempt_index=2,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        state="PENDING",
    )
    existing_job.id = 1
    make_transient_to_detached(existing_job)

    with Session() as tracked_session:
        tracked_session.add(existing_job)
        connection.session = MagicMock(spec=sessionmaker)
        connection.session.return_value.__enter__.return_value = tracked_session
        mysql_storage = MySQLStorage(connection=connection)

        def apply_update(_statement):
            existing_job.state = "RUNNING"
            return MagicMock()

        with (
            patch.object(tracked_session, "scalars") as scalars,
            patch.object(tracked_session, "execute", side_effect=apply_update) as execute,
            patch.object(tracked_session, "flush"),
            patch.object(tracked_session, "commit"),
            patch.object(tracked_session, "refresh"),
            patch.object(mysql_storage, "get_job_latest_attempt_index") as get_latest_attempt,
        ):
            scalars.return_value.first.return_value = existing_job
            result = mysql_storage.update_job_record(
                job_id=existing_job.job_id,
                job_attempt_index=None,
                state="RUNNING",
            )
            locked_query = scalars.call_args.args[0]
            update_query = execute.call_args.args[0]

    assert locked_query._for_update_arg is not None
    get_latest_attempt.assert_not_called()
    assert str(update_query) == str(
        update(SlurmJobExecutionRecord)
        .where(SlurmJobExecutionRecord.job_id == existing_job.job_id)
        .where(SlurmJobExecutionRecord.job_attempt_index == existing_job.job_attempt_index)
        .values(state="RUNNING")
    )
    assert result.old_record is not None
    assert result.updated_record is not None
    assert result.old_record is not result.updated_record
    assert result.old_record.state == "PENDING"
    assert result.updated_record.state == "RUNNING"
    assert inspect(result.old_record).transient
    assert inspect(result.updated_record).session is None


def test_update_job_record_returns_empty_snapshots_when_no_record_matches(mysql_storage, session):
    session.scalars.return_value.first.return_value = None

    result = mysql_storage.update_job_record(
        job_id="missing",
        job_attempt_index=0,
        state="RUNNING",
    )

    assert result.updated_record is None
    assert result.old_record is None
    session.execute.assert_not_called()
    session.commit.assert_called_once()


def test_upsert_job_record_snapshot_does_not_detach_tracked_record(connection, mysql_storage):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="tracked-update",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
    )
    incoming_update = create_example_slurm_job_record(
        job_id="tracked-update",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        state="FAILED",
    )

    existing_job.id = 1
    make_transient_to_detached(existing_job)
    with Session() as tracked_session:
        tracked_session.add(existing_job)
        assert inspect(existing_job).persistent
        connection.write_session.return_value.__enter__.return_value = tracked_session

        with (
            patch.object(tracked_session, "connection"),
            patch.object(tracked_session, "scalars") as scalars,
            patch.object(tracked_session, "flush"),
            patch.object(tracked_session, "commit"),
            patch.object(tracked_session, "refresh"),
        ):
            scalars.return_value.first.return_value = existing_job
            result = mysql_storage.upsert_job_record(incoming_update)

    assert result.old_record is not None
    assert result.old_record is not result.inserted_record
    assert result.old_record.state == "Pending"
    assert result.inserted_record.state == "FAILED"
    assert inspect(result.old_record).transient
    assert inspect(result.old_record).session is None
    assert inspect(result.inserted_record).session is None


def test_upsert_job_record_preserves_existing_anomalies_when_update_omits_them(mysql_storage, session):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="10000",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        anomaly_time=now - timedelta(minutes=1),
    )
    assert existing_job.anomalies is not None

    # Simulate an API/operator status update that doesn't carry anomaly fields.
    incoming_update = create_example_slurm_job_record(
        job_id="10000",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        state="FAILED",
        anomaly_time=None,
    )
    assert incoming_update.anomalies is None

    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, old_record = mysql_storage.upsert_job_record(incoming_update)

    assert created is False
    assert old_record is not None
    assert old_record is not updated_record
    assert old_record.state == "Pending"
    assert updated_record.state == "FAILED"
    assert updated_record.anomalies is not None
    assert len(updated_record.anomalies) == 1
    assert updated_record.anomalies[0].anomaly_type == "Hang"
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(existing_job)


def test_upsert_job_record_rejects_out_of_range_timestamp_before_database(mysql_storage, connection):
    incoming = SlurmJobExecutionRecord(
        job_id="invalid-timestamp",
        job_attempt_index=0,
        workflow_id="invalid-timestamp",
        start_time=datetime(2106, 2, 7, 6, 28, 14, tzinfo=UTC),
        last_updated_at=datetime.now(UTC),
    )

    with pytest.raises(RecoveryStorageInputValidationError, match="start_time.*MySQL TIMESTAMP range"):
        mysql_storage.upsert_job_record(incoming)

    connection.session.assert_not_called()


def test_upsert_job_record_does_not_commit_when_database_round_trip_fails(mysql_storage, session):
    incoming = SlurmJobExecutionRecord(
        job_id="failed-round-trip",
        job_attempt_index=0,
        workflow_id="failed-round-trip",
        last_updated_at=datetime.now(UTC),
    )
    session.scalars.return_value.first.return_value = None
    session.refresh.side_effect = TypeError("database returned an unreadable value")

    with pytest.raises(RecoveryStorageException, match="database returned an unreadable value"):
        mysql_storage.upsert_job_record(incoming)

    session.flush.assert_called_once_with()
    session.refresh.assert_called_once_with(incoming)
    session.expunge.assert_not_called()
    session.commit.assert_not_called()
    session.rollback.assert_called_once_with()


@pytest.mark.parametrize(
    "metadata,has_start,corrected",
    [
        ({"scheduler": "slurm"}, True, True),
        ({"scheduler": "slurm"}, False, False),
        ({"scheduler": "kubernetes"}, True, False),
        (None, True, False),
    ],
)
def test_upsert_job_record_corrects_only_present_slurm_start(mysql_storage, session, metadata, has_start, corrected):
    """Slurm corrections preserve missing starts, other producers, and snapshots."""
    estimate = datetime(2026, 10, 2, 19, 25, 8, tzinfo=UTC)
    actual = datetime(2026, 10, 2, 19, 33, 20, tzinfo=UTC)
    existing = SlurmJobExecutionRecord(
        job_id="16",
        job_attempt_index=1,
        workflow_id="retry-runtime",
        state="PENDING",
        start_time=estimate,
        last_updated_at=actual,
    )
    incoming = SlurmJobExecutionRecord(
        job_id="16",
        job_attempt_index=1,
        workflow_id="retry-runtime",
        state="COMPLETED",
        start_time=actual if has_start else None,
        scheduler_metadata=metadata,
        last_updated_at=actual,
    )
    session.scalars.return_value.first.return_value = existing
    stored, created, old = mysql_storage.upsert_job_record(incoming)
    assert created is False
    assert stored.start_time == (actual if corrected else estimate)
    assert stored.job_attempt_index == 1
    assert old.start_time == estimate


def test_upsert_job_record_preserves_existing_fields_when_update_has_none(mysql_storage, session):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="20000",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        start_time=now - timedelta(minutes=5),
        last_updated_at=now,
    )
    assert existing_job.cluster == "test_cluster"
    assert existing_job.user == "test_user"
    assert existing_job.node_count == 5
    assert existing_job.partition == "recovery"

    incoming_update = SlurmJobExecutionRecord(
        job_id="20000",
        job_attempt_index=0,
        workflow_id="workflow-20000",
        state="RUNNING",
        last_updated_at=now,
    )
    assert incoming_update.cluster is None
    assert incoming_update.user is None
    assert incoming_update.node_count is None
    assert incoming_update.partition is None

    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(incoming_update)

    assert created is False
    assert updated_record.state == "RUNNING"
    assert updated_record.cluster == "test_cluster"
    assert updated_record.user == "test_user"
    assert updated_record.node_count == 5
    assert updated_record.partition == "recovery"
    assert updated_record.command == "sbatch test.sh"
    assert updated_record.reservation == "recovery_prod"
    assert updated_record.heimdall_job_id == generate_heimdall_job_id("20000", 0)
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(existing_job)


def test_upsert_job_record_populates_heimdall_id_log_path_when_creating(mysql_storage, session):
    now = datetime.now(UTC)
    job_record = SlurmJobExecutionRecord(
        job_id="compatibility-create",
        job_attempt_index=2,
        workflow_id="workflow-create",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = None

    stored_record, created, old_record = mysql_storage.upsert_job_record(
        job_record,
        populate_heimdall_id_log_path=True,
    )

    expected_heimdall_job_id = generate_heimdall_job_id("compatibility-create", 2)
    assert created is True
    assert old_record is None
    assert stored_record.heimdall_job_id == expected_heimdall_job_id
    assert stored_record.log_file_paths == [str(expected_heimdall_job_id)]


@pytest.mark.parametrize("existing_log_file_paths", [None, []])
def test_upsert_job_record_compatibility_log_path_uses_locked_existing_id(
    mysql_storage,
    session,
    existing_log_file_paths,
):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="compatibility-update",
        attempt_index=3,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        log_file_paths=existing_log_file_paths,
    )
    existing_heimdall_job_id = uuid.uuid4()
    existing_job.heimdall_job_id = existing_heimdall_job_id

    incoming_update = SlurmJobExecutionRecord(
        job_id="compatibility-update",
        heimdall_job_id=uuid.uuid4(),
        job_attempt_index=3,
        workflow_id="workflow-compatibility-update",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(
        incoming_update,
        populate_heimdall_id_log_path=True,
    )

    assert created is False
    assert updated_record.heimdall_job_id == existing_heimdall_job_id
    assert updated_record.log_file_paths == [str(existing_heimdall_job_id)]
    assert incoming_update.log_file_paths == [str(existing_heimdall_job_id)]
    locked_query = session.scalars.call_args.args[0]
    assert locked_query._for_update_arg is not None


def test_upsert_job_record_compatibility_log_path_preserves_existing_nonempty_paths(mysql_storage, session):
    now = datetime.now(UTC)
    existing_log_file_paths = ["/logs/real.out", "/logs/direct.out"]
    existing_job = create_example_slurm_job_record(
        job_id="compatibility-existing-paths",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        log_file_paths=existing_log_file_paths,
    )
    existing_heimdall_job_id = uuid.uuid4()
    existing_job.heimdall_job_id = existing_heimdall_job_id
    incoming_update = SlurmJobExecutionRecord(
        job_id="compatibility-existing-paths",
        job_attempt_index=0,
        workflow_id="workflow-compatibility-existing-paths",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(
        incoming_update,
        populate_heimdall_id_log_path=True,
    )

    assert created is False
    assert updated_record.heimdall_job_id == existing_heimdall_job_id
    assert updated_record.log_file_paths == existing_log_file_paths
    assert incoming_update.log_file_paths is None


def test_upsert_job_record_compatibility_log_path_fills_null_existing_id(mysql_storage, session):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="compatibility-null-id",
        attempt_index=4,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        log_file_paths=None,
    )
    existing_job.heimdall_job_id = None
    incoming_update = SlurmJobExecutionRecord(
        job_id="compatibility-null-id",
        job_attempt_index=4,
        workflow_id="workflow-compatibility-null-id",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(
        incoming_update,
        populate_heimdall_id_log_path=True,
    )

    expected_heimdall_job_id = generate_heimdall_job_id("compatibility-null-id", 4)
    assert created is False
    assert updated_record.heimdall_job_id == expected_heimdall_job_id
    assert updated_record.log_file_paths == [str(expected_heimdall_job_id)]


@pytest.mark.parametrize(
    ("incoming_log_file_paths", "expected_log_file_paths"),
    [
        (None, ["/logs/last-known-good.out"]),
        ([], []),
        (["/logs/direct.out"], ["/logs/direct.out"]),
    ],
)
def test_upsert_job_record_default_log_path_semantics_remain_unchanged(
    mysql_storage,
    session,
    incoming_log_file_paths,
    expected_log_file_paths,
):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="default-log-path-update",
        attempt_index=0,
        submit_time=now - timedelta(minutes=10),
        last_updated_at=now,
        log_file_paths=["/logs/last-known-good.out"],
    )
    incoming_update = SlurmJobExecutionRecord(
        job_id="default-log-path-update",
        job_attempt_index=0,
        workflow_id="workflow-default-log-path-update",
        log_file_paths=incoming_log_file_paths,
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(incoming_update)

    assert created is False
    assert updated_record.log_file_paths == expected_log_file_paths


def test_upsert_job_record_authoritatively_updates_workflow_id_independently_of_job_name(mysql_storage, session):
    now = datetime.now(UTC)
    existing_job = create_example_slurm_job_record(
        job_id="30000",
        attempt_index=0,
        job_name="independent-job-name",
        submit_time=now - timedelta(minutes=10),
        start_time=now - timedelta(minutes=5),
        last_updated_at=now,
    )
    existing_job.workflow_id = "old-workflow"

    incoming_update = SlurmJobExecutionRecord(
        job_id="30000",
        job_attempt_index=0,
        workflow_id="new-workflow",
        job_name="independent-job-name",
        state="RUNNING",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing_job

    updated_record, created, _old_record = mysql_storage.upsert_job_record(incoming_update)

    assert created is False
    assert updated_record.workflow_id == "new-workflow"
    assert updated_record.job_name == "independent-job-name"
    assert updated_record.cluster == "test_cluster"
    assert updated_record.user == "test_user"
    assert updated_record.partition == "recovery"
    assert updated_record.start_time == now - timedelta(minutes=5)
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(existing_job)


def test_upsert_job_record_raises_after_exhausting_deadlock_retries(mysql_storage, session):
    """A real MySQL deadlock (error 1213) on every attempt must exhaust the bounded retry
    budget and surface as RecoveryStorageException, not retry forever or hang."""
    now = datetime.now(UTC)
    incoming = SlurmJobExecutionRecord(
        job_id="deadlock-exhausted",
        job_attempt_index=0,
        workflow_id="workflow-deadlock-exhausted",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = None
    session.commit.side_effect = OperationalError(
        "INSERT INTO job_execution_records ...",
        {},
        Exception(1213, "Deadlock found when trying to get lock; try restarting transaction"),
    )

    with patch("recovery_libs.storage.mysql_storage.time.sleep") as mock_sleep, pytest.raises(RecoveryStorageException):
        mysql_storage.upsert_job_record(incoming)

    # One initial attempt plus UPSERT_CONFLICT_MAX_RETRIES retries, each preceded by a backoff sleep.
    assert session.commit.call_count == UPSERT_CONFLICT_MAX_RETRIES + 1
    assert session.rollback.call_count == UPSERT_CONFLICT_MAX_RETRIES + 1
    assert _transaction_method_names(session) == [
        method for _ in range(UPSERT_CONFLICT_MAX_RETRIES + 1) for method in ("scalars", "commit", "rollback")
    ]
    assert mock_sleep.call_count == UPSERT_CONFLICT_MAX_RETRIES


def test_upsert_job_record_raises_after_exhausting_duplicate_key_retries(mysql_storage, session):
    """A real MySQL duplicate-key conflict (error 1062) on every attempt -- e.g. two workers
    racing to insert the same brand-new job under READ COMMITTED -- must exhaust the bounded
    retry budget and surface as RecoveryStorageException, not retry forever or hang."""
    now = datetime.now(UTC)
    incoming = SlurmJobExecutionRecord(
        job_id="duplicate-key-exhausted",
        job_attempt_index=0,
        workflow_id="workflow-duplicate-key-exhausted",
        last_updated_at=now,
    )
    session.scalars.return_value.first.return_value = None
    session.commit.side_effect = IntegrityError(
        "INSERT INTO job_execution_records ...",
        {},
        Exception(1062, "Duplicate entry 'duplicate-key-exhausted-0' for key 'job_id_attempt_unique_index'"),
    )

    with patch("recovery_libs.storage.mysql_storage.time.sleep") as mock_sleep, pytest.raises(RecoveryStorageException):
        mysql_storage.upsert_job_record(incoming)

    assert session.commit.call_count == UPSERT_CONFLICT_MAX_RETRIES + 1
    assert session.rollback.call_count == UPSERT_CONFLICT_MAX_RETRIES + 1
    assert _transaction_method_names(session) == [
        method for _ in range(UPSERT_CONFLICT_MAX_RETRIES + 1) for method in ("scalars", "commit", "rollback")
    ]
    assert mock_sleep.call_count == UPSERT_CONFLICT_MAX_RETRIES


def test_upsert_job_record_retries_duplicate_key_conflict_then_succeeds(mysql_storage, session):
    """A duplicate-key conflict on the first attempt (the losing side of a same-key insert
    race) must be retried transparently and succeed once the retry's SELECT ... FOR UPDATE
    finds the row the winning transaction just committed."""
    now = datetime.now(UTC)
    incoming = SlurmJobExecutionRecord(
        job_id="duplicate-key-retry-succeeds",
        job_attempt_index=0,
        workflow_id="workflow-duplicate-key-retry-succeeds",
        last_updated_at=now,
    )
    existing_job = create_example_slurm_job_record(
        job_id="duplicate-key-retry-succeeds",
        attempt_index=0,
        submit_time=now,
        last_updated_at=now,
    )
    session.scalars.return_value.first.side_effect = [None, existing_job]
    session.commit.side_effect = [
        IntegrityError(
            "INSERT INTO job_execution_records ...",
            {},
            Exception(1062, "Duplicate entry 'duplicate-key-retry-succeeds-0' for key 'job_id_attempt_unique_index'"),
        ),
        None,
    ]

    with patch("recovery_libs.storage.mysql_storage.time.sleep") as mock_sleep:
        updated_record, created, old_record = mysql_storage.upsert_job_record(incoming)

    assert created is False
    assert updated_record is existing_job
    assert old_record is not None
    assert old_record is not updated_record
    assert session.commit.call_count == 2
    session.rollback.assert_called_once_with()
    assert _transaction_method_names(session) == ["scalars", "commit", "rollback", "scalars", "commit"]
    mock_sleep.assert_called_once()


def test_upsert_job_record_duplicate_key_retry_does_not_clobber_winners_log_paths(mysql_storage, session):
    """A losing transaction's placeholder log_file_paths (written by
    populate_heimdall_id_log_path on the failed create attempt) must not survive into the retry
    and overwrite real log paths already committed by the transaction that won the same-key
    insert race."""
    now = datetime.now(UTC)
    incoming = SlurmJobExecutionRecord(
        job_id="duplicate-key-preserves-log-paths",
        job_attempt_index=0,
        workflow_id="workflow-duplicate-key-preserves-log-paths",
        last_updated_at=now,
    )
    winner_log_paths = ["/logs/real/path.out"]
    existing_job = create_example_slurm_job_record(
        job_id="duplicate-key-preserves-log-paths",
        attempt_index=0,
        submit_time=now,
        last_updated_at=now,
        log_file_paths=winner_log_paths,
    )
    session.scalars.return_value.first.side_effect = [None, existing_job]
    session.commit.side_effect = [
        IntegrityError(
            "INSERT INTO job_execution_records ...",
            {},
            Exception(1062, "Duplicate entry 'duplicate-key-preserves-log-paths-0' for key 'heimdall_job_id'"),
        ),
        None,
    ]

    with patch("recovery_libs.storage.mysql_storage.time.sleep"):
        updated_record, created, old_record = mysql_storage.upsert_job_record(
            incoming,
            populate_heimdall_id_log_path=True,
        )

    assert created is False
    assert updated_record is existing_job
    assert old_record is not None
    assert old_record.log_file_paths == winner_log_paths
    assert updated_record.log_file_paths == winner_log_paths
    session.rollback.assert_called_once_with()
    assert _transaction_method_names(session) == ["scalars", "commit", "rollback", "scalars", "commit"]


def test_upsert_advice_rejects_replacing_absorbing_terminated_state(mysql_storage, session):
    now = datetime.now(UTC)
    existing = AdviceRecord(
        heimdall_id=uuid.uuid4(),
        visible=True,
        job_id="dag-child",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        advice_state=AdviceState.TERMINATED.value,
        created_at=now,
        updated_at=now,
    )
    incoming = AdviceRecord(
        heimdall_id=existing.heimdall_id,
        visible=True,
        job_id=existing.job_id,
        scheduler_metadata=existing.scheduler_metadata,
        attempt_index=existing.attempt_index,
        advice_state=AdviceState.RELEASED.value,
        created_at=now,
        updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing

    with pytest.raises(RecoveryStorageInputValidationError, match="Cannot replace Terminated advice"):
        mysql_storage.upsert_advice(incoming)

    session.commit.assert_not_called()


def test_upsert_advice_retries_deadlock_then_updates_winning_row(mysql_storage, session):
    """A MySQL deadlock (1213) on the first INSERT, which loses a race between two first
    writers, must be rolled back and retried. The retry must not fail the caller. It finds
    the row that the winning writer committed and updates that row."""
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    incoming = AdviceRecord(
        heimdall_id=advice_id,
        visible=False,
        job_id="advice-deadlock-retry",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        created_at=now,
        updated_at=now,
    )
    winner = AdviceRecord(
        heimdall_id=advice_id,
        visible=False,
        job_id="advice-deadlock-retry",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        generation=1,
        created_at=now,
        updated_at=now,
    )
    session.scalars.return_value.first.side_effect = [None, winner]
    session.commit.side_effect = [
        OperationalError(
            "INSERT INTO advice ...",
            {},
            Exception(1213, "Deadlock found when trying to get lock; try restarting transaction"),
        ),
        None,
    ]

    with patch("recovery_libs.storage.mysql_storage.time.sleep") as mock_sleep:
        stored, created, old_record = mysql_storage.upsert_advice(incoming)

    assert created is False
    assert stored is winner
    assert old_record is not None
    assert _transaction_method_names(session) == ["scalars", "commit", "rollback", "scalars", "commit"]
    mock_sleep.assert_called_once()


def test_upsert_advice_rejects_visible_empty_recommendation(mysql_storage, session):
    now = datetime.now(UTC)
    advice = AdviceRecord(
        heimdall_id=uuid.uuid4(),
        visible=True,
        job_id="empty-advice",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        created_at=now,
        updated_at=now,
    )

    with pytest.raises(RecoveryStorageInputValidationError, match="must contain a desired state"):
        mysql_storage.upsert_advice(advice)

    session.commit.assert_not_called()


def test_upsert_advice_increments_generation_only_for_action_affecting_changes(mysql_storage, session):
    now = datetime.now(UTC)
    original_desired_state_changed_at = now - timedelta(minutes=10)
    advice_id = uuid.uuid4()
    existing = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="generation-job",
        scheduler_metadata={"scheduler": "slurm", "environment": "prod", "diagnostic": "old"},
        attempt_index=0,
        advice_state=None,
        node_inclusions=["node-a"],
        issues=[{"message": "old evidence"}],
        generation=3,
        desired_state_changed_at=original_desired_state_changed_at,
        created_at=now,
        updated_at=now,
    )
    incoming = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id=existing.job_id,
        scheduler_metadata={"scheduler": "slurm", "environment": "prod", "diagnostic": "new"},
        attempt_index=0,
        advice_state=None,
        node_inclusions=["node-a"],
        issues=[{"message": "new evidence"}],
        created_at=now,
        updated_at=now,
    )
    session.scalars.return_value.first.return_value = existing

    updated, created, _old_record = mysql_storage.upsert_advice(incoming)

    assert created is False
    assert updated.generation == 3
    assert updated.desired_state_changed_at == original_desired_state_changed_at

    changed_incoming = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id=existing.job_id,
        scheduler_metadata=incoming.scheduler_metadata,
        attempt_index=0,
        advice_state=None,
        node_inclusions=["node-b"],
        issues=incoming.issues,
        created_at=now,
        updated_at=now,
    )
    updated, created, _old_record = mysql_storage.upsert_advice(changed_incoming)

    assert created is False
    assert updated.generation == 4
    assert updated.desired_state_changed_at > original_desired_state_changed_at


def test_converged_release_status_is_persisted(mysql_storage, session):
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    advice = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="released-job",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=1,
        advice_state=AdviceState.RELEASED.value,
        generation=2,
        created_at=now,
        updated_at=now,
    )
    status_record = AdviceReconciliationStatusRecord(
        advice_id=advice_id,
        observed_generation=2,
        disposition=AdviceReconciliationDisposition.CONVERGED.value,
        reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
        observed_at=now,
        transitioned_at=now,
    )
    advice_query = MagicMock()
    advice_query.first.return_value = advice
    status_query = MagicMock()
    status_query.first.return_value = None
    session.scalars.side_effect = [advice_query, status_query]

    persisted = mysql_storage.upsert_advice_reconciliation_status(status_record)

    assert persisted is status_record
    session.commit.assert_called_once()


def test_terminal_advice_status_cannot_be_reclassified(mysql_storage, session):
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    advice = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="terminal-status-job",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        advice_state=AdviceState.TERMINATED.value,
        generation=1,
        created_at=now,
        updated_at=now,
    )
    existing_status = AdviceReconciliationStatusRecord(
        advice_id=advice_id,
        observed_generation=1,
        disposition=AdviceReconciliationDisposition.CONVERGED.value,
        reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
        observed_at=now,
        transitioned_at=now,
    )
    incoming_status = AdviceReconciliationStatusRecord(
        advice_id=advice_id,
        observed_generation=1,
        disposition=AdviceReconciliationDisposition.ABANDONED.value,
        reason=AdviceReconciliationReason.TARGET_STATE_FINAL.value,
        observed_at=now,
        transitioned_at=now,
    )
    advice_query = MagicMock()
    advice_query.first.return_value = advice
    status_query = MagicMock()
    status_query.first.return_value = existing_status
    session.scalars.side_effect = [advice_query, status_query]

    with pytest.raises(RecoveryStorageInputValidationError, match="terminal reconciliation status"):
        mysql_storage.upsert_advice_reconciliation_status(incoming_status)

    session.commit.assert_not_called()


def test_upsert_advice_returns_locked_transient_pre_update_snapshot(connection):
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    existing = AdviceRecord(
        heimdall_id=advice_id,
        visible=False,
        job_id="advice-snapshot",
        scheduler_metadata={"namespace": "old"},
        attempt_index=0,
        advice_state=AdviceState.RESTARTED.value,
        node_exclusions=["node-old"],
        created_at=now - timedelta(minutes=1),
        updated_at=now - timedelta(minutes=1),
    )
    incoming = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="advice-snapshot",
        scheduler_metadata={"namespace": "new"},
        attempt_index=0,
        advice_state=AdviceState.TERMINATED.value,
        node_exclusions=["node-new"],
        created_at=now,
        updated_at=now,
    )

    make_transient_to_detached(existing)
    with Session() as tracked_session:
        tracked_session.add(existing)
        connection.write_session = MagicMock(spec=sessionmaker)
        connection.write_session.return_value.__enter__.return_value = tracked_session
        connection.write_pool_enabled = True
        mysql_storage = MySQLStorage(connection=connection)

        with (
            patch.object(tracked_session, "scalars") as scalars,
            patch.object(tracked_session, "commit"),
            patch.object(tracked_session, "refresh"),
        ):
            scalars.return_value.first.return_value = existing
            result = mysql_storage.upsert_advice(incoming)
            locked_query = scalars.call_args.args[0]

    assert locked_query._for_update_arg is not None
    assert result.was_created is False
    assert result.old_record is not None
    assert result.old_record is not result.inserted_record
    assert result.old_record.visible is False
    assert result.old_record.advice_state == AdviceState.RESTARTED.value
    assert result.old_record.scheduler_metadata == {"namespace": "old"}
    assert result.old_record.node_exclusions == ["node-old"]
    assert result.inserted_record.visible is True
    assert result.inserted_record.advice_state == AdviceState.TERMINATED.value
    assert inspect(result.old_record).transient
    assert inspect(result.old_record).session is None
    assert inspect(result.inserted_record).session is None


def test_delete_advice_returns_visible_to_hidden_transition(mysql_storage, session):
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    advice_record = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="soft-delete",
        attempt_index=0,
        advice_state=AdviceState.RESTARTED.value,
        created_at=now,
        updated_at=now,
    )
    session.scalars.return_value.first.return_value = advice_record

    result = mysql_storage.delete_advice(advice_id)

    assert result.advice_record is advice_record
    assert result.was_deleted is True
    assert advice_record.visible is False
    locked_query = session.scalars.call_args.args[0]
    assert locked_query._for_update_arg is not None
    session.flush.assert_called_once()
    session.refresh.assert_called_once_with(advice_record)
    session.expunge.assert_called_once_with(advice_record)
    session.commit.assert_called_once()


def test_delete_advice_returns_already_hidden_record_without_writing(mysql_storage, session):
    now = datetime.now(UTC)
    advice_record = AdviceRecord(
        heimdall_id=uuid.uuid4(),
        visible=False,
        job_id="already-hidden",
        attempt_index=0,
        advice_state=AdviceState.RESTARTED.value,
        created_at=now,
        updated_at=now,
    )
    session.scalars.return_value.first.return_value = advice_record

    result = mysql_storage.delete_advice(advice_record.heimdall_id)

    assert result.advice_record is advice_record
    assert result.was_deleted is False
    session.expunge.assert_called_once_with(advice_record)
    session.flush.assert_not_called()
    session.commit.assert_not_called()


def test_delete_advice_returns_missing_record_without_writing(mysql_storage, session):
    session.scalars.return_value.first.return_value = None

    result = mysql_storage.delete_advice(uuid.uuid4())

    assert result.advice_record is None
    assert result.was_deleted is False
    session.expunge.assert_not_called()
    session.flush.assert_not_called()
    session.commit.assert_not_called()


# ============================================================================
# DAG Dependency CRUD Tests
# ============================================================================


def _create_test_dag_dependency(**overrides):
    """Helper to create a DagDependency instance for testing."""
    from recovery_libs.storage.dataclass import DagDependency, DagDependencyActionStatus

    defaults = {
        "dependent_job_id": "12345",
        "dependent_job_action": "release",
        "dependency_job_identifier": "preprocess_job",
        "dependency_job_identifier_type": "name",
        "dependency_trigger_condition": "completed",
        "action_status": DagDependencyActionStatus.NOT_ELIGIBLE.value,
        "action_time": None,
    }
    defaults.update(overrides)
    return DagDependency(**defaults)


def test_upsert_dag_dependency_create(session, mysql_storage):
    """Test creating a new DAG dependency record when none exists."""
    session.scalars.return_value.first.return_value = None  # No existing record
    record = _create_test_dag_dependency()
    _, updated = mysql_storage.upsert_dag_dependency(record)
    assert updated is True
    session.add.assert_called_once_with(record)
    session.commit.assert_called_once()


def test_upsert_dag_dependency_existing_unchanged(session, mysql_storage):
    """Test that upserting with identical status returns existing without update."""
    existing_dep = _create_test_dag_dependency()
    existing_dep.id = 42
    session.scalars.return_value.first.return_value = existing_dep

    new_dep = _create_test_dag_dependency()  # Same action_status and action_time
    result, updated = mysql_storage.upsert_dag_dependency(new_dep)

    assert result.id == 42
    assert updated is False
    session.add.assert_not_called()


def test_upsert_dag_dependency_existing_status_changed(session, mysql_storage):
    """Test that upserting with different status updates the existing record."""
    from recovery_libs.storage.dataclass import DagDependencyActionStatus

    existing_dep = _create_test_dag_dependency(action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value)
    existing_dep.id = 42
    session.scalars.return_value.first.return_value = existing_dep

    new_dep = _create_test_dag_dependency(action_status=DagDependencyActionStatus.IN_PROGRESS.value)
    result, updated = mysql_storage.upsert_dag_dependency(new_dep)

    assert updated is True
    assert result.action_status == DagDependencyActionStatus.IN_PROGRESS.value
    session.commit.assert_called_once()


@pytest.mark.parametrize("status", ["in_progress", "successful", "failed", "aborted"])
def test_upsert_dag_dependency_cannot_regress_started_gate(session, mysql_storage, status):
    existing_dep = _create_test_dag_dependency(action_status=status)
    existing_dep.id = 42
    session.scalars.return_value.first.return_value = existing_dep

    result, updated = mysql_storage.upsert_dag_dependency(_create_test_dag_dependency())

    assert result.action_status == status
    assert updated is False
    session.commit.assert_not_called()


def test_read_dag_dependencies_with_dependent_job_id(session, mysql_storage):
    """Test read_dag_dependencies filtering by dependent_job_id."""
    dep1 = _create_test_dag_dependency(dependency_job_identifier="parent_a")
    dep2 = _create_test_dag_dependency(dependency_job_identifier="parent_b")
    session.execute.return_value.scalar.return_value = 2
    session.scalars.return_value.all.return_value = [dep1, dep2]

    results, total = mysql_storage.read_dag_dependencies(dependent_job_id="12345")
    assert len(results) == 2
    assert total == 2


def test_read_dag_dependencies_with_parent_filter(session, mysql_storage):
    """Test read_dag_dependencies filtering by parent identifier."""
    dep = _create_test_dag_dependency()
    session.execute.return_value.scalar.return_value = 1
    session.scalars.return_value.all.return_value = [dep]

    results, total = mysql_storage.read_dag_dependencies(
        dependency_job_identifier="preprocess_job", dependency_job_identifier_type="name"
    )
    assert len(results) == 1
    assert total == 1


def test_read_dag_dependencies_with_action_statuses(session, mysql_storage):
    """Test read_dag_dependencies filtering by action statuses."""
    from recovery_libs.storage.dataclass import DagDependencyActionStatus

    dep = _create_test_dag_dependency(action_status=DagDependencyActionStatus.IN_PROGRESS.value)
    session.execute.return_value.scalar.return_value = 1
    session.scalars.return_value.all.return_value = [dep]

    results, total = mysql_storage.read_dag_dependencies(action_statuses=[DagDependencyActionStatus.IN_PROGRESS.value])
    assert len(results) == 1
    assert total == 1


def test_read_dag_dependencies_with_multiple_statuses(session, mysql_storage):
    """Test read_dag_dependencies filtering by multiple action statuses."""
    from recovery_libs.storage.dataclass import DagDependencyActionStatus

    dep1 = _create_test_dag_dependency(action_status=DagDependencyActionStatus.IN_PROGRESS.value)
    dep2 = _create_test_dag_dependency(action_status=DagDependencyActionStatus.FAILED.value)
    session.execute.return_value.scalar.return_value = 2
    session.scalars.return_value.all.return_value = [dep1, dep2]

    results, total = mysql_storage.read_dag_dependencies(
        action_statuses=[DagDependencyActionStatus.IN_PROGRESS.value, DagDependencyActionStatus.FAILED.value]
    )
    assert len(results) == 2
    assert total == 2


def test_read_dag_dependencies_with_time_filter(session, mysql_storage):
    """Test read_dag_dependencies with action_time_before filter."""
    from recovery_libs.storage.dataclass import DagDependencyActionStatus

    dep = _create_test_dag_dependency(
        action_status=DagDependencyActionStatus.IN_PROGRESS.value,
        action_time=datetime(2026, 4, 15, tzinfo=UTC),
    )
    session.execute.return_value.scalar.return_value = 1
    session.scalars.return_value.all.return_value = [dep]

    threshold = datetime(2026, 4, 16, tzinfo=UTC)
    results, total = mysql_storage.read_dag_dependencies(
        action_statuses=[DagDependencyActionStatus.IN_PROGRESS.value],
        action_time_before=threshold,
    )
    assert len(results) == 1
    assert total == 1


def test_read_dag_dependencies_empty(session, mysql_storage):
    """Test read_dag_dependencies when no records match."""
    session.execute.return_value.scalar.return_value = 0
    session.scalars.return_value.all.return_value = []

    results, total = mysql_storage.read_dag_dependencies(dependent_job_id="99999")
    assert results == []
    assert total == 0


def test_read_dag_dependencies_pagination(session, mysql_storage):
    """Test read_dag_dependencies with limit and offset."""
    dep = _create_test_dag_dependency()
    session.execute.return_value.scalar.return_value = 10
    session.scalars.return_value.all.return_value = [dep]

    results, total = mysql_storage.read_dag_dependencies(dependent_job_id="12345", limit=1, offset=5)
    assert len(results) == 1
    assert total == 10


def test_read_dag_dependencies_requires_identifier_type(mysql_storage):
    """Test that dependency_job_identifier_type is required when dependency_job_identifier is provided."""
    from recovery_libs.storage.base import RecoveryStorageInputValidationError

    with pytest.raises(RecoveryStorageInputValidationError):
        mysql_storage.read_dag_dependencies(dependency_job_identifier="parent_job")
