# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import datetime

from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)


def test_create_node_event():
    """Test creating a node event record with basic attributes"""
    timestamp = datetime.now()
    event = NodeEvent.create(
        node="test_node",
        job_id="123",
        job_attempt_index=0,
        timestamp=timestamp,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )

    assert event.node == "test_node"
    assert event.job_id == "123"
    assert event.job_attempt_index == 0
    assert event.timestamp == timestamp
    assert event.error_attribution_type == ErrorAttributionType.FAST
    assert event.error_attribution_origin == ErrorAttributionOrigin.FACT
    assert ErrorAttributionSource.APPLICATION_LOGS in event.error_attribution_sources
    assert ErrorAttributionSource.SYSLOGS in event.error_attribution_sources
    assert event.exclusion_type == ExclusionType.TEMPORARY
    assert event.fact_status == FactStatus.OK


def test_create_node_event_with_original_exclusion_type():
    """Test creating a node event record with original_exclusion_type"""
    timestamp = datetime.now()
    event = NodeEvent.create(
        node="test_node",
        job_id="123",
        job_attempt_index=0,
        timestamp=timestamp,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.NONE,
        original_exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )

    assert event.node == "test_node"
    assert event.exclusion_type == ExclusionType.NONE
    assert event.original_exclusion_type == ExclusionType.TEMPORARY


def test_node_event_to_json():
    """Test converting a node event record to JSON format"""
    timestamp = datetime.now()
    event = NodeEvent(
        node="json_node",
        timestamp=timestamp,
        job_id="123",
        job_attempt_index=0,
        job_start_time=timestamp,
        job_end_time=timestamp,
        job_anomaly_id=uuid.uuid4(),
        cluster="test-cluster",
        error_attribution_type=ErrorAttributionType.SLOW,
        error_attribution_origin=ErrorAttributionOrigin.ADI,
        error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status=FactStatus.ERROR,
    )

    json_data = event.to_json()
    assert json_data["node"] == "json_node"
    assert json_data["job_id"] == "123"
    assert json_data["job_attempt_index"] == 0
    assert json_data["job_start_time"] == timestamp.isoformat()
    assert json_data["job_end_time"] == timestamp.isoformat()
    assert json_data["job_anomaly_id"] == str(event.job_anomaly_id)
    assert json_data["timestamp"] == timestamp.isoformat()
    assert json_data["error_attribution_type"] == ErrorAttributionType.SLOW.value
    assert json_data["error_attribution_origin"] == ErrorAttributionOrigin.ADI.value
    assert json_data["error_attribution_sources"] == [ErrorAttributionSource.SYSLOGS.value]
    assert json_data["exclusion_type"] == ExclusionType.CONFIRMED.value
    assert json_data["fact_status"] == FactStatus.ERROR.value


def test_node_event_from_json():
    """Test creating a node event record from JSON data"""
    timestamp = datetime.now()
    anomaly_id = uuid.uuid4()
    json_data = {
        "node": "from_json_node",
        "timestamp": timestamp.isoformat(),
        "job_id": "123",
        "job_attempt_index": 0,
        "job_start_time": timestamp.isoformat(),
        "job_end_time": timestamp.isoformat(),
        "job_anomaly_id": str(anomaly_id),
        "cluster": "test-cluster",
        "error_attribution": ErrorAttribution.APPLICATION.value,
        "error_attribution_type": ErrorAttributionType.REVISED.value,
        "error_attribution_origin": ErrorAttributionOrigin.ADI.value,
        "error_attribution_sources": [
            ErrorAttributionSource.EPILOG_RESULTS.value,
            ErrorAttributionSource.HEALTH_CHECKS.value,
        ],
        "exclusion_type": ExclusionType.CONFIRMED.value,
        "fact_status": FactStatus.OK.value,
    }

    event = NodeEvent.from_json(json_data)
    assert event.node == "from_json_node"
    assert event.timestamp == timestamp
    assert event.job_id == "123"
    assert event.job_attempt_index == 0
    assert event.job_start_time == timestamp
    assert event.job_end_time == timestamp
    assert event.job_anomaly_id == anomaly_id
    assert event.cluster == "test-cluster"
    assert event.error_attribution == ErrorAttribution.APPLICATION
    assert event.error_attribution_type == ErrorAttributionType.REVISED
    assert event.error_attribution_origin == ErrorAttributionOrigin.ADI
    assert ErrorAttributionSource.EPILOG_RESULTS in event.error_attribution_sources
    assert ErrorAttributionSource.HEALTH_CHECKS in event.error_attribution_sources
    assert event.exclusion_type == ExclusionType.CONFIRMED
    assert event.fact_status == FactStatus.OK


def test_node_event_json_serialization():
    """Test that a node event can be serialized to JSON and back

    This test verifies that:
    1. A node event can be converted to JSON using to_json()
    2. The JSON data can be converted back to a node event using from_json()
    3. The resulting node event matches the original in all fields
    """
    # Create original event with various fields set
    original_event = NodeEvent.create(
        node="test_node",
        timestamp=datetime(2024, 1, 15, 12, 30, 45),
        job_id="123",
        job_attempt_index=2,
        job_start_time=datetime(2024, 1, 15, 12, 30, 45),
        job_end_time=datetime(2024, 1, 15, 12, 30, 45),
        job_anomaly_id=uuid.uuid4(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.SYSTEM,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.CONFIRMED,
        original_exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )

    # Convert to JSON and back
    json_data = original_event.to_json()
    reconstructed_event = NodeEvent.from_json(json_data)

    # Verify all fields match
    assert reconstructed_event.node == original_event.node
    assert reconstructed_event.timestamp == original_event.timestamp
    assert reconstructed_event.job_id == original_event.job_id
    assert reconstructed_event.job_attempt_index == original_event.job_attempt_index
    assert reconstructed_event.job_start_time == original_event.job_start_time
    assert reconstructed_event.job_end_time == original_event.job_end_time
    assert reconstructed_event.cluster == original_event.cluster
    assert reconstructed_event.error_attribution == original_event.error_attribution
    assert reconstructed_event.error_attribution_type == original_event.error_attribution_type
    assert reconstructed_event.error_attribution_origin == original_event.error_attribution_origin
    assert reconstructed_event.error_attribution_sources == original_event.error_attribution_sources
    assert reconstructed_event.exclusion_type == original_event.exclusion_type
    assert reconstructed_event.original_exclusion_type == original_event.original_exclusion_type
    assert reconstructed_event.fact_status == original_event.fact_status


def test_node_event_default_values():
    """Test that node event record uses correct default values when fields are omitted"""
    now = datetime.now()
    event = NodeEvent.create(
        node="default_node",
        timestamp=now,
        job_id="123",
        job_attempt_index=0,
    )

    assert event.node == "default_node"
    assert event.timestamp == now
    assert event.job_id == "123"
    assert event.job_attempt_index == 0
    assert event.job_start_time is None
    assert event.job_end_time is None
    assert event.job_anomaly_id is None
    assert event.cluster == ""
    assert event.error_attribution == ErrorAttribution.UNDEFINED
    assert event.error_attribution_type == ErrorAttributionType.UNDEFINED
    assert event.error_attribution_origin == ErrorAttributionOrigin.UNDEFINED
    assert event.error_attribution_sources == []
    assert event.exclusion_type == ExclusionType.UNDEFINED
    assert event.original_exclusion_type == ExclusionType.UNDEFINED
    assert event.fact_status == FactStatus.UNDEFINED

    # Convert to JSON and back to make sure missing optional fields are not causing issues
    json_data = event.to_json()
    reconstructed_event = NodeEvent.from_json(json_data)
    assert reconstructed_event == event


def test_multiple_attribution_sources():
    """Test that multiple attribution sources can be added to a node event"""
    event = NodeEvent.create(
        node="multi_source_node",
        job_id="123",
        job_attempt_index=1,
        timestamp=datetime.now(),
        error_attribution_sources=[
            ErrorAttributionSource.APPLICATION_LOGS,
            ErrorAttributionSource.SYSLOGS,
            ErrorAttributionSource.NODE_METRICS,
        ],
    )
    assert len(event.error_attribution_sources) == 3
    assert ErrorAttributionSource.APPLICATION_LOGS in event.error_attribution_sources
    assert ErrorAttributionSource.SYSLOGS in event.error_attribution_sources
    assert ErrorAttributionSource.NODE_METRICS in event.error_attribution_sources


def test_error_attribution_enum():
    """
    Test that ErrorAttribution enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert ErrorAttribution.UNDEFINED.value == 0, (
        "Cannot change the value of UNDEFINED if already used in a database table"
    )
    assert ErrorAttribution.UNKNOWN.value == 1, "Cannot change the value of UNKNOWN if already used in a database table"
    assert ErrorAttribution.HARDWARE.value == 2, (
        "Cannot change the value of HARDWARE if already used in a database table"
    )
    assert ErrorAttribution.SYSTEM.value == 3, "Cannot change the value of SYSTEM if already used in a database table"
    assert ErrorAttribution.APPLICATION.value == 4, (
        "Cannot change the value of APPLICATION if already used in a database table"
    )


def test_error_attribution_type_enum():
    """
    Test that ErrorAttributionType enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert ErrorAttributionType.UNDEFINED.value == 0, (
        "Cannot change the value of UNDEFINED if already used in a database table"
    )
    assert ErrorAttributionType.NONE.value == 1, "Cannot change the value of NONE if already used in a database table"
    assert ErrorAttributionType.FAST.value == 2, "Cannot change the value of FAST if already used in a database table"
    assert ErrorAttributionType.SLOW.value == 3, "Cannot change the value of SLOW if already used in a database table"
    assert ErrorAttributionType.REVISED.value == 4, (
        "Cannot change the value of REVISED if already used in a database table"
    )


def test_error_attribution_origin_enum():
    """
    Test that ErrorAttributionOrigin enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert ErrorAttributionOrigin.UNDEFINED.value == 0, (
        "Cannot change the value of UNDEFINED if already used in a database table"
    )
    assert ErrorAttributionOrigin.NONE.value == 1, "Cannot change the value of NONE if already used in a database table"
    assert ErrorAttributionOrigin.FACT.value == 2, "Cannot change the value of FACT if already used in a database table"
    assert ErrorAttributionOrigin.ADI.value == 3, "Cannot change the value of ADI if already used in a database table"


def test_error_attribution_source_enum():
    """
    Test that ErrorAttributionSource enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert ErrorAttributionSource.UNDEFINED.value == 0, (
        "Cannot change the value of UNDEFINED if already used in a database table"
    )
    assert ErrorAttributionSource.NONE.value == 1, "Cannot change the value of NONE if already used in a database table"
    assert ErrorAttributionSource.APPLICATION_LOGS.value == 2, (
        "Cannot change the value of APPLICATION_LOGS if already used in a database table"
    )
    assert ErrorAttributionSource.SYSLOGS.value == 3, (
        "Cannot change the value of SYSLOGS if already used in a database table"
    )
    assert ErrorAttributionSource.NODE_METRICS.value == 4, (
        "Cannot change the value of NODE_METRICS if already used in a database table"
    )
    assert ErrorAttributionSource.SCHEDULER_METRICS.value == 5, (
        "Cannot change the value of SCHEDULER_METRICS if already used in a database table"
    )
    assert ErrorAttributionSource.NETWORK_METRICS.value == 6, (
        "Cannot change the value of NETWORK_METRICS if already used in a database table"
    )
    assert ErrorAttributionSource.HEALTH_CHECKS.value == 7, (
        "Cannot change the value of HEALTH_CHECKS if already used in a database table"
    )
    assert ErrorAttributionSource.EPILOG_RESULTS.value == 8, (
        "Cannot change the value of EPILOG_RESULTS if already used in a database table"
    )


def test_exclusion_type_enum():
    """
    Test that ExclusionType enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert ExclusionType.UNDEFINED.value == 0, (
        "Cannot change the value of UNDEFINED if already used in a database table"
    )
    assert ExclusionType.NONE.value == 1, "Cannot change the value of NONE if already used in a database table"
    assert ExclusionType.TEMPORARY.value == 2, (
        "Cannot change the value of TEMPORARY if already used in a database table"
    )
    assert ExclusionType.CONFIRMED.value == 3, (
        "Cannot change the value of CONFIRMED if already used in a database table"
    )
    assert ExclusionType.REVOKED.value == 4, "Cannot change the value of REVOKED if already used in a database table"


def test_fact_status_enum():
    """
    Test that FactStatus enum contains expected values. Once the enum is used in a database,
    the values are fixed and cannot be changed.
    """
    assert FactStatus.UNDEFINED.value == 0, "Cannot change the value of UNDEFINED if already used in a database table"
    assert FactStatus.NONE.value == 1, "Cannot change the value of NONE if already used in a database table"
    assert FactStatus.OK.value == 2, "Cannot change the value of OK if already used in a database table"
    assert FactStatus.ERROR.value == 3, "Cannot change the value of ERROR if already used in a database table"
