# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import UTC, datetime

from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)


class TestAnomalyAttributionResultWithGlobalEvents:
    """Test suite for AnomalyAttributionResult with GlobalEvent support"""

    def test_attribution_result_with_global_events_only(self):
        """Test AnomalyAttributionResult containing only GlobalEvents"""
        job_anomaly_id = uuid.uuid4()
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        error = JobExecutionError.create(
            code=100,
            message="Application error",
            description="Global application failure",
            signature="APP_ERROR",
        )

        global_event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-123",
            job_attempt_index=0,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
            fact_status=FactStatus.OK,
        )

        result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.CRASH,
            anomaly_uuid=str(job_anomaly_id),
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={error: [global_event]},
        )

        assert result.job_id == "test-job-123"
        assert len(result.global_attributions) == 1
        assert error in result.global_attributions
        assert len(result.global_attributions[error]) == 1
        assert isinstance(result.global_attributions[error][0], GlobalEvent)

    def test_attribution_result_with_mixed_events(self):
        """Test AnomalyAttributionResult containing both NodeEvents and GlobalEvents"""
        job_anomaly_id = uuid.uuid4()
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        error = JobExecutionError.create(
            code=200,
            message="Mixed error",
            description="Both node and global issues",
            signature="MIXED_ERROR",
        )

        node_event = NodeEvent.create(
            node="node-01",
            timestamp=timestamp,
            job_id="test-job-456",
            job_attempt_index=1,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.NODE_METRICS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        global_event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-456",
            job_attempt_index=1,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.SYSTEM,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            fact_status=FactStatus.OK,
        )

        result = AnomalyAttributionResult(
            job_id="test-job-456",
            job_attempt_index=1,
            anomaly_type=EventType.HANG,
            anomaly_uuid=str(job_anomaly_id),
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={error: [node_event]},
            global_attributions={error: [global_event]},
        )

        assert len(result.node_attributions[error]) == 1
        assert isinstance(result.node_attributions[error][0], NodeEvent)
        assert len(result.global_attributions[error]) == 1
        assert isinstance(result.global_attributions[error][0], GlobalEvent)

    def test_attribution_result_serialization_with_global_events(self):
        """Test serialization of AnomalyAttributionResult with GlobalEvents"""
        job_anomaly_id = uuid.uuid4()
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        error = JobExecutionError.create(
            code=300,
            message="Test error",
            description="Test description",
            signature="TEST_SIG",
        )

        global_event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-789",
            job_attempt_index=2,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.SLOW,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
            fact_status=FactStatus.OK,
        )

        result = AnomalyAttributionResult(
            job_id="test-job-789",
            job_attempt_index=2,
            anomaly_type=EventType.CRASH,
            anomaly_uuid=str(job_anomaly_id),
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={},
            global_attributions={error: [global_event]},
        )

        json_str = result.to_json()
        assert "test-job-789" in json_str
        assert "TEST_SIG" in json_str
        assert str(ErrorAttribution.APPLICATION.value) in json_str

    def test_attribution_result_deserialization_with_global_events(self):
        """Test deserialization of AnomalyAttributionResult with GlobalEvents"""
        job_anomaly_id = uuid.uuid4()

        json_data = {
            "job_id": "test-job-999",
            "job_attempt_index": 0,
            "anomaly_type": "CRASH",
            "anomaly_uuid": str(job_anomaly_id),
            "attribution_type": ErrorAttributionType.FAST.value,
            "node_attributions": {},
            "global_attributions": {
                '{"code": 400, "message": "Error msg", "description": "Desc", "signature": "SIG"}': [
                    {
                        # GlobalEvent (no 'node' field)
                        "timestamp": "2024-01-01T12:00:00+00:00",
                        "job_id": "test-job-999",
                        "job_attempt_index": 0,
                        "job_anomaly_id": str(job_anomaly_id),
                        "error_attribution": ErrorAttribution.APPLICATION.value,
                        "error_attribution_type": ErrorAttributionType.FAST.value,
                        "error_attribution_origin": ErrorAttributionOrigin.FACT.value,
                        "error_attribution_sources": [ErrorAttributionSource.APPLICATION_LOGS.value],
                        "fact_status": FactStatus.OK.value,
                    }
                ]
            },
        }

        result = AnomalyAttributionResult.from_json(json_data)

        assert result.job_id == "test-job-999"
        assert len(result.global_attributions) == 1

        # Get the first (and only) error and its events
        error_events = next(iter(result.global_attributions.values()))
        assert len(error_events) == 1
        assert isinstance(error_events[0], GlobalEvent)
        assert error_events[0].error_attribution == ErrorAttribution.APPLICATION

    def test_attribution_result_roundtrip_with_mixed_events(self):
        """Test roundtrip serialization with mixed NodeEvent and GlobalEvent"""
        job_anomaly_id = uuid.uuid4()
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        error = JobExecutionError.create(
            code=500,
            message="Roundtrip test",
            description="Testing serialization",
            signature="ROUNDTRIP",
        )

        node_event = NodeEvent.create(
            node="node-99",
            timestamp=timestamp,
            job_id="roundtrip-job",
            job_attempt_index=5,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.REVISED,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.HEALTH_CHECKS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        global_event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="roundtrip-job",
            job_attempt_index=5,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.SYSTEM,
            error_attribution_type=ErrorAttributionType.REVISED,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            fact_status=FactStatus.ERROR,
        )

        original_result = AnomalyAttributionResult(
            job_id="roundtrip-job",
            job_attempt_index=5,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid=str(job_anomaly_id),
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={error: [node_event]},
            global_attributions={error: [global_event]},
        )

        # Serialize
        json_str = original_result.to_json()

        # Deserialize
        import json

        json_data = json.loads(json_str)
        restored_result = AnomalyAttributionResult.from_json(json_data)

        # Verify
        assert restored_result.job_id == original_result.job_id
        assert restored_result.job_attempt_index == original_result.job_attempt_index
        assert restored_result.anomaly_type == original_result.anomaly_type
        assert restored_result.attribution_type == original_result.attribution_type

        restored_node_events = next(iter(restored_result.node_attributions.values()))
        assert len(restored_node_events) == 1
        assert isinstance(restored_node_events[0], NodeEvent)
        assert restored_node_events[0].node == "node-99"

        restored_global_events = next(iter(restored_result.global_attributions.values()))
        assert len(restored_global_events) == 1
        assert isinstance(restored_global_events[0], GlobalEvent)
        assert restored_global_events[0].error_attribution == ErrorAttribution.SYSTEM
