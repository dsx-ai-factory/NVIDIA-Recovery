# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import UTC, datetime

from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    FactStatus,  # Import to ensure SQLAlchemy relationships are set up
)


class TestGlobalEvent:
    """Test suite for GlobalEvent model"""

    def test_global_event_create(self):
        """Test creating a GlobalEvent instance"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        job_anomaly_id = uuid.uuid4()

        event = GlobalEvent.create(
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

        assert event.timestamp == timestamp
        assert event.job_id == "test-job-123"
        assert event.job_attempt_index == 0
        assert event.job_anomaly_id == job_anomaly_id
        assert event.error_attribution == ErrorAttribution.APPLICATION
        assert event.error_attribution_type == ErrorAttributionType.FAST
        assert event.error_attribution_origin == ErrorAttributionOrigin.FACT
        assert event.error_attribution_sources == [ErrorAttributionSource.APPLICATION_LOGS]
        assert event.fact_status == FactStatus.OK

    def test_global_event_create_with_defaults(self):
        """Test creating a GlobalEvent with default values"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-123",
            job_attempt_index=0,
        )

        assert event.timestamp == timestamp
        assert event.job_id == "test-job-123"
        assert event.job_attempt_index == 0
        assert event.error_attribution == ErrorAttribution.UNDEFINED
        assert event.error_attribution_type == ErrorAttributionType.UNDEFINED
        assert event.error_attribution_origin == ErrorAttributionOrigin.UNDEFINED
        assert event.error_attribution_sources == []
        assert event.fact_status == FactStatus.UNDEFINED

    def test_global_event_to_json(self):
        """Test GlobalEvent serialization to JSON"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        job_anomaly_id = uuid.uuid4()

        event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-123",
            job_attempt_index=2,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.SYSTEM,
            error_attribution_type=ErrorAttributionType.SLOW,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS, ErrorAttributionSource.NODE_METRICS],
            fact_status=FactStatus.ERROR,
        )

        json_data = event.to_json()

        assert json_data["timestamp"] == "2024-01-01T12:00:00+00:00"
        assert json_data["job_id"] == "test-job-123"
        assert json_data["job_attempt_index"] == 2
        assert json_data["job_anomaly_id"] == str(job_anomaly_id)
        assert json_data["error_attribution"] == ErrorAttribution.SYSTEM.value
        assert json_data["error_attribution_type"] == ErrorAttributionType.SLOW.value
        assert json_data["error_attribution_origin"] == ErrorAttributionOrigin.ADI.value
        assert json_data["error_attribution_sources"] == [
            ErrorAttributionSource.SYSLOGS.value,
            ErrorAttributionSource.NODE_METRICS.value,
        ]
        assert json_data["fact_status"] == FactStatus.ERROR.value

    def test_global_event_from_json(self):
        """Test GlobalEvent deserialization from JSON"""
        job_anomaly_id = uuid.uuid4()
        json_data = {
            "timestamp": "2024-01-01T12:00:00+00:00",
            "job_id": "test-job-456",
            "job_attempt_index": 1,
            "job_anomaly_id": str(job_anomaly_id),
            "error_attribution": ErrorAttribution.HARDWARE.value,
            "error_attribution_type": ErrorAttributionType.REVISED.value,
            "error_attribution_origin": ErrorAttributionOrigin.FACT.value,
            "error_attribution_sources": [
                ErrorAttributionSource.HEALTH_CHECKS.value,
                ErrorAttributionSource.EPILOG_RESULTS.value,
            ],
            "fact_status": FactStatus.OK.value,
        }

        event = GlobalEvent.from_json(json_data)

        assert event.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        assert event.job_id == "test-job-456"
        assert event.job_attempt_index == 1
        assert event.job_anomaly_id == job_anomaly_id
        assert event.error_attribution == ErrorAttribution.HARDWARE
        assert event.error_attribution_type == ErrorAttributionType.REVISED
        assert event.error_attribution_origin == ErrorAttributionOrigin.FACT
        assert event.error_attribution_sources == [
            ErrorAttributionSource.HEALTH_CHECKS,
            ErrorAttributionSource.EPILOG_RESULTS,
        ]
        assert event.fact_status == FactStatus.OK

    def test_global_event_roundtrip_serialization(self):
        """Test that GlobalEvent can be serialized and deserialized without data loss"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        job_anomaly_id = uuid.uuid4()

        original_event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-789",
            job_attempt_index=3,
            job_anomaly_id=job_anomaly_id,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.HEIMDALL,
            error_attribution_sources=[
                ErrorAttributionSource.APPLICATION_LOGS,
                ErrorAttributionSource.SYSLOGS,
            ],
            fact_status=FactStatus.OK,
        )

        # Serialize to JSON
        json_data = original_event.to_json()

        # Deserialize from JSON
        restored_event = GlobalEvent.from_json(json_data)

        # Verify all fields match
        assert restored_event.timestamp == original_event.timestamp
        assert restored_event.job_id == original_event.job_id
        assert restored_event.job_attempt_index == original_event.job_attempt_index
        assert restored_event.job_anomaly_id == original_event.job_anomaly_id
        assert restored_event.error_attribution == original_event.error_attribution
        assert restored_event.error_attribution_type == original_event.error_attribution_type
        assert restored_event.error_attribution_origin == original_event.error_attribution_origin
        assert restored_event.error_attribution_sources == original_event.error_attribution_sources
        assert restored_event.fact_status == original_event.fact_status

    def test_global_event_with_none_timestamps(self):
        """Test GlobalEvent with None for optional fields"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        event = GlobalEvent.create(
            timestamp=timestamp,
            job_id="test-job-999",
            job_attempt_index=0,
            job_anomaly_id=None,
        )

        json_data = event.to_json()
        assert json_data["job_anomaly_id"] is None

        restored_event = GlobalEvent.from_json(json_data)
        assert restored_event.job_anomaly_id is None
