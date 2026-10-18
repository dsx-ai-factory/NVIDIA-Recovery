# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from pydantic import ValidationError
from recovery_libs.audit import AuditEventLevel
from recovery_libs.schema.event_type import EventType
from watcher.models.audit import WatcherAnomalyAuditEvent, WatcherAnomalyAuditPayload


def test_watcher_anomaly_audit_event_serializes_for_delivery() -> None:
    event = WatcherAnomalyAuditEvent(
        heimdall_job_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
        event_level=AuditEventLevel.OPERATIONAL,
        payload=WatcherAnomalyAuditPayload(
            job_id="12345",
            attempt_index=2,
            anomaly_type=EventType.CRASH,
            anomaly_uuid="019c8cab-e1cc-7a79-95ac-5de8d3e41000",
            workflow_step="crash_action",
            event_occurred_at=1_755_899_000,
        ),
        event_produced_at=1_755_900_000,
        idempotency_key="019c8cab-e1cc-7a79-95ac-5de8d3e41000",
    )

    assert event.model_dump(mode="json") == {
        "heimdall_job_id": "018f89d8-7550-7df1-a59b-5a5a20d80ef6",
        "event_producer": "watcher",
        "event_type": "anomaly_detected",
        "event_level": "operational",
        "payload": {
            "job_id": "12345",
            "attempt_index": 2,
            "anomaly_type": "CRASH",
            "anomaly_uuid": "019c8cab-e1cc-7a79-95ac-5de8d3e41000",
            "workflow_step": "crash_action",
            "event_occurred_at": 1_755_899_000,
        },
        "event_produced_at": 1_755_900_000,
        "idempotency_key": "019c8cab-e1cc-7a79-95ac-5de8d3e41000",
    }


@pytest.mark.parametrize("anomaly_type", EventType)
def test_watcher_anomaly_audit_payload_accepts_event_types(anomaly_type: EventType) -> None:
    payload = WatcherAnomalyAuditPayload(
        job_id="12345",
        attempt_index=0,
        anomaly_type=anomaly_type,
        anomaly_uuid="anomaly-1",
        event_occurred_at=1,
    )

    assert payload.anomaly_type is anomaly_type


@pytest.mark.parametrize("attempt_index", [-1, True, 1.5, "1"])
def test_watcher_anomaly_audit_payload_rejects_invalid_attempt_index(attempt_index: object) -> None:
    with pytest.raises(ValidationError):
        WatcherAnomalyAuditPayload(
            job_id="12345",
            attempt_index=attempt_index,
            anomaly_type=EventType.CRASH,
            anomaly_uuid="anomaly-1",
            event_occurred_at=1,
        )


def test_watcher_anomaly_audit_payload_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        WatcherAnomalyAuditPayload(
            job_id="12345",
            attempt_index=0,
            anomaly_type=EventType.CRASH,
            anomaly_uuid="anomaly-1",
            event_occurred_at=1,
            raw_log="do not persist raw log content",
        )


@pytest.mark.parametrize("event_occurred_at", [-1, True, 1.5, "1"])
def test_watcher_anomaly_audit_payload_rejects_invalid_occurrence_time(event_occurred_at: object) -> None:
    with pytest.raises(ValidationError):
        WatcherAnomalyAuditPayload(
            job_id="12345",
            attempt_index=0,
            anomaly_type=EventType.CRASH,
            anomaly_uuid="anomaly-1",
            event_occurred_at=event_occurred_at,
        )
