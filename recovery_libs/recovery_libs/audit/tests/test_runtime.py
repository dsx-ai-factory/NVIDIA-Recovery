# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest
from pydantic import ValidationError
from recovery_libs.audit import (
    AuditBuffer,
    AuditEventBuffer,
    AuditTrailSettings,
    LoggingSender,
    NotificationSender,
    ProducerEvent,
    create_audit_buffer,
)


def _create_buffer(settings: AuditTrailSettings) -> AuditBuffer[ProducerEvent]:
    return create_audit_buffer(
        settings,
        logger=logging.getLogger("test.audit.buffer"),
    )


def test_default_audit_buffer_delivers_events_to_logging(caplog: pytest.LogCaptureFixture) -> None:
    buffer = _create_buffer(AuditTrailSettings())
    event = ProducerEvent(
        heimdall_job_id="job-123",
        event_producer="test",
        event_type="job.started",
        event_level="operational",
        payload={},
        event_produced_at=123456,
    )

    with caplog.at_level(logging.INFO, logger="test.audit.buffer"):
        buffer.start()
        try:
            buffer.add(event)
        finally:
            buffer.stop()

    assert len(caplog.records) == 1
    assert caplog.records[0].heimdall_job_id == event.heimdall_job_id
    assert caplog.records[0].idempotency_key == event.idempotency_key


def test_audit_buffer_uses_logging_destination_and_buffer_settings() -> None:
    buffer = _create_buffer(
        AuditTrailSettings(
            max_batch_size=7,
            max_buffer_size=13,
            flush_interval_seconds=2.5,
        )
    )

    assert isinstance(buffer, AuditEventBuffer)
    assert isinstance(buffer.sender.__self__, LoggingSender)
    assert buffer.max_batch_size == 7
    assert buffer.max_buffer_size == 13
    assert buffer.flush_interval_seconds == 2.5


def test_audit_buffer_uses_notification_destination() -> None:
    buffer = _create_buffer(
        AuditTrailSettings(
            destination="http://heimdall-notifications:8080/v1/audit/events",
        )
    )

    assert isinstance(buffer, AuditEventBuffer)
    assert isinstance(buffer.sender.__self__, NotificationSender)


def test_notification_destination_accepts_backend_batch_limit() -> None:
    settings = AuditTrailSettings(
        destination="http://heimdall-notifications:8080/v1/audit/events",
        max_batch_size=1000,
    )

    assert settings.max_batch_size == 1000


def test_notification_destination_rejects_batch_over_backend_limit() -> None:
    with pytest.raises(ValidationError, match="max_batch_size must not exceed 1000 when destination is configured"):
        AuditTrailSettings(
            destination="http://heimdall-notifications:8080/v1/audit/events",
            max_batch_size=1001,
        )


def test_logging_destination_allows_batch_over_notification_limit() -> None:
    settings = AuditTrailSettings(max_batch_size=1001)

    assert settings.max_batch_size == 1001
