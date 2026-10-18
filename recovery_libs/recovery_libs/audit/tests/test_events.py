# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid

from recovery_libs.audit import AuditEventLevel, ProducerEvent


def _event() -> ProducerEvent:
    return ProducerEvent(
        heimdall_job_id="job-1",
        event_producer="job-manager",
        event_type="job-requeued",
        event_level=AuditEventLevel.OPERATIONAL,
        payload={"reason": "recovery"},
        event_produced_at=1_700_000_000_123_456,
    )


def test_producer_event_defaults_to_unique_uuid4_idempotency_keys() -> None:
    first = _event()
    second = _event()

    assert uuid.UUID(first.idempotency_key).version == 4
    assert uuid.UUID(second.idempotency_key).version == 4
    assert first.idempotency_key != second.idempotency_key
