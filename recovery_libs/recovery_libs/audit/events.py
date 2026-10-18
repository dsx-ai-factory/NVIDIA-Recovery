# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from enum import StrEnum

from pydantic import BaseModel, Field


class AuditEventLevel(StrEnum):
    """Categories of audit events."""

    RECOVERY = "recovery"
    OPERATIONAL = "operational"


class ProducerEvent[EventPayload](BaseModel, frozen=True):
    """An audit event emitted by a Heimdall service."""

    heimdall_job_id: str
    event_producer: str
    event_type: str
    event_level: AuditEventLevel
    payload: EventPayload
    event_produced_at: int = Field(description="Producer emission time as Unix microseconds.")
    idempotency_key: str = Field(default_factory=lambda: str(uuid.uuid4()))
