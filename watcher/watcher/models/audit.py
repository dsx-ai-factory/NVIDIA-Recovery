# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from recovery_libs.audit import ProducerEvent
from recovery_libs.schema.event_type import EventType


class WatcherAnomalyAuditPayload(BaseModel):
    """Watcher-specific details for an anomaly-detected audit event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str = Field(min_length=1)
    """Scheduler job identifier associated with the anomaly."""
    attempt_index: int = Field(ge=0, strict=True)
    """Scheduler attempt index associated with the anomaly."""
    anomaly_type: EventType
    anomaly_uuid: str = Field(min_length=1)
    workflow_step: str | None = Field(default=None, min_length=1)
    event_occurred_at: int = Field(ge=0, strict=True)
    """Source event time as Unix microseconds, before Watcher processing delay."""


class WatcherAnomalyAuditEvent(ProducerEvent[WatcherAnomalyAuditPayload], frozen=True):
    """Audit event produced when Watcher confirms an anomaly detection."""

    event_producer: Literal["watcher"] = "watcher"
    event_type: Literal["anomaly_detected"] = "anomaly_detected"
