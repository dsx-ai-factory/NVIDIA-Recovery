# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from enum import Enum, StrEnum, auto
from typing import Annotated, Any

import sqlalchemy as sa
from pydantic import Field as PydanticField
from pydantic import model_validator
from recovery_libs.storage.type_decorators import TZDateTimeMicrosecondsType
from sqlmodel import Field as SQLModelField
from uuid6 import uuid7

from notifications.models.base import BaseModel
from notifications.models.database_types import utc_now

MAX_AUDIT_EVENTS_PER_PAGE = 100
_Int64 = Annotated[int, PydanticField(json_schema_extra={"format": "int64"})]


class AuditEventLevel(StrEnum):
    """Supported audit event levels."""

    RECOVERY = auto()
    OPERATIONAL = auto()


class AuditEventHumanSummaryGranularityClass(Enum):
    """Supported granularity levels for an audit-event summary."""

    COARSE = 1
    FINE = 11


class AuditEventHumanSummary(BaseModel):
    """Human-readable descriptions of the audit event."""

    title: str
    """Event title -- should be similar to the audit event type."""

    description: str
    """Concise description of the event."""

    granularity: AuditEventHumanSummaryGranularityClass
    """Granularity used to group events in human-facing views."""


class AuditEventBase(BaseModel):
    """Fields shared by submitted and returned audit events."""

    heimdall_job_id: str
    """NVIDIA Recovery's internal identifier for the job."""

    event_producer: str
    """The component that produced the event."""

    event_type: str
    """The type of event."""

    event_level: AuditEventLevel
    """The audit event level."""

    payload: dict[str, Any]
    """Event-specific structured data."""

    event_produced_at: _Int64
    """Producer emission time as Unix microseconds."""


class AuditEvent(AuditEventBase):
    """An audit event visible through the query API."""

    summary: AuditEventHumanSummary
    """Human-readable descriptions of the audit event."""


class ProducerAuditEvent(AuditEventBase):
    """An audit event submitted by an NVIDIA Recovery component."""

    idempotency_key: str
    """Producer key used to discard duplicate events."""


class ProducerAuditEventBatch(BaseModel):
    """A batch of audit events to ingest."""

    items: list[ProducerAuditEvent]


class AuditEventPartialSuccess(BaseModel):
    """Partial result; accepted equals submitted minus rejected."""

    rejected_events: int = PydanticField(ge=1)
    """Number of rejected events."""

    error_message: str
    """Human-readable explanation of why events were rejected."""


class AuditEventIngestionResponse(BaseModel):
    """Result of accepting an audit-event batch."""

    partial_success: AuditEventPartialSuccess | None = None
    """Present only when the server rejected some events."""


class AuditEventPageCursor(BaseModel):
    """Opaque cursors for navigating between audit event pages."""

    prev_cursor: str
    """Cursor for the previous page; empty on the first page."""

    next_cursor: str
    """Cursor for the next page, or an empty string at the end."""


class AuditEventPage(BaseModel):
    """One page of audit events."""

    cursor: AuditEventPageCursor
    """Opaque cursors for the adjacent pages."""

    items: list[AuditEvent]


class AuditEventQuery(BaseModel):
    """Supported audit event query filters."""

    event_level: AuditEventLevel | None = None
    """Audit event level to match exactly."""

    event_producer: str | None = None
    """Producer service to match."""

    event_type: str | None = None
    """Event type to match exactly."""

    min_event_produced_at: _Int64 | None = None
    """Minimum Unix-microsecond event timestamp, inclusive."""

    max_event_produced_at: _Int64 | None = None
    """Maximum Unix-microsecond event timestamp, inclusive."""

    limit: int = PydanticField(default=MAX_AUDIT_EVENTS_PER_PAGE, ge=1, le=MAX_AUDIT_EVENTS_PER_PAGE)
    """Maximum number of events to return."""

    sort_by: str = "event_produced_at:desc"
    """Producer time order: asc or desc."""

    cursor: str | None = None
    """Opaque cursor returned for an adjacent page."""

    heimdall_job_id: str | None = None
    """NVIDIA Recovery job ID; exclusive with job_id."""

    job_id: str | None = PydanticField(default=None, min_length=1, pattern=r"\S")
    """Scheduler job ID; omit attempt_index for all attempts."""

    attempt_index: int | None = PydanticField(default=None, ge=0)
    """Scheduler job attempt; requires job_id."""

    @model_validator(mode="after")
    def validate_query(self):
        if self.sort_by not in {"event_produced_at:asc", "event_produced_at:desc"}:
            raise ValueError("'sort_by' must be 'event_produced_at:asc' or 'event_produced_at:desc'.")

        if (
            self.min_event_produced_at is not None
            and self.max_event_produced_at is not None
            and self.min_event_produced_at > self.max_event_produced_at
        ):
            raise ValueError("'min_event_produced_at' must not be greater than 'max_event_produced_at'.")

        if self.attempt_index is not None and self.job_id is None:
            raise ValueError("'attempt_index' requires 'job_id'.")
        if self.heimdall_job_id is not None and self.job_id is not None:
            raise ValueError("'heimdall_job_id' cannot be combined with 'job_id' and 'attempt_index'.")
        return self


class AuditEventOrm(BaseModel, table=True):
    """Append-only database representation of an audit event."""

    __tablename__ = "AuditEvents"
    __table_args__ = (
        sa.UniqueConstraint("idempotency_key", name="uq_AuditEvents_idempotency_key"),
        sa.Index("ix_AuditEvents_scheduler_job_time_id", "scheduler_job_id", "event_produced_at", "id"),
    )

    id: uuid.UUID = SQLModelField(default_factory=uuid7, primary_key=True)
    heimdall_job_id: str = SQLModelField(index=True)
    scheduler_job_id: str | None = SQLModelField(
        default=None,
        sa_column=sa.Column(
            sa.String(255).with_variant(sa.String(255, collation="utf8mb4_0900_bin"), "mysql"),
            nullable=True,
        ),
    )
    """Internal projection of a validated payload identity; absent on unmappable history."""
    event_producer: str = SQLModelField(index=True)
    event_type: str
    event_level: str = SQLModelField(
        sa_column=sa.Column(
            sa.Enum("recovery", "operational", name="audit_event_level"),
            index=True,
            nullable=False,
        )
    )
    payload: dict[str, Any] = SQLModelField(sa_column=sa.Column(sa.JSON(), nullable=False))
    idempotency_key: str = SQLModelField(sa_column=sa.Column(sa.String(255), nullable=False))
    recorded_at: datetime = SQLModelField(
        default_factory=utc_now,
        sa_column=sa.Column(TZDateTimeMicrosecondsType(), index=True, nullable=False),
    )
    event_produced_at: int = SQLModelField(sa_column=sa.Column(sa.BigInteger(), index=True, nullable=False))
