# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import operator
import uuid
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Any

import sqlalchemy as sa
from fastapi import APIRouter, HTTPException, Query, status
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, col, select

from notifications.audit.identity import scheduler_job_id_from_payload
from notifications.audit.summary.summary import summarize_audit_event
from notifications.dependencies import SessionDep
from notifications.models.audit_events import (
    AuditEvent,
    AuditEventBase,
    AuditEventIngestionResponse,
    AuditEventLevel,
    AuditEventOrm,
    AuditEventPage,
    AuditEventPageCursor,
    AuditEventPartialSuccess,
    AuditEventQuery,
    ProducerAuditEvent,
    ProducerAuditEventBatch,
)
from notifications.models.database_types import utc_now

MAX_AUDIT_EVENTS_PER_BATCH = 1000


class AuditEventPersistenceOverloadedError(Exception):
    """Raised when the database rejects a batch before accepting it."""


class InvalidAuditEventCursorError(ValueError):
    """Raised when a query cursor does not identify an audit event."""


@dataclass(frozen=True)
class AuditEventIngestionResult:
    """Outcome of accepting an audit-event batch."""

    rejected_events: int = 0
    error_message: str = ""

    def __post_init__(self) -> None:
        if self.rejected_events < 0:
            raise ValueError("rejected_events must not be negative")


router = APIRouter(
    prefix="/v1/audit",
    tags=["audit"],
)


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a batch of audit events",
    response_model_exclude_none=True,
    response_description="The batch acceptance result.",
    responses={
        status.HTTP_413_CONTENT_TOO_LARGE: {"description": "The batch contains too many events."},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "The overloaded audit event store rejected the batch."},
    },
)
async def audit_events_create(
    event_batch: ProducerAuditEventBatch,
    session: SessionDep,
) -> AuditEventIngestionResponse:
    if len(event_batch.items) > MAX_AUDIT_EVENTS_PER_BATCH:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"A batch may contain at most {MAX_AUDIT_EVENTS_PER_BATCH} events.",
        )

    try:
        result = insert_audit_events(session, event_batch.items)
    except AuditEventPersistenceOverloadedError as error:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="The audit event store is overloaded; retry the entire batch.",
        ) from error

    if result.rejected_events > len(event_batch.items):
        raise RuntimeError("audit event store rejected more events than were submitted")
    if result.rejected_events == len(event_batch.items) and event_batch.items:
        raise RuntimeError("audit event store reported complete rejection as partial success")
    if result.rejected_events == 0:
        return AuditEventIngestionResponse()
    return AuditEventIngestionResponse(
        partial_success=AuditEventPartialSuccess(
            rejected_events=result.rejected_events,
            error_message=result.error_message,
        )
    )


@router.get(
    "/events",
    summary="Query audit events",
)
async def audit_events_get(
    session: SessionDep,
    filter_query: Annotated[AuditEventQuery, Query()],
) -> AuditEventPage:
    try:
        return query_audit_events(session, filter_query)
    except InvalidAuditEventCursorError as error:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error


def insert_audit_events(
    session: Session,
    events: Sequence[ProducerAuditEvent],
) -> AuditEventIngestionResult:
    """Persist events idempotently and report row-level database rejections."""
    if not events:
        return AuditEventIngestionResult()

    recorded_at = utc_now()
    rows = [
        {
            "heimdall_job_id": event.heimdall_job_id,
            "scheduler_job_id": scheduler_job_id_from_payload(event.heimdall_job_id, event.payload),
            "event_producer": event.event_producer,
            "event_type": event.event_type,
            "event_level": event.event_level,
            "payload": event.payload,
            "idempotency_key": event.idempotency_key,
            "recorded_at": recorded_at + timedelta(microseconds=index),
            "event_produced_at": event.event_produced_at,
        }
        for index, event in enumerate(events)
    ]

    try:
        session.execute(_idempotent_insert(session, rows))
        session.commit()
    except sa.exc.TimeoutError as error:
        session.rollback()
        raise AuditEventPersistenceOverloadedError from error
    except (sa.exc.DataError, sa.exc.IntegrityError) as error:
        # Isolate row-level rejections only after the atomic batch statement has rolled back.
        session.rollback()
        return _insert_rows_with_savepoints(session, rows, error)
    except Exception:
        session.rollback()
        raise
    return AuditEventIngestionResult()


def query_audit_events(session: Session, query: AuditEventQuery) -> AuditEventPage:
    """Return one stable page matching the query endpoint's filters and cursors."""
    statement = select(AuditEventOrm)
    if query.event_level is not None:
        statement = statement.where(AuditEventOrm.event_level == query.event_level)
    if query.event_producer is not None:
        statement = statement.where(AuditEventOrm.event_producer == query.event_producer)
    if query.event_type is not None:
        if session.get_bind().dialect.name == "mysql":
            # Helm defaults to utf8mb3; convert before applying a utf8mb4 collation.
            # Binary NO PAD comparison preserves case, accents, and trailing spaces.
            event_type = sa.cast(col(AuditEventOrm.event_type), CHAR(charset="utf8mb4"))
            statement = statement.where(event_type.collate("utf8mb4_0900_bin") == query.event_type)
        else:
            statement = statement.where(AuditEventOrm.event_type == query.event_type)
    if query.min_event_produced_at is not None:
        statement = statement.where(AuditEventOrm.event_produced_at >= query.min_event_produced_at)
    if query.max_event_produced_at is not None:
        statement = statement.where(AuditEventOrm.event_produced_at <= query.max_event_produced_at)
    heimdall_job_id = query.heimdall_job_id
    if query.job_id is not None:
        if query.attempt_index is None:
            statement = statement.where(AuditEventOrm.scheduler_job_id == query.job_id)
        else:
            heimdall_job_id = str(generate_heimdall_job_id(query.job_id, query.attempt_index))
    if heimdall_job_id is not None:
        # Keep exact-attempt access to historical rows without a scheduler projection.
        statement = statement.where(AuditEventOrm.heimdall_job_id == heimdall_job_id)

    descending = query.sort_by == "event_produced_at:desc"
    before = operator.gt if descending else operator.lt
    after = operator.lt if descending else operator.gt
    at_or_after = operator.le if descending else operator.ge
    order = sa.desc if descending else sa.asc
    reverse_order = sa.asc if descending else sa.desc

    cursor_event = _cursor_event(session, query.cursor)
    prev_cursor = ""
    if cursor_event is not None:
        # A cursor identifies the first row of its page. Select the prior
        # page in reverse order, then return that page's first row.
        previous_rows = list(
            session.exec(
                statement.where(
                    sa.or_(
                        before(col(AuditEventOrm.event_produced_at), cursor_event.event_produced_at),
                        sa.and_(
                            col(AuditEventOrm.event_produced_at) == cursor_event.event_produced_at,
                            before(col(AuditEventOrm.id), cursor_event.id),
                        ),
                    )
                )
                .order_by(reverse_order(col(AuditEventOrm.event_produced_at)), reverse_order(col(AuditEventOrm.id)))
                .limit(query.limit)
            )
        )
        if previous_rows:
            prev_cursor = str(previous_rows[-1].id)
        statement = statement.where(
            sa.or_(
                after(col(AuditEventOrm.event_produced_at), cursor_event.event_produced_at),
                sa.and_(
                    col(AuditEventOrm.event_produced_at) == cursor_event.event_produced_at,
                    at_or_after(col(AuditEventOrm.id), cursor_event.id),
                ),
            )
        )

    rows = list(
        session.exec(
            statement.order_by(order(col(AuditEventOrm.event_produced_at)), order(col(AuditEventOrm.id))).limit(
                query.limit + 1
            )
        )
    )
    has_next_page = len(rows) > query.limit
    page = rows[: query.limit]
    next_cursor = str(rows[query.limit].id) if has_next_page else ""
    return AuditEventPage(
        cursor=AuditEventPageCursor(prev_cursor=prev_cursor, next_cursor=next_cursor),
        items=[_public_event(row) for row in page],
    )


def _cursor_event(session: Session, cursor: str | None) -> AuditEventOrm | None:
    if not cursor:
        return None
    try:
        cursor_id = uuid.UUID(cursor)
    except ValueError as error:
        raise InvalidAuditEventCursorError("invalid audit event cursor") from error

    cursor_event = session.get(AuditEventOrm, cursor_id)
    if cursor_event is None:
        raise InvalidAuditEventCursorError("invalid audit event cursor")
    return cursor_event


def _idempotent_insert(session: Session, rows: list[dict[str, Any]]):
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "mysql":
        statement = mysql_insert(AuditEventOrm).values(rows)
        # MySQL has no targeted DO NOTHING clause. This self-assignment ignores
        # duplicate keys without INSERT IGNORE suppressing unrelated data errors.
        return statement.on_duplicate_key_update(idempotency_key=AuditEventOrm.idempotency_key)
    if dialect_name == "sqlite":
        return sqlite_insert(AuditEventOrm).values(rows).on_conflict_do_nothing(index_elements=["idempotency_key"])
    raise RuntimeError(f"unsupported audit event database dialect: {dialect_name}")


def _insert_rows_with_savepoints(
    session: Session,
    rows: list[dict[str, Any]],
    batch_error: sa.exc.DataError | sa.exc.IntegrityError,
) -> AuditEventIngestionResult:
    """Retry rows individually after the single-statement batch fast path fails.

    Each retry gets its own savepoint so a rejected row can roll back without
    discarding earlier successful rows. Successful rows remain uncommitted
    until the outer transaction commits after every retry; if every row fails,
    the original batch error is raised and the outer transaction rolls back.
    """
    rejected_events = 0
    try:
        with session.begin():
            for row in rows:
                try:
                    with session.begin_nested():
                        session.execute(_idempotent_insert(session, [row]))
                except (sa.exc.DataError, sa.exc.IntegrityError):
                    rejected_events += 1

            if rejected_events == len(rows):
                raise batch_error
    except sa.exc.TimeoutError as error:
        session.rollback()
        raise AuditEventPersistenceOverloadedError from error
    except Exception:
        session.rollback()
        raise

    if rejected_events == 0:
        return AuditEventIngestionResult()

    event_noun = "event" if rejected_events == 1 else "events"
    return AuditEventIngestionResult(
        rejected_events=rejected_events,
        error_message=f"The database rejected {rejected_events} audit {event_noun}.",
    )


def _public_event(row: AuditEventOrm) -> AuditEvent:
    event = AuditEventBase(
        heimdall_job_id=row.heimdall_job_id,
        event_producer=row.event_producer,
        event_type=row.event_type,
        event_level=AuditEventLevel(row.event_level),
        payload=deepcopy(row.payload),
        event_produced_at=row.event_produced_at,
    )
    return AuditEvent(
        **event.model_dump(),
        summary=summarize_audit_event(event),
    )
