# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid

import pytest
import sqlalchemy as sa
from notifications.audit.backfill_scheduler_job_id import backfill_scheduler_job_ids
from notifications.audit.identity import scheduler_job_id_from_payload
from notifications.models.audit_events import AuditEventOrm, AuditEventQuery, ProducerAuditEvent
from notifications.routers.audit_events import (
    AuditEventIngestionResult,
    AuditEventPersistenceOverloadedError,
    InvalidAuditEventCursorError,
    insert_audit_events,
    query_audit_events,
)
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from sqlalchemy.dialects import mysql
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select


def _producer_event(
    *,
    heimdall_job_id: str = "job-1",
    event_producer: str = "slurm-advisor-controller",
    event_level: str = "recovery",
    event_produced_at: int = 1_700_000_000_123_456,
    idempotency_key: str | None = None,
) -> ProducerAuditEvent:
    return ProducerAuditEvent(
        heimdall_job_id=heimdall_job_id,
        event_producer=event_producer,
        event_type="advice_scheduler_action_completed",
        event_level=event_level,
        payload={"action": "release", "success": True},
        idempotency_key=idempotency_key or str(uuid.uuid4()),
        event_produced_at=event_produced_at,
    )


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite://",
        connect_args={"autocommit": False, "check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def test_schema_uses_api_event_level_and_storage_types() -> None:
    event_level = AuditEventOrm.__table__.columns["event_level"]
    payload = AuditEventOrm.__table__.columns["payload"]
    event_produced_at = AuditEventOrm.__table__.columns["event_produced_at"]

    assert isinstance(event_level.type, sa.Enum)
    assert event_level.type.name == "audit_event_level"
    assert event_level.type.enums == ["recovery", "operational"]
    assert isinstance(payload.type, sa.JSON)
    assert isinstance(event_produced_at.type, sa.BigInteger)


def test_insert_accepts_an_empty_batch(session: Session) -> None:
    assert insert_audit_events(session, []) == AuditEventIngestionResult()


def test_orm_and_bulk_insert_generate_uuid7_ids(session: Session) -> None:
    orm_event = AuditEventOrm(
        heimdall_job_id="job-orm",
        event_producer="test",
        event_type="test",
        event_level="operational",
        payload={},
        idempotency_key="orm-event",
        event_produced_at=1,
    )
    assert orm_event.id.version == 7

    insert_audit_events(session, [_producer_event(), _producer_event()])

    stored_ids = session.exec(select(AuditEventOrm.id)).all()
    assert len(set(stored_ids)) == 2
    assert all(stored_id.version == 7 for stored_id in stored_ids)


def test_insert_is_idempotent_and_preserves_the_first_event(session: Session) -> None:
    event = _producer_event()
    conflicting_duplicate = event.model_copy(
        update={
            "event_type": "different_event_type",
            "payload": {"action": "cancel", "success": False},
            "event_produced_at": event.event_produced_at + 1,
        }
    )

    assert insert_audit_events(session, [event, conflicting_duplicate]) == AuditEventIngestionResult()
    assert insert_audit_events(session, [conflicting_duplicate]) == AuditEventIngestionResult()

    page = query_audit_events(session, AuditEventQuery(heimdall_job_id=event.heimdall_job_id))
    assert page.cursor.prev_cursor == ""
    assert page.cursor.next_cursor == ""
    assert [item.model_dump(mode="json") for item in page.items] == [
        {
            **event.model_dump(mode="json", exclude={"idempotency_key"}),
            "summary": {
                "title": "Scheduler action performed",
                "description": "Action release performed successfully",
                "granularity": 1,
            },
        }
    ]


def test_mysql_insert_uses_a_narrow_duplicate_key_noop(mocker) -> None:
    session = mocker.MagicMock(spec=Session)
    session.get_bind.return_value.dialect.name = "mysql"

    assert insert_audit_events(session, [_producer_event()]) == AuditEventIngestionResult()

    statement = session.execute.call_args.args[0]
    sql = str(statement.compile(dialect=mysql.dialect()))
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "idempotency_key = `AuditEvents`.idempotency_key" in sql


def test_insert_partially_persists_rows_rejected_by_the_database(
    session: Session,
) -> None:
    session.execute(
        sa.text(
            """
            CREATE TRIGGER reject_audit_event
            BEFORE INSERT ON "AuditEvents"
            WHEN NEW.idempotency_key = 'reject-me'
            BEGIN
                SELECT RAISE(ABORT, 'rejected audit event');
            END
            """
        )
    )
    session.commit()
    events = [
        _producer_event(event_produced_at=100, idempotency_key="accepted-1"),
        _producer_event(event_produced_at=200, idempotency_key="reject-me"),
        _producer_event(event_produced_at=300, idempotency_key="accepted-2"),
    ]

    result = insert_audit_events(session, events)

    assert result == AuditEventIngestionResult(
        rejected_events=1,
        error_message="The database rejected 1 audit event.",
    )
    assert len(events) - result.rejected_events == 2
    page = query_audit_events(session, AuditEventQuery())
    assert [event.event_produced_at for event in page.items] == [300, 100]


def test_insert_fails_when_the_database_rejects_every_row(
    session: Session,
) -> None:
    session.execute(
        sa.text(
            """
            CREATE TRIGGER reject_every_audit_event
            BEFORE INSERT ON "AuditEvents"
            BEGIN
                SELECT RAISE(ABORT, 'rejected audit event');
            END
            """
        )
    )
    session.commit()

    with pytest.raises(sa.exc.IntegrityError):
        insert_audit_events(session, [_producer_event(), _producer_event()])

    assert query_audit_events(session, AuditEventQuery()).items == []


def test_insert_rolls_back_fallback_rows_if_the_store_times_out(
    session: Session,
    mocker,
) -> None:
    original_execute = session.execute
    execute_count = 0

    def execute_with_timeout(statement, *args, **kwargs):
        nonlocal execute_count
        execute_count += 1
        if execute_count == 1:
            raise sa.exc.IntegrityError("batch insert", {}, RuntimeError("row rejected"))
        if execute_count == 3:
            raise sa.exc.TimeoutError
        return original_execute(statement, *args, **kwargs)

    execute_mock = mocker.patch.object(session, "execute", side_effect=execute_with_timeout)

    with pytest.raises(AuditEventPersistenceOverloadedError):
        insert_audit_events(session, [_producer_event(), _producer_event()])

    mocker.stop(execute_mock)
    assert query_audit_events(session, AuditEventQuery()).items == []


def test_query_filters_and_paginates_with_a_stable_cursor(session: Session) -> None:
    # Insert out of producer-time order to prove recorded_at does not control pagination.
    matching_events = [_producer_event(event_produced_at=timestamp) for timestamp in (300, 100, 200)]
    insert_audit_events(
        session,
        [
            *matching_events,
            _producer_event(event_producer="job-manager", event_produced_at=200),
            _producer_event(heimdall_job_id="job-2", event_produced_at=200),
            _producer_event(event_level="operational", event_produced_at=200),
        ],
    )

    query = AuditEventQuery(
        event_level="recovery",
        event_producer="slurm-advisor-controller",
        min_event_produced_at=100,
        max_event_produced_at=300,
        sort_by="event_produced_at:asc",
        limit=2,
        heimdall_job_id="job-1",
    )
    first_page = query_audit_events(session, query)
    second_page = query_audit_events(session, query.model_copy(update={"cursor": first_page.cursor.next_cursor}))
    first_page_again = query_audit_events(session, query.model_copy(update={"cursor": second_page.cursor.prev_cursor}))

    assert first_page.cursor.prev_cursor == ""
    assert first_page.cursor.next_cursor
    assert [event.event_produced_at for event in first_page.items] == [100, 200]
    assert second_page.cursor.prev_cursor
    assert second_page.cursor.next_cursor == ""
    assert [event.event_produced_at for event in second_page.items] == [300]
    assert [event.event_produced_at for event in first_page_again.items] == [100, 200]


def test_query_resolves_scheduler_identity_to_indexed_heimdall_id(session: Session) -> None:
    job_id = "12345"
    attempt_index = 2
    matching_event = _producer_event(
        heimdall_job_id=str(generate_heimdall_job_id(job_id, attempt_index)),
        event_produced_at=100,
    )
    insert_audit_events(
        session,
        [
            matching_event,
            _producer_event(heimdall_job_id=str(generate_heimdall_job_id(job_id, 1)), event_produced_at=200),
        ],
    )

    page = query_audit_events(session, AuditEventQuery(job_id=job_id, attempt_index=attempt_index))

    assert [event.event_produced_at for event in page.items] == [100]
    assert matching_event.payload == {"action": "release", "success": True}


@pytest.mark.parametrize(
    "query",
    [
        {"attempt_index": 2},
        {"heimdall_job_id": "internal-id", "job_id": "12345"},
        {"heimdall_job_id": "internal-id", "attempt_index": 0},
        {"job_id": "", "attempt_index": 2},
        {"job_id": "   ", "attempt_index": 2},
        {"job_id": "12345", "attempt_index": -1},
        {"heimdall_job_id": "internal-id", "job_id": "12345", "attempt_index": 2},
    ],
)
def test_query_rejects_ambiguous_or_partial_job_identity(query: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="job_id|attempt_index|heimdall_job_id"):
        AuditEventQuery(**query)


def test_backfill_is_bounded_resumable_and_keeps_unmappable_history(session: Session) -> None:
    rows = [
        AuditEventOrm(
            **_producer_event(
                heimdall_job_id=str(generate_heimdall_job_id("historical", attempt)),
                event_produced_at=attempt,
            ).model_dump(exclude={"payload"}),
            payload={"job_id": "historical", "attempt_index": attempt} if attempt != 1 else {},
        )
        for attempt in range(5)
    ]
    session.add_all(rows)
    session.commit()
    for row in rows:
        session.refresh(row)
    original_events = [row.model_dump(exclude={"scheduler_job_id"}) for row in rows]

    first = backfill_scheduler_job_ids(session, batch_size=2)
    assert (first.scanned, first.updated, first.unmappable_ids) == (2, 1, [rows[1].id])
    assert first.next_after_id == rows[1].id
    assert first.through_id == rows[-1].id
    second = backfill_scheduler_job_ids(
        session, batch_size=2, after_id=first.next_after_id, through_id=first.through_id
    )
    assert (second.scanned, second.updated, second.unmappable_ids) == (2, 2, [])
    final = backfill_scheduler_job_ids(
        session, batch_size=2, after_id=second.next_after_id, through_id=second.through_id
    )
    assert (final.scanned, final.updated, final.next_after_id) == (1, 1, None)
    for row in rows:
        session.refresh(row)
    assert [row.model_dump(exclude={"scheduler_job_id"}) for row in rows] == original_events

    replay = backfill_scheduler_job_ids(session)
    assert (replay.scanned, replay.updated, replay.unmappable_ids) == (5, 0, [rows[1].id])
    assert len(query_audit_events(session, AuditEventQuery(job_id="historical")).items) == 4
    assert len(query_audit_events(session, AuditEventQuery(job_id="historical", attempt_index=1)).items) == 1


@pytest.mark.parametrize("batch_size", [0, 1001])
def test_backfill_rejects_unbounded_batches(session: Session, batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        backfill_scheduler_job_ids(session, batch_size=batch_size)


def test_backfill_handles_empty_history(session: Session) -> None:
    result = backfill_scheduler_job_ids(session)
    assert (result.scanned, result.updated, result.through_id, result.next_after_id) == (0, 0, None, None)


@pytest.mark.parametrize("payload", [None, [], "invalid"])
def test_legacy_non_object_payload_is_unmappable(payload) -> None:
    assert scheduler_job_id_from_payload(str(generate_heimdall_job_id("12345", 0)), payload) is None


@pytest.mark.parametrize("cursor", ["not-a-cursor", str(uuid.uuid4())])
def test_query_rejects_unknown_cursors(session: Session, cursor: str) -> None:
    with pytest.raises(InvalidAuditEventCursorError, match="invalid audit event cursor"):
        query_audit_events(session, AuditEventQuery(cursor=cursor))


def test_insert_reports_session_pool_timeout_as_overload(mocker) -> None:
    session = mocker.MagicMock(spec=Session)
    session.get_bind.return_value.dialect.name = "mysql"
    session.execute.side_effect = sa.exc.TimeoutError
    with pytest.raises(AuditEventPersistenceOverloadedError):
        insert_audit_events(session, [_producer_event()])

    session.rollback.assert_called_once_with()
