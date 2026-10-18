# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from notifications.audit.backfill_scheduler_job_id import backfill_scheduler_job_ids
from notifications.models.audit_events import AuditEventOrm, AuditEventQuery, ProducerAuditEvent
from notifications.models.notification_types import NotificationType
from notifications.models.notifications import NotificationOrm
from notifications.routers.audit_events import insert_audit_events, query_audit_events
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage import dataclass as storage_dataclass
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import URL, Engine, make_url
from sqlmodel import Session, select
from uuid6 import uuid7

from notifications import database as notifications_database

STORAGE_ALEMBIC_ROOT = Path(storage_dataclass.__file__).parent / "alembic"
NOTIFICATIONS_ALEMBIC_ROOT = Path(notifications_database.__file__).parent / "_migrations"
STORAGE_TABLES = {
    "advice",
    "advice_reconciliation_status",
    "dag_dependency",
    "global_events",
    "job_events",
    "job_execution_errors",
    "job_execution_records",
    "key_value_cache",
    "node_events",
    "recovery_milestones",
    "recovery_sequences",
}
NOTIFICATIONS_TABLES = {"AuditEvents", "Notifications", "UserPreferences"}
VERSION_TABLES = {"alembic_version", "alembic_versions__notifications"}
ALL_TABLES = STORAGE_TABLES | NOTIFICATIONS_TABLES | VERSION_TABLES


@pytest.fixture
def database_engine(disposable_database: Engine) -> Engine:
    """Notifications caches its own engine handle in a module-level global when a test
    sets it; reset that global on teardown so it doesn't leak into the next test."""
    try:
        yield disposable_database
    finally:
        notifications_database._engine = None


def _config(root: Path) -> Config:
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root))
    config.set_main_option("sqlalchemy.url", os.environ["DATABASE_URL"].replace("%", "%%"))
    if root == STORAGE_ALEMBIC_ROOT:
        config.attributes["configure_logger"] = False
    return config


def _upgrade(owner: str) -> None:
    command.upgrade(_config(STORAGE_ALEMBIC_ROOT if owner == "storage" else NOTIFICATIONS_ALEMBIC_ROOT), "head")


def _downgrade(owner: str) -> None:
    command.downgrade(_config(STORAGE_ALEMBIC_ROOT if owner == "storage" else NOTIFICATIONS_ALEMBIC_ROOT), "base")


@pytest.mark.parametrize("owner_order", [("storage", "notifications"), ("notifications", "storage")])
@pytest.mark.parametrize("downgrade_owner", ["storage", "notifications"])
def test_combined_baselines_are_independent_and_drift_free(
    database_engine: Engine, owner_order: tuple[str, str], downgrade_owner: str
) -> None:
    for owner in owner_order:
        _upgrade(owner)

    assert set(inspect(database_engine).get_table_names()) == ALL_TABLES
    audit_columns = {column["name"]: column for column in inspect(database_engine).get_columns("AuditEvents")}
    assert isinstance(audit_columns["event_level"]["type"], mysql.ENUM)
    assert audit_columns["event_level"]["type"].enums == ["recovery", "operational"]
    assert isinstance(audit_columns["payload"]["type"], mysql.JSON)
    assert isinstance(audit_columns["event_produced_at"]["type"], mysql.BIGINT)
    for root in (STORAGE_ALEMBIC_ROOT, NOTIFICATIONS_ALEMBIC_ROOT):
        config = _config(root)
        command.check(config)
        command.current(config, check_heads=True)

    _downgrade(downgrade_owner)
    remaining_owner_tables = NOTIFICATIONS_TABLES if downgrade_owner == "storage" else STORAGE_TABLES
    assert remaining_owner_tables.issubset(inspect(database_engine).get_table_names())

    _upgrade(downgrade_owner)
    assert set(inspect(database_engine).get_table_names()) == ALL_TABLES


def test_representative_storage_and_notifications_crud(database_engine: Engine) -> None:
    _upgrade("storage")
    _upgrade("notifications")
    url = make_url(os.environ["DATABASE_URL"])
    assert url.host is not None and url.port is not None and url.username is not None and url.database is not None

    storage_connection = MySQLConnection(
        host=url.host,
        port=url.port,
        username=url.username,
        password=url.password or "",
        database_name=url.database,
    )
    storage = MySQLStorage(storage_connection)
    try:
        record = SlurmJobExecutionRecord(
            job_id="baseline-job",
            job_attempt_index=0,
            job_name="train",
            workflow_id="baseline-workflow",
            last_updated_at=datetime(2026, 8, 7, tzinfo=UTC),
        )
        assert storage.upsert_job_record(record)[1] is True
        assert storage.read_job_record("baseline-job", 0).workflow_id == "baseline-workflow"
    finally:
        storage_connection.engine.dispose()

    notification = NotificationOrm(
        recipient="baseline-user",
        title="Baseline ready",
        message="Both schema owners are current.",
        type=NotificationType.info,
    )
    with Session(database_engine) as session:
        session.add(notification)
        session.commit()
        session.refresh(notification)
        assert session.exec(select(NotificationOrm).where(NotificationOrm.id == notification.id)).one().recipient == (
            "baseline-user"
        )

        audit_event = ProducerAuditEvent(
            heimdall_job_id="baseline-job",
            event_producer="slurm-advisor-controller",
            event_type="advice_scheduler_action_completed",
            event_level="recovery",
            payload={"action": "release", "success": True},
            idempotency_key="baseline-audit-event",
            event_produced_at=1_786_089_600_123_456,
        )
        conflicting_duplicate = audit_event.model_copy(
            update={
                "event_type": "different_event_type",
                "payload": {"action": "cancel", "success": False},
                "event_produced_at": audit_event.event_produced_at + 1,
            }
        )
        insert_audit_events(session, [audit_event, conflicting_duplicate])
        insert_audit_events(session, [conflicting_duplicate])
        page = query_audit_events(session, AuditEventQuery(heimdall_job_id="baseline-job"))
        assert page.cursor.prev_cursor == ""
        assert page.cursor.next_cursor == ""
        assert len(page.items) == 1
        assert page.items[0].event_type == "advice_scheduler_action_completed"
        assert page.items[0].payload == {"action": "release", "success": True}


def test_notifications_runtime_bootstrap_upgrades_empty_database_and_is_idempotent(database_engine: Engine) -> None:
    notifications_database._engine = database_engine

    notifications_database.run_migrations()
    notifications_database.run_migrations()

    assert set(inspect(database_engine).get_table_names()) == NOTIFICATIONS_TABLES | {"alembic_versions__notifications"}


def test_audit_scheduler_migration_backfill_and_mixed_writers(database_engine: Engine) -> None:
    """Old inserts survive expansion, final catch-up covers late commits, and filters are exact on MySQL."""
    command.upgrade(_config(NOTIFICATIONS_ALEMBIC_ROOT), "000000000003")
    old_table = Table("AuditEvents", MetaData(), autoload_with=database_engine)
    delayed_id = uuid7()

    def legacy_row(attempt: int, *, valid: bool = True, row_id: uuid.UUID | None = None) -> dict:
        return {
            "id": (row_id or uuid7()).hex,
            "heimdall_job_id": str(generate_heimdall_job_id("job-a", attempt)),
            "event_producer": "job-manager",
            "event_type": "selected",
            "event_level": "recovery",
            "payload": {"job_id": "job-a", "attempt_index": attempt} if valid else {"legacy": "retained"},
            "idempotency_key": str(uuid.uuid4()),
            "recorded_at": datetime(2026, 10, 6, tzinfo=UTC),
            "event_produced_at": 100,
        }

    old_rows = [legacy_row(0), legacy_row(1), legacy_row(99, valid=False)]
    with database_engine.begin() as connection:
        connection.execute(old_table.insert(), old_rows)
    _upgrade("notifications")
    indexes = {index["name"]: index["column_names"] for index in inspect(database_engine).get_indexes("AuditEvents")}
    assert indexes["ix_AuditEvents_scheduler_job_time_id"] == ["scheduler_job_id", "event_produced_at", "id"]
    assert indexes["ix_AuditEvents_heimdall_job_id"] == ["heimdall_job_id"]
    columns = {column["name"]: column for column in inspect(database_engine).get_columns("AuditEvents")}
    assert columns["scheduler_job_id"]["nullable"] is True

    with Session(database_engine) as session:
        before = {row.id: row.model_dump(exclude={"scheduler_job_id"}) for row in session.exec(select(AuditEventOrm))}
        events = [
            ProducerAuditEvent(
                heimdall_job_id=str(generate_heimdall_job_id(job_id, attempt)),
                event_producer="job-manager",
                event_type=event_type,
                event_level="recovery",
                payload={"job_id": job_id, "attempt_index": attempt},
                idempotency_key=str(uuid.uuid4()),
                event_produced_at=100,
            )
            for job_id, attempt, event_type in [
                ("job-a", 2, "selected"),
                ("job-a", 3, "selected"),
                ("Job-a", 0, "selected"),
                ("job-a ", 0, "selected"),
                ("job-a", 2, "Selected"),
                ("job-a", 2, "selected "),
            ]
        ]
        insert_audit_events(session, events)
        first = backfill_scheduler_job_ids(session, batch_size=2)
        assert first.updated == 2
        # This old writer allocated its ID before the scan, but commits behind its cursor.
        with database_engine.begin() as connection:
            connection.execute(old_table.insert(), legacy_row(4, row_id=delayed_id))
        cursor = first.next_after_id
        unmappable_ids = []
        while cursor is not None:
            result = backfill_scheduler_job_ids(session, batch_size=2, after_id=cursor, through_id=first.through_id)
            assert result.scanned <= 2
            unmappable_ids.extend(result.unmappable_ids)
            cursor = result.next_after_id
        assert unmappable_ids == [uuid.UUID(old_rows[-1]["id"])]
        assert session.get(AuditEventOrm, delayed_id).scheduler_job_id is None

        final = backfill_scheduler_job_ids(session)
        assert final.updated == 1
        assert final.unmappable_ids == unmappable_ids
        assert backfill_scheduler_job_ids(session).updated == 0
        for row_id, original in before.items():
            assert session.get(AuditEventOrm, row_id).model_dump(exclude={"scheduler_job_id"}) == original

        query = AuditEventQuery(job_id="job-a", event_type="selected", limit=2, sort_by="event_produced_at:asc")
        pages = []
        while True:
            page = query_audit_events(session, query)
            pages.append(page)
            if not page.cursor.next_cursor:
                break
            query.cursor = page.cursor.next_cursor
            assert len(pages) < 5
        assert [item.payload["attempt_index"] for page in pages for item in page.items] == [4, 0, 1, 2, 3]
        for index in range(len(pages) - 1, 0, -1):
            query.cursor = pages[index].cursor.prev_cursor
            assert query_audit_events(session, query) == pages[index - 1]
        legacy = query_audit_events(session, AuditEventQuery(job_id="job-a", attempt_index=99))
        assert [item.payload for item in legacy.items] == [{"legacy": "retained"}]

    command.check(_config(NOTIFICATIONS_ALEMBIC_ROOT))


@pytest.mark.regression
def test_event_type_filter_is_exact_on_utf8mb3_database(database_engine: Engine) -> None:
    with database_engine.begin() as connection:
        connection.execute(
            text(f"ALTER DATABASE `{database_engine.url.database}` CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci")
        )
    _upgrade("notifications")
    with database_engine.connect() as connection:
        charset = connection.execute(
            text(
                "SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'AuditEvents' AND COLUMN_NAME = 'event_type'"
            )
        ).scalar_one()
    assert charset == "utf8mb3"

    with Session(database_engine) as session:
        insert_audit_events(
            session,
            [
                ProducerAuditEvent(
                    heimdall_job_id=str(generate_heimdall_job_id("job-a", attempt)),
                    event_producer="job-manager",
                    event_type=event_type,
                    event_level="recovery",
                    payload={"job_id": "job-a", "attempt_index": attempt},
                    idempotency_key=str(uuid.uuid4()),
                    event_produced_at=100,
                )
                for attempt, event_type in enumerate(["selected", "Selected", "selected "])
            ],
        )
        page = query_audit_events(session, AuditEventQuery(job_id="job-a", event_type="selected"))

    assert [item.event_type for item in page.items] == ["selected"]


@pytest.mark.parametrize("owner", ["storage", "notifications"])
def test_unknown_revision_fails_without_changing_tables_or_data(database_engine: Engine, owner: str) -> None:
    _upgrade("storage")
    _upgrade("notifications")
    version_table = "alembic_version" if owner == "storage" else "alembic_versions__notifications"
    marker_table = "key_value_cache" if owner == "storage" else "Notifications"

    with database_engine.begin() as connection:
        if owner == "storage":
            connection.execute(
                text(
                    "INSERT INTO key_value_cache (`key`, value, timestamp, ttl_secs) "
                    "VALUES ('baseline-marker', 'retained', CURRENT_TIMESTAMP, 60)"
                )
            )
        else:
            connection.execute(
                text(
                    "INSERT INTO Notifications "
                    "(recipient, id, title, message, type, created_time, via_callback) "
                    "VALUES ('baseline-user', REPLACE(UUID(), '-', ''), 'marker', 'retained', 'info', "
                    "CURRENT_TIMESTAMP(6), 'DISABLED')"
                )
            )
        connection.execute(text(f"UPDATE `{version_table}` SET version_num = 'unsupported_revision'"))

    tables_before = set(inspect(database_engine).get_table_names())
    with database_engine.connect() as connection:
        rows_before = connection.execute(text(f"SELECT COUNT(*) FROM `{marker_table}`")).scalar_one()

    if owner == "storage":
        url: URL = make_url(os.environ["DATABASE_URL"])
        storage_connection = MySQLConnection(
            host=url.host or "",
            port=url.port or 3306,
            username=url.username or "",
            password=url.password or "",
            database_name=url.database or "",
        )
        try:
            with pytest.raises(RecoveryStorageException, match="fresh empty database/schema"):
                storage_connection.bootstrap()
        finally:
            storage_connection.engine.dispose()
    else:
        notifications_database._engine = database_engine
        with pytest.raises(RuntimeError, match="fresh empty database/schema"):
            notifications_database.run_migrations()

    assert set(inspect(database_engine).get_table_names()) == tables_before
    with database_engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM `{marker_table}`")).scalar_one() == rows_before


@pytest.mark.parametrize("charset", ("utf8mb3", "utf8mb4"))
def test_audit_event_type_filter_preserves_exact_matching_across_charsets(
    database_engine: Engine, charset: str
) -> None:
    """Deployment defaults must not break case-, accent-, or space-sensitive audit filters."""
    _upgrade("notifications")
    with database_engine.begin() as connection:
        connection.execute(
            text(f"ALTER TABLE `AuditEvents` MODIFY `event_type` VARCHAR(255) CHARACTER SET {charset} NOT NULL")
        )
    with Session(database_engine) as session:
        events = [
            ProducerAuditEvent(
                heimdall_job_id="filter-job",
                event_producer="job-manager",
                event_type=event_type,
                event_level="recovery",
                payload={},
                idempotency_key=str(uuid.uuid4()),
                event_produced_at=1_786_089_600_123_456 + index,
            )
            for index, event_type in enumerate(("selected", "SELECTED", "sélècted", "selected "))
        ]
        insert_audit_events(session, events)
        page = query_audit_events(session, AuditEventQuery(heimdall_job_id="filter-job", event_type="selected"))
        assert [event.event_type for event in page.items] == ["selected"]
