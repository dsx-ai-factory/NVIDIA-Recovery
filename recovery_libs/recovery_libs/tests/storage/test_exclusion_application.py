# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Behavioral coverage for attribution outcomes through production storage APIs."""

import importlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from recovery_libs.schema.anomaly_isolator.exclusion_application import ExclusionApplicationStatus
from recovery_libs.schema.job_manager.models import (
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    AdviceState,
)
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.dataclass import AdviceReconciliationStatusRecord, AdviceRecord
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.storage.node_events import ErrorAttributionType, ExclusionType, NodeEvent
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

NOW = datetime(2026, 11, 23, tzinfo=UTC)


@pytest.fixture
def connection():
    """Use real SQL transactions without requiring a shared database."""
    engine = create_engine("sqlite://")
    for model in (JobExecutionError, NodeEvent, AdviceRecord, AdviceReconciliationStatusRecord):
        model.__table__.create(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    yield SimpleNamespace(session=factory, write_session=factory, write_pool_enabled=True)
    engine.dispose()


def advice_record(**overrides):
    """Create one restart recommendation with independently specified exclusions."""
    return AdviceRecord(
        **{
            "heimdall_id": uuid.uuid4(),
            "visible": True,
            "job_id": "123",
            "attempt_index": 0,
            "advice_state": AdviceState.RESTARTED.value,
            "node_exclusions": ["node-a", "node-b"],
            "generation": 2,
            "created_at": NOW,
            "updated_at": NOW,
            **overrides,
        }
    )


def status_record(advice, **overrides):
    """Describe SAC's fresh confirmation of this exact generation."""
    record = AdviceReconciliationStatusRecord(
        **{
            "advice_id": advice.heimdall_id,
            "observed_generation": advice.generation,
            "disposition": AdviceReconciliationDisposition.CONVERGED.value,
            "reason": AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
            "observed_at": NOW,
            "transitioned_at": NOW,
            **overrides,
        }
    )
    # SQLite only auto-assigns INTEGER primary keys; production uses a BIGINT.
    record.id = 1
    return record


def attribution(node="node-a", **overrides):
    """Create an effective FAST exclusion before recovery confirmation."""
    return NodeEvent(
        **{
            "node": node,
            "job_id": "123",
            "job_attempt_index": 0,
            "timestamp": NOW,
            "error_attribution_type": ErrorAttributionType.FAST,
            "exclusion_type": ExclusionType.CONFIRMED,
            **overrides,
        }
    )


def store_events(connection, events):
    """Persist the post-policy attribution result through its real storage ingress."""
    AttributionStorage(connection).store_attribution(
        JobExecutionError(code=1, signature="gpu-failure", message="GPU failure", description="GPU lost"), events
    )


def outcomes(connection):
    """Return each persisted row's independently observable application outcome."""
    with connection.session() as session:
        return [event.exclusion_applied for event in session.scalars(select(NodeEvent).order_by(NodeEvent.id))]


def test_convergence_confirms_only_latest_eligible_fast_excluded_nodes(connection):
    """Old, SLOW, suppressed, unrelated-node and unrelated-attempt rows remain unconfirmed."""
    old_anomaly, latest_anomaly = uuid.uuid4(), uuid.uuid4()
    events = [
        attribution(job_anomaly_id=old_anomaly, timestamp=NOW - timedelta(seconds=1)),
        attribution(job_anomaly_id=latest_anomaly),
        attribution("node-b", job_anomaly_id=latest_anomaly, exclusion_type=ExclusionType.TEMPORARY),
        attribution("node-c", job_anomaly_id=latest_anomaly),
        attribution("node-a", job_anomaly_id=latest_anomaly, error_attribution_type=ErrorAttributionType.SLOW),
        attribution("node-b", job_anomaly_id=latest_anomaly, exclusion_type=ExclusionType.NONE),
        attribution(job_anomaly_id=latest_anomaly, job_attempt_index=1),
        attribution(job_anomaly_id=latest_anomaly, job_id="other-job"),
    ]
    store_events(connection, events)
    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    storage = MySQLStorage(connection)
    storage.upsert_advice_reconciliation_status(status_record(advice))
    storage.upsert_advice_reconciliation_status(status_record(advice))
    assert outcomes(connection) == [
        ExclusionApplicationStatus.UNKNOWN,
        ExclusionApplicationStatus.APPLIED,
        ExclusionApplicationStatus.APPLIED,
        ExclusionApplicationStatus.UNKNOWN,
        ExclusionApplicationStatus.NOT_APPLICABLE,
        ExclusionApplicationStatus.NOT_APPLICABLE,
        ExclusionApplicationStatus.UNKNOWN,
        ExclusionApplicationStatus.UNKNOWN,
    ]


@pytest.mark.parametrize(
    ("advice_changes", "status_changes"),
    [
        ({"advice_state": AdviceState.TERMINATED.value}, {}),
        ({"advice_state": AdviceState.RELEASED.value}, {}),
        ({"node_exclusions": []}, {}),
        ({}, {"observed_generation": 1}),
        ({}, {"disposition": AdviceReconciliationDisposition.ABANDONED.value}),
    ],
)
def test_other_terminal_outcomes_do_not_confirm_exclusions(connection, advice_changes, status_changes):
    """A final status alone is insufficient evidence of an effective recovery exclusion."""
    store_events(connection, [attribution()])
    advice = advice_record(**advice_changes)
    with connection.session() as session:
        session.add(advice)
        session.commit()
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice, **status_changes))
    assert outcomes(connection) == [ExclusionApplicationStatus.UNKNOWN]


def test_suppressed_new_anomaly_does_not_confirm_previous_anomaly(connection):
    """Select the latest anomaly before the safety-policy filter."""
    store_events(
        connection,
        [
            attribution(job_anomaly_id=uuid.uuid4(), timestamp=NOW - timedelta(seconds=1)),
            attribution(job_anomaly_id=uuid.uuid4(), exclusion_type=ExclusionType.NONE),
        ],
    )
    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    assert outcomes(connection) == [ExclusionApplicationStatus.UNKNOWN, ExclusionApplicationStatus.NOT_APPLICABLE]


def test_additive_migration_preserves_history_without_inventing_applied_status(monkeypatch):
    """Upgrade existing rows to UNKNOWN and remove only the added field on rollback."""
    migration = importlib.import_module(
        "recovery_libs.storage.alembic.versions.000000000007_restore_exclusion_application"
    )
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE node_events (id INTEGER PRIMARY KEY, node VARCHAR(255))"))
        connection.execute(text("INSERT INTO node_events (id, node) VALUES (1, 'node-a')"))
        monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
        migration.upgrade()
        assert connection.execute(text("SELECT node, exclusion_applied FROM node_events")).one() == (
            "node-a",
            "UNKNOWN",
        )
        migration.downgrade()
        assert connection.execute(text("SELECT * FROM node_events")).one() == (1, "node-a")
    engine.dispose()


def test_latest_report_is_selected_even_when_its_observation_time_is_older(connection):
    """Delayed telemetry does not make an older reported anomaly authoritative."""
    store_events(
        connection,
        [
            attribution(job_anomaly_id=uuid.uuid4(), timestamp=NOW),
            attribution(job_anomaly_id=uuid.uuid4(), timestamp=NOW - timedelta(minutes=5)),
        ],
    )
    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    assert outcomes(connection) == [ExclusionApplicationStatus.UNKNOWN, ExclusionApplicationStatus.APPLIED]


def test_late_attribution_observes_already_persisted_convergence(connection):
    """Results arriving after status confirmation must not remain UNKNOWN forever."""
    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    store_events(connection, [attribution(job_anomaly_id=uuid.uuid4())])
    assert outcomes(connection) == [ExclusionApplicationStatus.APPLIED]


def test_projection_failure_rolls_back_status_and_outcomes_together(connection, monkeypatch):
    """A failed projection cannot terminally close Advice without confirming its rows."""
    from recovery_libs.storage import mysql_storage
    from recovery_libs.storage.base import RecoveryStorageException

    store_events(connection, [attribution()])
    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    original_confirm = mysql_storage.confirm_exclusions

    def fail_after_projection(session, advice, status):
        original_confirm(session, advice, status)
        raise RuntimeError("interrupted before commit")

    monkeypatch.setattr(mysql_storage, "confirm_exclusions", fail_after_projection)
    with pytest.raises(RecoveryStorageException):
        MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    assert outcomes(connection) == [ExclusionApplicationStatus.UNKNOWN]
    with connection.session() as session:
        assert session.scalars(select(AdviceReconciliationStatusRecord)).all() == []
    monkeypatch.setattr(mysql_storage, "confirm_exclusions", original_confirm)
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    assert outcomes(connection) == [ExclusionApplicationStatus.APPLIED]


def test_late_attribution_projection_failure_rolls_back_insert(connection, monkeypatch):
    """Retrying interrupted late ingestion restores both evidence and its confirmed outcome."""
    from recovery_libs.storage import attribution_storage
    from recovery_libs.storage.base import RecoveryStorageException

    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    MySQLStorage(connection).upsert_advice_reconciliation_status(status_record(advice))
    original_confirm = attribution_storage.confirm_exclusions

    def fail_after_projection(session, advice, status):
        original_confirm(session, advice, status)
        raise RuntimeError("interrupted before commit")

    monkeypatch.setattr(attribution_storage, "confirm_exclusions", fail_after_projection)
    events = [attribution()]
    with pytest.raises(RecoveryStorageException):
        store_events(connection, events)
    assert outcomes(connection) == []
    monkeypatch.setattr(attribution_storage, "confirm_exclusions", original_confirm)
    store_events(connection, events)
    assert outcomes(connection) == [ExclusionApplicationStatus.APPLIED]


@pytest.mark.parametrize("late_ingestion", [False, True])
def test_transient_conflict_retries_entire_outcome_transaction(connection, monkeypatch, late_ingestion):
    """A deadlock after projection is rolled back and retried without losing confirmation."""
    from recovery_libs.storage import attribution_storage, mysql_storage
    from sqlalchemy.exc import OperationalError

    advice = advice_record()
    with connection.session() as session:
        session.add(advice)
        session.commit()
    storage = MySQLStorage(connection)
    if late_ingestion:
        storage.upsert_advice_reconciliation_status(status_record(advice))
        module = attribution_storage
    else:
        store_events(connection, [attribution()])
        module = mysql_storage
    original_confirm = module.confirm_exclusions
    calls = 0

    def deadlock_once(session, advice, status):
        nonlocal calls
        original_confirm(session, advice, status)
        calls += 1
        if calls == 1:
            raise OperationalError("UPDATE", {}, Exception(1213, "deadlock victim"))
        return 1

    monkeypatch.setattr(module, "confirm_exclusions", deadlock_once)
    monkeypatch.setattr(mysql_storage, "_conflict_retry_delay", lambda attempt: 0)
    if late_ingestion:
        store_events(connection, [attribution()])
    else:
        storage.upsert_advice_reconciliation_status(status_record(advice))
    assert calls == 2
    assert outcomes(connection) == [ExclusionApplicationStatus.APPLIED]
    with connection.session() as session:
        assert len(session.scalars(select(AdviceReconciliationStatusRecord)).all()) == 1
