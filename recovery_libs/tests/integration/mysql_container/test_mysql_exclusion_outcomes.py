# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify attribution outcomes and the additive schema change on isolated MySQL."""

import importlib
import uuid
from datetime import UTC, datetime

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from recovery_libs.schema.anomaly_isolator.exclusion_application import ExclusionApplicationStatus
from recovery_libs.schema.job_manager.models import AdviceReconciliationDisposition, AdviceReconciliationReason
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.dataclass import AdviceReconciliationStatusRecord, AdviceRecord, SlurmJobExecutionRecord
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import ErrorAttributionType, ExclusionType, NodeEvent
from sqlalchemy import text


def test_mysql_convergence_persists_public_attribution_outcome(storage):
    """Fresh schema, status persistence, and attribution reads agree on confirmed constraints."""
    now = datetime.now(UTC)
    job_id = f"exclusion-{uuid.uuid4().hex[:12]}"
    job = SlurmJobExecutionRecord(job_id=job_id, job_attempt_index=0, workflow_id="qualification", last_updated_at=now)
    storage.upsert_job_record(job)
    advice = AdviceRecord(
        heimdall_id=job.heimdall_job_id,
        visible=True,
        job_id=job_id,
        attempt_index=0,
        advice_state="Restarted",
        node_exclusions=["node-a"],
        created_at=now,
        updated_at=now,
    )
    persisted = storage.upsert_advice(advice).inserted_record
    attribution_storage = AttributionStorage(storage.connection)
    attribution_storage.store_attribution(
        JobExecutionError(code=1, message="GPU lost", description="GPU failure", signature="gpu-lost"),
        [
            NodeEvent(
                node="node-a",
                timestamp=now,
                job_id=job_id,
                job_attempt_index=0,
                job_anomaly_id=uuid.uuid4(),
                error_attribution_type=ErrorAttributionType.FAST,
                exclusion_type=ExclusionType.CONFIRMED,
            )
        ],
    )
    storage.upsert_advice_reconciliation_status(
        AdviceReconciliationStatusRecord(
            advice_id=persisted.heimdall_id,
            observed_generation=persisted.generation,
            disposition=AdviceReconciliationDisposition.CONVERGED.value,
            reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
            observed_at=now,
            transitioned_at=now,
        )
    )
    rows = attribution_storage.read_node_attributions_for_job(job_id, 0)
    assert len(rows) == 1
    assert rows[0].node == "node-a"
    assert rows[0].exclusion_applied == ExclusionApplicationStatus.APPLIED


def test_mysql_additive_outcome_migration_preserves_diagnosis(storage, monkeypatch):
    """MySQL upgrade defaults existing outcomes to unknown and rollback retains diagnoses."""
    migration = importlib.import_module(
        "recovery_libs.storage.alembic.versions.000000000007_restore_exclusion_application"
    )
    AttributionStorage(storage.connection).store_attribution(
        JobExecutionError(code=2, message="Existing failure", description="Historical diagnosis", signature="legacy"),
        [NodeEvent(node="legacy-node", timestamp=datetime.now(UTC))],
    )
    # The module fixture owns a disposable database. Exercise only the new DDL,
    # restoring it before another test can use the current ORM model.
    with storage.connection.engine.begin() as connection:
        monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
        before = connection.execute(text("SELECT id, node FROM node_events ORDER BY id")).all()
        migration.downgrade()
        try:
            assert connection.execute(text("SELECT id, node FROM node_events ORDER BY id")).all() == before
        finally:
            migration.upgrade()
        after = connection.execute(text("SELECT id, node, exclusion_applied FROM node_events ORDER BY id")).all()
        assert [(row.id, row.node) for row in after] == [(row.id, row.node) for row in before]
        assert all(row.exclusion_applied == "UNKNOWN" for row in after)


@pytest.mark.parametrize("first_writer", ["attribution", "status"])
def test_mysql_attribution_and_status_writers_converge_in_either_commit_order(storage, monkeypatch, first_writer):
    """Overlapping ingestion and convergence cannot leave the effective exclusion UNKNOWN."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, local

    from sqlalchemy.orm import Session, sessionmaker

    now = datetime.now(UTC)
    job_id = f"ordering-{uuid.uuid4().hex[:12]}"
    job = SlurmJobExecutionRecord(job_id=job_id, job_attempt_index=0, workflow_id="qualification", last_updated_at=now)
    storage.upsert_job_record(job)
    advice = storage.upsert_advice(
        AdviceRecord(
            heimdall_id=job.heimdall_job_id,
            visible=True,
            job_id=job_id,
            attempt_index=0,
            advice_state="Restarted",
            node_exclusions=["node-a"],
            created_at=now,
            updated_at=now,
        )
    ).inserted_record
    attribution_storage = AttributionStorage(storage.connection)
    error = JobExecutionError(code=1, message="GPU lost", description="GPU failure", signature="ordering-gpu-lost")
    # Seed the error before synchronization so both writers' first transactional
    # read is the Advice lock, rather than unrelated error lookup/creation.
    attribution_storage.store_attribution(error, [])
    first_ready, second_started, release_first = Event(), Event(), Event()
    writer = local()

    class CoordinatedSession(Session):
        """Hold the first transaction after projection until the second requests its lock.

        Pausing before commit ensures the first writer has already checked for its
        counterpart's data. The second writer must wait, then observe that commit.
        """

        def scalars(self, statement, *args, **kwargs):
            if not getattr(writer, "started", False):
                writer.started = True
                if writer.role != first_writer:
                    second_started.set()
            return super().scalars(statement, *args, **kwargs)

        def commit(self):
            if writer.role == first_writer:
                first_ready.set()
                assert release_first.wait(timeout=15), "first writer was not released"
            return super().commit()

    monkeypatch.setattr(
        storage.connection,
        "write_session",
        sessionmaker(bind=storage.connection.write_engine, class_=CoordinatedSession),
    )

    def run(role):
        writer.role = role
        writer.started = False
        if role == "status":
            storage.upsert_advice_reconciliation_status(
                AdviceReconciliationStatusRecord(
                    advice_id=advice.heimdall_id,
                    observed_generation=advice.generation,
                    disposition=AdviceReconciliationDisposition.CONVERGED.value,
                    reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
                    observed_at=now,
                    transitioned_at=now,
                )
            )
        else:
            attribution_storage.store_attribution(
                error,
                [
                    NodeEvent(
                        node="node-a",
                        timestamp=now,
                        job_id=job_id,
                        job_attempt_index=0,
                        job_anomaly_id=uuid.uuid4(),
                        error_attribution_type=ErrorAttributionType.FAST,
                        exclusion_type=ExclusionType.CONFIRMED,
                    )
                ],
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(run, first_writer)
        try:
            assert first_ready.wait(timeout=15), "first writer did not reach its commit boundary"
            second = pool.submit(run, "status" if first_writer == "attribution" else "attribution")
            assert second_started.wait(timeout=15), "second writer did not start its Advice locking read"
        finally:
            release_first.set()
        first.result(timeout=15)
        second.result(timeout=15)
    rows = attribution_storage.read_node_attributions_for_job(job_id, 0)
    assert len(rows) == 1
    assert rows[0].exclusion_applied == ExclusionApplicationStatus.APPLIED
