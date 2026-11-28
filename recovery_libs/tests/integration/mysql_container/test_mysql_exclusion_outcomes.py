# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify attribution outcomes and the additive schema change on isolated MySQL."""

import importlib
import uuid
from datetime import UTC, datetime

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
