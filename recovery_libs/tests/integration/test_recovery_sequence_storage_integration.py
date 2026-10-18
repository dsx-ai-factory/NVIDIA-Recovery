# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from recovery_libs.storage.base import RecoveryStorageInputValidationError
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    RecoveryMilestoneRecord,
    RecoverySequenceRecord,
    SlurmJobExecutionRecord,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage


@pytest.fixture(scope="module")
def mysql_connection(db_config):
    connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=db_config["database_name"],
    )
    yield connection
    connection.engine.dispose()


@pytest.fixture(scope="module")
def mysql_storage(mysql_connection):
    storage = MySQLStorage(connection=mysql_connection)
    storage.bootstrap()
    assert storage.ready()
    return storage


@pytest.fixture(autouse=True)
def clean_recovery_tables(mysql_connection):
    yield
    with mysql_connection.session() as session:
        session.query(AdviceRecord).filter(AdviceRecord.job_id == "recovery-sequence-job").delete()
        session.query(RecoveryMilestoneRecord).delete()
        session.query(RecoverySequenceRecord).delete()
        session.query(SlurmJobExecutionRecord).filter(
            SlurmJobExecutionRecord.job_id == "recovery-sequence-job"
        ).delete()
        session.commit()


def _sequence(*, operation: str = "restart", sequence_id: uuid.UUID | None = None) -> RecoverySequenceRecord:
    now = datetime.now(UTC)
    sequence_id = sequence_id or uuid.uuid4()
    return RecoverySequenceRecord(
        sequence_id=sequence_id,
        scheduler_system="slurm",
        scheduler_scope="integration",
        job_id="recovery-sequence-job",
        source_attempt_index=3,
        operation=operation,
        traceparent=f"00-{sequence_id.hex}-00f067aa0ba902b7-01",
        tracestate=f"candidate={sequence_id.hex[:8]}",
        trigger_type="job_state",
        trigger_identifier=f"trigger-{sequence_id}",
        triggered_at=now,
        status="active",
        completed_at=None,
        created_at=now,
        updated_at=now,
    )


def test_begin_or_get_is_atomic_and_keeps_one_canonical_context(mysql_storage):
    candidates = [_sequence() for _ in range(8)]
    candidates_by_id = {candidate.sequence_id: candidate for candidate in candidates}

    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        results = list(executor.map(mysql_storage.begin_or_get_recovery_sequence, candidates))

    returned = [record for record, _created in results]
    assert len({record.sequence_id for record in returned}) == 1
    assert len({record.traceparent for record in returned}) == 1
    assert sum(1 for _record, created in results if created) == 1
    winner = candidates_by_id[returned[0].sequence_id]
    assert all(record.traceparent == winner.traceparent for record in returned)
    assert all(record.tracestate == winner.tracestate for record in returned)
    assert all(record.trigger_identifier == winner.trigger_identifier for record in returned)


def test_operation_is_part_of_sequence_natural_key(mysql_storage):
    restart, restart_created = mysql_storage.begin_or_get_recovery_sequence(_sequence(operation="restart"))
    dag_release, dag_created = mysql_storage.begin_or_get_recovery_sequence(_sequence(operation="dag_release"))

    assert restart_created is True
    assert dag_created is True
    assert restart.sequence_id != dag_release.sequence_id


def test_advice_upsert_attaches_only_the_canonical_sequence_context(mysql_storage):
    heimdall_id = uuid.uuid4()
    mysql_storage.upsert_job_record(
        SlurmJobExecutionRecord(
            heimdall_job_id=heimdall_id,
            job_id="recovery-sequence-job",
            job_attempt_index=3,
            workflow_id="recovery-workflow",
            last_updated_at=datetime.now(UTC),
        )
    )
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence())
    now = datetime.now(UTC)
    persisted, created, old_record = mysql_storage.upsert_advice(
        AdviceRecord(
            heimdall_id=heimdall_id,
            visible=True,
            job_id="recovery-sequence-job",
            scheduler_metadata={"scheduler": "slurm", "environment": "integration"},
            attempt_index=3,
            advice_state="Restarted",
            recovery_sequence_id=uuid.uuid4(),
            recovery_traceparent="untrusted",
            recovery_operation="terminate",
            created_at=now,
            updated_at=now,
        )
    )

    assert created is True
    assert old_record is None
    assert persisted.recovery_sequence_id == sequence.sequence_id
    assert persisted.recovery_traceparent == sequence.traceparent
    assert persisted.recovery_tracestate == sequence.tracestate
    assert persisted.recovery_triggered_at == sequence.triggered_at
    assert persisted.recovery_operation == "restart"


def test_milestone_idempotency_deduplicates_one_occurrence(mysql_storage):
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence())
    completed_at = datetime.now(UTC)
    milestone = RecoveryMilestoneRecord(
        sequence_id=sequence.sequence_id,
        idempotency_key="release-occurrence-1",
        operation="restart",
        action="slurm.release",
        outcome="success",
        completes_sequence=True,
        completed_at=completed_at,
        created_at=completed_at,
    )

    first, first_created, first_completed = mysql_storage.record_recovery_milestone(milestone)
    duplicate, duplicate_created, duplicate_completed = mysql_storage.record_recovery_milestone(milestone)
    stored_sequence = mysql_storage.read_recovery_sequence(sequence.sequence_id)

    assert first_created is True
    assert first_completed is True
    assert duplicate_created is False
    assert duplicate_completed is False
    assert duplicate.id == first.id
    assert stored_sequence is not None
    assert stored_sequence.status == "completed"
    assert stored_sequence.completed_at == completed_at


def test_duplicate_milestone_key_rejects_conflicting_payload(mysql_storage):
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence())
    completed_at = datetime.now(UTC)
    original = RecoveryMilestoneRecord(
        sequence_id=sequence.sequence_id,
        idempotency_key="one-occurrence",
        operation="restart",
        action="slurm.release",
        outcome="error",
        completes_sequence=False,
        completed_at=completed_at,
        created_at=completed_at,
    )
    conflicting = RecoveryMilestoneRecord(
        sequence_id=sequence.sequence_id,
        idempotency_key="one-occurrence",
        operation="restart",
        action="slurm.release",
        outcome="success",
        completes_sequence=False,
        completed_at=completed_at + timedelta(seconds=1),
        created_at=completed_at + timedelta(seconds=1),
    )

    mysql_storage.record_recovery_milestone(original)

    with pytest.raises(RecoveryStorageInputValidationError, match="idempotency key"):
        mysql_storage.record_recovery_milestone(conflicting)


def test_only_first_explicit_completion_sets_sequence_timestamp(mysql_storage):
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence())
    first_completed_at = datetime.now(UTC)

    milestones = (
        RecoveryMilestoneRecord(
            sequence_id=sequence.sequence_id,
            idempotency_key="requeue-hold",
            operation="restart",
            action="slurm.requeue_hold",
            outcome="success",
            completes_sequence=False,
            completed_at=first_completed_at - timedelta(seconds=2),
            created_at=first_completed_at,
        ),
        RecoveryMilestoneRecord(
            sequence_id=sequence.sequence_id,
            idempotency_key="release-success-1",
            operation="restart",
            action="slurm.release",
            outcome="success",
            completes_sequence=True,
            completed_at=first_completed_at,
            created_at=first_completed_at,
        ),
        RecoveryMilestoneRecord(
            sequence_id=sequence.sequence_id,
            idempotency_key="release-success-2",
            operation="restart",
            action="slurm.release",
            outcome="success",
            completes_sequence=True,
            completed_at=first_completed_at + timedelta(seconds=5),
            created_at=first_completed_at + timedelta(seconds=5),
        ),
    )
    for milestone in milestones:
        mysql_storage.record_recovery_milestone(milestone)

    stored_sequence = mysql_storage.read_recovery_sequence(sequence.sequence_id)
    assert stored_sequence is not None
    assert stored_sequence.completed_at == first_completed_at


def test_successful_action_does_not_complete_without_explicit_intent(mysql_storage):
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence(operation="dag_release"))
    completed_at = datetime.now(UTC)
    mysql_storage.record_recovery_milestone(
        RecoveryMilestoneRecord(
            sequence_id=sequence.sequence_id,
            idempotency_key="dag-release",
            operation="dag_release",
            action="slurm.release",
            outcome="success",
            completes_sequence=False,
            completed_at=completed_at,
            created_at=completed_at,
        )
    )

    stored_sequence = mysql_storage.read_recovery_sequence(sequence.sequence_id)
    assert stored_sequence is not None
    assert stored_sequence.status == "active"
    assert stored_sequence.completed_at is None


def test_explicit_completion_is_independent_of_operation_and_action(mysql_storage):
    sequence, _ = mysql_storage.begin_or_get_recovery_sequence(_sequence(operation="terminate"))
    completed_at = datetime.now(UTC)
    _, created, completed = mysql_storage.record_recovery_milestone(
        RecoveryMilestoneRecord(
            sequence_id=sequence.sequence_id,
            idempotency_key="terminate-cancel",
            operation="terminate",
            action="slurm.cancel",
            outcome="success",
            completes_sequence=True,
            completed_at=completed_at,
            created_at=completed_at,
        )
    )

    stored_sequence = mysql_storage.read_recovery_sequence(sequence.sequence_id)
    assert created is True
    assert completed is True
    assert stored_sequence is not None
    assert stored_sequence.status == "completed"
    assert stored_sequence.completed_at == completed_at
