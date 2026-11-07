# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise task admission and retry isolation against the real MySQL store."""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest.mock import MagicMock

import job_manager.recovery as recovery_module
import pytest
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.recovery import RecoveryService
from job_manager.service import JobManager
from job_manager.storage_utils import maybe_persist_requeue_terminal_advice, persist_advice_record, persist_job_record
from opentelemetry.sdk.trace import TracerProvider
from recovery_libs.advice import check_max_requeue_reached
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    RecoveryOperation,
    RecoveryTriggerType,
    SystemRequeueConfig,
)
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import AdviceRecord, SlurmJobExecutionRecord
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.storage.scheduler_metadata import SchedulerMetadataConflictError
from sqlalchemy import text


@pytest.fixture(autouse=True)
def recovery_tracer(monkeypatch):
    provider = TracerProvider()
    monkeypatch.setattr(recovery_module, "_TRACER", provider.get_tracer(__name__))
    yield
    provider.shutdown()


def _task(parent, task=0, attempt=0, *, raw=None):
    job_id = f"{parent}_{task}"
    return SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=attempt,
        workflow_id=str(parent),
        state="FAILED",
        last_updated_at=datetime.now(UTC),
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_job_id": job_id,
            "slurm_raw_job_id": str(raw or parent + task),
            "slurm_array_job_id": str(parent),
            "slurm_array_task_id": str(task),
        },
        aps_spec=ApsSlurmJobSpec(auto_resume_mode=AutoResumeModeEnum.REQUEUE, max_requeue_times=2),
    )


def _parent():
    return 100000 + uuid.uuid4().int % 1000000000


def _advice(job):
    return AdviceRecord(
        heimdall_id=generate_heimdall_job_id(job.job_id, job.job_attempt_index),
        job_id=job.job_id,
        attempt_index=job.job_attempt_index,
        scheduler_metadata=job.scheduler_metadata,
        advice_state="Restarted",
        visible=True,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


@pytest.mark.parametrize("first", ["job", "advice"])
def test_concurrent_first_admission_rejects_losing_identity_before_recovery(storage, first):
    """Concurrent first identity observations cannot disagree across Job and Advice."""
    parent = _parent()
    job = _task(parent)
    advice = _advice(_task(parent, raw=parent + 1))
    # Advice requires a Job foreign key. The existing Job has not yet learned
    # scheduler identity, so neither writer initially has an identity to compare.
    initial = _task(parent)
    initial.scheduler_metadata = {"scheduler": "slurm"}
    storage.create_job_record(initial)
    audit = MagicMock()
    writes = {
        "job": lambda: persist_job_record(storage, audit, job),
        "advice": lambda: persist_advice_record(storage, audit, advice),
    }
    started = threading.Event()
    other = "advice" if first == "job" else "job"

    def competing_write():
        started.set()
        return writes[other]()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with storage.job_attempt_scope(job.job_id, 0):
            assert storage.read_job_record(job.job_id, 0).scheduler_metadata == {"scheduler": "slurm"}
            assert storage.read_advice_by_job(job.job_id, 0) is None
            pending = executor.submit(competing_write)
            assert started.wait(5)
            writes[first]()
        with pytest.raises(SchedulerMetadataConflictError):
            pending.result(timeout=10)
    retained_job = storage.read_job_record(job.job_id, 0)
    assert retained_job.scheduler_metadata == (job.scheduler_metadata if first == "job" else {"scheduler": "slurm"})
    assert (storage.read_advice_by_job(job.job_id, 0) is not None) == (first == "advice")
    sequence = storage.read_recovery_sequence_by_natural_key(
        scheduler_system="slurm",
        scheduler_scope="unspecified",
        job_id=job.job_id,
        source_attempt_index=0,
        operation="restart",
    )
    assert (sequence is not None) == (first == "advice")
    assert audit.add.call_count == (1 if first == "advice" else 0)


def test_conflicting_recovery_context_cannot_create_a_sequence(storage):
    job = _task(_parent())
    persist_job_record(storage, MagicMock(), job)
    conflicting = job.scheduler_metadata | {"slurm_raw_job_id": "99"}
    with pytest.raises(SchedulerMetadataConflictError):
        RecoveryService(storage).begin_for_job(
            job,
            operation=RecoveryOperation.RESTART,
            trigger_type=RecoveryTriggerType.OPERATOR,
            scheduler_metadata=conflicting,
        )
    assert (
        storage.read_recovery_sequence_by_natural_key(
            scheduler_system="slurm",
            scheduler_scope="unspecified",
            job_id=job.job_id,
            source_attempt_index=0,
            operation="restart",
        )
        is None
    )


def test_admission_scope_releases_on_failure_and_rejects_cross_attempt_nesting(storage):
    job = _task(_parent())
    with pytest.raises(ValueError), storage.job_attempt_scope(job.job_id, 0):
        with (
            storage.job_attempt_scope(job.job_id, 0),
            pytest.raises(RecoveryStorageException),
            storage.job_attempt_scope(job.job_id, 1),
        ):
            pytest.fail("Different attempts cannot share an admission scope")
        raise ValueError("abort admission")
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(persist_job_record, storage, MagicMock(), job).result(timeout=10)
    assert result.was_created


def test_admission_uses_one_connection_even_with_a_single_connection_pool(storage):
    url = storage.connection.engine.url
    connection = MySQLConnection(
        host=url.host,
        port=url.port,
        username=url.username,
        password=url.password,
        database_name=url.database,
        write_pool_size=1,
        write_max_overflow=0,
    )
    isolated = MySQLStorage(connection)
    job = _task(_parent())
    try:
        with isolated.job_attempt_scope(job.job_id, 0):
            with connection.session() as session:
                lock_connection_id = session.scalar(text("SELECT CONNECTION_ID()"))
            persist_job_record(isolated, MagicMock(), job)
            persist_advice_record(isolated, MagicMock(), _advice(job))
            with connection.write_session() as session:
                assert session.scalar(text("SELECT CONNECTION_ID()")) == lock_connection_id
        assert isolated.read_advice_by_job(job.job_id, 0).recovery_sequence_id is not None
    finally:
        connection.engine.dispose()
        connection.write_engine.dispose()


def test_sibling_retry_allowances_survive_storage_reload(storage):
    """Exhausting one task leaves its sibling's two independent requeues intact."""
    parent = _parent()
    for task, attempts in ((0, range(3)), (1, range(2))):
        for attempt in attempts:
            persist_job_record(storage, MagicMock(), _task(parent, task, attempt))
    # A new service/storage facade has no process-local retry counters.
    restarted = MySQLStorage(storage.connection)
    exhausted = restarted.read_job_record(f"{parent}_0")
    eligible = restarted.read_job_record(f"{parent}_1")
    config = SystemRequeueConfig(max_requeue_times=2, requeue_on_completion=False)
    audit = MagicMock()
    for record in (exhausted, eligible):
        maybe_persist_requeue_terminal_advice(
            restarted,
            record,
            audit_buffer=audit,
            system_requeue_config=config,
            get_jobs=lambda **_: [],
        )
    assert restarted.read_advice_by_job(exhausted.job_id, 2).advice_state == "Terminated"
    assert restarted.read_advice_by_job(eligible.job_id, 1) is None
    assert (
        check_max_requeue_reached(
            storage_to_http_job_status(eligible),
            system_requeue_config=config,
            get_jobs=lambda **_: [],
        )[0]
        is False
    )
    persist_job_record(restarted, audit, _task(parent, 1, 2))
    latest_sibling = restarted.read_job_record(eligible.job_id)
    maybe_persist_requeue_terminal_advice(
        restarted,
        latest_sibling,
        audit_buffer=audit,
        system_requeue_config=config,
        get_jobs=lambda **_: [],
    )
    assert restarted.read_advice_by_job(eligible.job_id, 2).advice_state == "Terminated"
    assert restarted.read_job_record(exhausted.job_id).job_attempt_index == 2


def test_admission_timeout_does_not_block_other_task_identity(storage):
    """A busy task rejects a bounded waiter while a sibling remains independent."""
    parent = _parent()
    blocked, sibling = _task(parent), _task(parent, task=1)

    def wait_for_busy_task():
        with storage.connection.job_attempt_scope(blocked.job_id, 0, timeout_seconds=0):
            pytest.fail("A competing owner must not enter the held scope")

    with ThreadPoolExecutor(max_workers=1) as executor, storage.job_attempt_scope(blocked.job_id, 0):
        with pytest.raises(RecoveryStorageException, match="Timed out"):
            executor.submit(wait_for_busy_task).result(timeout=5)
        assert executor.submit(persist_job_record, storage, MagicMock(), sibling).result(timeout=5).was_created
    assert storage.read_job_record(blocked.job_id, 0) is None


def test_conflicting_attribution_identity_cannot_claim_or_publish(storage):
    """Old inconsistent rows cannot bypass validation via optional trace context."""
    job = _task(_parent())
    storage.create_job_record(job)
    conflicting = _advice(_task(int(job.scheduler_metadata["slurm_array_job_id"]), raw=1))
    # Simulate conflicting rows admitted by an older replica without the scope.
    storage.upsert_advice(conflicting)
    manager = object.__new__(JobManager)
    manager.storage = storage
    manager.config = {"service": {"adi_attribution_request_emit_enabled": True}}
    manager.adi_attribution_request_publisher = MagicMock()
    manager.recovery_service = RecoveryService(storage)
    with pytest.raises(SchedulerMetadataConflictError):
        manager._maybe_emit_attribution_request(job)
    manager.adi_attribution_request_publisher.publish_message.assert_not_called()
    assert storage.claim_attribution_request_emission(job.job_id, job.job_attempt_index)


@pytest.mark.parametrize("loss", ["kill", "invalidate", "release"])
def test_correlation_connection_loss_cannot_commit_advice_or_audit(storage, monkeypatch, loss):
    """Optional correlation failures cannot resume admission on an unlocked connection."""
    job = _task(_parent())
    persist_job_record(storage, MagicMock(), job)
    original_read = storage.read_recovery_sequence_by_natural_key

    def lose_admission_connection(**_kwargs):
        with storage.connection.session() as session:
            pinned = session.bind
            connection_id = session.scalar(text("SELECT CONNECTION_ID()"))
        if loss == "kill":
            with storage.connection.engine.connect() as killer:
                killer.exec_driver_sql(f"KILL {int(connection_id)}")
        elif loss == "invalidate":
            pinned.invalidate()
        else:
            pinned.exec_driver_sql("SELECT RELEASE_ALL_LOCKS()")
            pinned.rollback()
        # This is deliberately swallowed by the optional correlation caller.
        # Admission itself must remain poisoned and reject further effects.
        raise RuntimeError("correlation unavailable after losing ownership")

    monkeypatch.setattr(storage, "read_recovery_sequence_by_natural_key", lose_admission_connection)
    audit = MagicMock()
    with pytest.raises(RecoveryStorageException):
        persist_advice_record(storage, audit, _advice(job))
    audit.add.assert_not_called()
    assert storage.read_advice_by_job(job.job_id, 0) is None
    # A new admission may recover, but it must repeat identity validation.
    monkeypatch.setattr(storage, "read_recovery_sequence_by_natural_key", original_read)
    persist_advice_record(storage, audit, _advice(job))
    assert storage.read_advice_by_job(job.job_id, 0).recovery_sequence_id is not None


def test_lost_admission_lock_rejects_commit_on_an_existing_session(storage):
    """A cached session cannot commit its writes after the named lock disappears."""
    job = _task(_parent())
    persist_job_record(storage, MagicMock(), job)
    with pytest.raises(RecoveryStorageException), storage.job_attempt_scope(job.job_id, 0):
        factory = storage.connection.write_session
        with factory() as session:
            session.execute(
                text("UPDATE job_execution_records SET state = 'COMPLETED' WHERE job_id = :job_id"),
                {"job_id": job.job_id},
            )
            session.bind.exec_driver_sql("SELECT RELEASE_ALL_LOCKS()")
            session.commit()
    assert storage.read_job_record(job.job_id, 0).state == "FAILED"


def test_attribution_connection_loss_preserves_claim_for_fresh_retry(storage, monkeypatch):
    """A killed correlation connection cannot consume an unpublished request."""
    job = _task(_parent())
    job.anomalies = [
        JobAnomaly(anomaly_type=EventType.CRASH.value, timestamp=datetime.now(UTC), uuid=str(uuid.uuid4()))
    ]
    persist_job_record(storage, MagicMock(), job)
    manager = object.__new__(JobManager)
    manager.storage = storage
    manager.config = {"service": {"adi_attribution_request_emit_enabled": True}}
    manager.adi_attribution_request_publisher = MagicMock()
    manager.recovery_service = RecoveryService(storage)
    original_read = storage.read_recovery_sequence_by_natural_key

    def kill_correlation_connection(**_kwargs):
        with storage.connection.session() as session:
            connection_id = session.scalar(text("SELECT CONNECTION_ID()"))
        with storage.connection.engine.connect() as killer:
            killer.exec_driver_sql(f"KILL {int(connection_id)}")
        raise RuntimeError("correlation connection lost")

    monkeypatch.setattr(storage, "read_recovery_sequence_by_natural_key", kill_correlation_connection)
    with pytest.raises(RecoveryStorageException):
        manager._maybe_emit_attribution_request(job)
    manager.adi_attribution_request_publisher.publish_message.assert_not_called()
    monkeypatch.setattr(storage, "read_recovery_sequence_by_natural_key", original_read)
    manager._maybe_emit_attribution_request(job)
    manager.adi_attribution_request_publisher.publish_message.assert_called_once()
    # Subsequent duplicate triggers still respect the durable emission claim.
    manager._maybe_emit_attribution_request(job)
    manager.adi_attribution_request_publisher.publish_message.assert_called_once()
