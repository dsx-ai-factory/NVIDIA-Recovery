# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from job_manager.job_scheduler_identity import preserve_job_scheduler_identity
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage.base import RecoveryStorageInputValidationError
from recovery_libs.storage.dataclass import AdviceRecord
from test_utils import create_example_slurm_job_record


def _identity(task="0"):
    return {
        "scheduler": "slurm",
        "slurm_job_id": f"4200_{task}",
        "slurm_raw_job_id": str(4201 + int(task)),
        "slurm_array_job_id": "4200",
        "slurm_array_task_id": task,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"slurm_job_id": "4200_1"},
        {"slurm_array_job_id": "0"},
        {"slurm_array_job_id": 4200},
        {"slurm_array_task_id": "-1"},
        {"slurm_array_task_id": "00"},
        {"slurm_array_task_id": True},
        {"slurm_array_task_id": "1"},
        {"slurm_raw_job_id": "4200_0"},
        {"slurm_raw_job_id": "0"},
        {"slurm_array_task_id": "4294967294"},
        {"slurm_array_job_id": "4294967295"},
    ],
)
def test_slurm_identity_rejects_inconsistent_or_non_decimal_metadata(changes):
    with pytest.raises(RecoveryStorageInputValidationError):
        preserve_job_scheduler_identity("4200_0", _identity() | changes)


@pytest.mark.parametrize("missing", ["slurm_array_job_id", "slurm_array_task_id"])
def test_slurm_identity_requires_complete_array_pair(missing):
    incoming = _identity()
    del incoming[missing]
    with pytest.raises(RecoveryStorageInputValidationError):
        preserve_job_scheduler_identity("4200_0", incoming)
    assert preserve_job_scheduler_identity("4200_0", incoming, _identity()) == _identity()


def test_non_slurm_identity_is_opaque():
    incoming = {"scheduler": "other", "slurm_array_task_id": True, "slurm_job_id": "unrelated"}
    assert preserve_job_scheduler_identity("opaque-id", incoming) == incoming
    assert preserve_job_scheduler_identity("opaque-id", None, incoming) is None


@pytest.mark.parametrize("endpoint", ["/jobs", "/advice"])
@pytest.mark.parametrize("known,status", [(False, 400), (True, 409)])
def test_http_identity_conflict_precedes_recovery_and_persistence(endpoint, known, status):
    identity = _identity()
    record = create_example_slurm_job_record(job_id="4200_0", attempt_index=0)
    record.scheduler_metadata = identity
    record.heimdall_job_id = generate_heimdall_job_id("4200_0", 0)
    metadata = identity | {"slurm_array_task_id": "1"}
    payload = {"job_id": "4200_0", "scheduler_metadata": metadata}
    if endpoint == "/jobs":
        payload.update(job_attempt_index=0, state="FAILED", workflow_id="array-workflow")
    else:
        payload.update(id=str(record.heimdall_job_id), restart_count=0, advice={"state": "Restarted"})
    with patch("job_manager.main.job_manager") as manager:
        manager.storage.read_job_record.return_value = record if known else None
        manager.storage.read_advice_by_job.return_value = None
        response = TestClient(app).post(endpoint, json=payload)
        assert response.status_code == status, response.text
        manager.upsert_job_record.assert_not_called()
        manager.upsert_advice_record.assert_not_called()
        manager._generate_crash_event.assert_not_called()
        manager._begin_sequence_for_advice.assert_not_called()
        manager.update_job_record.assert_not_called()
        manager.audit_buffer.emit.assert_not_called()


@pytest.mark.parametrize("task", ["0", "1"])
def test_advice_partial_metadata_retains_task_identity_before_sequence(task):
    identity = _identity(task)
    job_id = identity["slurm_job_id"]
    job = create_example_slurm_job_record(job_id=job_id, attempt_index=0)
    job.scheduler_metadata = identity
    job.heimdall_job_id = generate_heimdall_job_id(job_id, 0)
    with patch("job_manager.main.job_manager") as manager:
        manager.storage.read_job_record.return_value = job
        manager.storage.read_advice_by_job.return_value = None
        manager.upsert_advice_record.side_effect = lambda record: (record, True, None)
        response = TestClient(app).post(
            "/advice",
            json={
                "id": str(job.heimdall_job_id),
                "job_id": job_id,
                "restart_count": 0,
                "scheduler_metadata": {"scheduler": "slurm", "diagnostic": "latest"},
                "advice": {"state": "Restarted"},
            },
        )
        assert response.status_code == 200, response.text
        expected = identity | {"diagnostic": "latest"}
        assert manager._begin_sequence_for_advice.call_args.args[0].scheduler_metadata == expected
        assert response.json()["schedulerMetadata"] == expected
        stored = manager.upsert_advice_record.call_args.args[0]
        assert isinstance(stored, AdviceRecord)
        assert stored.heimdall_id == job.heimdall_job_id


@pytest.mark.parametrize("conflict", [False, True])
def test_job_admission_preserves_or_rejects_advice_first_identity(conflict):
    """Advice can arrive before the first Job observation for the same task."""
    from datetime import UTC, datetime

    identity = _identity()
    existing = AdviceRecord(
        heimdall_id=generate_heimdall_job_id("4200_0", 0),
        job_id="4200_0",
        attempt_index=0,
        scheduler_metadata=identity,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    incoming = {"diagnostic": "latest"}
    if conflict:
        incoming["slurm_raw_job_id"] = "999"
    with patch("job_manager.main.job_manager") as manager:
        manager.storage.read_job_record.return_value = None
        manager.storage.read_advice_by_job.return_value = existing
        manager.upsert_job_record.side_effect = lambda record, **_: (record, True, None)
        response = TestClient(app).post(
            "/jobs",
            json={
                "job_id": "4200_0",
                "job_attempt_index": 0,
                "workflow_id": "array-workflow",
                "state": "RUNNING",
                "scheduler_metadata": incoming,
            },
        )
        assert response.status_code == (409 if conflict else 200), response.text
        if conflict:
            manager.upsert_job_record.assert_not_called()
            manager._generate_crash_event.assert_not_called()
        else:
            assert manager.upsert_job_record.call_args.args[0].scheduler_metadata == identity | incoming


@pytest.mark.parametrize("existing_kind", ["job", "advice"])
def test_job_queue_rejects_conflicts_before_terminal_side_effects(existing_kind):
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from job_manager.service import JobManager

    incoming = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="FAILED")
    incoming.scheduler_metadata = _identity() | {"slurm_raw_job_id": "999"}
    job = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="RUNNING")
    job.scheduler_metadata = _identity()
    advice = AdviceRecord(
        heimdall_id=generate_heimdall_job_id("4200_0", 0),
        job_id="4200_0",
        attempt_index=0,
        scheduler_metadata=_identity(),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    manager = MagicMock(spec=JobManager)
    manager.storage = MagicMock()
    manager.attribution_storage = MagicMock()
    manager._is_dag_support_enabled.return_value = False
    manager._is_advice_management_enabled.return_value = False
    manager.storage.read_job_record.return_value = job if existing_kind == "job" else None
    manager.storage.read_advice_by_job.return_value = advice if existing_kind == "advice" else None
    manager.storage.get_job_latest_attempt_index.return_value = 0 if existing_kind == "job" else -1

    JobManager.get_job_update_message_callback(manager)(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        incoming.to_json().encode(),
    )

    manager.storage.create_job_record.assert_not_called()
    manager.update_job_record.assert_not_called()
    manager._generate_crash_event.assert_not_called()
    manager.send_notification.assert_not_called()


def test_job_queue_partial_observation_preserves_task_identity():
    from unittest.mock import MagicMock

    from job_manager.service import JobManager

    incoming = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="RUNNING")
    incoming.scheduler_metadata = {"scheduler": "slurm", "diagnostic": "latest"}
    known = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="RUNNING")
    known.sticky_exclusion_node_list = []
    known.scheduler_metadata = _identity()
    manager = MagicMock(spec=JobManager)
    manager.storage = MagicMock()
    manager.attribution_storage = MagicMock()
    manager._is_dag_support_enabled.return_value = False
    manager._is_advice_management_enabled.return_value = False
    manager.storage.read_job_record.return_value = known
    manager.storage.read_advice_by_job.return_value = None
    manager.storage.get_job_latest_attempt_index.return_value = 0

    JobManager.get_job_update_message_callback(manager)(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        incoming.to_json().encode(),
    )

    assert manager.update_job_record.call_args.kwargs["scheduler_metadata"] == _identity() | incoming.scheduler_metadata


@pytest.mark.parametrize("writer", ["terminal", "dag"])
def test_internal_advice_conflict_does_not_call_recovery_callback(writer):
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from job_manager.job_mapping import storage_to_http_job_status
    from job_manager.storage_utils import create_terminal_advice_for_job, persist_dag_release_advice
    from recovery_libs.schema.job_manager.models import JobTerminalAdvice
    from recovery_libs.storage.scheduler_metadata import SchedulerMetadataConflictError

    job = create_example_slurm_job_record(job_id="4200_0", attempt_index=0)
    job.scheduler_metadata = _identity()
    advice = AdviceRecord(
        heimdall_id=generate_heimdall_job_id("4200_0", 0),
        job_id="4200_0",
        attempt_index=0,
        scheduler_metadata=_identity() | {"slurm_raw_job_id": "999"},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    storage = MagicMock()
    storage.read_job_record.return_value = job
    storage.read_advice_by_job.return_value = advice
    callback, audit = MagicMock(), MagicMock()
    if writer == "terminal":
        assert (
            create_terminal_advice_for_job(
                storage,
                audit_buffer=audit,
                job_id="4200_0",
                job_attempt_index=0,
                terminal_advice=JobTerminalAdvice.USER_CANCELED,
                status=storage_to_http_job_status(job),
                on_advice_ready=callback,
            )
            is None
        )
    else:
        with pytest.raises(SchedulerMetadataConflictError):
            persist_dag_release_advice(storage, job, audit_buffer=audit, on_advice_ready=callback)
    callback.assert_not_called()
    storage.upsert_advice.assert_not_called()
    audit.add.assert_not_called()


@pytest.mark.parametrize("metadata", [None, {}])
def test_queue_metadata_omission_keeps_partial_update_semantics(metadata):
    from unittest.mock import MagicMock

    from job_manager.service import JobManager

    incoming = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="RUNNING")
    incoming.scheduler_metadata = metadata
    known = create_example_slurm_job_record(job_id="4200_0", attempt_index=0, state="RUNNING")
    known.sticky_exclusion_node_list = []
    known.scheduler_metadata = _identity() | {"environment": "cluster-a", "managed_by": "controller"}
    manager = MagicMock(spec=JobManager)
    manager.storage = MagicMock()
    manager.attribution_storage = MagicMock()
    manager._is_dag_support_enabled.return_value = False
    manager._is_advice_management_enabled.return_value = False
    manager.storage.read_job_record.return_value = known
    manager.storage.read_advice_by_job.return_value = None
    manager.storage.get_job_latest_attempt_index.return_value = 0

    JobManager.get_job_update_message_callback(manager)(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        incoming.to_json().encode(),
    )

    assert manager.update_job_record.call_args.kwargs["scheduler_metadata"] == metadata
    assert manager.reconcile_anomaly_advice.call_args.args[0].scheduler_metadata == known.scheduler_metadata
