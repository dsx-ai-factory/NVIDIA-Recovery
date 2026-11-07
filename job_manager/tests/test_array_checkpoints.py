# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Checkpoint task isolation through Job Manager's production event callback."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pika
import pytest
from fastapi.testclient import TestClient
from job_manager.main import app
from job_manager.service import JobManager
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import MySQLStorage

START = datetime(2026, 11, 1, tzinfo=UTC)


def task(index, attempt=0, *, raw=None):
    job_id = f"4200_{index}"
    return SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=attempt,
        workflow_id="arrays",
        last_updated_at=START,
        start_time=START + timedelta(hours=attempt),
        end_time=START + timedelta(hours=attempt + 1),
        heimdall_job_id=generate_heimdall_job_id(job_id, attempt),
        log_file_paths=["/logs/shared.log"],
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_job_id": job_id,
            "slurm_array_job_id": "4200",
            "slurm_array_task_id": str(index),
            "slurm_raw_job_id": raw or str(4200 + index),
        },
    )


def checkpoint(job_id="4200_0", *, kind=EventType.CHECKPOINTSAVE, attempt=0, metadata=None, hour=0.5):
    extras = {"slurm_job_attempt": attempt} if attempt is not None else {}
    if metadata is not None:
        extras["scheduler_metadata"] = metadata
    return Event(
        type=kind,
        timestamp=str(int((START + timedelta(hours=hour)).timestamp()) * 10**9),
        attributes=Attributes(job_id=job_id, uuid="source-checkpoint", log_file_name="shared.log", **extras),
    )


@pytest.fixture
def manager():
    manager = object.__new__(JobManager)
    manager.storage = MagicMock(spec=MySQLStorage)
    manager.storage.job_attempt_scope = MagicMock()
    manager.audit_buffer = MagicMock()
    manager.persist_anomaly_advice = MagicMock()
    manager._redeliver_watcher_event = MagicMock()
    manager.update_job_record = MagicMock(return_value=True)
    return manager


def deliver(manager, event):
    manager.get_watcher_job_event_message_callback()(
        MagicMock(), MagicMock(), pika.BasicProperties(), event.model_dump_json().encode()
    )


def configure_records(manager, records):
    def read(job_id, job_attempt_index=None):
        matches = [
            record
            for record in records
            if record.job_id == job_id and (job_attempt_index is None or record.job_attempt_index == job_attempt_index)
        ]
        return deepcopy(max(matches, key=lambda record: record.job_attempt_index)) if matches else None

    def page(*, scheduler_metadata=None, job_id=None, offset=0, limit=50, **_):
        matches = [
            record
            for record in records
            if (job_id is None or record.job_id == job_id)
            and all(
                (record.scheduler_metadata or {}).get(key) == value for key, value in (scheduler_metadata or {}).items()
            )
        ]
        matches.sort(key=lambda record: record.job_attempt_index, reverse=True)
        return deepcopy(matches[offset : offset + limit]), len(matches)

    manager.storage.read_job_record.side_effect = read
    manager.storage.read_job_records.side_effect = page


@pytest.mark.parametrize(
    "label,metadata",
    [
        ("4200_0", None),
        ("4200", {"slurm_array_job_id": "4200", "slurm_array_task_id": "0"}),
        ("4200", {"slurm_raw_job_id": "4200"}),
    ],
)
def test_save_resolves_task_zero_and_audits_only_after_success(manager, label, metadata):
    configure_records(manager, [task(0), task(1)])
    deliver(manager, checkpoint(label, metadata=metadata))
    updates = manager.update_job_record.call_args.kwargs
    assert updates["job_id"] == "4200_0"
    assert updates["job_attempt_index"] == 0
    assert len(updates["checkpoints"]) == 1
    audit = manager.audit_buffer.add.call_args.args[0]
    assert audit.heimdall_job_id == str(generate_heimdall_job_id("4200_0", 0))
    assert audit.payload.job_id == "4200_0"
    assert audit.payload.checkpoint_kind == "save"
    assert audit.payload.source_event_uuid == "source-checkpoint"
    assert audit.payload.event_occurred_at == int((START + timedelta(minutes=30)).timestamp()) * 10**6
    manager._redeliver_watcher_event.assert_not_called()


@pytest.mark.parametrize(
    "label,metadata",
    [
        ("4200", None),
        ("4200", {"slurm_job_id": "4200"}),
        ("4200_1", {"slurm_array_job_id": "4200", "slurm_array_task_id": "0"}),
        ("4200", {"slurm_array_job_id": "4200"}),
        ("4200", {"slurm_array_job_id": "4200", "slurm_array_task_id": 0}),
    ],
)
def test_ambiguous_or_conflicting_event_does_not_write_or_audit(manager, label, metadata):
    configure_records(manager, [task(0), task(1)])
    deliver(manager, checkpoint(label, metadata=metadata))
    manager.update_job_record.assert_not_called()
    manager.audit_buffer.add.assert_not_called()
    manager._redeliver_watcher_event.assert_called_once()


def test_raw_mapping_requires_unique_task_across_all_pages(manager):
    records = [task(0, attempt) for attempt in range(51)] + [task(1, raw="4200")]
    configure_records(manager, records)
    deliver(manager, checkpoint("4200", metadata={"slurm_raw_job_id": "4200"}))
    assert manager.storage.read_job_records.call_count == 2
    manager.update_job_record.assert_not_called()
    manager.audit_buffer.add.assert_not_called()


def test_explicit_unregistered_attempt_redelivers_then_matches_same_task(manager):
    records = [task(0), task(1, 1)]
    configure_records(manager, records)
    event = checkpoint("4200", attempt=1, metadata={"slurm_array_job_id": "4200", "slurm_array_task_id": "0"})
    deliver(manager, event)
    manager.update_job_record.assert_not_called()
    manager._redeliver_watcher_event.assert_called_once()
    records.append(task(0, 1))
    deliver(manager, event)
    assert manager.update_job_record.call_args.kwargs["job_id"] == "4200_0"
    assert manager.update_job_record.call_args.kwargs["job_attempt_index"] == 1


def test_delayed_checkpoint_uses_source_time_with_shared_path(manager):
    configure_records(manager, [task(0), task(0, 1), task(1)])
    deliver(manager, checkpoint(attempt=None))
    assert manager.update_job_record.call_args.kwargs["job_attempt_index"] == 0


def test_overlapping_attempts_are_not_disambiguated_by_shared_path(manager):
    first, second = task(0), task(0, 1)
    first.end_time = second.end_time
    configure_records(manager, [first, second])
    deliver(manager, checkpoint(attempt=None, hour=1.5))
    manager.update_job_record.assert_not_called()
    manager.audit_buffer.add.assert_not_called()
    manager._redeliver_watcher_event.assert_called_once()


def test_load_association_does_not_create_progress(manager):
    configure_records(manager, [task(0)])
    deliver(manager, checkpoint(kind=EventType.CHECKPOINTLOAD))
    manager.update_job_record.assert_not_called()
    assert manager.audit_buffer.add.call_args.args[0].payload.checkpoint_kind == "load"


def test_failed_save_has_no_success_audit_and_retry_keeps_stable_identity(manager):
    configure_records(manager, [task(0)])
    manager.update_job_record.side_effect = [RecoveryStorageException("unavailable"), True, True]
    event = checkpoint()
    deliver(manager, event)
    manager.audit_buffer.add.assert_not_called()
    manager._redeliver_watcher_event.assert_called_once()
    deliver(manager, event)
    deliver(manager, event)
    first, second = [call.args[0] for call in manager.audit_buffer.add.call_args_list]
    assert first.idempotency_key == second.idempotency_key
    manager.update_job_record.side_effect = None
    configure_records(manager, [task(1)])
    deliver(manager, checkpoint("4200_1"))
    sibling = manager.audit_buffer.add.call_args.args[0]
    assert sibling.idempotency_key != first.idempotency_key


def test_audit_delivery_failure_never_retries_checkpoint_write(manager):
    configure_records(manager, [task(0)])
    manager.audit_buffer.add.side_effect = RuntimeError("audit unavailable")
    deliver(manager, checkpoint())
    manager.update_job_record.assert_called_once()
    manager._redeliver_watcher_event.assert_not_called()
    manager.persist_anomaly_advice.assert_called_once()


@pytest.mark.parametrize("query", ["[]", "{}", '{"x":1}', '{"x":null}', "not-json"])
def test_metadata_query_rejects_malformed_filter_before_storage(query):
    with patch("job_manager.main.job_manager") as manager:
        response = TestClient(app).get("/jobs", params={"scheduler_metadata": query})
        assert response.status_code == 400
        manager.storage.read_job_records.assert_not_called()


def test_metadata_query_forwards_generic_filter_and_roundtrips_identity():
    with patch("job_manager.main.job_manager") as manager:
        manager.storage.read_job_records.return_value = ([task(0)], 1)
        response = TestClient(app).get(
            "/jobs",
            params={
                "scheduler_metadata": '{"scheduler":"slurm","slurm_raw_job_id":"4200"}',
                "limit": 1,
            },
        )
        assert response.status_code == 200
        assert response.json()["statuses"][0]["schedulerMetadata"]["slurm_array_task_id"] == "0"
        assert manager.storage.read_job_records.call_args.kwargs["scheduler_metadata"] == {
            "scheduler": "slurm",
            "slurm_raw_job_id": "4200",
        }


@pytest.mark.parametrize(
    "metadata",
    [
        {"slurm_array_job_id": "0", "slurm_array_task_id": "0"},
        {"slurm_array_job_id": None, "slurm_array_task_id": None},
        {"slurm_job_id": None},
        {"slurm_array_job_id": "4200", "slurm_array_task_id": "4294967294"},
        {"slurm_array_job_id": "4200", "slurm_array_task_id": True},
        {"slurm_array_job_id": "4200", "slurm_array_task_id": "00"},
        {"scheduler": "other", "slurm_array_job_id": "4200", "slurm_array_task_id": "0"},
        {"slurm_job_id": "4200_1", "slurm_array_job_id": "4200", "slurm_array_task_id": "0"},
    ],
)
def test_invalid_identity_has_no_checkpoint_or_audit(manager, metadata):
    configure_records(manager, [task(0)])
    deliver(manager, checkpoint("4200", metadata=metadata))
    manager.update_job_record.assert_not_called()
    manager.audit_buffer.add.assert_not_called()


def test_checkpoint_rereads_progress_inside_attempt_scope(manager):
    initial, after_lock = task(0), task(0)
    from recovery_libs.schema.job_manager.models import JobCheckpointInfo

    after_lock.checkpoints = [JobCheckpointInfo(timestamp=START, uuid="concurrent-save")]
    manager.storage.read_job_record.side_effect = [initial, after_lock]
    deliver(manager, checkpoint())
    assert [item.uuid for item in manager.update_job_record.call_args.kwargs["checkpoints"]] == [
        "concurrent-save",
        "source-checkpoint",
    ]
    manager.storage.job_attempt_scope.assert_called_once_with("4200_0", 0)


def test_shared_path_without_source_time_cannot_identify_task_attempt(manager):
    configure_records(manager, [task(0), task(0, 1)])
    event = checkpoint(attempt=None)
    event.timestamp = None
    deliver(manager, event)
    manager.update_job_record.assert_not_called()
    manager.audit_buffer.add.assert_not_called()


def test_raw_provenance_waits_for_registration(manager):
    records = []
    configure_records(manager, records)
    event = checkpoint("4200", metadata={"slurm_raw_job_id": "4200"})
    deliver(manager, event)
    manager.update_job_record.assert_not_called()
    manager._redeliver_watcher_event.assert_called_once()
    records.append(task(0))
    deliver(manager, event)
    assert manager.update_job_record.call_args.kwargs["job_id"] == "4200_0"
