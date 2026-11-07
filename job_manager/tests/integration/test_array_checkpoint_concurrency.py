# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Concurrent checkpoint deliveries preserve task progress through the real MySQL boundary."""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pika
from job_manager.service import JobManager
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord


def test_concurrent_checkpoint_callbacks_preserve_saves_and_replay_identity(storage, monkeypatch):
    """Independent saves must survive serialized read/append/write, without touching another task or attempt."""
    parent = str(100000 + uuid.uuid4().int % 1000000000)
    target = f"{parent}_0"
    sibling = f"{parent}_1"
    for task, attempt in ((0, 0), (0, 1), (1, 1)):
        job_id = f"{parent}_{task}"
        storage.create_job_record(
            SlurmJobExecutionRecord(
                job_id=job_id,
                job_attempt_index=attempt,
                workflow_id=parent,
                heimdall_job_id=generate_heimdall_job_id(job_id, attempt),
                last_updated_at=datetime.now(UTC),
                scheduler_metadata={
                    "scheduler": "slurm",
                    "slurm_job_id": job_id,
                    "slurm_array_job_id": parent,
                    "slurm_array_task_id": str(task),
                },
            )
        )

    manager = object.__new__(JobManager)
    manager.storage = storage
    manager.audit_buffer = MagicMock()
    manager.persist_anomaly_advice = MagicMock()
    manager._redeliver_watcher_event = MagicMock()
    callback = manager.get_watcher_job_event_message_callback()
    source_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    events = [
        Event(
            type=EventType.CHECKPOINTSAVE,
            timestamp=str(int(datetime.now(UTC).timestamp()) * 10**9),
            attributes=Attributes(job_id=target, slurm_job_attempt=1, uuid=source_id),
        )
        for source_id in source_ids
    ]

    def deliver(event):
        callback(MagicMock(), MagicMock(), pika.BasicProperties(), event.model_dump_json().encode())

    # Both callbacks must resolve their initial snapshots before either enters
    # the production named lock. The actual storage scope, reread, and writes
    # remain active; the barrier only makes the competing delivery reproducible.
    ready = threading.Barrier(2, timeout=10)
    actual_scope = storage.job_attempt_scope

    @contextmanager
    def concurrent_scope(job_id, attempt_index):
        ready.wait()
        with actual_scope(job_id, attempt_index):
            yield

    with monkeypatch.context() as patcher:
        patcher.setattr(storage, "job_attempt_scope", concurrent_scope)
        with ThreadPoolExecutor(max_workers=2) as executor:
            pending = [executor.submit(deliver, event) for event in events]
            for result in pending:
                result.result(timeout=20)

    persisted = storage.read_job_record(target, 1)
    assert {checkpoint.uuid for checkpoint in persisted.checkpoints} == set(source_ids)
    assert len(persisted.checkpoints) == 2
    deliver(events[0])
    replayed = storage.read_job_record(target, 1)
    assert replayed.checkpoints == persisted.checkpoints
    assert not storage.read_job_record(target, 0).checkpoints
    assert not storage.read_job_record(sibling, 1).checkpoints
    manager._redeliver_watcher_event.assert_not_called()
    assert manager.persist_anomaly_advice.call_count == 3

    audits = [call.args[0] for call in manager.audit_buffer.add.call_args_list]
    assert len(audits) == 3
    for event in audits:
        assert event.event_type == "checkpoint_associated"
        assert event.heimdall_job_id == str(generate_heimdall_job_id(target, 1))
        assert event.payload.job_id == target
        assert event.payload.attempt_index == 1
        assert event.payload.checkpoint_kind == "save"
    replay_audits = [event for event in audits if event.payload.source_event_uuid == source_ids[0]]
    assert len(replay_audits) == 2
    assert replay_audits[0].idempotency_key == replay_audits[1].idempotency_key
    assert len({event.idempotency_key for event in audits}) == 2
    assert {event.payload.source_event_uuid for event in audits} == set(source_ids)
