# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise independent array-task retry allowances against the real MySQL store."""

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import job_manager.recovery as recovery_module
import pytest
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.storage_utils import maybe_persist_requeue_terminal_advice, persist_job_record
from opentelemetry.sdk.trace import TracerProvider
from recovery_libs.advice import check_max_requeue_reached
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.job_manager.models import SystemRequeueConfig
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import MySQLStorage


@pytest.fixture(autouse=True)
def recovery_tracer(monkeypatch):
    provider = TracerProvider()
    monkeypatch.setattr(recovery_module, "_TRACER", provider.get_tracer(__name__))
    yield
    provider.shutdown()


def _task(parent, task=0, attempt=0):
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
            "slurm_raw_job_id": str(parent + task),
            "slurm_array_job_id": str(parent),
            "slurm_array_task_id": str(task),
        },
        aps_spec=ApsSlurmJobSpec(auto_resume_mode=AutoResumeModeEnum.REQUEUE, max_requeue_times=2),
    )


def _parent():
    return 100000 + uuid.uuid4().int % 1000000000


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
