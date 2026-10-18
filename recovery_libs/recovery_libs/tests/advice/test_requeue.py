# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

from recovery_libs.advice.requeue import check_max_requeue_reached
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.job_manager.constants import INFINITE_SYSTEM_REQUEUE_LIMIT
from recovery_libs.schema.job_manager.models import JobCheckpointInfo, SlurmJobExecutionStatus, SystemRequeueConfig
from recovery_libs.storage.dataclass import SlurmJobFinalStates
from recovery_libs.tests.advice.helpers import random_job_execution_status


def test_check_max_requeue_reached_when_under_limit():
    status = random_job_execution_status(job_id="12345", job_attempt_index=1)
    status.aps_spec = ApsSlurmJobSpec(max_requeue_times=5)

    reached, _ = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=10),
        get_jobs=lambda **kwargs: [],
    )

    assert reached is False


def test_check_max_requeue_reached_when_attempt_exceeds_limit():
    status = random_job_execution_status(job_id="12345", job_attempt_index=3)
    status.aps_spec = ApsSlurmJobSpec(max_requeue_times=3)

    reached, message = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=10),
        get_jobs=lambda **kwargs: [],
    )

    assert reached is True
    assert "requeue limit" in message


def test_check_max_requeue_reached_completed_when_requeue_on_completion_disabled():
    status = random_job_execution_status(job_id="12345", job_attempt_index=0, state=SlurmJobFinalStates.COMPLETED)
    status.aps_spec = ApsSlurmJobSpec(requeue_on_completion=False)

    reached, message = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(requeue_on_completion=True),
        get_jobs=lambda **kwargs: [],
    )

    assert reached is True
    assert "COMPLETED" in message


def test_check_max_requeue_reached_unlimited_system_limit():
    status = random_job_execution_status(job_id="12345", job_attempt_index=99)

    reached, _ = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=INFINITE_SYSTEM_REQUEUE_LIMIT),
        get_jobs=lambda **kwargs: [],
    )

    assert reached is False


def test_check_max_requeue_reached_checkpoint_early_stop():
    status = random_job_execution_status(job_id="12345", job_attempt_index=2)
    attempts = [
        random_job_execution_status(job_id="12345", job_attempt_index=2, state="FAILED"),
        random_job_execution_status(job_id="12345", job_attempt_index=1, state="FAILED"),
    ]

    reached, message = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(
            max_requeue_times=10,
            checkpoint_based_requeue_stop_threshold=2,
        ),
        get_jobs=lambda **kwargs: attempts,
    )

    assert reached is True
    assert "checkpoint" in message.lower()


def test_check_max_requeue_reached_checkpoint_found_breaks_sequence():
    status = random_job_execution_status(job_id="12345", job_attempt_index=2)
    attempts = [
        random_job_execution_status(job_id="12345", job_attempt_index=2, state="FAILED"),
        SlurmJobExecutionStatus(
            job_id="12345",
            job_attempt_index=1,
            workflow_id="workflow-test",
            state="FAILED",
            job_name="job-name",
            submit_time="2024-01-02 00:00:00",  # type: ignore[arg-type]
            allocated_node_list=["rank0_hostname"],
            user="test_user",
            checkpoints=[JobCheckpointInfo(timestamp=datetime.now(UTC), uuid="cp-1")],
        ),
    ]

    reached, _ = check_max_requeue_reached(
        status,
        system_requeue_config=SystemRequeueConfig(
            max_requeue_times=10,
            checkpoint_based_requeue_stop_threshold=2,
        ),
        get_jobs=lambda **kwargs: attempts,
    )

    assert reached is False
