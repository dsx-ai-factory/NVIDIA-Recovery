# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""One real array keeps task recovery, checkpoint progress, and audit identity independent."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from e2e_harness import audit
from e2e_harness.actions import launch_array, restart, shared_directory, staging_directory, touch_file
from e2e_harness.checks import (
    advice_reason,
    audit_trail,
    gateway_audit_matches,
    job_state,
    recorded_attempt,
    task_checkpoint_audit,
    terminated_advice,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.retry_policy import eager_requeue_enabled, system_retry_limit
from e2e_harness.stages import LIMIT_SETTLE_SECONDS
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Step, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@pytest.fixture(autouse=True)
def retry_policy(
    e2e_config: E2EConfig, stack: StackControl, slurm_stack: StackControl, recovery: RecoveryClient
) -> Iterator[None]:
    """Use only backend Advice and a task policy cap of two, on the dedicated test stack."""

    if e2e_config.profile != "advisor-act":
        pytest.skip("array recovery requires the action-enabled advisor profile")
    if eager_requeue_enabled(slurm_stack):
        pytest.skip("array retry accounting requires eager requeue disabled")
    with system_retry_limit(
        stack,
        recovery,
        slurm_stack=slurm_stack,
        job_manager=e2e_config.workload_name("job-manager"),
        limit=-1,
        checkpoint_threshold=10,
    ):
        yield


@scenario
def test_task_recovery_survives_restart_without_spending_sibling_budget(
    e2e_config: E2EConfig, slurm_stack: StackControl
) -> Scenario[RecoveryClient]:
    gate_directory = staging_directory()
    prefix = f"{gate_directory}/fail"
    timeout = e2e_config.validation_timeout(600)
    task_zero_steps: list[Step[RecoveryClient]] = []
    for attempt in range(3):
        task_zero_steps.extend(
            (
                eventually(
                    f"task zero attempt {attempt} observed",
                    on(
                        "zero",
                        job_state(JobState.RUNNING, restarts=attempt),
                        recorded_attempt(attempt=attempt, state="RUNNING", checkpointed=attempt == 1),
                    ),
                    on("one", job_state(JobState.RUNNING, restarts=0), recorded_attempt(checkpointed=False)),
                    within=timeout,
                ),
                act(f"fail task zero attempt {attempt}", touch_file(f"{prefix}.0.{attempt}")),
            )
        )
        if attempt < 2:
            task_zero_steps.append(
                eventually(
                    f"task zero attempt {attempt} recovered independently",
                    on("zero", job_state(JobState.RUNNING, restarts=attempt + 1), _recovery_audit(attempt)),
                    on("one", job_state(JobState.RUNNING, restarts=0)),
                    within=timeout,
                )
            )
        if attempt == 0:
            task_zero_steps.extend(
                (
                    act("restart Job Manager", restart(e2e_config.workload_name("job-manager"))),
                    act("restart Slurm controller adapter", restart("slurm-advisor-controller", stack=slurm_stack)),
                )
            )
    return Scenario(
        name="58-job-array-recovery",
        setup=(
            act("stage owned failure gates", shared_directory(gate_directory)),
            act(
                "submit one two-task array",
                launch_array(
                    JobSpec(
                        name="job-array-recovery",
                        workload=slurm_batch(Path(__file__).with_name("job.sh")),
                        environment={
                            "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                            "FAILURE_GATE_PREFIX": prefix,
                        },
                        metadata=recovery_metadata(
                            SCHEDULER_BOUNDARY_RECOVERY,
                            {"max_requeue_times": 2, "log_file_paths_shared_across_attempts": True},
                        ),
                    ),
                    roles={"zero": 0, "one": 1},
                ),
            ),
        ),
        stages=(
            *task_zero_steps,
            eventually(
                "task zero exhausted its own budget",
                on(
                    "zero",
                    job_state(JobState.FAILED, restarts=2),
                    terminated_advice(attempt=2),
                    advice_reason("has reached the requeue limit of 2.", attempt=2),
                ),
                on("one", job_state(JobState.RUNNING, restarts=0)),
                within=timeout,
            ),
            act("fail sibling for its first time", touch_file(f"{prefix}.1.0")),
            eventually(
                "sibling retains its own retry allowance",
                on("one", job_state(JobState.RUNNING, restarts=1), _recovery_audit(0)),
                on("zero", job_state(JobState.FAILED, restarts=2)),
                within=timeout,
            ),
            eventually(
                "task and attempt scoped checkpoint and audit evidence",
                on(
                    "zero",
                    gateway_audit_matches(),
                    task_checkpoint_audit(saved_attempt=1),
                    *(recorded_attempt(attempt=i, checkpointed=i == 1) for i in range(3)),
                ),
                on(
                    "one",
                    gateway_audit_matches(),
                    task_checkpoint_audit(saved_attempt=None),
                    recorded_attempt(attempt=0, checkpointed=False),
                    recorded_attempt(attempt=1, checkpointed=False),
                ),
                within=e2e_config.watcher_evidence_timeout_seconds,
            ),
            consistently(
                "exhausted task stays stopped while sibling runs",
                on("zero", job_state(JobState.FAILED, restarts=2)),
                on("one", job_state(JobState.RUNNING, restarts=1)),
                for_seconds=LIMIT_SETTLE_SECONDS,
            ),
        ),
    )


def _recovery_audit(attempt: int):
    return audit_trail(
        lambda job: (
            audit.advice_reached_event(job, attempt=attempt, state="Restarted", cause="CRASH"),
            audit.scheduler_action_event(job, attempt=attempt, action="slurm.requeue_hold"),
            audit.scheduler_action_event(job, attempt=attempt, action="slurm.release"),
            audit.advice_reconciliation_converged_event(job, attempt=attempt),
        ),
        attempt=attempt,
    )
