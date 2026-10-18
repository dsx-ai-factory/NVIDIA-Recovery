# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Checkpoint stopping and numeric retry caps independently bound recovery."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from e2e_harness import audit
from e2e_harness.actions import shared_directory, staging_directory, touch_file
from e2e_harness.checks import (
    accounted,
    advice,
    advice_reason,
    audit_trail,
    job_state,
    recorded_attempt,
    requeue_notified,
    restarted,
    state_based_crash,
    terminated_advice,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.retry_policy import eager_requeue_enabled, system_retry_limit
from e2e_harness.stages import LIMIT_SETTLE_SECONDS, STATE_CRASH_MARKER
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Step, act, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive

CHECKPOINT_STOP = "checkpoint in the last 3 consecutive qualified failed attempts"
REQUEUE_LIMIT_OF_ONE = "has reached the requeue limit of 1."


@pytest.fixture(autouse=True)
def retry_policy(
    request: pytest.FixtureRequest,
    e2e_config: E2EConfig,
    stack: StackControl,
    slurm_stack: StackControl,
    recovery: RecoveryClient,
) -> Iterator[None]:
    """Apply the test's `retry_policy` marker to Job Manager for the duration of the test."""

    if e2e_config.profile != "advisor-act":
        pytest.skip("checkpoint-stop enforcement requires the action-enabled advisor profile")
    if eager_requeue_enabled(slurm_stack):
        pytest.skip("retry-policy enforcement requires eager requeue to be disabled")
    policy = request.node.get_closest_marker("retry_policy")
    with system_retry_limit(
        stack,
        recovery,
        slurm_stack=slurm_stack,
        job_manager=e2e_config.workload_name("job-manager"),
        limit=policy.kwargs["system_limit"],
        checkpoint_threshold=3,
    ):
        yield


@scenario
@pytest.mark.retry_policy(system_limit=-1)
def test_three_total_failures_without_numeric_caps(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return checkpoint_stop(
        e2e_config, checkpoint_attempt=None, job_limit=None, final_attempt=2, stop_reason=CHECKPOINT_STOP
    )


@scenario
@pytest.mark.retry_policy(system_limit=10)
def test_saved_checkpoint_resets_failure_count(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return checkpoint_stop(
        e2e_config, checkpoint_attempt=0, job_limit=None, final_attempt=3, stop_reason=CHECKPOINT_STOP
    )


@scenario
@pytest.mark.retry_policy(system_limit=1)
def test_saved_checkpoint_does_not_reset_system_cap(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return checkpoint_stop(
        e2e_config, checkpoint_attempt=1, job_limit=None, final_attempt=1, stop_reason=REQUEUE_LIMIT_OF_ONE
    )


@scenario
@pytest.mark.retry_policy(system_limit=-1)
def test_saved_checkpoint_does_not_reset_job_cap(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return checkpoint_stop(
        e2e_config, checkpoint_attempt=1, job_limit=1, final_attempt=1, stop_reason=REQUEUE_LIMIT_OF_ONE
    )


def checkpoint_stop(
    e2e_config: E2EConfig,
    *,
    checkpoint_attempt: int | None,
    job_limit: int | None,
    final_attempt: int,
    stop_reason: str,
) -> Scenario[RecoveryClient]:
    """Fail every attempt through `final_attempt`, saving a checkpoint only in `checkpoint_attempt`."""

    # A unique staging directory owns this run's failure gates and is removed when the scenario ends.
    gate_directory = staging_directory()
    gate_prefix = f"{gate_directory}/job.sh.fail"
    attempts = tuple(
        step
        for attempt in range(final_attempt + 1)
        for step in _fail_attempt(e2e_config, attempt, gate_prefix, attempt == checkpoint_attempt, final_attempt)
    )
    stopped = (
        job_state(JobState.FAILED, restarts=final_attempt),
        accounted(JobState.FAILED, restarts=final_attempt),
    )
    return Scenario(
        name="55-checkpoint-stop-guardrail",
        job=JobSpec(
            name="checkpoint-stop-guardrail",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "FAILURE_GATE_PREFIX": gate_prefix,
                "CHECKPOINT_ATTEMPT": str(-1 if checkpoint_attempt is None else checkpoint_attempt),
            },
            metadata=recovery_metadata(
                SCHEDULER_BOUNDARY_RECOVERY,
                # Slurm reuses slurm-<jobid>.out on every requeue; declaring that keeps a later
                # checkpoint save from being attributed to attempt 0.
                {"max_requeue_times": job_limit, "log_file_paths_shared_across_attempts": True},
            ),
        ),
        stages=(
            act("stage gate directory", shared_directory(gate_directory)),
            *attempts,
            eventually(
                "stopped",
                *stopped,
                terminated_advice(attempt=final_attempt),
                within=e2e_config.watcher_evidence_timeout_seconds,
            ),
            eventually(
                "termination audited",
                audit_trail(
                    lambda job: (
                        audit.advice_reached_event(
                            job, attempt=final_attempt, state="Terminated", cause="max_requeue_reached"
                        ),
                        audit.advice_reconciliation_converged_event(job, attempt=final_attempt),
                    ),
                    attempt=final_attempt,
                ),
                advice_reason(stop_reason, attempt=final_attempt),
                within=90,
            ),
            # Persisted history decides which consecutive failures qualify, so a misattributed or replayed
            # checkpoint must not appear on any other attempt.
            eventually(
                "checkpoint history",
                *(
                    recorded_attempt(attempt=attempt, state="FAILED", checkpointed=attempt == checkpoint_attempt)
                    for attempt in range(final_attempt + 1)
                ),
                within=60,
            ),
            # A requeue-held successor would also break the stop, even before it runs.
            consistently("stays stopped", *stopped, for_seconds=LIMIT_SETTLE_SECONDS),
        ),
    )


def _fail_attempt(
    e2e_config: E2EConfig, attempt: int, gate_prefix: str, checkpointed: bool, final_attempt: int
) -> tuple[Step[RecoveryClient], ...]:
    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    observed = (
        eventually(
            f"attempt {attempt} running",
            job_state(JobState.RUNNING, restarts=attempt),
            recorded_attempt(attempt=attempt, state="RUNNING", checkpointed=checkpointed),
            within=e2e_config.validation_timeout(600),
        ),
        # Trigger an application exit, not scheduler cancellation.
        act(f"fail attempt {attempt}", touch_file(f"{gate_prefix}.{attempt}")),
    )
    if attempt == final_attempt:
        return observed
    return (
        *observed,
        eventually(
            f"attempt {attempt} restarted", restarted(times=attempt + 1), within=e2e_config.validation_timeout(600)
        ),
        eventually(
            f"attempt {attempt} recovered",
            state_based_crash(attempt=attempt),
            advice(issue_type="CRASH", marker=STATE_CRASH_MARKER, source="Heimdall Job Manager", attempt=attempt),
            requeue_notified(anomaly_type="CRASH", attempt=attempt),
            within=evidence_window,
        ),
        eventually(
            f"attempt {attempt} audited",
            audit_trail(
                lambda job: (
                    audit.advice_reached_event(job, attempt=attempt, state="Restarted", cause="CRASH"),
                    audit.scheduler_action_event(job, attempt=attempt, action="slurm.requeue_hold"),
                    audit.scheduler_action_event(job, attempt=attempt, action="slurm.release"),
                    audit.advice_reconciliation_converged_event(job, attempt=attempt),
                ),
                attempt=attempt,
            ),
            within=90,
        ),
    )
