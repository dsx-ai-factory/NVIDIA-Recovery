# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exhausting a parent's effective requeue limit cancels its held child."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.checks import (
    accounted,
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_id_of,
    job_state,
    no_advice_except,
    restarted,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_TERMINATED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.stages import LIMIT_SETTLE_SECONDS
from e2e_harness.workloads import JOB_STATE_CRASH_RECOVERY, dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@pytest.mark.nightly
@scenario
def test_exhausted_parent_limit_terminates_child_without_child_requeue(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-parent-limit-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(720)
    parent_stopped = (job_state(JobState.FAILED, restarts=1), accounted(JobState.FAILED, restarts=1))
    child_aborted = (
        advice_state(ADVICE_STATE_TERMINATED),
        gates(
            gate_on_job("parent"),
            status="aborted",
            reason_code="PARENT_TERMINATED",
            reason_details={
                "parent_job_id": job_id_of("parent"),
                "parent_terminal_advice_reason": "max_requeue_reached",
            },
        ),
    )
    return Scenario(
        name="38-dag-parent-requeue-limit",
        jobs={
            "parent": dag_job(
                "dag-exhausted-parent",
                workflow=workflow,
                runtime=20,
                behavior="fail-always",
                policy=JOB_STATE_CRASH_RECOVERY,
                aps={"max_requeue_times": 1},
            ),
            "child": lambda run: dag_job(
                "dag-exhausted-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually("child held", on("child", held()), within=within),
            eventually("parent restarted", on("parent", restarted(times=1)), within=within),
            # The parent's first, retryable failure must not abort the child.
            consistently("child still held", on("child", held(), no_advice_except()), for_seconds=10),
            eventually(
                "parent stopped at limit",
                on("parent", *parent_stopped, advice_state(ADVICE_STATE_TERMINATED, attempt=1)),
                within=within,
            ),
            consistently("parent stays stopped", on("parent", *parent_stopped), for_seconds=LIMIT_SETTLE_SECONDS),
            eventually(
                "child terminated", on("child", *child_aborted), within=e2e_config.watcher_evidence_timeout_seconds
            ),
            eventually(
                "child cancelled, never requeued", on("child", job_state(JobState.CANCELLED, restarts=0)), within=within
            ),
            # Neither job may come back: no parent successor, and the child's decision does not flap.
            consistently(
                "branch stays stopped",
                on("parent", job_state(JobState.FAILED, restarts=1)),
                on("child", job_state(JobState.CANCELLED, restarts=0), *child_aborted),
                for_seconds=2 * e2e_config.recovery_poll_interval_seconds,
            ),
            eventually(
                "audited",
                on(
                    "parent",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                            audit.job_state_transition_event(
                                job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                            ),
                            audit.advice_reached_event(job, state="Restarted", cause="CRASH"),
                            audit.scheduler_action_event(job, action="slurm.requeue_hold"),
                            audit.scheduler_action_event(job, action="slurm.release"),
                            audit.advice_application_applied_event(job),
                            audit.advice_reconciliation_converged_event(job),
                        )
                    ),
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, attempt=1),
                            audit.job_started_event(job, attempt=1),
                            audit.job_state_transition_event(
                                job, attempt=1, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                            ),
                            audit.advice_reached_event(job, attempt=1, state="Terminated", cause="max_requeue_reached"),
                            audit.advice_reconciliation_converged_event(job, attempt=1),
                        ),
                        attempt=1,
                    ),
                ),
                on(
                    "child",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": "PENDING"}),
                            audit.advice_reached_event(
                                job, state="Terminated", cause=audit.DAG_TERMINATION_ADVICE_REASON, visible=True
                            ),
                            audit.scheduler_action_event(job, action="slurm.cancel"),
                            audit.job_state_transition_event(job, old_states=("PENDING",), new_state="CANCELLED"),
                            audit.advice_application_applied_event(job),
                            audit.advice_reconciliation_converged_event(job),
                        )
                    ),
                ),
                within=90,
            ),
        ),
    )
