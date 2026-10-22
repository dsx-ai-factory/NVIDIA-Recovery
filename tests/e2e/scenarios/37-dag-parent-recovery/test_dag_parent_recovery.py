# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A retryable parent failure keeps its child held until the parent recovers."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_state,
    no_advice_except,
    restarted,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.stages import recovery_telemetry
from e2e_harness.workloads import JOB_STATE_CRASH_RECOVERY, dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@pytest.mark.nightly
@scenario
def test_retryable_parent_failure_does_not_abort_child(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    workflow = f"dag-parent-recovery-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(600)
    parent_gate = gate_on_job("parent")
    return Scenario(
        name="37-dag-parent-recovery",
        jobs={
            "parent": dag_job(
                "dag-retryable-parent",
                workflow=workflow,
                runtime=20,
                behavior="fail-once",
                policy=JOB_STATE_CRASH_RECOVERY,
                aps={"max_requeue_times": 1},
            ),
            "child": lambda run: dag_job(
                "dag-retryable-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually("child gated", on("child", held(), gates(parent_gate, status="not_eligible")), within=within),
            eventually("parent restarted", on("parent", restarted(times=1)), within=within),
            # A retryable failure must not open or abort the child's gate.
            consistently("child still held", on("child", held(), no_advice_except()), for_seconds=10),
            eventually(
                "parent recovered",
                on("parent", job_state(JobState.COMPLETED, restarts=1)),
                within=within,
            ),
            eventually(
                "child released",
                on("child", advice_state(ADVICE_STATE_RELEASED), gates(parent_gate, status="successful")),
                within=within,
            ),
            eventually("child completed", on("child", job_state(JobState.COMPLETED, restarts=0)), within=within),
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
                                job, attempt=1, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="COMPLETED"
                            ),
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
                                job, state="Released", cause=audit.DAG_RELEASE_ADVICE_REASON, visible=True
                            ),
                            audit.scheduler_action_event(job, action="slurm.release"),
                            audit.job_started_event(job),
                            audit.job_state_transition_event(
                                job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="COMPLETED"
                            ),
                            audit.advice_application_applied_event(job),
                            audit.advice_reconciliation_converged_event(job),
                        )
                    ),
                ),
                within=90,
            ),
            *recovery_telemetry(e2e_config, operation="dag_release", role="child"),
        ),
    )
