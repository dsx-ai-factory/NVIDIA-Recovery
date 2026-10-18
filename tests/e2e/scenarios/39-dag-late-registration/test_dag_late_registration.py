# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A child registered after a final parent reconciles from persisted state."""

from __future__ import annotations

import uuid
from collections.abc import Callable

from e2e_harness import audit
from e2e_harness.actions import cancel_job
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    job_id_of,
    job_state,
    recorded_attempt,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, ADVICE_STATE_TERMINATED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Run, Scenario, act, eventually, on, submit
from recovery_libs.testing.scenario_plugin import scenario


def _late_child(name: str, workflow: str) -> Callable[[Run[RecoveryClient]], JobSpec]:
    def build(run: Run[RecoveryClient]) -> JobSpec:
        return dag_job(
            name,
            workflow=workflow,
            runtime=5,
            depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
            action="release",
            hold=True,
        )

    return build


@scenario
def test_child_registered_after_completed_parent_releases_immediately(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-late-complete-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(360)
    return Scenario(
        name="39-dag-late-registration",
        jobs={"parent": dag_job("dag-late-complete-parent", workflow=workflow, runtime=10)},
        stages=(
            eventually(
                "parent completed and recorded",
                on("parent", job_state(JobState.COMPLETED, restarts=0), recorded_attempt(state="COMPLETED")),
                within=within,
            ),
            # The child is registered only after its parent is final: what this scenario tests.
            act("register child late", submit("child", _late_child("dag-late-complete-child", workflow))),
            eventually(
                "child released",
                on("child", advice_state(ADVICE_STATE_RELEASED), gates(gate_on_job("parent"), status="successful")),
                within=within,
            ),
            eventually("child completed", on("child", job_state(JobState.COMPLETED, restarts=0)), within=within),
            eventually(
                "audited",
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
        ),
    )


@scenario
def test_child_registered_after_terminated_parent_cancels_immediately(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-late-terminated-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="39-dag-late-registration",
        jobs={"parent": dag_job("dag-late-terminated-parent", workflow=workflow, runtime=300, behavior="long-running")},
        stages=(
            # Slurm running does not imply Job Manager has persisted the attempt.
            eventually(
                "parent running and recorded",
                on("parent", job_state(JobState.RUNNING), recorded_attempt(state="RUNNING")),
                within=within,
            ),
            act("cancel parent", cancel_job("parent")),
            eventually(
                "parent terminated",
                on("parent", job_state(JobState.CANCELLED, restarts=0), advice_state(ADVICE_STATE_TERMINATED)),
                within=within,
            ),
            # The child is registered only after its parent is final: what this scenario tests.
            act("register child late", submit("child", _late_child("dag-late-terminated-child", workflow))),
            eventually(
                "child terminated",
                on(
                    "child",
                    advice_state(ADVICE_STATE_TERMINATED),
                    gates(
                        gate_on_job("parent"),
                        status="aborted",
                        reason_code="PARENT_TERMINATED",
                        reason_details={
                            "parent_job_id": job_id_of("parent"),
                            "parent_terminal_advice_reason": "user_canceled",
                        },
                    ),
                ),
                within=within,
            ),
            eventually("child cancelled", on("child", job_state(JobState.CANCELLED, restarts=0)), within=within),
            eventually(
                "audited",
                on(
                    "parent",
                    audit_trail(
                        lambda job: (
                            audit.job_state_transition_event(
                                job, old_states=("RUNNING", "COMPLETING"), new_state="CANCELLED"
                            ),
                            audit.advice_reached_event(job, state="Terminated", cause="user_canceled", visible=True),
                            audit.advice_reconciliation_converged_event(job),
                        )
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
