# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A terminal parent decision closes and cancels the entire held DAG branch."""

from __future__ import annotations

import uuid
from pathlib import Path

from e2e_harness import audit
from e2e_harness.actions import cancel_job
from e2e_harness.checks import (
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_state,
    terminal_advice,
    terminated_advice,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Step, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

JOB_SCRIPT = Path(__file__).with_name("job.sh")


def _job(name: str, workflow: str, *, parent_id: str | None = None) -> JobSpec:
    aps: dict[str, object] = {"workflow_id": workflow}
    if parent_id is not None:
        aps |= {"dag_dependencies": [dependency_by_id(parent_id)], "dag_action": "release"}
    return JobSpec(
        name=name,
        workload=slurm_batch(JOB_SCRIPT, hold=parent_id is not None),
        metadata=recovery_metadata(SCHEDULER_BOUNDARY_RECOVERY, aps),
    )


@scenario
def test_cancelled_parent_terminates_held_child_and_grandchild(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    workflow = f"dag-e2e-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(300)
    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    dependents = (("child", "parent"), ("grandchild", "child"))
    return Scenario(
        name="35-dag-parent-termination",
        jobs={
            "parent": _job("dag-parent", workflow),
            "child": lambda run: _job("dag-child", workflow, parent_id=run.jobs["parent"].job_id),
            "grandchild": lambda run: _job("dag-grandchild", workflow, parent_id=run.jobs["child"].job_id),
        },
        stages=(
            eventually(
                "branch gated",
                on("parent", job_state(JobState.RUNNING)),
                on("child", held()),
                on("grandchild", held()),
                *(
                    on(
                        role,
                        gates(gate_on_job(parent), status="not_eligible", reason_code="WAITING_FOR_PARENT_COMPLETION"),
                    )
                    for role, parent in dependents
                ),
                within=within,
            ),
            act("cancel parent", cancel_job("parent")),
            eventually(
                "parent terminated",
                on("parent", job_state(JobState.CANCELLED, restarts=0), terminal_advice()),
                within=evidence_window,
            ),
            eventually(
                "branch aborted",
                *(
                    on(
                        role,
                        gates(gate_on_job(parent), status="aborted", reason_code="PARENT_TERMINATED"),
                        terminated_advice(attempt=0),
                    )
                    for role, parent in dependents
                ),
                within=evidence_window,
            ),
            *(_branch_cancelled(within) if e2e_config.profile == "advisor-act" else _branch_left_pending(e2e_config)),
            eventually(
                "parent audited",
                on(
                    "parent",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                            audit.job_state_transition_event(
                                job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="CANCELLED"
                            ),
                            audit.advice_reached_event(job, state="Terminated", cause="user_canceled", visible=True),
                            audit.advice_reconciliation_converged_event(job),
                        )
                    ),
                ),
                within=90,
            ),
        ),
    )


def _branch_cancelled(within: float) -> tuple[Step[RecoveryClient], ...]:
    """With actions enabled, Heimdall cancels each held dependent."""

    return (
        eventually(
            "branch cancelled",
            *(on(role, job_state(JobState.CANCELLED, restarts=0)) for role in ("child", "grandchild")),
            within=within,
        ),
        eventually(
            "branch audited",
            *(
                on(
                    role,
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
                )
                for role in ("child", "grandchild")
            ),
            within=90,
        ),
    )


def _branch_left_pending(e2e_config: E2EConfig) -> tuple[Step[RecoveryClient], ...]:
    """In observe mode, Heimdall records the cancellation it would apply but leaves the jobs pending."""

    return (
        consistently(
            "branch left pending",
            *(on(role, job_state(JobState.PENDING)) for role in ("child", "grandchild")),
            for_seconds=2 * e2e_config.recovery_poll_interval_seconds,
        ),
        eventually(
            "branch audited",
            *(
                on(
                    role,
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": "PENDING"}),
                            audit.advice_reached_event(
                                job, state="Terminated", cause=audit.DAG_TERMINATION_ADVICE_REASON, visible=True
                            ),
                            audit.advice_application_evaluated_event(job, action="slurm.cancel"),
                        )
                    ),
                )
                for role in ("child", "grandchild")
            ),
            within=90,
        ),
    )
