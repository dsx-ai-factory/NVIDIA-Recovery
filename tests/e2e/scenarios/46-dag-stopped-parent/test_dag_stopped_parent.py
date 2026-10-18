# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A production STOPPED parent upsert terminates its held child."""

from __future__ import annotations

import uuid

from e2e_harness import audit
from e2e_harness.actions import post_job_state
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_id_of,
    job_state,
    recorded_attempt,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_TERMINATED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, act, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_stopped_parent_production_ingress_terminates_child(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    """Slinky has no STOPPED command, so exercise the deployed scheduler-state ingress."""

    workflow = f"dag-stopped-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="46-dag-stopped-parent",
        jobs={
            "parent": dag_job("dag-stopped-parent", workflow=workflow, runtime=300, behavior="long-running"),
            "child": lambda run: dag_job(
                "dag-stopped-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "parent running, child gated",
                # The audit expects a RUNNING-to-STOPPED transition, so Job Manager must have recorded RUNNING first.
                on("parent", job_state(JobState.RUNNING), recorded_attempt(state="RUNNING")),
                on("child", held(), gates(gate_on_job("parent"), status="not_eligible")),
                within=within,
            ),
            act("stop the parent", post_job_state("parent", state="STOPPED", reason="e2e-production-ingress-stop")),
            eventually(
                "terminated",
                on("parent", advice_state(ADVICE_STATE_TERMINATED)),
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
                on(
                    "parent",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                            audit.job_state_transition_event(job, old_states=("RUNNING",), new_state="STOPPED"),
                            audit.advice_reached_event(job, state="Terminated", cause="user_canceled", visible=True),
                            audit.scheduler_action_event(job, action="slurm.cancel"),
                            audit.job_state_transition_event(
                                job,
                                old_states=("STOPPED", "RUNNING"),
                                new_state="CANCELLED",
                                name="job-cancelled-after-stop",
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
