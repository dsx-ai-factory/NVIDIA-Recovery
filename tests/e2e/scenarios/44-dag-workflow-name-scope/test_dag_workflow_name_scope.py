# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Name dependencies resolve only to the exact parent in the child's workflow."""

from __future__ import annotations

import uuid

from e2e_harness import audit
from e2e_harness.actions import cancel_job
from e2e_harness.checks import advice_state, audit_trail, gate_on_name, gates, held, job_state, no_advice_except
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, ADVICE_STATE_TERMINATED, dependency_by_name
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_same_name_parent_in_another_workflow_cannot_release_or_terminate_child(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    intended_workflow = f"dag-name-intended-{uuid.uuid4().hex}"
    other_workflow = f"dag-name-other-{uuid.uuid4().hex}"
    shared_name = f"dag-shared-parent-{uuid.uuid4().hex[:8]}"
    parent_gate = gate_on_name(shared_name)
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="44-dag-workflow-name-scope",
        jobs={
            "intended_parent": dag_job(shared_name, workflow=intended_workflow, runtime=50),
            "other_parent": dag_job(shared_name, workflow=other_workflow, runtime=300, behavior="long-running"),
            "child": dag_job(
                "dag-name-scoped-child",
                workflow=intended_workflow,
                runtime=5,
                depends_on=(dependency_by_name(shared_name),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "child gated",
                on("other_parent", job_state(JobState.RUNNING)),
                on("child", held(), gates(parent_gate, status="not_eligible")),
                within=within,
            ),
            act("cancel same-name parent in another workflow", cancel_job("other_parent")),
            eventually(
                "other parent terminated",
                on("other_parent", job_state(JobState.CANCELLED, restarts=0), advice_state(ADVICE_STATE_TERMINATED)),
                within=within,
            ),
            consistently("child unaffected", on("child", held(), no_advice_except()), for_seconds=10),
            eventually(
                "intended parent completed",
                on("intended_parent", job_state(JobState.COMPLETED, restarts=0)),
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
                    "other_parent",
                    audit_trail(
                        lambda job: (
                            audit.job_state_transition_event(
                                job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="CANCELLED"
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
