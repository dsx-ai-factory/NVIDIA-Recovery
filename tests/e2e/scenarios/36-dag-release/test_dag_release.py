# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A held child opens only after its mixed name and ID parent gates complete."""

from __future__ import annotations

import uuid

from e2e_harness import audit
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    component_log,
    gate_on_job,
    gate_on_name,
    gates,
    held,
    job_state,
    no_advice_except,
    started_after,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, dependency_by_id, dependency_by_name
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_mixed_parent_gate_releases_only_after_both_parents_complete(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-release-{uuid.uuid4().hex}"
    parent_name = f"dag-name-parent-{uuid.uuid4().hex[:8]}"
    parent_gates = (gate_on_name(parent_name), gate_on_job("id_parent"))
    within = e2e_config.validation_timeout(360)
    return Scenario(
        name="36-dag-release",
        jobs={
            "name_parent": dag_job(parent_name, workflow=workflow, runtime=20),
            "id_parent": dag_job("dag-id-parent", workflow=workflow, runtime=45),
            "child": lambda run: dag_job(
                "dag-mixed-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_name(parent_name), dependency_by_id(run.jobs["id_parent"].job_id)),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually("child gated", on("child", held(), gates(*parent_gates, status="not_eligible")), within=within),
            eventually(
                "name parent completed", on("name_parent", job_state(JobState.COMPLETED, restarts=0)), within=within
            ),
            consistently("child still held", on("child", held(), no_advice_except()), for_seconds=10),
            eventually(
                "id parent completed", on("id_parent", job_state(JobState.COMPLETED, restarts=0)), within=within
            ),
            eventually(
                "released",
                on("child", advice_state(ADVICE_STATE_RELEASED), gates(*parent_gates, status="successful")),
                within=within,
            ),
            eventually(
                "child completed after parents",
                on("child", job_state(JobState.COMPLETED, restarts=0)),
                started_after("child", "name_parent", "id_parent"),
                within=within,
            ),
            eventually(
                "release applied",
                on(
                    "child",
                    component_log(
                        "slurm-advisor-controller",
                        '"msg":"applied advice job release"',
                        lambda job: f'"job_id":"{job.job_id}"',
                    ),
                ),
                within=within,
            ),
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
