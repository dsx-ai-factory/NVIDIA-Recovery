# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fan-out branches both complete before their shared descendant starts."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.checks import (
    accounted,
    advice_converged,
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_state,
    no_advice_except,
    started_after,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

DESCENDANTS = ("left", "right", "leaf")


@pytest.mark.nightly
@scenario
def test_fan_out_and_diamond_release_each_descendant_after_its_dependencies(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("scenario requires E2E_PROFILE=advisor-act")
    workflow = f"dag-diamond-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    leaf_gates = (gate_on_job("left"), gate_on_job("right"))
    settled = tuple(on(role, advice_converged(), accounted(JobState.COMPLETED, restarts=0)) for role in DESCENDANTS)

    def branch(name: str, runtime: int):
        return lambda run: dag_job(
            name,
            workflow=workflow,
            runtime=runtime,
            depends_on=(dependency_by_id(run.jobs["root"].job_id),),
            action="release",
            hold=True,
        )

    return Scenario(
        name="49-dag-diamond",
        jobs={
            "root": dag_job("dag-diamond-root", workflow=workflow, runtime=30),
            "left": branch("dag-diamond-left", 15),
            "right": branch("dag-diamond-right", 35),
            "leaf": lambda run: dag_job(
                "dag-diamond-leaf",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["left"].job_id), dependency_by_id(run.jobs["right"].job_id)),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "descendants gated",
                *(on(role, held()) for role in DESCENDANTS),
                on("leaf", gates(*leaf_gates, status="not_eligible")),
                within=within,
            ),
            eventually(
                "branches released",
                *(
                    on(role, advice_state(ADVICE_STATE_RELEASED), gates(gate_on_job("root"), status="successful"))
                    for role in ("left", "right")
                ),
                within=within,
            ),
            eventually("left branch completed", on("left", job_state(JobState.COMPLETED, restarts=0)), within=within),
            # One finished branch must not release the leaf.
            consistently(
                "leaf waits for right branch",
                on("leaf", held(), no_advice_except(ADVICE_STATE_RELEASED)),
                for_seconds=8,
            ),
            eventually(
                "leaf released",
                on("leaf", advice_state(ADVICE_STATE_RELEASED), gates(*leaf_gates, status="successful")),
                within=within,
            ),
            eventually(
                "all completed in order",
                *(on(role, job_state(JobState.COMPLETED, restarts=0)) for role in ("root", *DESCENDANTS)),
                started_after("left", "root"),
                started_after("right", "root"),
                started_after("leaf", "left", "right"),
                within=within,
            ),
            eventually("release Advice converged", *settled, within=within),
            consistently("no descendant requeued", *settled, for_seconds=10),
            eventually(
                "audited",
                *(
                    on(
                        role,
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
                    )
                    for role in DESCENDANTS
                ),
                within=90,
            ),
        ),
    )
