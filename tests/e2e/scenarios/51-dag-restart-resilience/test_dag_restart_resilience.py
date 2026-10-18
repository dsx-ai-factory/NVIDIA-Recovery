# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Persisted DAG state converges after Job Manager or SAC restarts."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.actions import post_job_state, restart, restore_scale, scale_to_zero
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_state,
    no_advice_except,
    recorded_attempt,
    started_after,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, dependency_by_id
from e2e_harness.services import E2EServices
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Evidence, Run, Scenario, Stage, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive

CONTROLLER = "slurm-advisor-controller"


@pytest.mark.nightly
@scenario
def test_job_manager_restart_preserves_registered_gate(e2e_config: E2EConfig) -> Scenario[E2EServices]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("scenario requires E2E_PROFILE=advisor-act")
    within = e2e_config.validation_timeout(480)
    return restart_scenario(
        e2e_config,
        "dag-jm-restart",
        parent_runtime=40,
        stages=(
            act("restart Job Manager", restart(e2e_config.workload_name("job-manager"))),
            eventually("gate survived the restart", on("child", _gate_follows_parent), within=within),
            eventually("parent completed", on("parent", job_state(JobState.COMPLETED, restarts=0)), within=within),
            eventually(
                "released",
                on("child", advice_state(ADVICE_STATE_RELEASED), gates(gate_on_job("parent"), status="successful")),
                within=within,
            ),
        ),
    )


@pytest.mark.nightly
@scenario
def test_sac_restart_consumes_release_advice_persisted_while_sac_was_down(
    e2e_config: E2EConfig,
) -> Scenario[E2EServices]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("scenario requires E2E_PROFILE=advisor-act")
    within = e2e_config.validation_timeout(480)
    return restart_scenario(
        e2e_config,
        "dag-sac-restart",
        parent_runtime=30,
        stages=(
            act("stop the advisor controller", scale_to_zero(CONTROLLER)),
            eventually("parent completed", on("parent", job_state(JobState.COMPLETED, restarts=0)), within=within),
            act("observe the parent completed", post_job_state("parent", state="COMPLETED", reason="None")),
            eventually("released while down", on("child", advice_state(ADVICE_STATE_RELEASED)), within=within),
            consistently(
                "child held while down",
                on(
                    "child",
                    held(),
                    job_state(JobState.PENDING, restarts=0),
                    no_advice_except(ADVICE_STATE_RELEASED),
                ),
                for_seconds=10,
            ),
            act("restart the advisor controller", restore_scale(CONTROLLER)),
        ),
    )


def _gate_follows_parent(run: Run[E2EServices]) -> Evidence:
    """The child's gate matches its parent: still not eligible while the parent runs, successful once it completed.

    A restart can outlast the parent, so the gate's expected status comes from the parent's state, not the clock.
    """

    status = "successful" if run.jobs["parent"].state is JobState.COMPLETED else "not_eligible"
    return gates(gate_on_job("parent"), status=status)(run)


def restart_scenario(
    e2e_config: E2EConfig, prefix: str, *, parent_runtime: int, stages: tuple[Stage[E2EServices], ...]
) -> Scenario[E2EServices]:
    """A held child gated on a running parent, the restart `stages`, and the child completing after its parent."""

    workflow = f"{prefix}-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="51-dag-restart-resilience",
        jobs={
            "parent": dag_job(f"{prefix}-parent", workflow=workflow, runtime=parent_runtime),
            "child": lambda run: dag_job(
                f"{prefix}-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "parent recorded, child gated",
                on("parent", job_state(JobState.RUNNING), recorded_attempt()),
                on("child", held(), gates(gate_on_job("parent"), status="not_eligible")),
                within=within,
            ),
            *stages,
            eventually(
                "child completed after parent",
                on("child", job_state(JobState.COMPLETED, restarts=0)),
                started_after("child", "parent"),
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
