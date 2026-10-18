# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Repeated parent and child observations never duplicate DAG termination."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.actions import cancel_job, post_job_state
from e2e_harness.checks import (
    accounted,
    advice_converged,
    advice_identity,
    advice_state,
    advice_unchanged,
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
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on, remember
from recovery_libs.testing.scenario_plugin import scenario


@pytest.mark.nightly
@scenario
def test_duplicate_terminal_observations_preserve_dependency_reason_and_terminal_advice(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("scenario requires E2E_PROFILE=advisor-act")
    workflow = f"dag-idempotence-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    aborted_gate = gates(
        gate_on_job("parent"),
        status="aborted",
        reason_code="PARENT_TERMINATED",
        reason_details={"parent_job_id": job_id_of("parent"), "parent_terminal_advice_reason": "user_canceled"},
    )
    observe_both_cancelled = (
        act("observe the parent cancelled", post_job_state("parent", state="CANCELLED", reason="user_canceled")),
        act("observe the child cancelled", post_job_state("child", state="CANCELLED", reason="user_canceled")),
    )
    return Scenario(
        name="54-dag-idempotence",
        jobs={
            "parent": dag_job("dag-idempotence-parent", workflow=workflow, runtime=300, behavior="long-running"),
            "child": lambda run: dag_job(
                "dag-idempotence-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            # Slurm can report RUNNING before Job Manager ingests the attempt. Establish its nonterminal audit
            # history before cancelling it so the cancellation transition asserted below can be recorded.
            eventually(
                "parent recorded running, child held",
                on(
                    "parent",
                    job_state(JobState.RUNNING),
                    recorded_attempt(state="RUNNING"),
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                        )
                    ),
                ),
                on("child", held()),
                within=within,
            ),
            act("cancel the parent", cancel_job("parent")),
            eventually("parent cancelled", on("parent", job_state(JobState.CANCELLED, restarts=0)), within=within),
            eventually(
                "child terminated", on("child", advice_state(ADVICE_STATE_TERMINATED), aborted_gate), within=within
            ),
            eventually("child cancelled", on("child", job_state(JobState.CANCELLED, restarts=0)), within=within),
            # First allow the child's real final scheduler observation to add any non-DAG terminal audit issue
            # before taking the stable baseline.
            *observe_both_cancelled,
            eventually(
                "child settled", on("child", advice_state(ADVICE_STATE_TERMINATED), advice_converged()), within=within
            ),
            act("remember the settled Advice", remember("settled", advice_identity("child"))),
            *observe_both_cancelled * 3,
            eventually("gate stays aborted", on("child", aborted_gate), within=within),
            consistently(
                "child stays cancelled with stable Advice",
                on(
                    "child",
                    advice_unchanged("settled"),
                    advice_converged(),
                    accounted(JobState.CANCELLED, restarts=0),
                    aborted_gate,
                ),
                for_seconds=15,
            ),
            eventually("child still cancelled", on("child", job_state(JobState.CANCELLED, restarts=0)), within=within),
            eventually(
                "audited",
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
