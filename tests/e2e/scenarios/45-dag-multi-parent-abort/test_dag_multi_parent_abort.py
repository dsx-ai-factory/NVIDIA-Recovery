# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""One terminated parent closes every gate on a multi-parent child."""

from __future__ import annotations

import uuid

from e2e_harness import audit
from e2e_harness.actions import cancel_job
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    gate_on_job,
    gates,
    held,
    job_id_of,
    job_state,
    no_lifecycle_advice,
    recorded_attempt,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_TERMINATED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_one_terminated_parent_cancels_child_while_other_parent_remains_running(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-multi-abort-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    long_running = {"workflow": workflow, "runtime": 300, "behavior": "long-running"}
    return Scenario(
        name="45-dag-multi-parent-abort",
        jobs={
            "aborted_parent": dag_job("dag-aborted-parent", **long_running),
            "other_parent": dag_job("dag-other-parent", **long_running),
            "child": lambda run: dag_job(
                "dag-multi-abort-child",
                workflow=workflow,
                runtime=5,
                depends_on=(
                    dependency_by_id(run.jobs["aborted_parent"].job_id),
                    dependency_by_id(run.jobs["other_parent"].job_id),
                ),
                action="release",
                hold=True,
            ),
        },
        stages=(
            # Job Manager must record the parent running before the scenario cancels it.
            eventually(
                "parents running, child held",
                on("aborted_parent", job_state(JobState.RUNNING), recorded_attempt(state="RUNNING")),
                on("other_parent", job_state(JobState.RUNNING)),
                on("child", held()),
                within=within,
            ),
            act("cancel one parent", cancel_job("aborted_parent")),
            eventually(
                "parent cancelled", on("aborted_parent", job_state(JobState.CANCELLED, restarts=0)), within=within
            ),
            eventually(
                "child terminated",
                on(
                    "child",
                    advice_state(ADVICE_STATE_TERMINATED),
                    gates(
                        gate_on_job("aborted_parent"),
                        gate_on_job("other_parent"),
                        status="aborted",
                        reason_code="PARENT_TERMINATED",
                        reason_details={
                            "parent_job_id": job_id_of("aborted_parent"),
                            "parent_terminal_advice_reason": "user_canceled",
                        },
                    ),
                ),
                within=e2e_config.watcher_evidence_timeout_seconds,
            ),
            eventually("child cancelled", on("child", job_state(JobState.CANCELLED, restarts=0)), within=within),
            consistently(
                "other parent untouched",
                on("other_parent", job_state(JobState.RUNNING), no_lifecycle_advice()),
                for_seconds=10,
            ),
            eventually(
                "audited",
                on(
                    "aborted_parent",
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
