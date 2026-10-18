# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Observe mode plans DAG actions but never mutates the Slurm allocation."""

from __future__ import annotations

import uuid

import pytest
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
    no_advice_except,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, ADVICE_STATE_TERMINATED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario


def _require_observe_profile(e2e_config: E2EConfig) -> None:
    if e2e_config.profile != "advisor-observe":
        pytest.skip("scenario requires E2E_PROFILE=advisor-observe")


def _child(name: str, workflow: str):
    return lambda run: dag_job(
        name,
        workflow=workflow,
        runtime=5,
        depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
        action="release",
        hold=True,
    )


@pytest.mark.nightly
@scenario
def test_observe_mode_plans_release_while_child_remains_held(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    _require_observe_profile(e2e_config)
    workflow = f"dag-observe-release-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(360)
    return Scenario(
        name="50-dag-observe-mode",
        jobs={
            "parent": dag_job("dag-observe-release-parent", workflow=workflow, runtime=20),
            "child": _child("dag-observe-release-child", workflow),
        },
        stages=(
            eventually("child held", on("child", held()), within=within),
            eventually("parent completed", on("parent", job_state(JobState.COMPLETED, restarts=0)), within=within),
            eventually(
                "release planned",
                on(
                    "child",
                    advice_state(ADVICE_STATE_RELEASED),
                    audit_trail(lambda job: (audit.advice_application_evaluated_event(job, action="slurm.release"),)),
                ),
                within=within,
            ),
            # Observe mode advises the release but must never apply it.
            consistently(
                "child stays held", on("child", held(), no_advice_except(ADVICE_STATE_RELEASED)), for_seconds=15
            ),
            eventually(
                "audited",
                on(
                    "child",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(
                                job, job_subset={"state": "PENDING", "state_reason": "JobHeldUser"}
                            ),
                            audit.advice_reached_event(
                                job, state="Released", cause=audit.DAG_RELEASE_ADVICE_REASON, visible=True
                            ),
                            audit.advice_application_evaluated_event(job, action="slurm.release"),
                        )
                    ),
                ),
                within=90,
            ),
        ),
    )


@pytest.mark.nightly
@scenario
def test_observe_mode_plans_termination_while_child_remains_held(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    _require_observe_profile(e2e_config)
    workflow = f"dag-observe-terminate-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(420)
    return Scenario(
        name="50-dag-observe-mode",
        jobs={
            "parent": dag_job("dag-observe-terminate-parent", workflow=workflow, runtime=300, behavior="long-running"),
            "child": _child("dag-observe-terminate-child", workflow),
        },
        stages=(
            eventually(
                "parent running, child held",
                on("parent", job_state(JobState.RUNNING)),
                on("child", held()),
                within=within,
            ),
            act("cancel parent", cancel_job("parent")),
            eventually("parent cancelled", on("parent", job_state(JobState.CANCELLED, restarts=0)), within=within),
            eventually(
                "termination planned",
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
                    audit_trail(lambda job: (audit.advice_application_evaluated_event(job, action="slurm.cancel"),)),
                ),
                within=within,
            ),
            # Observe mode advises the cancellation but must never apply it.
            consistently(
                "child stays held", on("child", held(), no_advice_except(ADVICE_STATE_TERMINATED)), for_seconds=15
            ),
            eventually(
                "audited",
                on(
                    "child",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(
                                job, job_subset={"state": "PENDING", "state_reason": "JobHeldUser"}
                            ),
                            audit.advice_reached_event(
                                job, state="Terminated", cause=audit.DAG_TERMINATION_ADVICE_REASON, visible=True
                            ),
                            audit.advice_application_evaluated_event(job, action="slurm.cancel"),
                        )
                    ),
                ),
                within=90,
            ),
        ),
    )
