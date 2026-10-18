# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""A held DAG retry is watchdog eligible after the initial gate has opened."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.actions import requeue_hold_job, restore_scale, scale_to_zero
from e2e_harness.checks import (
    advice_state,
    audit_trail,
    component_log,
    held,
    job_state,
    no_advice_except,
    recorded_attempt,
)
from e2e_harness.config import E2EConfig
from e2e_harness.dag import ADVICE_STATE_RELEASED, dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

# Stopping the advisor controller affects every scenario running at the same time.
pytestmark = [pytest.mark.exclusive, pytest.mark.requeue_hold]

CONTROLLER = "slurm-advisor-controller"


@pytest.mark.nightly
@scenario
def test_watchdog_releases_held_dag_retry_from_current_slurm_state(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-retry-watchdog-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(540)
    return Scenario(
        name="48-dag-retry-watchdog",
        jobs={
            "parent": dag_job("dag-retry-watchdog-parent", workflow=workflow, runtime=10),
            "child": lambda run: dag_job(
                "dag-retry-watchdog-child",
                workflow=workflow,
                runtime=300,
                behavior="long-running",
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually("child held", on("child", held()), within=within),
            eventually(
                "child released by its gate",
                on("parent", job_state(JobState.COMPLETED, restarts=0)),
                on("child", advice_state(ADVICE_STATE_RELEASED), job_state(JobState.RUNNING, restarts=0)),
                within=within,
            ),
            act("requeue the child held", requeue_hold_job("child")),
            eventually("retry held", on("child", held(), job_state(JobState.PENDING, restarts=1)), within=within),
            # The retry is a new attempt: the opened attempt-0 gate must not release it on its own.
            consistently(
                "retry stays held",
                on("child", held(), job_state(JobState.PENDING, restarts=1), no_advice_except(attempt=1)),
                for_seconds=20,
            ),
            eventually("retry recorded", on("child", recorded_attempt(attempt=1, state="PENDING")), within=within),
            # Stop the controller only after Job Manager has observed the held retry. Otherwise the controller can
            # re-apply the attempt-0 Released Advice at the same two-minute boundary and win the race to release it.
            act("stop the advisor controller", scale_to_zero(CONTROLLER)),
            eventually("retry running", on("child", job_state(JobState.RUNNING, restarts=1)), within=within),
            eventually(
                "watchdog released the retry",
                on(
                    "child",
                    component_log(
                        "slurm-advisor-controller-watchdog",
                        '"msg":"watchdog release candidate"',
                        '"outcome":"applied"',
                        lambda job: f'"job_id":"{job.job_id}"',
                        '"attempt_index":1',
                    ),
                ),
                within=within,
            ),
            act("restart the advisor controller", restore_scale(CONTROLLER)),
            eventually(
                "audited",
                on(
                    "child",
                    audit_trail(
                        lambda job: (
                            audit.job_observed_event(
                                job, attempt=1, job_subset={"state": "PENDING", "state_reason": "JobHeldUser"}
                            ),
                            audit.watchdog_release_completed_event(job, attempt=1),
                            audit.job_started_event(job, attempt=1),
                        ),
                        attempt=1,
                    ),
                ),
                within=90,
            ),
        ),
    )
