# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""An unavailable Advisor API fails closed and later converges."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.actions import restore_scale, scale_to_zero
from e2e_harness.checks import (
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
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@pytest.mark.nightly
@scenario
def test_api_outage_keeps_child_held_until_gateway_recovers(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("scenario requires E2E_PROFILE=advisor-act")
    workflow = f"dag-api-outage-{uuid.uuid4().hex}"
    gateway = e2e_config.workload_name("api-gateway")
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="52-dag-api-outage",
        jobs={
            "parent": dag_job("dag-api-outage-parent", workflow=workflow, runtime=30),
            "child": lambda run: dag_job(
                "dag-api-outage-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "parent running, child held",
                on("parent", job_state(JobState.RUNNING)),
                on("child", held()),
                within=within,
            ),
            act("stop the Advisor API", scale_to_zero(gateway)),
            eventually(
                "parent completed during outage", on("parent", job_state(JobState.COMPLETED, restarts=0)), within=within
            ),
            # Without the API, the controller cannot confirm the gate and must not release the child.
            consistently("child held during outage", on("child", held(), no_advice_except()), for_seconds=15),
            act("restore the Advisor API", restore_scale(gateway)),
            eventually(
                "child released",
                on("child", advice_state(ADVICE_STATE_RELEASED), gates(gate_on_job("parent"), status="successful")),
                within=within,
            ),
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
