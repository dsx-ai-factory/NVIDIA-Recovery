# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The watchdog never opens a valid dependency-blocked attempt-zero gate."""

from __future__ import annotations

import uuid

import pytest
from e2e_harness import audit
from e2e_harness.checks import aps_spec_lacks, audit_trail, gate_on_job, gates, held, no_advice_except
from e2e_harness.config import E2EConfig
from e2e_harness.dag import dependency_by_id
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import dag_job
from recovery_libs.testing.scenario import Scenario, consistently, eventually, on
from recovery_libs.testing.scenario_plugin import scenario

LOCAL_WATCHDOG_RELEASE_TIMEOUT_SECONDS = 120


@pytest.mark.nightly
@scenario
def test_initial_dag_attempt_remains_held_past_watchdog_timeout_without_per_job_flag(
    e2e_config: E2EConfig,
) -> Scenario[RecoveryClient]:
    workflow = f"dag-initial-watchdog-{uuid.uuid4().hex}"
    within = e2e_config.validation_timeout(480)
    return Scenario(
        name="47-dag-initial-watchdog",
        jobs={
            "parent": dag_job("dag-watchdog-blocking-parent", workflow=workflow, runtime=300, behavior="long-running"),
            "child": lambda run: dag_job(
                "dag-watchdog-initial-child",
                workflow=workflow,
                runtime=5,
                depends_on=(dependency_by_id(run.jobs["parent"].job_id),),
                action="release",
                hold=True,
            ),
        },
        stages=(
            eventually(
                "child gated without watchdog opt-in",
                on(
                    "child",
                    held(),
                    aps_spec_lacks("watchdog_eligible"),
                    gates(gate_on_job("parent"), status="not_eligible"),
                ),
                within=within,
            ),
            consistently(
                "held past watchdog timeout",
                on("child", held(), no_advice_except()),
                for_seconds=LOCAL_WATCHDOG_RELEASE_TIMEOUT_SECONDS + 20,
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
                        )
                    ),
                ),
                within=90,
            ),
        ),
    )
