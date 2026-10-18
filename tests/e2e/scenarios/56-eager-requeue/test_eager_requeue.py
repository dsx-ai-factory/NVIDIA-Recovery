# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Opt-in coverage of direct failed-job recovery when eager requeue is enabled."""

from __future__ import annotations

from pathlib import Path

import pytest
from e2e_harness import audit
from e2e_harness.checks import audit_trail, job_state, restarted
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.retry_policy import eager_requeue_enabled
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@scenario
def test_eager_requeue_recovers_failed_attempt(
    e2e_config: E2EConfig, slurm_stack: StackControl
) -> Scenario[RecoveryClient]:
    """Prove that the opted-in eager path creates a successful successor."""
    if e2e_config.profile != "advisor-act":
        pytest.skip("eager requeue requires the action-enabled advisor profile")
    if not eager_requeue_enabled(slurm_stack):
        pytest.skip("eager requeue is disabled in the deployed controller")
    within = e2e_config.validation_timeout(600)
    return Scenario(
        name="56-eager-requeue",
        job=JobSpec(
            name="eager-requeue",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={"JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}"},
            metadata=recovery_metadata(SCHEDULER_BOUNDARY_RECOVERY, {"max_requeue_times": 1}),
        ),
        stages=(
            eventually("restarted", restarted(times=1), within=within),
            # A successful successor leaves no failed allocation that eager recovery could resurrect afterwards.
            eventually("successor completed", job_state(JobState.COMPLETED, restarts=1), within=within),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        # Only direct failed-job recovery emits this event; an Advice-driven restart cannot satisfy it.
                        audit.automatic_requeue_hold_event(job),
                        audit.scheduler_action_event(job, action="slurm.release"),
                        audit.advice_application_applied_event(job),
                        audit.advice_reconciliation_converged_event(job),
                    )
                ),
                within=90,
            ),
        ),
    )
