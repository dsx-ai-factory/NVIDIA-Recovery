# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Repeated failure stops at the job's own requeue limit."""

from __future__ import annotations

from pathlib import Path

import pytest
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.retry_policy import eager_requeue_enabled
from e2e_harness.stages import state_based_recovery, stopped_at_requeue_limit
from e2e_harness.workloads import JOB_STATE_CRASH_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.scenario import Scenario
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@scenario
def test_repeated_failure_stops_at_requeue_limit(
    e2e_config: E2EConfig, slurm_stack: StackControl
) -> Scenario[RecoveryClient]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("job retry-limit enforcement requires the action-enabled advisor profile")
    if eager_requeue_enabled(slurm_stack):
        pytest.skip("retry-policy enforcement requires eager requeue to be disabled")
    return Scenario(
        name="32-requeue-limit-guardrail",
        job=JobSpec(
            name="requeue-limit-guardrail",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={"JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}"},
            metadata=recovery_metadata(JOB_STATE_CRASH_RECOVERY, {"max_requeue_times": 1}),
        ),
        stages=(
            *state_based_recovery(e2e_config, restart_within=600),
            *stopped_at_requeue_limit(e2e_config),
        ),
    )
