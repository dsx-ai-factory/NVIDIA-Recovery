# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Repeated failure stops at the system-wide requeue limit when the job sets none."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from e2e_harness.checks import advice_reason
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.retry_policy import eager_requeue_enabled, system_retry_limit
from e2e_harness.stages import state_based_recovery, stopped_at_requeue_limit
from e2e_harness.workloads import JOB_STATE_CRASH_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.scenario import Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@pytest.fixture
def system_limit_of_one(
    e2e_config: E2EConfig, stack: StackControl, slurm_stack: StackControl, recovery: RecoveryClient
) -> Iterator[None]:
    if e2e_config.profile != "advisor-act":
        pytest.skip("system retry-limit enforcement requires the action-enabled advisor profile")
    if eager_requeue_enabled(slurm_stack):
        pytest.skip("retry-policy enforcement requires eager requeue to be disabled")
    with system_retry_limit(
        stack, recovery, slurm_stack=slurm_stack, job_manager=e2e_config.workload_name("job-manager"), limit=1
    ):
        yield


@scenario
def test_repeated_failure_stops_at_system_requeue_limit(
    e2e_config: E2EConfig, system_limit_of_one: None
) -> Scenario[RecoveryClient]:
    return Scenario(
        name="57-system-requeue-limit",
        job=JobSpec(
            name="system-requeue-limit",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={"JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}"},
            metadata=recovery_metadata(JOB_STATE_CRASH_RECOVERY, {"max_requeue_times": None}),
        ),
        stages=(
            *state_based_recovery(e2e_config, restart_within=600),
            *stopped_at_requeue_limit(e2e_config),
            eventually(
                "stopped by the system limit",
                advice_reason("has reached the requeue limit of 1.", attempt=1),
                within=90,
            ),
        ),
    )
