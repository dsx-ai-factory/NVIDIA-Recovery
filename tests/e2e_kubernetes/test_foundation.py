# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise CPU workload execution through the staged scenario launcher boundary."""

import os

import pytest
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Evidence, Run, Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario


def pods_running(run: Run[None]) -> Evidence:
    """Require the launcher's owned Pod observation to reach the expected running count."""
    return Evidence(run.job.state == JobState.RUNNING, f"owned workload state: {run.job.state}")


@pytest.mark.parametrize("iteration", range(2))
@scenario
def test_jobset_runs(iteration: int) -> Scenario[None]:
    """Repeat the same logical workload name; xdist also runs these cases concurrently."""
    return Scenario(
        name=f"JobSet CPU foundation {iteration}",
        job=JobSpec(
            name="foundation-smoke",
            workload={
                "image": os.environ.get("KUBERNETES_SMOKE_IMAGE", "busybox:1.37.0"),
                "command": ["sh", "-c", "echo foundation-ready; sleep 300"],
                "replicas": 2,
            },
        ),
        stages=(eventually("expected Pods running", pods_running, within=180),),
    )
