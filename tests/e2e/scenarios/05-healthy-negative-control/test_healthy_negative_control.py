# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Healthy completion must not create failure or recovery evidence."""

from __future__ import annotations

from pathlib import Path

from e2e_harness import audit
from e2e_harness.checks import audit_trail, healthy_status, job_state, no_advice, no_recovery_actions
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import HEALTHY_OBSERVE, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_healthy_negative_control(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return Scenario(
        name="05-healthy-negative-control",
        job=JobSpec(
            name="healthy-negative-control",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={"JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}"},
            metadata=recovery_metadata(HEALTHY_OBSERVE),
        ),
        stages=(
            eventually(
                "completed", job_state(JobState.COMPLETED, restarts=0), within=e2e_config.validation_timeout(300)
            ),
            eventually("recorded", healthy_status(), within=60),
            consistently("no recovery", healthy_status(), no_advice(), no_recovery_actions(), for_seconds=10),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        audit.job_observed_event(
                            job,
                            job_subset={
                                "state": audit.OneOf(("PENDING", "RUNNING")),
                                "aps_spec": {"auto_resume_mode": "requeue"},
                            },
                        ),
                        audit.job_state_transition_event(
                            job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="COMPLETED"
                        ),
                    )
                ),
                within=90,
            ),
        ),
    )
