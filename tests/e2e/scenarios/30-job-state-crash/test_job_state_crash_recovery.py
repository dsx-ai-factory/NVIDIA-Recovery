# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Job Manager state-based crash recovery scenario."""

from __future__ import annotations

from pathlib import Path

from e2e_harness import audit
from e2e_harness.checks import audit_trail
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.stages import recovery_traced, state_based_recovery
from e2e_harness.workloads import JOB_STATE_CRASH_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.scenario import Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_job_state_crash_recovery(e2e_config: E2EConfig, fixtures: Fixtures) -> Scenario[RecoveryClient]:
    return Scenario(
        name="30-job-state-crash",
        job=JobSpec(
            name="job-state-crash",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "JITTER_SEC": f"{e2e_config.jitter_seconds(0.2):g}",
                "FAILURE_DELAY": "10",
                "APP_LOG_PATH": fixtures.path("scenarios/30-job-state-crash/job_output.txt"),
            },
            metadata=recovery_metadata(JOB_STATE_CRASH_RECOVERY),
        ),
        stages=(
            *state_based_recovery(e2e_config, restart_within=300),
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
                            job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                        ),
                        audit.advice_reached_event(
                            job,
                            state="Restarted",
                            cause="CRASH",
                            advice_subset={"fact_attribution": {"anomaly_type": "CRASH"}},
                        ),
                        audit.scheduler_action_event(job, action="slurm.requeue_hold"),
                        audit.scheduler_action_event(job, action="slurm.release"),
                        audit.advice_application_applied_event(job),
                        audit.advice_reconciliation_converged_event(job),
                    )
                ),
                within=90,
            ),
            *recovery_traced(e2e_config),
        ),
    )
