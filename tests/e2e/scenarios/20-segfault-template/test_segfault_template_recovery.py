# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native replay-then-segfault recovery scenario."""

from __future__ import annotations

from pathlib import Path

from e2e_harness import audit
from e2e_harness.checks import (
    advice,
    audit_trail,
    job_state,
    log_line,
    no_hardware_attribution,
    requeue_notified,
    restarted,
    single_node,
    watcher_anomaly,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import WATCHER_CRASH_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.launch.jobs import Job, JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario


def crash_marker(job: Job) -> str:
    return f"HEIMDALL_E2E_CRASH job_id={job.job_id} attempt=0"


@scenario
def test_segfault_template_recovery(e2e_config: E2EConfig, fixtures: Fixtures) -> Scenario[RecoveryClient]:
    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    attribution = e2e_config.attribution_mode == "available"
    return Scenario(
        name="20-segfault-template",
        job=JobSpec(
            name="segfault-template",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "JITTER_SEC": f"{e2e_config.jitter_seconds(0.2):g}",
                "APP_LOG_PATH": fixtures.path("scenarios/20-segfault-template/job_output.txt"),
            },
            metadata=recovery_metadata(WATCHER_CRASH_RECOVERY),
        ),
        stages=(
            # The attribution negative control below needs one node to attribute against.
            eventually(
                "running on one node",
                job_state(JobState.RUNNING),
                single_node(),
                within=e2e_config.validation_timeout(120),
            ),
            eventually("restarted", restarted(times=1), within=e2e_config.validation_timeout(420)),
            eventually("collected", log_line(crash_marker), within=evidence_window),
            eventually("detected", watcher_anomaly("CRASH", marker=crash_marker), within=evidence_window),
            eventually(
                "advised",
                advice(issue_type="CRASH", marker=crash_marker, source="Heimdall Watcher"),
                within=evidence_window,
            ),
            eventually("requeue notified", requeue_notified(anomaly_type="CRASH"), within=evidence_window),
            *(
                (
                    consistently(
                        "no hardware attribution",
                        no_hardware_attribution(),
                        for_seconds=e2e_config.attribution_timeout_seconds,
                    ),
                )
                if attribution
                else ()
            ),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                        audit.job_state_transition_event(
                            job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                        ),
                        audit.advice_reached_event(job, state="Restarted", cause="CRASH"),
                        *((audit.adi_fast_attribution_exchange_event(job),) if attribution else ()),
                        audit.scheduler_action_event(job, action="slurm.requeue_hold"),
                        audit.scheduler_action_event(job, action="slurm.release"),
                        audit.advice_application_applied_event(job),
                        audit.advice_reconciliation_converged_event(job),
                    )
                ),
                within=90,
            ),
        ),
    )
