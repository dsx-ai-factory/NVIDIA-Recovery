# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Watcher no-output recovery scenario."""

from __future__ import annotations

from pathlib import Path

from e2e_harness import audit
from e2e_harness.checks import advice, audit_trail, log_line, requeue_notified, restarted, watcher_anomaly
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import WATCHER_NO_OUTPUT_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.launch.jobs import Job, JobSpec
from recovery_libs.testing.scenario import Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario


def stopped_output_marker(job: Job) -> str:
    return f"HEIMDALL_E2E_NOOUTPUT job_id={job.job_id} attempt=0"


def detection_marker(job: Job) -> str:
    return f"nooutput event detected for job_id {job.job_id}"


@scenario
def test_no_output_hang_recovery(e2e_config: E2EConfig, fixtures: Fixtures) -> Scenario[RecoveryClient]:
    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    return Scenario(
        name="31-no-output-hang",
        job=JobSpec(
            name="no-output-hang",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "JITTER_SEC": f"{e2e_config.jitter_seconds(0.3):g}",
                "APP_LOG_PATH": fixtures.path("scenarios/31-no-output-hang/job_output.txt"),
            },
            metadata=recovery_metadata(WATCHER_NO_OUTPUT_RECOVERY),
        ),
        stages=(
            eventually("restarted", restarted(times=1), within=e2e_config.validation_timeout(1200)),
            eventually("collected", log_line(stopped_output_marker), within=evidence_window),
            eventually(
                "detected",
                watcher_anomaly("HANGNOOUTPUT", marker=detection_marker),
                within=evidence_window,
                known_gap=(
                    f"ADR 0005 qualification gap: the stopped-output marker reached {e2e_config.log_backend}, "
                    "but Watcher persisted no HANGNOOUTPUT detection"
                ),
            ),
            eventually(
                "advised",
                advice(issue_type="HANGNOOUTPUT", marker=detection_marker, source="Heimdall Watcher"),
                within=evidence_window,
            ),
            eventually("requeue notified", requeue_notified(anomaly_type="HANGNOOUTPUT"), within=evidence_window),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                        audit.job_state_transition_event(
                            job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                        ),
                        audit.watcher_anomaly_detected_event(job, anomaly_type="HANGNOOUTPUT"),
                        audit.advice_reached_event(job, state="Restarted", cause="HANGNOOUTPUT"),
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
