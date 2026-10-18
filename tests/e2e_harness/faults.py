# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The shared GPU fault-injection scenario: a job writes kernel faults to syslog, crashes, and is recovered."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Stage, eventually

from e2e_harness import audit
from e2e_harness.checks import (
    advice,
    audit_trail,
    hardware_attribution,
    job_state,
    log_line,
    recovered_away_from_initial_node,
    requeue_notified,
    restarted,
    single_node,
    system_log_line,
    watcher_anomaly,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import WATCHER_CRASH_RECOVERY, recovery_metadata, slurm_batch


def fault_injection(
    e2e_config: E2EConfig,
    fixtures: Fixtures,
    *,
    scenario_dir: Path,
    crash_marker: str,
    exclusion_type: str,
    signature: str = "",
) -> Scenario[RecoveryClient]:
    """Inject the scenario's kernel log into the job's node syslog, crash the job, and follow its recovery.

    With Attribution available, the fault must also exclude the initial node with `exclusion_type`.
    """

    name = scenario_dir.name
    if not e2e_config.log_backend_url:
        pytest.fail(f"{name} requires a configured {e2e_config.log_backend} endpoint so Watcher can detect the failure")
    syslog_marker = f"HEIMDALL_E2E_SYSLOG_{uuid.uuid4().hex}"
    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    attribution = e2e_config.attribution_mode == "available"
    restart: tuple[Stage[RecoveryClient], ...] = (
        (
            # Exclusion is visible only while the recovered attempt runs; it is observed before that attempt ends.
            eventually(
                "recovered on another node",
                recovered_away_from_initial_node(),
                within=e2e_config.validation_timeout(2400),
            ),
        )
        if attribution
        else (eventually("restarted", restarted(times=1), within=e2e_config.validation_timeout(2400)),)
    )
    return Scenario(
        name=name,
        job=JobSpec(
            name=name,
            workload=slurm_batch(scenario_dir / "job.sh"),
            environment={
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "JITTER_SEC": f"{e2e_config.jitter_seconds(0.2):g}",
                "RECOVERY_ATTEMPT_HOLD_SEC": f"{e2e_config.recovery_attempt_hold_seconds:g}",
                "SYSLOG_INGESTION_DELAY_SEC": "30",
                "APP_LOG_PATH": fixtures.path(f"scenarios/{name}/job_output.txt"),
                "KERNEL_LOG_PATH": fixtures.path(f"scenarios/{name}/kern.log"),
                "SYSLOG_INJECTOR": fixtures.path("e2e_harness/inject-syslog.sh"),
                "SYSLOG_MARKER": syslog_marker,
            },
            metadata=recovery_metadata(WATCHER_CRASH_RECOVERY),
        ),
        stages=(
            # The syslog fixture is written for one exact node, so the job must start on one.
            eventually(
                "running on one node",
                job_state(JobState.RUNNING),
                single_node(),
                within=e2e_config.validation_timeout(600),
            ),
            *restart,
            eventually("syslog collected", system_log_line(syslog_marker), within=evidence_window),
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
                    eventually(
                        "attributed",
                        hardware_attribution(exclusion_type, signature=signature),
                        within=evidence_window,
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
                        audit.advice_reached_event(
                            job,
                            state="Restarted",
                            cause="CRASH",
                            advice_subset={"fact_attribution": {"anomaly_type": "CRASH"}},
                        ),
                        *((audit.adi_fast_attribution_exchange_event(job),) if attribution else ()),
                        audit.scheduler_action_event(job, action="slurm.requeue_hold"),
                        *(
                            (audit.scheduler_action_event(job, action="slurm.update_constraints"),)
                            if attribution
                            else ()
                        ),
                        audit.scheduler_action_event(job, action="slurm.release"),
                        audit.advice_application_applied_event(job),
                        audit.advice_reconciliation_converged_event(job),
                    )
                ),
                within=90,
            ),
        ),
    )
