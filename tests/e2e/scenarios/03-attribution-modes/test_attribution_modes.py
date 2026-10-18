# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise Attribution ON, OFF, and provider failure through real crash recovery."""

from __future__ import annotations

from pathlib import Path

import pytest
from e2e_harness import audit
from e2e_harness.actions import disable_attribution, scale_to_zero, staged_files, staging_directory, touch_file
from e2e_harness.checks import (
    advice,
    audit_trail,
    deployment_available,
    job_state,
    log_line,
    no_attribution_exchange,
    requeue_notified,
    restarted,
    watcher_anomaly,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import WATCHER_CRASH_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import Job, JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Step, act, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive

OFF_OBSERVATION_SECONDS = 15
CRASH_JOB = Path(__file__).parents[1] / "10-simple-segfault" / "job.sh"


def crash_marker(job: Job) -> str:
    return f"HEIMDALL_E2E_CRASH job_id={job.job_id} attempt=0"


def recovery_events(job: audit.SubmittedJob) -> tuple[audit.EventExpectation, ...]:
    return (
        audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
        audit.job_state_transition_event(job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"),
        audit.watcher_anomaly_detected_event(job, anomaly_type="CRASH"),
        audit.advice_reached_event(job, state="Restarted", cause="CRASH"),
        audit.scheduler_action_event(job, action="slurm.requeue_hold"),
        audit.scheduler_action_event(job, action="slurm.release"),
        audit.advice_application_applied_event(job),
        audit.advice_reconciliation_converged_event(job),
    )


@scenario
@pytest.mark.parametrize("attribution_state", ("on", "off", "provider-unavailable"))
def test_attribution_modes(attribution_state: str, e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    """Keep recovery causal while changing only the deployed Attribution path."""

    if e2e_config.attribution_mode != "available":
        pytest.skip("mode qualification requires the protected E2E_ATTRIBUTION_MODE=available stack")
    assert not e2e_config.stack_namespace or e2e_config.stack_namespace == e2e_config.slurm_namespace, (
        "mode qualification requires the protected stack and Slurm in the same namespace"
    )
    directory = staging_directory()
    # The gate stays closed through the mode change, so the job cannot crash before the mode is in place.
    gate = f"{directory}/attribution-ready"
    evidence_window = e2e_config.watcher_evidence_timeout_seconds

    mode_change: tuple[Step[RecoveryClient], ...] = ()
    if attribution_state == "off":
        isolator = e2e_config.workload_name("anomaly-detection-isolator")
        mode_change = (act("disable attribution", disable_attribution(isolator)),)
    elif attribution_state == "provider-unavailable":
        mode_change = (
            act("stop the attribution provider", scale_to_zero(e2e_config.workload_name("attribution-service"))),
            eventually(
                "adapter still running",
                deployment_available(e2e_config.workload_name("attribution-adapter")),
                within=e2e_config.stack_timeout_seconds,
            ),
        )

    if attribution_state == "off":
        audited: tuple[Step[RecoveryClient], ...] = (
            eventually("audited", audit_trail(recovery_events), within=90),
            consistently("no attribution exchange", no_attribution_exchange(), for_seconds=OFF_OBSERVATION_SECONDS),
        )
    else:
        response_subset = (
            {"error": {"type": "service_error"}, "attributions": []}
            if attribution_state == "provider-unavailable"
            else None
        )
        audited = (
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        *recovery_events(job),
                        audit.adi_fast_attribution_exchange_event(job, response_subset=response_subset),
                    )
                ),
                within=90,
            ),
        )

    return Scenario(
        name="03-attribution-modes",
        setup=(act("stage crash job", staged_files(directory, CRASH_JOB)),),
        job=JobSpec(
            name="attribution-mode-recovery",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            environment={
                "ATTRIBUTION_GATE": gate,
                "ATTRIBUTION_CRASH_JOB": f"{directory}/{CRASH_JOB.name}",
                "JOB_MIN_RUNTIME": f"{e2e_config.job_min_runtime_seconds:g}",
                "FAULT_DELAY": "30",
                "POST_FAULT_DELAY": "60",
            },
            metadata=recovery_metadata(WATCHER_CRASH_RECOVERY),
        ),
        stages=(
            eventually("running", job_state(JobState.RUNNING), within=e2e_config.validation_timeout(600)),
            *mode_change,
            act("open the gate", touch_file(gate)),
            eventually("restarted", restarted(times=1), within=e2e_config.validation_timeout(600)),
            eventually("collected", log_line(crash_marker), within=evidence_window),
            eventually("detected", watcher_anomaly("CRASH", marker=crash_marker), within=evidence_window),
            eventually(
                "advised",
                advice(issue_type="CRASH", marker=crash_marker, source="Heimdall Watcher"),
                within=evidence_window,
            ),
            eventually("requeue notified", requeue_notified(anomaly_type="CRASH"), within=evidence_window),
            *audited,
        ),
    )
