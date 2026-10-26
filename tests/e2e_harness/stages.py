# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stage sequences that several scenarios share."""

from __future__ import annotations

from recovery_libs.testing.launch.jobs import JobState
from recovery_libs.testing.scenario import Check, Stage, consistently, eventually, on

from e2e_harness import audit
from e2e_harness.checks import (
    accounted,
    advice,
    audit_trail,
    job_state,
    recovery_trace,
    requeue_notified,
    restarted,
    service_logs_exported,
    state_based_crash,
    terminated_advice,
)
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient

STATE_CRASH_MARKER = "State-based CRASH event for failed job"

LIMIT_SETTLE_SECONDS = 30
"""How long a job stopped at its requeue limit must stay stopped, in both Slurm's controller and accounting."""


def state_based_recovery(e2e_config: E2EConfig, *, restart_within: float) -> tuple[Stage[RecoveryClient], ...]:
    """Job Manager detects the failed attempt from its state, advises a restart, and Slurm restarts it once."""

    evidence_window = e2e_config.watcher_evidence_timeout_seconds
    return (
        eventually("restarted", restarted(times=1), within=e2e_config.validation_timeout(restart_within)),
        eventually("detected", state_based_crash(), within=evidence_window),
        eventually(
            "advised",
            advice(issue_type="CRASH", marker=STATE_CRASH_MARKER, source="Heimdall Job Manager"),
            within=evidence_window,
        ),
        eventually("requeue notified", requeue_notified(anomaly_type="CRASH"), within=evidence_window),
    )


def recovery_telemetry(
    e2e_config: E2EConfig, *, operation: str = "restart", role: str | None = None
) -> tuple[Stage[RecoveryClient], ...]:
    """The recovery's backend, Advice, and controller spans form one trace, and its services' logs were exported.

    Both arrive through the services' production OTLP exporters and the stack's shared OTLP endpoint. The local
    harness deploys that endpoint with Tempo and enables both stages; a run against a cluster without them skips
    each stage whose backend it does not name. Only restarts include an attribution span, and only when the
    credentialed attribution service is available.
    """

    stages: list[Stage[RecoveryClient]] = []
    within = e2e_config.watcher_evidence_timeout_seconds
    if e2e_config.recovery_trace_enabled:
        traced: Check[RecoveryClient] = recovery_trace(
            operation=operation,
            require_attribution=operation == "restart" and e2e_config.attribution_mode == "available",
        )
        stages.append(eventually("traced", traced if role is None else on(role, traced), within=within))
    if e2e_config.otlp_logs:
        logged = service_logs_exported()
        stages.append(eventually("logs exported", logged if role is None else on(role, logged), within=within))
    return tuple(stages)


def stopped_at_requeue_limit(e2e_config: E2EConfig) -> tuple[Stage[RecoveryClient], ...]:
    """After one restart the job fails again, Job Manager terminates it, and Slurm does not restart it again.

    The job must stay stopped both right after it stops and once Job Manager's termination has converged.
    """

    stopped = (job_state(JobState.FAILED, restarts=1), accounted(JobState.FAILED, restarts=1))
    return (
        eventually(
            "stopped at limit",
            *stopped,
            terminated_advice(attempt=1),
            within=e2e_config.watcher_evidence_timeout_seconds,
        ),
        consistently("stays stopped", *stopped, for_seconds=LIMIT_SETTLE_SECONDS),
        eventually(
            "recovery audited",
            audit_trail(
                lambda job: (
                    audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                    audit.job_state_transition_event(
                        job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                    ),
                    audit.advice_reached_event(job, state="Restarted", cause="CRASH"),
                    audit.scheduler_action_event(job, action="slurm.requeue_hold"),
                    audit.scheduler_action_event(job, action="slurm.release"),
                    audit.advice_application_applied_event(job),
                    audit.advice_reconciliation_converged_event(job),
                )
            ),
            within=90,
        ),
        eventually(
            "termination audited",
            audit_trail(
                lambda job: (
                    audit.job_observed_event(job, attempt=1),
                    audit.job_started_event(job, attempt=1),
                    audit.job_state_transition_event(
                        job, attempt=1, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="FAILED"
                    ),
                    audit.advice_reached_event(job, attempt=1, state="Terminated", cause="max_requeue_reached"),
                    audit.advice_reconciliation_converged_event(job, attempt=1),
                ),
                attempt=1,
            ),
            within=90,
        ),
        # A requeue-held successor would also break the limit, even before it runs.
        consistently("still stopped", *stopped, for_seconds=LIMIT_SETTLE_SECONDS),
    )
