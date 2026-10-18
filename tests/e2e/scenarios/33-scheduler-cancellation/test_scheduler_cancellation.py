# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scheduler cancellation must produce exact terminal Advice without recovery."""

from __future__ import annotations

from pathlib import Path

from e2e_harness import audit
from e2e_harness.actions import cancel_job
from e2e_harness.checks import audit_trail, job_state, no_recovery_actions, scheduler_state, terminal_advice
from e2e_harness.config import E2EConfig
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, act, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_scheduler_cancellation(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    return Scenario(
        name="33-scheduler-cancellation",
        job=JobSpec(
            name="scheduler-cancellation",
            workload=slurm_batch(Path(__file__).with_name("job.sh")),
            metadata=recovery_metadata(SCHEDULER_BOUNDARY_RECOVERY),
        ),
        stages=(
            # Cancelling before Job Manager records the running attempt can fall
            # between SAC polls and leave no attempt to reconcile.
            eventually(
                "registered",
                job_state(JobState.RUNNING),
                scheduler_state("RUNNING", slurm_state="RUNNING"),
                within=e2e_config.validation_timeout(300),
            ),
            act("cancel", cancel_job()),
            eventually("cancelled", job_state(JobState.CANCELLED, restarts=0), within=60),
            eventually("terminal advice", terminal_advice(), within=e2e_config.watcher_evidence_timeout_seconds),
            consistently("no recovery", no_recovery_actions(), for_seconds=10),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        audit.job_observed_event(job, job_subset={"state": audit.OneOf(("PENDING", "RUNNING"))}),
                        audit.job_state_transition_event(
                            job, old_states=("PENDING", "RUNNING", "COMPLETING"), new_state="CANCELLED"
                        ),
                        audit.advice_reached_event(job, state="Terminated", cause="user_canceled", visible=True),
                        audit.advice_reconciliation_converged_event(job),
                    )
                ),
                within=90,
            ),
        ),
    )
