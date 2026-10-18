# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Build the scheduler-specific and Heimdall-specific parts of scenario job specs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from recovery_libs.testing.launch.jobs import JobSpec

JOB_LOG_DIRECTORY = "/home/logs"


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    """Scenario-owned APS policy; there is deliberately no implicit requeue policy."""

    name: str
    mode: str = "act"
    log_based_crash_enabled: bool = True
    log_based_timeout_enabled: bool = True
    no_output_timeout_seconds: int | None = None

    def comment(self, aps_overrides: Mapping[str, object] | None = None) -> str:
        """Build APS metadata; an explicit None retry cap omits that optional field."""
        detection: dict[str, object] = {
            "log_based_crash_enabled": self.log_based_crash_enabled,
            "log_based_timeout_enabled": self.log_based_timeout_enabled,
        }
        if self.no_output_timeout_seconds is not None:
            detection["log_based_timeout_threshold_pretraining"] = self.no_output_timeout_seconds
            detection["log_based_timeout_threshold_trainingloop"] = self.no_output_timeout_seconds
        aps = {
            "mode": self.mode,
            "auto_resume_mode": "requeue",
            "max_requeue_times": 1,
            "anomaly_detection_config": detection,
        }
        if aps_overrides:
            aps.update(aps_overrides)
        if aps.get("max_requeue_times") is None:
            aps.pop("max_requeue_times", None)
        return json.dumps({"APS": aps}, sort_keys=True, separators=(",", ":"))


WATCHER_CRASH_RECOVERY = RecoveryPolicy("watcher-crash")
JOB_STATE_CRASH_RECOVERY = RecoveryPolicy("job-state-crash", log_based_crash_enabled=False)
WATCHER_NO_OUTPUT_RECOVERY = RecoveryPolicy(
    "watcher-no-output",
    no_output_timeout_seconds=30,
)
HEALTHY_OBSERVE = RecoveryPolicy("healthy-observe", mode="observe")
SCHEDULER_BOUNDARY_RECOVERY = RecoveryPolicy(
    "scheduler-boundary",
    log_based_crash_enabled=False,
    log_based_timeout_enabled=False,
)
SCHEDULER_BOUNDARY_OBSERVE = RecoveryPolicy(
    "scheduler-boundary-observe",
    mode="observe",
    log_based_timeout_enabled=False,
)


def recovery_metadata(policy: RecoveryPolicy, overrides: Mapping[str, object] | None = None) -> dict[str, Any]:
    """Job metadata carrying Heimdall's recovery policy (its APS spec)."""

    return json.loads(policy.comment(overrides))


def slurm_batch(script: Path, *, hold: bool = False) -> dict[str, Any]:
    """A Slurm workload running `script` from the shared job log directory, which the log collector watches."""

    return {"script": script.read_text(), "working_directory": JOB_LOG_DIRECTORY, "hold": hold}


DAG_JOB_SCRIPT = Path(__file__).with_name("dag_job.sh")


def dag_job(
    name: str,
    *,
    workflow: str,
    runtime: int = 10,
    behavior: str = "success",
    depends_on: Sequence[Mapping[str, object]] = (),
    action: str | None = None,
    hold: bool = False,
    policy: RecoveryPolicy = SCHEDULER_BOUNDARY_RECOVERY,
    aps: Mapping[str, object] | None = None,
) -> JobSpec:
    """A job in a Heimdall DAG workflow, running the shared DAG job for `runtime` seconds with `behavior`.

    `depends_on` takes `shared.dag.dependency_by_id` or `dependency_by_name` targets; `action` is what Heimdall
    does to this job once they complete, such as "release".
    """

    overrides: dict[str, object] = {"workflow_id": workflow, **(aps or {})}
    if depends_on:
        overrides["dag_dependencies"] = list(depends_on)
    if action:
        overrides["dag_action"] = action
    return JobSpec(
        name=name,
        workload=slurm_batch(DAG_JOB_SCRIPT, hold=hold),
        environment={"DAG_JOB_BEHAVIOR": behavior, "DAG_JOB_RUNTIME_SECONDS": str(runtime)},
        metadata=recovery_metadata(policy, overrides),
    )
