# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualification teardown must clean owned terminal workloads and retain their diagnostics."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from e2e_kubernetes.conftest import DiagnosticLauncher
from recovery_libs.testing.launch.jobs import Job, JobSpec, JobState
from recovery_libs.testing.scenario import Evidence, Scenario, eventually, run_scenario
from workload_map_live.conftest import cancel_owned


class FakeLauncher:
    """Submits each spec as a workload already in `state`, and records what cleanup asks of it."""

    def __init__(self, state: JobState = JobState.FAILED) -> None:
        self.state = state
        self.cancelled: list[str] = []
        self.unreadable: set[str] = set()
        self.undeletable: set[str] = set()

    def submit(self, spec: JobSpec) -> Job:
        return self.get(job_id=f"qualification/{spec.name}/original-uid")

    def get(self, *, job_id: str) -> Job:
        return Job(job_id, job_id.split("/")[1], self.state, (), 42, None, datetime(2026, 1, 1, tzinfo=UTC))

    def diagnostics(self, *, job_id: str) -> dict[str, str]:
        if job_id in self.unreadable:
            raise ConnectionError("API server unavailable")
        return {"job_id": job_id, "native_failure": "retry budget exhausted"}

    def cancel(self, *, job_id: str) -> None:
        if job_id in self.undeletable:
            raise ConnectionError("API server unavailable")
        self.cancelled.append(job_id)


def saved(directory: Path, job_id: str) -> dict[str, str]:
    return json.loads((directory / (job_id.replace("/", "_") + ".json")).read_text())


def test_terminal_workload_skipped_by_runner_is_cancelled_with_diagnostics(tmp_path: Path) -> None:
    """The runner leaves a natively failed workload alone; teardown must still save its evidence and delete it."""
    fake = FakeLauncher(JobState.FAILED)
    tracked = DiagnosticLauncher(fake, tmp_path)
    scenario = Scenario(
        name="native failure",
        job=JobSpec(name="owned", workload={}),
        stages=(eventually("terminal observed", lambda run: Evidence(run.job.state.is_final, "failed"), within=0),),
    )

    run_scenario(scenario, launcher=tracked, services=None, sleep=lambda _: None)
    assert fake.cancelled == []

    cancel_owned(tracked)

    identity = "qualification/owned/original-uid"
    assert fake.cancelled == [identity]
    assert saved(tmp_path, identity) == {"job_id": identity, "native_failure": "retry budget exhausted"}


def test_unreadable_diagnostics_do_not_prevent_cleanup(tmp_path: Path) -> None:
    """Evidence collection is best effort; the workload is still deleted and the gap is recorded."""
    fake = FakeLauncher()
    tracked = DiagnosticLauncher(fake, tmp_path)
    identity = tracked.submit(JobSpec(name="owned", workload={})).job_id
    fake.unreadable.add(identity)

    cancel_owned(tracked)

    assert fake.cancelled == [identity]
    evidence = saved(tmp_path, identity)
    assert evidence["job_id"] == identity
    assert "diagnostics_error" in evidence


def test_failed_deletion_does_not_stop_cleanup_of_other_owned_workloads(tmp_path: Path) -> None:
    """One undeletable workload is reported, but every other owned workload is still cleaned."""
    fake = FakeLauncher()
    tracked = DiagnosticLauncher(fake, tmp_path)
    stuck, other = (tracked.submit(JobSpec(name=name, workload={})).job_id for name in ("stuck", "other"))
    fake.undeletable.add(stuck)

    with pytest.raises(ExceptionGroup) as raised:
        cancel_owned(tracked)

    assert raised.group_contains(ConnectionError)
    assert fake.cancelled == [other]
    assert saved(tmp_path, stuck)["job_id"] == stuck
    assert saved(tmp_path, other)["job_id"] == other
