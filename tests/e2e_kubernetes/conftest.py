# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bind foundation scenarios to JobSet without deploying infrastructure from pytest."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from recovery_libs.testing.launch.jobs import Job, JobSpec
from recovery_libs.testing.launch.launcher import Launcher, launcher_named

pytest_plugins = [
    "recovery_libs.testing.launch.pytest_plugin",
    "recovery_libs.testing.scenario_plugin",
    "recovery_libs.testing.trace_export",
]


class DiagnosticLauncher:
    """Capture owned workload evidence before deletion, including successful runs.

    The runner deletes active jobs before pytest reports the outcome. Capturing at
    that boundary preserves failure evidence; fixture teardown also deletes jobs
    that became terminal before the runner cleaned up.
    """

    def __init__(self, delegate: Any, directory: Path) -> None:
        self.delegate = delegate
        self.directory = directory
        self.owned: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    def submit(self, spec: JobSpec) -> Job:
        """Remember the identity returned by successful submission."""
        job = self.delegate.submit(spec)
        self.owned.append(job.job_id)
        return job

    def cancel(self, *, job_id: str) -> None:
        """Save diagnostics once, without letting a diagnostic failure prevent cleanup."""
        destination = self.directory / (job_id.replace("/", "_") + ".json")
        try:
            if not destination.exists():
                try:
                    evidence = self.delegate.diagnostics(job_id=job_id)
                except Exception as error:  # noqa: BLE001 - diagnostics must never prevent owned cleanup
                    evidence = {"diagnostics_error": repr(error), "job_id": job_id}
                destination.write_text(json.dumps(evidence, indent=2, default=str) + "\n")
        finally:
            self.delegate.cancel(job_id=job_id)


@pytest.fixture
def launcher(pytestconfig: pytest.Config, tmp_path: Path) -> Iterator[Launcher]:
    """Use a distinct launcher owner for every test and retain its diagnostic artifacts."""
    selected = pytestconfig.getoption("launcher")
    if selected != "kubernetes-jobset":
        raise pytest.UsageError("Kubernetes foundation tests require --launcher=kubernetes-jobset")
    tracked = DiagnosticLauncher(launcher_named(selected), tmp_path)
    yield tracked
    errors = []
    for job_id in tracked.owned:
        try:
            tracked.cancel(job_id=job_id)
        except Exception as error:  # noqa: BLE001 - attempt cleanup of every owned workload
            errors.append(error)
    if errors:
        raise ExceptionGroup("owned Kubernetes workload cleanup failed", errors)


@pytest.fixture
def scenario_services() -> None:
    """The foundation needs no Recovery services."""
