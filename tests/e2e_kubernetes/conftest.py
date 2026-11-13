# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bind foundation scenarios to JobSet without deploying infrastructure from pytest."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from e2e_harness.advice_operator import AdviceOperatorServices
from recovery_libs.testing.launch.jobs import Job, JobSpec
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient
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
def scenario_services(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[AdviceOperatorServices | None]:
    """Bind operator scenarios to an externally deployed fixture; foundation stays standalone."""
    if request.node.path.name != "test_advice_operator.py":
        yield None
        return
    endpoint = os.environ.get("ADVICE_FIXTURE_URL")
    if not endpoint:
        raise pytest.UsageError("ADVICE_FIXTURE_URL is required; use deploy/tilt/kubernetes/dev.sh test")
    context = os.environ.get("LAUNCHER_KUBERNETES_JOBSET_CONTEXT")
    if not context:
        raise pytest.UsageError("LAUNCHER_KUBERNETES_JOBSET_CONTEXT must name the owned development cluster")
    services = AdviceOperatorServices(
        KubectlJobSetClient(
            context=context,
            namespace=os.environ.get("LAUNCHER_KUBERNETES_JOBSET_NAMESPACE", "recovery-dev"),
        ),
        endpoint,
        tmp_path,
    )
    try:
        yield services
    finally:
        services.close()
