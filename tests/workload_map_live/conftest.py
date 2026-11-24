# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reuse owned launcher fixtures and attach pytest outcomes to capability evidence."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from e2e_harness.workload_map import WorkloadMapProbe
from e2e_kubernetes.conftest import DiagnosticLauncher
from recovery_libs.testing.launch.kubernetes import KubernetesJobSetLauncher
from recovery_libs.testing.launch.pytorch import KubernetesPyTorchLauncher


class AdmissionClient:
    """Apply the real probe before create; delegate reads and cleanup unchanged."""

    def __init__(self, probe: WorkloadMapProbe) -> None:
        self.probe = probe

    def __getattr__(self, name: str) -> Any:
        return getattr(self.probe.client, name)

    def create(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """Submit only the fully prepared manifest to Kubernetes."""
        return self.probe.client.create(self.probe.prepare(manifest))


@pytest.fixture
def launcher(
    pytestconfig: pytest.Config, tmp_path: Path, scenario_services: WorkloadMapProbe
) -> Iterator[DiagnosticLauncher]:
    """Instrument before submission and retain diagnostics and exact-UID cleanup."""
    selected = pytestconfig.getoption("launcher")
    types = {"kubernetes-jobset": KubernetesJobSetLauncher, "kubernetes-pytorch": KubernetesPyTorchLauncher}
    if selected not in types:
        raise pytest.UsageError("qualification requires a Kubernetes workload launcher")
    tracked = DiagnosticLauncher(types[selected](AdmissionClient(scenario_services)), tmp_path)
    yield tracked
    cancel_owned(tracked)


def cancel_owned(tracked: DiagnosticLauncher) -> None:
    """Save diagnostics for and cancel every workload this test submitted, including terminal ones.

    The scenario runner cancels only jobs that are still active, so a workload that already succeeded or failed
    would otherwise be left in the cluster without its evidence. Every owned workload is attempted even when an
    earlier one fails, and the failures are raised together.
    """
    errors = []
    for job_id in tracked.owned:
        try:
            tracked.cancel(job_id=job_id)
        except Exception as error:  # noqa: BLE001 - attempt cleanup of every owned workload
            errors.append(error)
    if errors:
        raise ExceptionGroup("owned qualification workload cleanup failed", errors)


# Define launcher locally so the nested plugin's session-scoped bare launcher
# cannot override per-test diagnostics and terminal-workload teardown.
pytest_plugins = ["e2e_kubernetes.conftest"]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Keep scenario failure distinct from individual capabilities already observed."""
    result = yield
    report = result.get_result()
    services = getattr(item, "funcargs", {}).get("scenario_services")
    if isinstance(services, WorkloadMapProbe):
        if report.failed:
            services.finish("fail")
        elif report.when == "call":
            services.finish("pass" if report.passed else "not_tested")
