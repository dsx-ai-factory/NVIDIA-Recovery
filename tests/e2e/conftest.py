# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bind the e2e harness to the deployed stack, and choose and order the scenarios a run executes."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import ExitStack

import pytest
from e2e_harness.config import E2EConfig, e2e_config_from_environment
from e2e_harness.recovery_client import RecoveryClient, ServiceUrls, open_recovery_client
from e2e_harness.services import E2EServices
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.kubernetes import StackControl, service_url
from recovery_libs.testing.stack_lock import stack_access as hold_stack

pytest_plugins = [
    "recovery_libs.testing.launch.pytest_plugin",
    "recovery_libs.testing.scenario_plugin",
    "recovery_libs.testing.trace_export",
]


@pytest.fixture(scope="session")
def e2e_config() -> E2EConfig:
    return e2e_config_from_environment(os.environ)


@pytest.fixture(autouse=True)
def stack_access(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Run exclusive scenarios alone; other scenarios share the stack under pytest-xdist."""

    lock_path = tmp_path_factory.getbasetemp().parent / "e2e-stack.lock"
    with hold_stack(lock_path, exclusive=request.node.get_closest_marker("exclusive") is not None):
        yield


@pytest.fixture(scope="session")
def stack(e2e_config: E2EConfig) -> StackControl:
    return StackControl(
        namespace=e2e_config.stack_namespace or e2e_config.slurm_namespace,
        kubeconfig=e2e_config.kubeconfig,
        timeout=e2e_config.stack_timeout_seconds,
    )


@pytest.fixture(scope="session")
def slurm_stack(e2e_config: E2EConfig) -> StackControl:
    return StackControl(
        namespace=e2e_config.slurm_namespace,
        kubeconfig=e2e_config.kubeconfig,
        timeout=e2e_config.stack_timeout_seconds,
    )


@pytest.fixture(scope="session")
def fixtures() -> Fixtures:
    return Fixtures()


@pytest.fixture(scope="session")
def recovery(e2e_config: E2EConfig) -> Iterator[RecoveryClient]:
    namespace = e2e_config.stack_namespace or e2e_config.slurm_namespace
    kubeconfig = e2e_config.kubeconfig
    log_service = ("loki-gateway", 80) if e2e_config.log_backend == "loki" else ("victorialogs", 9428)
    services = {
        "job_manager": ("E2E_JOB_MANAGER_URL", namespace, "job-manager", 8080),
        "notifications": ("E2E_NOTIFICATIONS_URL", namespace, e2e_config.workload_name("notifications"), 8080),
        "gateway": ("E2E_API_GATEWAY_URL", namespace, e2e_config.workload_name("api-gateway"), 8080),
        "logs": ("E2E_LOGS_URL", "monitoring", *log_service),
        "controller": ("E2E_CONTROLLER_URL", e2e_config.slurm_namespace, "slurm-advisor-controller", 9090),
    }
    with ExitStack() as stack:
        urls = {
            name: stack.enter_context(
                service_url(
                    namespace=ns, service=service, port=port, url=os.environ.get(variable), kubeconfig=kubeconfig
                )
            )
            for name, (variable, ns, service, port) in services.items()
        }
        with open_recovery_client(ServiceUrls(**urls), log_backend=e2e_config.log_backend) as client:
            yield client


@pytest.fixture(scope="session")
def scenario_services(recovery: RecoveryClient, stack: StackControl) -> E2EServices:
    return E2EServices.of(recovery, stack)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    def scenario_name(item: pytest.Item) -> str:
        marker = item.get_closest_marker("scenario")
        return str(marker.args[0]) if marker and marker.args else "99-unmarked"

    requested = tuple(os.environ.get("E2E_SCENARIOS", "").split())
    if requested:
        available = {scenario_name(item) for item in items}
        unknown = tuple(name for name in requested if name not in available)
        if unknown:
            names = ", ".join(unknown)
            raise pytest.UsageError(f"E2E_SCENARIOS requested unknown scenario: {names}")

        selected_names = set(requested)
        selected = [item for item in items if scenario_name(item) in selected_names]
        deselected = [item for item in items if scenario_name(item) not in selected_names]
        items[:] = selected
        config.hook.pytest_deselected(items=deselected)

    items.sort(
        key=lambda item: (
            item.get_closest_marker("run_first") is None,
            scenario_name(item),
            item.nodeid,
        )
    )
