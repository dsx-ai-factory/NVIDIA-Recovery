# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reuse owned launcher fixtures and attach pytest outcomes to capability evidence."""

import pytest
from e2e_harness.workload_map import WorkloadMapProbe
from e2e_kubernetes.conftest import launcher

__all__ = ["launcher"]

# Re-export launcher locally so the nested plugin's session-scoped bare launcher
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
