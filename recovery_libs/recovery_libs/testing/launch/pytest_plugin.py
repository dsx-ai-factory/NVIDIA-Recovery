# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select the launcher for a test run with `--launcher`, and skip tests that need what it cannot do."""

from __future__ import annotations

import os

import pytest

from recovery_libs.testing.launch.launcher import DEFAULT_LAUNCHER, LAUNCHERS, Launcher, launcher_named


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("launcher").addoption(
        "--launcher",
        choices=sorted(LAUNCHERS),
        default=os.environ.get("E2E_LAUNCHER", DEFAULT_LAUNCHER),
        help=f"launcher that submits test jobs (default: $E2E_LAUNCHER or {DEFAULT_LAUNCHER})",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "requeue_hold: test requeue-holds a job; skipped when the launcher cannot")


@pytest.fixture(scope="session")
def launcher(pytestconfig: pytest.Config) -> Launcher:
    return launcher_named(pytestconfig.getoption("launcher"))


@pytest.fixture(autouse=True)
def launcher_capabilities(request: pytest.FixtureRequest) -> None:
    if request.node.get_closest_marker("requeue_hold") is None:
        return
    if not getattr(request.getfixturevalue("launcher"), "supports_requeue_hold", False):
        pytest.skip(f"requeue-hold is not available through the {request.config.getoption('launcher')} launcher")
