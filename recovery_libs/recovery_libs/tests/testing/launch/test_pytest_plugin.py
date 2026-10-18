# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

pytest_plugins = ["pytester"]

SUITE = """
import pytest


@pytest.mark.requeue_hold
def test_requeue_hold():
    pass


def test_plain():
    pass
"""


def _run(pytester: pytest.Pytester, launcher: str) -> pytest.RunResult:
    pytester.makeconftest(f"""
import pytest


class Launcher:
    {launcher}


@pytest.fixture(scope="session")
def launcher():
    return Launcher()
""")
    pytester.makepyfile(SUITE)
    return pytester.runpytest("-p", "recovery_libs.testing.launch.pytest_plugin", "-rs", "--strict-markers")


def test_a_requeue_hold_test_is_skipped_when_the_launcher_cannot_hold(pytester: pytest.Pytester) -> None:
    result = _run(pytester, "pass")

    result.assert_outcomes(passed=1, skipped=1)
    result.stdout.fnmatch_lines(["*requeue-hold is not available*"])


def test_a_requeue_hold_test_runs_when_the_launcher_can_hold(pytester: pytest.Pytester) -> None:
    _run(pytester, "supports_requeue_hold = True").assert_outcomes(passed=2)
