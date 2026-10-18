# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reruns must distinguish unreadable reports from successful test runs."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]


@pytest.mark.parametrize(
    ("report_content", "succeeds", "scenarios"),
    [
        (None, False, None),
        ("<testsuite>", False, None),
        ('<testsuite><testcase name="passes" /></testsuite>', True, None),
        (
            (
                '<testsuite><testcase classname="tests.e2e.scenarios.10-simple-segfault.test_recovery"><failure />'
                '</testcase><testcase classname="tests.e2e.scenarios.30-job-state-crash.test_recovery"><error />'
                "</testcase></testsuite>"
            ),
            True,
            ["10-simple-segfault", "30-job-state-crash"],
        ),
    ],
    ids=(
        "missing-report",
        "malformed-report",
        "no-failures",
        "failed-and-errored-scenarios",
    ),
)
def test_rerun_failed_report_handling(
    tmp_path: Path, report_content: str | None, succeeds: bool, scenarios: list[str] | None
) -> None:
    """Reject parser failures and dispatch only scenarios from a valid failure report."""
    report = tmp_path / "junit.xml"
    if report_content is not None:
        report.write_text(report_content)

    # Replace recursive make at its command boundary to observe dispatch without
    # starting a cluster or running workload recovery scenarios.
    dispatch = tmp_path / "dispatch.json"
    capture_make = tmp_path / "capture_make.py"
    capture_make.write_text(
        "import json, os, sys\n"
        "from pathlib import Path\n"
        f"Path({str(dispatch)!r}).write_text(json.dumps({{"
        "'args': sys.argv[1:], 'scenarios': os.environ['E2E_SCENARIOS'].split()}))\n"
    )
    result = subprocess.run(
        [
            "make",
            "--no-print-directory",
            "e2e-rerun-failed",
            f"E2E_REPORT={report}",
            f"MAKE={sys.executable} {capture_make}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert (result.returncode == 0) is succeeds, result.stdout + result.stderr
    if scenarios is None:
        assert not dispatch.exists()
    else:
        recorded = json.loads(dispatch.read_text())
        assert recorded["scenarios"] == scenarios
        assert recorded["args"] == ["--no-print-directory", "e2e-rerun"]
