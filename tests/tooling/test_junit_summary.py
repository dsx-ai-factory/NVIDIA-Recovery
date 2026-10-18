# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Behavioral tests for the JUnit-to-Markdown CI summary."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
SUMMARY_SCRIPT = REPO_ROOT / "scripts" / "ci" / "render-junit-summary.py"


def _run_summary(reports_path: Path, output_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SUMMARY_SCRIPT), str(reports_path), "--output", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_summary_combines_reports_and_renders_failures(tmp_path: Path) -> None:
    first_report = tmp_path / "e2e-xunit-advisor-act" / "junit.xml"
    first_report.parent.mkdir()
    first_report.write_text(
        """<?xml version="1.0"?>
<testsuites>
  <testsuite name="first">
    <testcase classname="tests.smoke" name="test_passes" time="0.5" />
    <testcase classname="tests.failure" name="test_pipe|mention@team" time="1.25">
      <failure message="failure details are intentionally omitted">traceback</failure>
    </testcase>
  </testsuite>
</testsuites>
""",
        encoding="utf-8",
    )
    second_report = tmp_path / "e2e-xunit-second" / "nested" / "junit.xml"
    second_report.parent.mkdir(parents=True)
    second_report.write_text(
        """<?xml version="1.0"?>
<testsuite name="second">
  <testcase classname="tests.error" name="test_errors" time="0.125"><error /></testcase>
  <testcase classname="tests.skip" name="test_skips"><skipped /></testcase>
</testsuite>
""",
        encoding="utf-8",
    )
    output_path = tmp_path / "summary.md"

    result = _run_summary(tmp_path, output_path)

    assert result.returncode == 0, result.stderr
    summary = output_path.read_text(encoding="utf-8")
    assert "🔴 **4** tests — ✅ 1 passed · ❌ 1 failed · 💥 1 error · ⏭️ 1 skipped (in 1.88s)" in summary
    assert "| e2e-xunit-advisor-act | 2 | 1 | 1 | 0 | 0 | 1.75s |" in summary
    assert r"tests.failure::test\_pipe\|mention&#64;team" in summary
    assert r"tests.error::test\_errors" in summary
    assert "failure details are intentionally omitted" not in summary
    assert "2 JUnit report(s) collected" in summary


def test_summary_rejects_missing_and_malformed_reports(tmp_path: Path) -> None:
    output_path = tmp_path / "summary.md"
    missing = _run_summary(tmp_path, output_path)
    assert missing.returncode == 1
    assert "no junit.xml reports found" in missing.stderr
    assert not output_path.exists()

    malformed_report = tmp_path / "artifact" / "junit.xml"
    malformed_report.parent.mkdir()
    malformed_report.write_text("<testsuite>", encoding="utf-8")
    malformed = _run_summary(tmp_path, output_path)
    assert malformed.returncode == 1
    assert "cannot parse" in malformed.stderr
    assert not output_path.exists()


def test_summary_caps_failure_rows(tmp_path: Path) -> None:
    report = tmp_path / "artifact" / "junit.xml"
    report.parent.mkdir()
    testcases = "".join(
        f'<testcase classname="tests.cap" name="test_{index}"><failure /></testcase>' for index in range(55)
    )
    report.write_text(f"<testsuite>{testcases}</testsuite>", encoding="utf-8")
    output_path = tmp_path / "summary.md"

    result = _run_summary(tmp_path, output_path)

    assert result.returncode == 0, result.stderr
    summary = output_path.read_text(encoding="utf-8")
    assert "Failed and errored tests (55)" in summary
    assert summary.count("| ❌ failed |") == 50
    assert "_…and 5 more._" in summary
