#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Render downloaded JUnit XML reports as a bounded Markdown summary."""

from __future__ import annotations

import argparse
import html
import math
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

FAILURE_ROW_LIMIT = 50
FIELD_LENGTH_LIMIT = 300


class SummaryError(RuntimeError):
    """Report an invalid or unusable JUnit report collection."""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _duration_ms(raw_duration: str | None) -> int:
    try:
        seconds = float(raw_duration or "0")
    except ValueError:
        return 0
    if not math.isfinite(seconds) or seconds < 0:
        return 0
    return round(seconds * 1000)


def _test_status(testcase: ET.Element) -> str:
    child_names = {_local_name(child.tag) for child in testcase}
    if "error" in child_names:
        return "error"
    if "failure" in child_names:
        return "failed"
    if "skipped" in child_names:
        return "skipped"
    return "passed"


def _test_name(testcase: ET.Element) -> str:
    classname = (testcase.get("classname") or "").strip()
    name = (testcase.get("name") or "<unnamed test>").strip()
    value = f"{classname}::{name}" if classname else name
    return " ".join(value.split())[:FIELD_LENGTH_LIMIT]


def _source_name(reports_path: Path, report_path: Path) -> str:
    relative = report_path.relative_to(reports_path)
    return str(relative.parent) if relative.parent != Path(".") else report_path.name


def _empty_stats() -> dict[str, int]:
    return {"total": 0, "passed": 0, "failed": 0, "error": 0, "skipped": 0, "duration_ms": 0}


def _parse_report(reports_path: Path, report_path: Path) -> dict[str, Any]:
    try:
        root = ET.parse(report_path).getroot()
    except (ET.ParseError, OSError) as error:
        raise SummaryError(f"cannot parse {report_path}: {error}") from error

    source = _source_name(reports_path, report_path)
    stats = _empty_stats()
    failures: list[dict[str, Any]] = []
    for testcase in (element for element in root.iter() if _local_name(element.tag) == "testcase"):
        status = _test_status(testcase)
        duration_ms = _duration_ms(testcase.get("time"))
        stats["total"] += 1
        stats[status] += 1
        stats["duration_ms"] += duration_ms
        if status in {"failed", "error"}:
            failures.append(
                {
                    "duration_ms": duration_ms,
                    "name": _test_name(testcase),
                    "source": source,
                    "status": status,
                }
            )

    return {"failures": failures, "source": source, "stats": stats}


def _escape_cell(value: object) -> str:
    escaped = html.escape(" ".join(str(value).split())[:FIELD_LENGTH_LIMIT], quote=False)
    escaped = escaped.replace("\\", "\\\\").replace("@", "&#64;")
    for character in ("|", "`", "*", "_", "[", "]"):
        escaped = escaped.replace(character, f"\\{character}")
    return escaped


def _format_duration(milliseconds: object) -> str:
    duration = int(milliseconds) if isinstance(milliseconds, int | float) else 0
    return f"{duration}ms" if duration < 1000 else f"{duration / 1000:.2f}s"


def _stats_text(stats: dict[str, int]) -> str:
    parts = []
    for status, emoji in (("passed", "✅"), ("failed", "❌"), ("error", "💥"), ("skipped", "⏭️")):
        if stats[status]:
            parts.append(f"{emoji} {stats[status]} {status}")
    return " · ".join(parts) if parts else "no results"


def render_summary(reports_path: Path) -> str:
    """Return a deterministic Markdown summary for every nested ``junit.xml`` report."""

    report_paths = sorted(reports_path.rglob("junit.xml")) if reports_path.is_dir() else []
    if not report_paths:
        raise SummaryError(f"no junit.xml reports found under {reports_path}")

    reports = [_parse_report(reports_path, report_path) for report_path in report_paths]
    combined = _empty_stats()
    failures: list[dict[str, Any]] = []
    for report in reports:
        for key in combined:
            combined[key] += report["stats"][key]
        failures.extend(report["failures"])

    if combined["total"] == 0:
        raise SummaryError("JUnit reports contained no test cases")

    overall = "🔴" if combined["failed"] or combined["error"] else "🟢"
    lines = [
        "## E2E Test Summary",
        "",
        (
            f"{overall} **{combined['total']}** tests — {_stats_text(combined)} "
            f"(in {_format_duration(combined['duration_ms'])})"
        ),
        "",
        "| Report | Total | Passed | Failed | Errors | Skipped | Duration |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for report in reports:
        stats = report["stats"]
        lines.append(
            f"| {_escape_cell(report['source'])} | {stats['total']} | {stats['passed']} | {stats['failed']} | "
            f"{stats['error']} | {stats['skipped']} | {_format_duration(stats['duration_ms'])} |"
        )

    if failures:
        lines.extend(
            [
                "",
                f"<details><summary>Failed and errored tests ({len(failures)})</summary>",
                "",
                "| Status | Report | Test | Duration |",
                "|---|---|---|---:|",
            ]
        )
        for failure in failures[:FAILURE_ROW_LIMIT]:
            emoji = "❌" if failure["status"] == "failed" else "💥"
            lines.append(
                f"| {emoji} {failure['status']} | {_escape_cell(failure['source'])} | "
                f"{_escape_cell(failure['name'])} | {_format_duration(failure['duration_ms'])} |"
            )
        if len(failures) > FAILURE_ROW_LIMIT:
            lines.extend(["", f"_…and {len(failures) - FAILURE_ROW_LIMIT} more._"])
        lines.extend(["", "</details>"])

    lines.extend(["", f"{len(report_paths)} JUnit report(s) collected"])
    return "\n".join(lines)


def main() -> int:
    """Parse CLI arguments and write the rendered Markdown summary."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports_path", type=Path, help="directory containing nested junit.xml files")
    parser.add_argument("--output", required=True, type=Path, help="Markdown output path")
    args = parser.parse_args()

    try:
        summary = render_summary(args.reports_path)
    except SummaryError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    args.output.write_text(f"{summary}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
