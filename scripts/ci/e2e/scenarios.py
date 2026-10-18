#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""List E2E scenario names, or the scenarios that failed in a JUnit report.

Both commands print one scenario name per line, in the form E2E_SCENARIOS
accepts.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree

SCENARIO_ROOT = Path(__file__).resolve().parents[3] / "tests" / "e2e" / "scenarios"


def list_scenarios() -> list[str]:
    """Return every scenario directory name in suite order."""

    return sorted(path.name for path in SCENARIO_ROOT.glob("[0-9][0-9]-*") if path.is_dir())


def failed_scenarios(report: Path) -> list[str]:
    """Return scenarios with a failed or errored test case in a pytest JUnit report."""

    failed: set[str] = set()
    for case in ElementTree.parse(report).getroot().iter("testcase"):
        if case.find("failure") is None and case.find("error") is None:
            continue
        parts = (case.get("classname") or case.get("name") or "").split(".")
        if parts[:3] == ["tests", "e2e", "scenarios"] and len(parts) > 3:
            failed.add(parts[3])
    return sorted(failed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="print every scenario name")
    failed = commands.add_parser("failed", help="print scenarios that failed in a JUnit report")
    failed.add_argument("report", type=Path)
    args = parser.parse_args()

    if args.command == "list":
        names = list_scenarios()
    else:
        if not args.report.is_file():
            print(f"JUnit report not found: {args.report}", file=sys.stderr)
            return 1
        names = failed_scenarios(args.report)
    print("\n".join(names))
    return 0


if __name__ == "__main__":
    sys.exit(main())
