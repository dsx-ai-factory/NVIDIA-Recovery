#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Require immutable external action references in workflow and action YAML."""

import re
import sys
from pathlib import Path

import yaml


def references(document):
    """Yield action and reusable-workflow references from execution positions."""
    for job in document.get("jobs", {}).values():
        if "uses" in job:
            yield job["uses"]
        for step in job.get("steps", []):
            if "uses" in step:
                yield step["uses"]
    for step in document.get("runs", {}).get("steps", []):
        if "uses" in step:
            yield step["uses"]


def main():
    """Check supplied YAML files, returning failure for mutable references."""
    failed = False
    for filename in sys.argv[1:]:
        try:
            document = yaml.load(Path(filename).read_text(), Loader=yaml.BaseLoader)
            for ref in references(document):
                if ref.startswith("./"):
                    continue
                pattern = (
                    r"docker://[^@]+@sha256:[0-9a-fA-F]{64}"
                    if ref.startswith("docker://")
                    else r"[^@]+@[0-9a-fA-F]{40}"
                )
                if not re.fullmatch(pattern, ref):
                    print(
                        f"{filename}: pin {ref!r} to a full commit SHA (or container SHA-256 digest)", file=sys.stderr
                    )
                    failed = True
        except (OSError, yaml.YAMLError, AttributeError, TypeError) as error:
            print(f"{filename}: cannot check action pins: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
