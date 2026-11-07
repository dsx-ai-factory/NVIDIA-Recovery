# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Execute collector filename extraction against independent scheduler identity examples."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

CONFIG = Path(__file__).parents[2] / "scripts/ci/e2e/services/values/alloy-application-config.river"


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("/home/log-links/slurm-4200_0.out", "4200_0"),
        ("/home/log-links/slurm-4200_7.log", "4200_7"),
        ("/home/log-links/slurm-4200.out", "4200"),
        ("/home/log-links/job_4200.log", "4200"),
        ("/home/log-links/train-4200_date_2026-11-07.out", "4200"),
        ("/home/log-links/train-4200_date_2026.out", "4200"),
        ("/home/log-links/train-4200_7.out", None),
        ("/home/log-links/train-4200_0.log", None),
        ("/home/log-links/train-4200_7_date_2026-11-07.out", None),
        ("/home/log-links/slurm-4200_[0-7].out", None),
        ("/home/log-links/job_4200/unidentified.out", None),
    ],
)
def test_collector_labels_only_unambiguous_filename_identities(filename: str, expected: str | None) -> None:
    """Run the shipped regex stages in order; unknown custom pairs must never become sibling/standalone IDs.

    These expressions use the common Python/RE2 subset. Decode the River string literals so the test
    exercises the configured escaping and capture behavior, rather than duplicating the expressions.
    """

    expressions = re.findall(r'expression\s*=\s*("(?:[^"\\]|\\.)*")', CONFIG.read_text())
    assert expressions, "collector has no filename extraction stages"
    extracted: dict[str, str] = {}
    for expression in expressions:
        match = re.search(json.loads(expression), filename)
        if match is not None:
            extracted.update(match.groupdict())
    assert extracted.get("job_id") == expected
