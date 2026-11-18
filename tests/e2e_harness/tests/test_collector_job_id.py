# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check which job ID the reference collector extracts from a log filename.

The collector labels every log line with `job_id`, taken from the filename by the `stage.regex` blocks of
`alloy-application-config.river`. Backend components then match lines to jobs by that label, so a filename that yields
the wrong ID silently attributes a job's logs to another job. These tests read the real expressions from the config.

Alloy applies each expression with Go's `regexp` (leftmost-first, unanchored). Python's `re` resolves the alternations
and greedy repetition used here the same way, so applying the stages in order reproduces the collector's result. A
stage that does not match leaves the earlier result in place, and a later matching stage replaces it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CONFIG = Path(__file__).resolve().parents[3] / "scripts/ci/e2e/services/values/alloy-application-config.river"
EXPRESSION = re.compile(r'expression\s*=\s*"((?:[^"\\]|\\.)*)"')


def extraction_stages() -> list[re.Pattern[str]]:
    """The expressions that set `job_id`, in the order the collector applies them."""

    # River strings escape a backslash with another one, so `\\d` in the file is the regex `\d`.
    expressions = [match.replace("\\\\", "\\") for match in EXPRESSION.findall(CONFIG.read_text())]
    return [re.compile(expression) for expression in expressions if "(?P<job_id>" in expression]


def collected_job_id(filename: str) -> str | None:
    """The `job_id` label the collector would attach to lines read from `filename`."""

    job_id = None
    for stage in extraction_stages():
        if match := stage.search(filename):
            job_id = match.group("job_id")
    return job_id


def test_the_collector_config_defines_job_id_extraction() -> None:
    """The cases below would all pass vacuously if the expressions could not be found."""

    assert extraction_stages()


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        # Generated link names keep the full array-task ID whole, whatever the source file was called.
        ("/home/log-links/output-3f2a9c1d5e7b8a01-a0_12345_date_26-11-17_time_12-00-00.log", "12345"),
        ("/home/log-links/output-3f2a9c1d5e7b8a01-a0_42_7_date_26-11-17_time_12-00-00.log", "42_7"),
        # Digits in the source stem must not be mistaken for the job.
        ("/home/log-links/epoch-7-3f2a9c1d5e7b8a01-a2_99_date_26-11-17_time_12-00-00.log", "99"),
        # APS names: the trailing timestamp seconds are not the job ID, and a qualifier after it changes nothing.
        ("/home/log-links/train_42_date_26-11-17_time_12-00-00.log", "42"),
        ("/home/log-links/train_5558327_date_26-11-17_time_17-15-43_cycle1.log", "5558327"),
        # Legacy date names with either separator before the job ID.
        ("/home/log-links/worker-42_date_nov18.log", "42"),
        ("/home/log-links/worker_42_date_nov18.log", "42"),
        # Names without a date part take their numeric suffix, which is also what Slurm's defaults produce.
        ("/home/log-links/slurm-1234.out", "1234"),
        ("/home/log-links/report_17_date_export-42.out", "42"),
        # No identity to extract.
        ("/home/log-links/notes.log", None),
    ],
)
def test_collector_extracts_the_job_id(filename: str, expected: str | None) -> None:
    assert collected_job_id(filename) == expected
