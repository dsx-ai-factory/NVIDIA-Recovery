# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check which job ID the reference collector extracts from a log filename.

The collector labels every log line with `job_id`, taken from the filename by the `stage.regex` blocks of
`alloy-application-config.river`. Backend components then match lines to jobs by that label, so a filename that yields
the wrong ID silently attributes a job's logs to another job. These tests read the real expressions from the config.

Alloy applies each expression with Go's `regexp` (leftmost-first, unanchored). Python's `re` resolves the alternations
and greedy repetition used here the same way, and with `re.ASCII` its digit class matches only ASCII digits, as Go's
does. So applying the stages in order reproduces the collector's result. A stage that does not match leaves the earlier
result in place, and a later matching stage replaces it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CONFIG = Path(__file__).resolve().parents[3] / "scripts/ci/e2e/services/values/alloy-application-config.river"
# A block's closing brace sits on its own line; expressions contain braces such as `{16}`.
REGEX_STAGE = re.compile(r"stage\.regex\s*\{(.*?)^\s*\}", re.DOTALL | re.MULTILINE)
EXPRESSION = re.compile(r'expression\s*=\s*"((?:[^"\\]|\\.)*)"')
SOURCE = re.compile(r'source\s*=\s*"([^"]*)"')


def extraction_stages() -> list[tuple[re.Pattern[str], str | None]]:
    """Each stage that sets `job_id`, with the field it reads, in the order the collector applies them."""

    stages = []
    for block in REGEX_STAGE.findall(CONFIG.read_text()):
        expression = EXPRESSION.search(block)
        if expression is None or "(?P<job_id>" not in expression.group(1):
            continue
        source = SOURCE.search(block)
        # River strings escape a backslash with another one, so `\\d` in the file is the regex `\d`.
        pattern = re.compile(expression.group(1).replace("\\\\", "\\"), re.ASCII)
        stages.append((pattern, source.group(1) if source else None))
    return stages


def collected_job_id(filename: str) -> str | None:
    """The `job_id` label the collector would attach to lines read from `filename`."""

    job_id = None
    for stage, _ in extraction_stages():
        if match := stage.search(filename):
            job_id = match.group("job_id")
    return job_id


def test_the_collector_config_defines_job_id_extraction() -> None:
    """The cases below would all pass vacuously if the expressions could not be found."""

    assert extraction_stages()


def test_the_collector_reads_job_id_from_the_filename() -> None:
    """A stage without `source` reads the log line instead, so every case below would stop describing the collector."""

    assert all(source == "filename" for _, source in extraction_stages())


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
        # A numeric qualifier must not be taken for the job either.
        ("/home/log-links/train_42_date_26-11-17_time_12-00-00_cycle-7.log", "42"),
        # Legacy date names with either separator before the job ID.
        ("/home/log-links/worker-42_date_nov18.log", "42"),
        ("/home/log-links/worker_42_date_nov18.log", "42"),
        # Names without a date part take their numeric suffix, which is also what Slurm's defaults produce.
        ("/home/log-links/slurm-1234.out", "1234"),
        ("/home/log-links/report_17_date_export-42.out", "42"),
        # No identity to extract.
        ("/home/log-links/notes.log", None),
        # Go's `\d` is ASCII-only, so other Unicode digits are not an identity.
        ("/home/log-links/slurm-\u0664\u0662.out", None),
    ],
)
def test_collector_extracts_the_job_id(filename: str, expected: str | None) -> None:
    assert collected_job_id(filename) == expected
