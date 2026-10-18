# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

import pytest
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Attributes, Event
from watcher.lib.utils.job import get_job_attempt_for_event


def _job_attempt(attempt_index, start_time_seconds, end_time_seconds=None, job_id="123"):
    return SlurmJobExecutionStatus(
        job_id=job_id,
        workflow_id="test",
        job_attempt_index=attempt_index,
        start_time=datetime.fromtimestamp(start_time_seconds, UTC) if start_time_seconds is not None else None,
        end_time=datetime.fromtimestamp(end_time_seconds, UTC) if end_time_seconds is not None else None,
    )


@pytest.mark.parametrize(
    "attempt_execution_times,source_timestamp_ns,expected_attempt_index",
    [
        ([(0, 100, 200), (1, 300, None)], "150000000000", 0),
        ([(0, 100, 200), (1, 300, None)], "350000000000", 1),
        ([(0, 100, 200), (1, 300, None)], "250000000000", None),
        ([(0, 100, None), (1, 300, None)], "150000000000", 0),
        ([(0, 100, None), (1, 300, None)], "350000000000", 1),
        ([(0, 100, None), (1, None, None)], "150000000000", None),
        ([(0, 100, 200), (1, None, None)], "150000000000", 0),
        ([(0, 100, None), (1, None, None), (2, 300, None)], "150000000000", 0),
        ([(0, 100, 400), (1, 300, None)], "350000000000", None),
        ([(0, 100, 300), (1, 300, None)], "300000000000", None),
        ([(0, None, 200)], "150000000000", None),
        ([(0, 100, None)], None, None),
        ([(0, 100, None)], "invalid", None),
        ([(0, 100, None)], "0", None),
        ([], "150000000000", None),
    ],
)
def test_attempt_selection_requires_unique_source_window(
    attempt_execution_times, source_timestamp_ns, expected_attempt_index
):
    """Missing evidence and overlaps must not assign an event to the wrong retry."""
    event = Event(
        type=EventType.CRASH, timestamp=source_timestamp_ns, attributes=Attributes(job_id="123", uuid="event-1")
    )
    attempts = [
        _job_attempt(attempt_index, start_time_seconds, end_time_seconds)
        for attempt_index, start_time_seconds, end_time_seconds in attempt_execution_times
    ]
    matched_attempt = get_job_attempt_for_event(event, attempts)
    assert (matched_attempt.job_attempt_index if matched_attempt is not None else None) == expected_attempt_index


def test_attempt_selection_ignores_other_jobs():
    event = Event(type=EventType.CRASH, timestamp="150000000000", attributes=Attributes(job_id="123", uuid="event-1"))
    expected_attempt = _job_attempt(0, 100, 200)
    assert (
        get_job_attempt_for_event(event, [_job_attempt(1, 100, 200, job_id="456"), expected_attempt])
        is expected_attempt
    )
