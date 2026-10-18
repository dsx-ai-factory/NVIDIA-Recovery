# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Unit tests for time utilities."""

from datetime import UTC, datetime

import pytest
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.utils.time import parse_event_timestamp


class TestParseEventTimestamp:
    """Test suite for parse_event_timestamp."""

    def test_prefers_detection_time(self):
        """Uses detection_time when available."""
        event = Event(
            body="boom",
            type=EventType.CRASH,
            timestamp="1700000000",
            attributes=Attributes(job_id="job-1", detection_time="2025-01-01T00:00:00Z", uuid="uuid-1"),
        )

        result = parse_event_timestamp(event)

        assert result is not None
        assert result.astimezone(UTC) == datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_parses_seconds_timestamp(self):
        """Parses second-based epoch timestamps."""
        event = Event(body=None, type=EventType.CRASH, timestamp="1700000000")

        result = parse_event_timestamp(event)

        assert result == datetime.fromtimestamp(1700000000, tz=UTC)

    def test_parses_milliseconds_timestamp(self):
        """Parses millisecond-based epoch timestamps."""
        event = Event(body=None, type=EventType.CRASH, timestamp="170000000000")

        result = parse_event_timestamp(event)

        assert result == datetime.fromtimestamp(170000000000 / 1_000, tz=UTC)

    def test_parses_nanoseconds_timestamp(self):
        """Parses nanosecond-based epoch timestamps."""
        event = Event(body=None, type=EventType.CRASH, timestamp="1700000000000000000")

        result = parse_event_timestamp(event)

        expected = datetime.fromtimestamp(1700000000000000000 / 1_000_000_000, tz=UTC)
        assert result is not None
        assert result.timestamp() == pytest.approx(expected.timestamp(), abs=1e-6)

    def test_parses_iso_timestamp_string(self):
        """Parses non-digit timestamp strings."""
        event = Event(body=None, type=EventType.CRASH, timestamp="2025-01-02T03:04:05Z")

        result = parse_event_timestamp(event)

        assert result is not None
        assert result.astimezone(UTC) == datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_returns_none_for_invalid_timestamp(self):
        """Returns None when timestamps cannot be parsed."""
        event = Event(body=None, type=EventType.CRASH, timestamp="not-a-date")

        result = parse_event_timestamp(event)

        assert result is None
