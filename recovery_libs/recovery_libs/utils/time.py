# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import UTC, datetime

from dateutil import parser

from recovery_libs.schema.watcher.model import Event

logger = logging.getLogger(__name__)


def source_timestamp_in_attempt(
    timestamp: str | None, start_time: datetime | None, end_time: datetime | None = None
) -> bool:
    """Match a source nanosecond timestamp to a known attempt execution window.

    Detection/ingestion time and submission time cannot establish which retry
    produced a record in an append-only shared log. Missing evidence fails closed.
    """
    if not timestamp or start_time is None:
        return False
    try:
        source_time = datetime.fromtimestamp(int(timestamp) / 1e9, tz=UTC)
    except (ValueError, TypeError, OverflowError, OSError):
        return False
    start = start_time.replace(tzinfo=UTC) if start_time.tzinfo is None else start_time
    end = end_time.replace(tzinfo=UTC) if end_time and end_time.tzinfo is None else end_time
    return source_time >= start and (end is None or source_time <= end)


def parse_event_timestamp(event: Event) -> datetime | None:
    if event.attributes and event.attributes.detection_time:
        try:
            return parser.parse(event.attributes.detection_time)
        except (ValueError, OverflowError):
            logger.warning("Failed to parse watcher detection_time: %s", event.attributes.detection_time)
    if event.timestamp:
        if event.timestamp.isdigit():
            value = int(event.timestamp)
            # Heuristic: treat >1e12 as nanoseconds, >1e10 as milliseconds, else seconds.
            if value > 1_000_000_000_000:
                return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)
            if value > 10_000_000_000:
                return datetime.fromtimestamp(value / 1_000, tz=UTC)
            return datetime.fromtimestamp(value, tz=UTC)
        try:
            return parser.parse(event.timestamp)
        except (ValueError, OverflowError):
            logger.warning("Failed to parse event timestamp: %s", event.timestamp)
    return None
