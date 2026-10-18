# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

_MYSQL_TIMESTAMP_MIN_UTC = datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
_MYSQL_TIMESTAMP_MAX_UTC = datetime(2038, 1, 19, 3, 14, 7, 499999, tzinfo=UTC)


def normalize_mysql_timestamp(value: datetime, *, field_name: str = "datetime field") -> datetime:
    """Normalize a timezone-aware value to UTC and enforce MySQL's TIMESTAMP range."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TypeError(f"tzinfo is required for {field_name}, none is provided")

    normalized = value.astimezone(UTC)
    if normalized < _MYSQL_TIMESTAMP_MIN_UTC or normalized > _MYSQL_TIMESTAMP_MAX_UTC:
        raise ValueError(f"{field_name} is outside the supported MySQL TIMESTAMP range")
    return normalized
