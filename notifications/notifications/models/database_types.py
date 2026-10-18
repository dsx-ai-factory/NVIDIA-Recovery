# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return the current UTC time through a function that tests can freeze."""
    return datetime.now(tz=UTC)
