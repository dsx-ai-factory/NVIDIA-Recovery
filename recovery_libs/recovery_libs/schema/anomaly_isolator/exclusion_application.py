# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Application outcomes for effective node exclusions."""

from enum import Enum


class ExclusionApplicationStatus(str, Enum):
    """Whether an attribution's exclusion is applicable and confirmed satisfied."""

    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"
    NOT_APPLIED = "not_applied"
    APPLIED = "applied"
