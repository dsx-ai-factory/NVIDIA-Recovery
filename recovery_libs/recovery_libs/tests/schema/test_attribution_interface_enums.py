# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import enum

import pytest
from recovery_libs.schema.attribution_interface.enums import (
    AttributionErrorType,
    AttributionPhase,
    ErrorCategory,
    ResourceKind,
)


@pytest.mark.parametrize(
    "member, expected",
    [
        (ErrorCategory.HARDWARE, "hardware"),
        (ErrorCategory.SYSTEM_SOFTWARE, "system_software"),
        (ErrorCategory.APPLICATION, "application"),
        (ErrorCategory.AMBIGUOUS, "ambiguous"),
        (ErrorCategory.UNATTRIBUTED, "unattributed"),
        (ErrorCategory.INSUFFICIENT_TELEMETRY, "insufficient_telemetry"),
        (ResourceKind.NODE, "Node"),
        (ResourceKind.ALL_JOB_RESOURCES, "AllJobResources"),
        (AttributionPhase.FAST, "fast"),
        (AttributionPhase.SLOW, "slow"),
        (AttributionErrorType.TIMEOUT, "timeout"),
        (AttributionErrorType.INSUFFICIENT_TELEMETRY, "insufficient_telemetry"),
        (AttributionErrorType.SERVICE_ERROR, "service_error"),
        (AttributionErrorType.NO_ATTRIBUTION_AVAILABLE, "no_attribution_available"),
    ],
)
def test_enum_string_values(member, expected):
    """Every enum member serializes to its exact frozen string value and is a str."""
    assert member.value == expected
    assert member == expected
    assert isinstance(member, str)


@pytest.mark.parametrize(
    "enum_cls, count",
    [
        (ErrorCategory, 6),
        (ResourceKind, 2),
        (AttributionPhase, 2),
        (AttributionErrorType, 4),
    ],
)
def test_enum_is_strenum_and_member_count(enum_cls, count):
    """Each enum is a StrEnum with exactly the frozen member count (no drift)."""
    assert issubclass(enum_cls, enum.StrEnum)
    assert len(list(enum_cls)) == count
