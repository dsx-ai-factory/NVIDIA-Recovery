# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Enums for the workload attribution HTTP interface (design §6, §7.4).

All enums are StrEnum so pydantic serializes them to their string value and the
values cross the HTTP boundary as plain strings.
"""

import enum


class ErrorCategory(enum.StrEnum):
    """Category of an attributed error, spoken in workload vocabulary (design §6)."""

    HARDWARE = "hardware"
    SYSTEM_SOFTWARE = "system_software"
    APPLICATION = "application"
    AMBIGUOUS = "ambiguous"
    UNATTRIBUTED = "unattributed"
    INSUFFICIENT_TELEMETRY = "insufficient_telemetry"


class ResourceKind(enum.StrEnum):
    """Kind of resource an attribution targets (design §6)."""

    NODE = "Node"
    ALL_JOB_RESOURCES = "AllJobResources"


class AttributionPhase(enum.StrEnum):
    """Which pass of the two-phase attribution flow a request represents (design §8)."""

    FAST = "fast"
    SLOW = "slow"


class AttributionErrorType(enum.StrEnum):
    """Informational error type on a response; never changes the publish decision (design §10)."""

    TIMEOUT = "timeout"
    INSUFFICIENT_TELEMETRY = "insufficient_telemetry"
    SERVICE_ERROR = "service_error"
    NO_ATTRIBUTION_AVAILABLE = "no_attribution_available"
