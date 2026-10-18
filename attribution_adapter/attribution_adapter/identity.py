# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Public service identity, available without the optional provider client."""

from importlib.metadata import PackageNotFoundError, version

SERVICE_IDENTIFIER = "attribution-adapter"


def _service_version() -> str:
    """Return the installed package version, falling back to '0.0.0' if not installed."""
    try:
        return version("attribution-adapter")
    except PackageNotFoundError:
        return "0.0.0"
