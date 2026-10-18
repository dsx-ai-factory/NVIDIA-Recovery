# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Logger abstraction for notifications service.

This module provides a centralized logger configuration for the notifications
service, leveraging the recovery_libs logger utility with custom configurations.
"""

import logging
import os

from recovery_libs.utils import logger as recovery_logger

# Service-specific logger configuration
SERVICE_NAME = "notifications"
DEFAULT_LIBRARY_LEVELS = {
    "httpx": logging.WARNING,
    "urllib3": logging.WARNING,
    "httpcore": logging.WARNING,
}


def get_logger(
    name: str = SERVICE_NAME,
    level: int | None = None,
    custom_library_level: dict[str, int] | None = None,
    propagate: bool = False,
) -> logging.Logger:
    """Get a configured logger for the notifications service.

    This function wraps the recovery_libs get_logger utility with service-specific
    defaults. It configures logging to stdout with optional OTLP support if
    environment variables are set.

    Args:
        name: Logger name. Defaults to SERVICE_NAME.
        level: Logging level (e.g., logging.INFO, logging.DEBUG).
            If None, uses LOGGING_LEVEL or LOG_LEVEL environment variable.
        custom_library_level: Dictionary mapping library names to their desired
            log levels. If None, uses DEFAULT_LIBRARY_LEVELS.
        propagate: Whether to propagate logs to parent loggers.
            Defaults to False to prevent duplicate logs.

    Returns:
        Configured logging.Logger instance.
    """
    # Support both LOGGING_LEVEL (legacy) and LOG_LEVEL (standard)
    if level is None:
        level_str = os.environ.get("LOGGING_LEVEL") or os.environ.get("LOG_LEVEL", "INFO")
        level = logging.getLevelNamesMapping().get(level_str.upper(), logging.INFO)

    library_levels = custom_library_level or DEFAULT_LIBRARY_LEVELS

    return recovery_logger.get_logger(
        name=name,
        level=level,
        custom_library_level=library_levels,
        propagate=propagate,
        enable_otlp=True,  # Service logger exports to OTLP
    )


# Module-level logger instance
logger = get_logger()
