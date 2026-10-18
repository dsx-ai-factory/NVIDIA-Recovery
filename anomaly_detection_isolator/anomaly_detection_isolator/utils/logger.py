# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Logger abstraction for anomaly_detection_isolator service.

This module provides a centralized logger configuration for the anomaly_detection_isolator
service, leveraging the recovery_libs logger utility with custom configurations.
"""

import logging

from recovery_libs.utils import logger as recovery_logger

# Service-specific logger configuration
SERVICE_NAME = "anomaly_detection_isolator"
DEFAULT_LIBRARY_LEVELS = {
    "urllib3": logging.WARNING,
    "httpcore": logging.WARNING,
}


def get_logger(
    name: str = SERVICE_NAME,
    level: int = recovery_logger.LOG_LEVEL,
    custom_library_level: dict[str, int] | None = None,
    propagate: bool = False,
) -> logging.Logger:
    """Get a configured logger for the anomaly_detection_isolator service.

    This function wraps the recovery_libs get_logger utility with service-specific
    defaults. It configures logging to stdout with optional OTLP support if
    environment variables are set.

    Args:
        name: Logger name. Defaults to SERVICE_NAME.
        level: Logging level (e.g., logging.INFO, logging.DEBUG).
            Defaults to LOG_LEVEL from environment variable or INFO.
        custom_library_level: Dictionary mapping library names to their desired
            log levels. If None, uses DEFAULT_LIBRARY_LEVELS.
        propagate: Whether to propagate logs to parent loggers.
            Defaults to False to prevent duplicate logs.

    Returns:
        Configured logging.Logger instance.
    """
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
