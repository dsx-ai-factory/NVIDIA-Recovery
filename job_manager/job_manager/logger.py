# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os

from recovery_libs.utils.logger import get_logger

LIBRARY_LOGLEVEL = {
    "pika": logging.WARNING,
    "httpcore": logging.WARNING,
    "httpx": logging.WARNING,
    "uvicorn": logging.WARNING,
    "kubernetes.leaderelection.leaderelection": logging.WARNING,
}


def setup_app_logging(level: int | str = logging.INFO, propagate: bool = False) -> logging.Logger:
    """
    Set up the app logging configuration using the recovery_libs logger.

    Args:
        level (str, optional): The logging level to set for the logger.
            Defaults to logging.INFO.
        propagate (bool, optional): Whether to propagate logs to parent loggers.
            Defaults to False. Set to True in tests to enable pytest's caplog.

    Returns:
        logging.Logger: The configured logger object.
    """
    # Convert string level to int if needed
    numeric_level: int
    if isinstance(level, str):
        numeric_level = logging.getLevelNamesMapping()[level.upper()]
    else:
        numeric_level = level

    # Use the new get_logger from recovery_libs with custom library levels
    logger = get_logger(
        name="job_manager",
        level=numeric_level,
        custom_library_level=LIBRARY_LOGLEVEL,
        propagate=propagate,
        enable_otlp=True,  # Service logger exports to OTLP
    )

    return logger


# Create a centralized logger instance for the job_manager service
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = setup_app_logging(level=LOG_LEVEL)
