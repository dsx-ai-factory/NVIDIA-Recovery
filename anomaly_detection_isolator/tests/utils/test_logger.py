# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

"""Unit tests for anomaly_detection_isolator logger module."""

import logging
from unittest.mock import patch

from anomaly_detection_isolator.utils.logger import (
    DEFAULT_LIBRARY_LEVELS,
    get_logger,
    logger,
)
from recovery_libs.utils import logger as recovery_logger


@patch("anomaly_detection_isolator.utils.logger.recovery_logger.get_logger")
def test_get_logger_defaults(m):
    """Test default logger configuration."""
    get_logger()
    assert m.call_args[1] == {
        "name": "anomaly_detection_isolator",
        "level": recovery_logger.LOG_LEVEL,
        "custom_library_level": DEFAULT_LIBRARY_LEVELS,
        "propagate": False,
        "enable_otlp": True,
    }


@patch("anomaly_detection_isolator.utils.logger.recovery_logger.get_logger")
def test_get_logger_custom(m):
    """Test custom logger parameters."""
    custom = {"test": logging.ERROR}
    get_logger(name="test", level=logging.DEBUG, custom_library_level=custom, propagate=True)
    assert m.call_args[1] == {
        "name": "test",
        "level": logging.DEBUG,
        "custom_library_level": custom,
        "propagate": True,
        "enable_otlp": True,
    }


def test_module_logger():
    """Test module logger instance."""
    assert logger is not None and logger.name == "anomaly_detection_isolator"
