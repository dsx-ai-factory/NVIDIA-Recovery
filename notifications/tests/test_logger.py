# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for notifications logger module."""

import logging
import os
from unittest.mock import patch

from notifications.utils.logger import DEFAULT_LIBRARY_LEVELS, get_logger, logger


@patch("notifications.utils.logger.recovery_logger.get_logger")
def test_get_logger_defaults(m):
    """Test default logger configuration."""
    get_logger()
    assert m.call_args[1] == {
        "name": "notifications",
        "level": logging.INFO,
        "custom_library_level": DEFAULT_LIBRARY_LEVELS,
        "propagate": False,
        "enable_otlp": True,
    }


@patch("notifications.utils.logger.recovery_logger.get_logger")
def test_get_logger_custom(m):
    """Test custom logger and env var support."""
    custom = {"t": logging.ERROR}
    get_logger(name="t", level=logging.DEBUG, custom_library_level=custom, propagate=True)
    assert m.call_args[1] == {
        "name": "t",
        "level": logging.DEBUG,
        "custom_library_level": custom,
        "propagate": True,
        "enable_otlp": True,
    }
    with patch.dict(os.environ, {"LOGGING_LEVEL": "DEBUG"}):
        get_logger()
        assert m.call_args[1]["level"] == logging.DEBUG


def test_module_logger():
    """Test module logger instance."""
    assert logger is not None and logger.name == "notifications"
