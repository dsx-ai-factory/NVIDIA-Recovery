# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from job_manager.logger import logger, setup_app_logging


def test_logger_instance_created():
    """Test that logger instance is created with correct name."""
    assert logger.name == "job_manager"
    assert not logger.propagate


def test_setup_app_logging_with_propagate():
    """Test setup_app_logging with propagate=True for test environments."""
    test_logger = setup_app_logging(level="DEBUG", propagate=True)
    assert test_logger.name == "job_manager"
    assert test_logger.level == logging.DEBUG
    assert test_logger.propagate


def test_logger_logs_message(caplog):
    """Test that logger can log messages when propagate is enabled."""
    logger.propagate = True
    try:
        with caplog.at_level(logging.INFO, logger="job_manager"):
            logger.info("Test message")
        assert "Test message" in caplog.text
    finally:
        logger.propagate = False
