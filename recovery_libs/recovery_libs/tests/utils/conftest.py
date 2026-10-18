# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Pytest fixtures for utils tests."""

import logging
import os
from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def clean_logger() -> Generator[None, None, None]:
    """Ensure clean logger state between tests.

    Yields:
        None

    Cleanup:
        Removes all handlers from commonly used test loggers and resets level.
    """
    yield
    # Cleanup after test - clean both "recovery" and "test" loggers used in tests
    for logger_name in ["recovery", "test", "test2", "test_app", "test_lazy", "test_lib_logger"]:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.setLevel(logging.WARNING)
        logger.propagate = True


@pytest.fixture
def mock_env_vars() -> Generator[dict[str, str], None, None]:
    """Provide a dictionary for setting test environment variables.

    Yields:
        Empty dictionary that can be populated by tests.

    Cleanup:
        Removes any test environment variables that were set.
    """
    test_vars = {}
    original_env = os.environ.copy()

    yield test_vars

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def set_env_vars(mock_env_vars: dict[str, str]) -> None:
    """Apply mock environment variables to os.environ.

    Args:
        mock_env_vars: Dictionary of environment variables to set.
    """
    for key, value in mock_env_vars.items():
        os.environ[key] = value


@pytest.fixture
def mock_otlp_handler() -> Generator[Mock, None, None]:
    """Mock the OTLPHandler to avoid actual network calls.

    Yields:
        Mock OTLPHandler class.
    """
    with patch("recovery_libs.utils.logger.OTLPHandler") as mock:
        # Configure the mock instance to have a proper level attribute for logging system
        mock.return_value.level = logging.INFO
        yield mock


@pytest.fixture
def reset_otlp_singleton() -> Generator[None, None, None]:
    """Reset the shared LoggerProvider singleton before and after tests.

    This is needed because OTLPHandler uses a singleton pattern, and tests
    need a fresh singleton for each test to properly test initialization.

    Yields:
        None
    """
    from recovery_libs.utils.otlp_handler import _reset_shared_provider

    _reset_shared_provider()
    yield
    _reset_shared_provider()


@pytest.fixture
def mock_http_exporter(reset_otlp_singleton) -> Generator[Mock, None, None]:
    """Mock HTTP OTLP exporter.

    Args:
        reset_otlp_singleton: Ensures singleton is reset before mocking.

    Yields:
        Mock HTTPLogExporter class.
    """
    with patch("recovery_libs.utils.otlp_handler.HTTPLogExporter") as mock:
        yield mock


@pytest.fixture
def mock_grpc_exporter(reset_otlp_singleton) -> Generator[Mock, None, None]:
    """Mock gRPC OTLP exporter.

    Args:
        reset_otlp_singleton: Ensures singleton is reset before mocking.

    Yields:
        Mock GRPCLogExporter class.
    """
    with patch("recovery_libs.utils.otlp_handler.GRPCLogExporter") as mock:
        yield mock


@pytest.fixture
def mock_logger_provider(reset_otlp_singleton) -> Generator[Mock, None, None]:
    """Mock OpenTelemetry LoggerProvider.

    Args:
        reset_otlp_singleton: Ensures singleton is reset before mocking.

    Yields:
        Mock LoggerProvider class.
    """
    with patch("recovery_libs.utils.otlp_handler.LoggerProvider") as mock:
        yield mock
