# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""
Pytest configuration for integration tests.

This module provides shared fixtures and configuration for integration tests
that require external services like MySQL databases.
"""

from collections.abc import Iterator
from urllib.parse import urlparse

import pytest
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.testing.storage.mysql import mysql_connection_url

__all__ = ["mysql_connection_url"]


@pytest.fixture(scope="session")
def db_config(mysql_connection_url):
    """Database configuration derived from mysql_connection_url: DATABASE_URL if the
    caller already set one, otherwise a freshly started testcontainer."""
    url = urlparse(mysql_connection_url)
    return {
        "host": url.hostname,
        "port": url.port,
        "username": url.username,
        "password": url.password,
        "database_name": url.path.lstrip("/"),
    }


@pytest.fixture(scope="module")
def storage(db_config) -> Iterator[MySQLStorage]:
    """Bootstrapped storage on the session database. Rows persist across tests, so tests
    need collision-safe values such as unique IDs."""
    connection = MySQLConnection(**db_config)
    storage = MySQLStorage(connection=connection)
    assert storage.bootstrap() is True
    yield storage
    connection.engine.dispose()


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as an integration test requiring external services")


def pytest_collection_modifyitems(config, items):
    """Automatically mark all tests in integration directory as integration tests."""
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def check_mysql_available(db_config):
    """Resolving db_config already guarantees a reachable database -- via DATABASE_URL or
    a freshly started testcontainer -- so this fixture exists only so the
    mysql_container/ subdirectory can override it to skip that resolution entirely."""
    return True


@pytest.fixture(autouse=True, scope="session")
def setup_integration_tests(check_mysql_available):
    """
    Setup fixture that runs automatically for all integration tests.

    This ensures MySQL is available before any integration test runs.
    """
