# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reusable pytest fixtures and helpers for integration tests against a throwaway MySQL
container, rather than the externally-provisioned MYSQL_HOST most integration tests in
this repo assume. Two isolation strategies, at two different scopes:

- mysql_container and storage: one container per test *module*, shared by every test
  function in that module (e.g. for lock/race testing against a real, empty table).
  Tests that mutate shared state need collision-safe values (e.g. unique IDs), since
  nothing resets the schema between tests in the same module.
- mysql_connection_url and disposable_database: one container (or an externally
  provisioned DATABASE_URL) shared for the whole pytest *session*, with each test
  getting its own uniquely named database on it -- for suites with many tests where a
  fresh container per test, or even per module, would be too slow.

Import fixtures by name into a test module for pytest to pick them up, e.g.:

    from recovery_libs.testing.storage.mysql import check_mysql_available, mysql_container, storage

Requires pytest and testcontainers[mysql] to be installed in the importing project's own
dev dependencies -- this module is test-only and must never be imported by production code.
"""

import os
import re
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import SQLAlchemyError
from testcontainers.mysql import MySqlContainer

from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage

MYSQL_IMAGE = "mysql:8.0@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b"
ROOT_PASSWORD = "test-password"
DB_NAME = "recovery_test"


def _mysql_image() -> str:
    """Allows MYSQL_TEST_IMAGE to override the pinned default, e.g. to test against a
    different MySQL 8.0.x patch release or a mirror reachable from a restricted network.
    Rejects an override below MySQL 8.0 up front, since the collations this module and
    the real schema depend on don't exist before 8.0 -- without this check, an older
    image would fail confusingly later, during database or table creation instead. An
    image reference whose version can't be parsed (a custom tag, a digest-only
    reference) is let through unchecked rather than rejected on a guess."""
    image = os.environ.get("MYSQL_TEST_IMAGE", MYSQL_IMAGE)
    tag = image.split("@", 1)[0].rsplit(":", 1)[-1]
    major_version = re.match(r"(\d+)", tag)
    if major_version and int(major_version.group(1)) < 8:
        raise RuntimeError(
            f"MYSQL_TEST_IMAGE={image!r} targets MySQL {tag}, but disposable_database and "
            "the real schema require MySQL 8.0+ (utf8mb4_0900_* collations)."
        )
    return image


@pytest.fixture(scope="session")
def check_mysql_available():
    """Override the shared conftest fixture: a module that imports this fixture manages its
    own throwaway MySQL container rather than relying on the externally-provisioned
    MYSQL_HOST database, so that shared availability check doesn't apply."""
    return True


@pytest.fixture(scope="module")
def mysql_container():
    """Starts one throwaway container for the whole test module -- every test function
    that requests it (directly or via storage) shares the same running instance."""
    with MySqlContainer(_mysql_image(), root_password=ROOT_PASSWORD, dbname=DB_NAME) as mysql:
        yield mysql


@pytest.fixture
def storage(mysql_container) -> MySQLStorage:
    """Re-bootstraps the schema on the module-scoped container for each test. Bootstrap
    upgrades/verifies the schema rather than resetting it, so tables and rows written by
    an earlier test in the same module are still there."""
    connection = MySQLConnection(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        username="root",
        password=ROOT_PASSWORD,
        database_name=DB_NAME,
    )
    storage = MySQLStorage(connection=connection)
    assert storage.bootstrap(schema_upgrade=True) is True
    return storage


@pytest.fixture(scope="session")
def mysql_connection_url():
    """Reuses DATABASE_URL when the caller already has a database available (e.g. a
    developer without Docker, or a CI job that provisions one another way), otherwise
    starts one throwaway testcontainer for the whole pytest session -- shared across
    every test module that requests it, not just the module that starts it."""
    existing_url = os.environ.get("DATABASE_URL")
    if existing_url:
        yield existing_url
        return

    with MySqlContainer(
        _mysql_image(), username="root", password=ROOT_PASSWORD, root_password=ROOT_PASSWORD, dbname=DB_NAME
    ) as mysql:
        yield mysql.get_connection_url().replace("mysql://", "mysql+pymysql://", 1)


@pytest.fixture
def disposable_database(mysql_connection_url, monkeypatch) -> Iterator[Engine]:
    """Creates a uniquely named database on the connection from mysql_connection_url,
    points DATABASE_URL at it for the duration of the test, and drops it afterward --
    so tests can share one MySQL instance without colliding on schema state."""
    source_url = make_url(mysql_connection_url)
    database_name = f"{DB_NAME}_{uuid.uuid4().hex}"
    admin_engine = create_engine(source_url)
    created = False
    engine = None
    try:
        try:
            with admin_engine.begin() as connection:
                connection.execute(
                    text(f"CREATE DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
                )
            created = True
        except SQLAlchemyError as error:
            raise RuntimeError(
                "disposable_database requires credentials that can create and drop a database; "
                "the connection from mysql_connection_url was not modified"
            ) from error

        database_url = source_url.set(database=database_name)
        monkeypatch.setenv("DATABASE_URL", database_url.render_as_string(hide_password=False))
        engine = create_engine(database_url)
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        try:
            if created:
                with admin_engine.begin() as connection:
                    connection.execute(text(f"DROP DATABASE IF EXISTS `{database_name}`"))
        finally:
            admin_engine.dispose()
