# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Standalone integration test proving MySQLConnection's write pool actually runs at READ
COMMITTED against a real MySQL server, and that the default pool stays at REPEATABLE READ --
not just that the right connect_args dict got built (see the mocked unit tests in
tests/storage/test_mysql_connection.py).

Spins up its own throwaway MySQL container, following the pattern in
test_mysql_connection_ssl.py.
"""

import pytest
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.testing.storage.mysql import DB_NAME, ROOT_PASSWORD
from sqlalchemy import text


def _isolation_level(session) -> str:
    return session.execute(text("SELECT @@transaction_isolation")).scalar()


@pytest.mark.integration
def test_write_pool_enabled_runs_read_committed_default_pool_stays_repeatable_read(mysql_container):
    connection = MySQLConnection(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        username="root",
        password=ROOT_PASSWORD,
        database_name=DB_NAME,
    )

    with connection.session() as session:
        assert _isolation_level(session) == "REPEATABLE-READ"
    with connection.write_session() as session:
        assert _isolation_level(session) == "READ-COMMITTED"


@pytest.mark.integration
def test_write_pool_disabled_falls_back_to_default_pools_isolation_level(mysql_container):
    connection = MySQLConnection(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        username="root",
        password=ROOT_PASSWORD,
        database_name=DB_NAME,
        write_pool_enabled=False,
    )

    with connection.write_session() as session:
        assert _isolation_level(session) == "REPEATABLE-READ"
