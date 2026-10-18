# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from unittest.mock import MagicMock, patch

import pytest
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import RecoveryStorageException


@pytest.fixture
def connection():
    engine_patcher = patch("recovery_libs.storage.mysql_connection.create_engine")
    sessionmaker_patcher = patch("recovery_libs.storage.mysql_connection.sessionmaker")
    try:
        engine_patcher.start()
        sessionmaker_patcher.start()
        yield MySQLConnection(host="localhost", port=3306, username="test", password="test", database_name="test_db")
    finally:
        engine_patcher.stop()
        sessionmaker_patcher.stop()


@pytest.fixture(autouse=True)
def inspector(mocker):
    inspector_mock = MagicMock()
    inspector_mock.get_table_names.return_value = ["alembic_version"]
    mocker.patch("recovery_libs.storage.mysql_connection.inspect", return_value=inspector_mock)
    return inspector_mock


def test_init_creates_independent_write_pool_with_configured_size():
    """Constructs a lazy connection pool; doesn't require a network connection."""
    connection = MySQLConnection(
        host="localhost",
        port=3306,
        username="test",
        password="test",
        database_name="test_db",
        write_pool_size=7,
        write_max_overflow=3,
    )

    assert connection.engine is not connection.write_engine
    assert connection.session is not connection.write_session
    assert connection.write_engine.pool.size() == 7


def test_init_with_write_pool_disabled_reuses_default_engine():
    """Avoid extra connections from being opened when write pool is disabled."""
    connection = MySQLConnection(
        host="localhost",
        port=3306,
        username="test",
        password="test",
        database_name="test_db",
        write_pool_enabled=False,
    )

    assert connection.write_engine is connection.engine
    assert connection.write_session is connection.session
    assert connection.write_pool_enabled is False


@patch("recovery_libs.storage.mysql_connection.create_engine")
@patch("recovery_libs.storage.mysql_connection.sessionmaker")
def test_init_without_ssl_omits_ssl_connect_arg(mock_sessionmaker, mock_engine):
    """Test that connect_args has no 'ssl' key when ssl_enabled is False (the default)."""
    MySQLConnection(host="localhost", port=3306, username="test", password="test", database_name="test_db")

    connect_args = mock_engine.call_args.kwargs["connect_args"]
    assert "ssl" not in connect_args


@patch("recovery_libs.storage.mysql_connection.create_engine")
@patch("recovery_libs.storage.mysql_connection.sessionmaker")
def test_init_requires_strict_session_mode(mock_sessionmaker, mock_engine):
    MySQLConnection(host="localhost", port=3306, username="test", password="test", database_name="test_db")

    init_command = mock_engine.call_args.kwargs["connect_args"]["init_command"]
    assert "time_zone='+00:00'" in init_command
    assert "@@SESSION.sql_mode" in init_command
    assert "STRICT_TRANS_TABLES" in init_command


@patch("recovery_libs.storage.mysql_connection.os.path.isfile", return_value=True)
@patch("recovery_libs.storage.mysql_connection.create_engine")
@patch("recovery_libs.storage.mysql_connection.sessionmaker")
def test_init_with_ssl_enabled_sets_ssl_connect_arg(mock_sessionmaker, mock_engine, mock_isfile):
    """Test that connect_args carries the CA path when ssl_enabled is True and the file exists."""
    MySQLConnection(
        host="localhost",
        port=3306,
        username="test",
        password="test",
        database_name="test_db",
        ssl_enabled=True,
        ssl_ca="/etc/mysql-ca/ca.pem",
    )

    connect_args = mock_engine.call_args.kwargs["connect_args"]
    assert connect_args["ssl"] == {"ca": "/etc/mysql-ca/ca.pem", "check_hostname": False}
    mock_isfile.assert_called_once_with("/etc/mysql-ca/ca.pem")


def test_init_with_ssl_enabled_missing_ca_path_raises():
    """Test that ssl_enabled without ssl_ca fails fast instead of connecting without verification."""
    with pytest.raises(ValueError, match="ssl_ca must be set"):
        MySQLConnection(
            host="localhost", port=3306, username="test", password="test", database_name="test_db", ssl_enabled=True
        )


@patch("recovery_libs.storage.mysql_connection.os.path.isfile", return_value=False)
def test_init_with_ssl_enabled_missing_ca_file_raises(mock_isfile):
    """Test that ssl_enabled with a non-existent CA file fails fast at startup."""
    with pytest.raises(ValueError, match="does not exist"):
        MySQLConnection(
            host="localhost",
            port=3306,
            username="test",
            password="test",
            database_name="test_db",
            ssl_enabled=True,
            ssl_ca="/etc/mysql-ca/ca.pem",
        )


def test_ready_success(connection):
    """Test ready() returns True when database connection succeeds"""
    mock_conn = MagicMock()
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    result = connection.ready()
    assert result is True
    mock_conn.execute.assert_called_once()


def test_ready_failure(connection):
    """Test ready() returns False when database connection fails"""
    connection.engine.connect.side_effect = Exception("Connection failed")

    result = connection.ready()
    assert result is False


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_different_revision(mock_stringio, mock_config, mock_command, connection):
    # 3 return values for the execute function that correspond to 3 database queries:
    #   1. acquire the lock (return 1 to indicate lock acquired)
    #   2. release the lock (return 1 to indicate lock released)
    #   3. ping the database in the ready() function.
    scalar_mock = MagicMock(scalar=MagicMock(side_effect=[1, 1]))
    mock_conn = MagicMock(execute=MagicMock(side_effect=[scalar_mock, scalar_mock, None]))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    mock_output_buffer.getvalue.side_effect = [
        "revision_sha1",
        "revision_sha2 (head)",
        "revision_sha1",
        "revision_sha2 (head)",
        "revision_history",
    ]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert connection.bootstrap(schema_upgrade=True)

    assert mock_command.current.call_count == 2
    assert mock_command.heads.call_count == 2
    mock_command.history.assert_called_once()
    mock_command.upgrade.assert_called_once_with(mock_config, "head")


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_no_schema_upgrade_different_revision(mock_stringio, mock_config, mock_command, connection):
    # 3 return values for the execute function that correspond to 3 database queries:
    #   1. acquire the lock (return 1 to indicate lock acquired)
    #   2. release the lock (return 1 to indicate lock released)
    #   3. ping the database in the ready() function.
    scalar_mock = MagicMock(scalar=MagicMock(side_effect=[1, 1]))
    mock_conn = MagicMock(execute=MagicMock(side_effect=[scalar_mock, scalar_mock, None]))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    mock_output_buffer.getvalue.side_effect = ["revision_sha1", "revision_sha2 (head)", "revision_history"]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert not connection.bootstrap(schema_upgrade=False)

    mock_command.current.assert_called_once()
    mock_command.heads.assert_called_once()
    mock_command.history.assert_not_called()
    mock_command.upgrade.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_latest_revision_reached(mock_stringio, mock_config, mock_command, connection):
    # Only one return value for the execution function that corresponds to pinging the database.
    mock_conn = MagicMock(execute=MagicMock(return_value=None))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    #  head revision is the same as the current revision.
    mock_output_buffer.getvalue.side_effect = ["revision_sha1", "revision_sha1 (head)"]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert connection.bootstrap(schema_upgrade=True)

    mock_command.current.assert_called_once()
    mock_command.heads.assert_called_once()
    mock_command.upgrade.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_no_schema_upgrade_latest_revision_reached(mock_stringio, mock_config, mock_command, connection):
    # Only one return value for the execution function that corresponds to pinging the database.
    mock_conn = MagicMock(execute=MagicMock(return_value=None))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    #  head revision is the same as the current revision.
    mock_output_buffer.getvalue.side_effect = ["revision_sha1", "revision_sha1 (head)"]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert connection.bootstrap(schema_upgrade=False)

    mock_command.current.assert_called_once()
    mock_command.heads.assert_called_once()
    mock_command.upgrade.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_schema_upgrade_empty_current_revision(
    mock_stringio, mock_config, mock_command, connection, inspector
):
    # 3 return values for the execute function that correspond to 3 database queries:
    #   1. acquire the lock (return 1 to indicate lock acquired)
    #   2. release the lock (return 1 to indicate lock released)
    #   3. ping the database in the ready() function.
    scalar_mock = MagicMock(scalar=MagicMock(side_effect=[1, 1]))
    mock_conn = MagicMock(execute=MagicMock(side_effect=[scalar_mock, scalar_mock, None]))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    inspector.get_table_names.return_value = []
    mock_output_buffer.getvalue.side_effect = ["revision_sha2 (head)", "revision_sha2 (head)", "revision_history"]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert connection.bootstrap()

    mock_command.current.assert_not_called()
    assert mock_command.heads.call_count == 2
    mock_command.history.assert_called_once()
    mock_command.upgrade.assert_called_once_with(mock_config, "head")


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_schema_upgrade_lock_not_acquired(mock_stringio, mock_config, mock_command, connection, inspector):
    # 3 return values for the execute function that correspond to 3 database queries:
    #   1. acquire the lock (return 0 to indicate lock not acquired)
    #   2. check if the lock is free when waiting for the upgrade to be completed (return 1 as success)
    #   3. ping the database in the ready() function.
    mock_scalar = MagicMock(scalar=MagicMock(side_effect=[0, 1]))
    mock_conn = MagicMock(execute=MagicMock(side_effect=[mock_scalar, mock_scalar, None]))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    inspector.get_table_names.return_value = ["alembic_version"]
    mock_output_buffer = mock_stringio.return_value
    mock_output_buffer.getvalue.side_effect = [
        "revision_sha1",
        "revision_sha2 (head)",
        "revision_sha2",
        "revision_sha2 (head)",
    ]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert connection.bootstrap()

    assert mock_command.current.call_count == 2
    assert mock_command.heads.call_count == 2
    mock_command.upgrade.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_schema_upgrade_with_exception(mock_stringio, mock_config, mock_command, connection):
    # 3 return values for the execute function that correspond to 3 database queries:
    #   1. acquire the lock (return 1 to indicate lock acquired)
    #   2. release the lock (return 1 to indicate lock released)
    #   3. ping the database in the ready() function.
    mock_scalar = MagicMock(scalar=MagicMock(side_effect=[1, 1]))
    mock_conn = MagicMock(execute=MagicMock(side_effect=[mock_scalar, mock_scalar, None]))
    connection.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_output_buffer = mock_stringio.return_value
    mock_output_buffer.getvalue.side_effect = [
        "revision_sha1",
        "revision_sha2 (head)",
        "revision_sha1",
        "revision_sha2 (head)",
        "revision_history",
    ]

    mock_config.return_value = mock_config
    mock_config.attributes = {}

    mock_command.upgrade.side_effect = Exception("Error during schema upgrade")

    with pytest.raises(RecoveryStorageException):
        assert connection.bootstrap()

    mock_command.upgrade.assert_called_once_with(mock_config, "head")


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_rejects_unversioned_managed_tables(mock_stringio, mock_config, mock_command, connection, inspector):
    inspector.get_table_names.return_value = ["job_execution_records"]
    mock_config.return_value = mock_config
    mock_config.attributes = {}

    with pytest.raises(RecoveryStorageException, match="fresh empty database/schema"):
        connection.bootstrap()

    mock_command.current.assert_not_called()
    mock_command.upgrade.assert_not_called()
    connection.engine.connect.return_value.__enter__.return_value.execute.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_rejects_unknown_revision_without_upgrading(
    mock_stringio, mock_config, mock_command, connection, inspector
):
    inspector.get_table_names.return_value = ["alembic_version", "job_execution_records"]
    mock_config.return_value = mock_config
    mock_config.attributes = {}
    mock_command.current.side_effect = Exception("Can't locate revision identified by 'legacy_revision'")

    with pytest.raises(RecoveryStorageException, match="fresh empty database/schema"):
        connection.bootstrap()

    mock_command.upgrade.assert_not_called()
    connection.engine.connect.return_value.__enter__.return_value.execute.assert_not_called()


@patch("recovery_libs.storage.mysql_connection.command")
@patch("recovery_libs.storage.mysql_connection.Config")
@patch("recovery_libs.storage.mysql_connection.StringIO")
def test_bootstrap_check_only_does_not_mutate_empty_database(
    mock_stringio, mock_config, mock_command, connection, inspector
):
    inspector.get_table_names.return_value = []
    mock_output_buffer = mock_stringio.return_value
    mock_output_buffer.getvalue.return_value = "revision_sha2 (head)"
    mock_config.return_value = mock_config
    mock_config.attributes = {}

    assert not connection.bootstrap(schema_upgrade=False)

    mock_command.current.assert_not_called()
    mock_command.upgrade.assert_not_called()
    connection.engine.connect.return_value.__enter__.return_value.execute.assert_not_called()
