# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import logging
import os
import time
from io import StringIO
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from recovery_libs.storage.base import RecoveryStorageException

logger = logging.getLogger(__name__)

SCHEMA_UPGRADE_LOCK_NAME = "heimdall_mysql_schema_upgrade_lock"
SCHEMA_UPGRADE_TIMEOUT_SECS = 5
SCHEMA_UPGRADE_WAIT_INTERVAL_SECS = 5
VERSION_TABLE = "alembic_version"
MANAGED_TABLES = frozenset(
    {
        "advice",
        "dag_dependency",
        "global_events",
        "job_events",
        "job_execution_errors",
        "job_execution_records",
        "key_value_cache",
        "node_events",
        "recovery_milestones",
        "recovery_sequences",
    }
)
FRESH_DATABASE_REQUIRED = (
    "The database contains an unsupported or unversioned shared-storage schema. "
    "Provision a fresh empty database/schema; automatic migration, restamping, and data conversion are not supported."
)


class MySQLConnection:
    """
    A class to manage connections to the MySQL database, to be shared by multiple MySQL storage classes.

    Args:
        host (str): MySQL server hostname.
        port (int): MySQL server port.
        username (str): MySQL username.
        password (str): MySQL password.
        database_name (str): MySQL database name.
        echo (bool): Enable SQLAlchemy echo mode for debugging, default is False.
        alembic_directory (Optional[str]): Path to the Alembic directory where the alembic.ini config file and revision
            files exist. If not provided, the default Alembic directory in the same folder as this file will be used.
        ssl_enabled (bool): Whether to verify the server certificate over TLS using ssl_ca. Default is False.
        ssl_ca (Optional[str]): Path to a PEM CA bundle to verify the server certificate. Only used when
            ssl_enabled is True.
        write_pool_enabled (bool): Whether write_session/write_engine use a separate connection
            pool with READ COMMITTED set once per physical connection. Default is True. When
            False, write_session/write_engine alias session/engine instead, and callers needing
            READ COMMITTED must apply it themselves per-transaction (more per-call overhead, but
            still deadlock-safe) -- an escape hatch for environments where a second pool's extra
            connections aren't affordable against MySQL's max_connections.
        write_pool_size (int): pool_size for the write-specific engine, when write_pool_enabled
            is True. Default is SQLAlchemy's default of 5.
        write_max_overflow (int): max_overflow for the write-specific engine, when
            write_pool_enabled is True. Default is SQLAlchemy's default of 10.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database_name: str,
        echo: bool = False,
        alembic_directory: str | None = None,
        ssl_enabled: bool = False,
        ssl_ca: str | None = None,
        write_pool_enabled: bool = True,
        write_pool_size: int = 5,
        write_max_overflow: int = 10,
    ):
        super().__init__()
        self.url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}"
        self.write_pool_enabled = write_pool_enabled
        base_init_command = (
            "SET SESSION time_zone='+00:00', sql_mode=CONCAT_WS(',', @@SESSION.sql_mode, 'STRICT_TRANS_TABLES')"
        )
        connect_args: dict[str, Any] = {
            # We use UTC for all MySQL timestamps, so we need to set the session time zone to UTC
            # in case it is not already set to UTC, which may be true for some ACM MySQL deployments.
            # Retain server-selected modes while requiring invalid or out-of-range values to fail
            # instead of being coerced into durable data.
            "init_command": base_init_command
        }
        if ssl_enabled:
            if not ssl_ca:
                raise ValueError("ssl_ca must be set when ssl_enabled is True.")
            if not os.path.isfile(ssl_ca):
                raise ValueError(f"ssl_enabled is True but ssl_ca CA bundle does not exist at path: {ssl_ca}")
            # VERIFY_CA, not VERIFY_IDENTITY: check the cert chains to ssl_ca, not its hostname/SAN.
            connect_args["ssl"] = {"ca": ssl_ca, "check_hostname": False}
        self.engine = create_engine(
            url=self.url,
            echo=echo,
            connect_args=connect_args,
        )
        self.session = sessionmaker(bind=self.engine)
        if write_pool_enabled:
            # A separate engine, not just a different isolation_level on the shared one: that
            # would apply READ COMMITTED to every query on every connection in the pool, not
            # just the write path that needs it.
            write_connect_args = {
                "init_command": base_init_command + ", transaction_isolation='READ-COMMITTED'",
            }
            if ssl_enabled:
                write_connect_args["ssl"] = connect_args["ssl"]
            self.write_engine = create_engine(
                url=self.url,
                echo=echo,
                connect_args=write_connect_args,
                pool_size=write_pool_size,
                max_overflow=write_max_overflow,
            )
            self.write_session = sessionmaker(bind=self.write_engine)
        else:
            self.write_engine = self.engine
            self.write_session = self.session
        if alembic_directory:
            self.alembic_directory = alembic_directory
        else:
            self.alembic_directory = os.path.join(os.path.dirname(__file__), "alembic")

    def bootstrap(self, schema_upgrade: bool = True) -> bool:
        """
        Bootstrap the database by either upgrading the schema or checking if the schema is up-to-date.

        Args:
            schema_upgrade (bool): Run schema upgrade (if needed) through Alembic during bootstrap, default is True.
            If False, just check if the schema is up-to-date.
        """
        logger.info("Checking if database schema is up-to-date using Alembic.")
        logger.info(f"Alembic directory is set to be: {self.alembic_directory}.")

        # The way how Alembic's command module works is that it will print the result to stdout instead of directly
        # returning it. Therefore we need to use an output buffer to capture the output and then process it.
        output_buffer = StringIO()
        alembic_cfg = Config(file_=os.path.join(self.alembic_directory, "alembic.ini"), stdout=output_buffer)

        with self.engine.connect() as conn:
            alembic_cfg.set_main_option("sqlalchemy.url", self.url)
            alembic_cfg.set_main_option("script_location", self.alembic_directory)

            alembic_cfg.attributes["connection"] = conn
            alembic_cfg.attributes["configure_logger"] = False

            current_revision, head_revision = self._database_revisions(conn, alembic_cfg, output_buffer)

            # Use "in" instead of "==" because in addition to the revision SHA, the head_revision also contains a
            # "(head)" substring at the end.
            if current_revision and current_revision in head_revision:
                logger.info(
                    f"Database schema is already up-to-date at revision {current_revision}, no upgrade is needed."
                )
                return self.ready()

            if not schema_upgrade:
                logger.error(
                    f"Schema upgrade is disabled but current revision {current_revision} not head revision {head_revision}, aborting."
                )
                return False

            if self._acquire_schema_upgrade_lock(conn):
                try:
                    # Another replica may have completed the upgrade while this one was waiting for the lock.
                    current_revision, head_revision = self._database_revisions(conn, alembic_cfg, output_buffer)
                    if current_revision and current_revision in head_revision:
                        logger.info("Database schema reached the head revision while waiting for the upgrade lock.")
                        return self.ready()

                    logger.info(
                        "Current replica acquired the schema upgrade lock, upgrading the database schema to "
                        "the latest revision."
                    )
                    command.history(config=alembic_cfg)
                    history = output_buffer.getvalue().rstrip("\n").split("\n")
                    logger.debug(f"Database schema history: {';'.join(history)}")
                    output_buffer.truncate(0)

                    logger.info("Upgrading the database schema to the latest revision.")
                    command.upgrade(alembic_cfg, "head")
                    logger.info("Database schema upgrade is completed.")
                    conn.commit()
                except Exception as e:
                    logger.exception("Failure happened when upgrading database schema", extra={"error": str(e)})
                    raise RecoveryStorageException(f"Failure happened when upgrading database schema: {e}") from e
                finally:
                    logger.info("Releasing the schema upgrade lock.")
                    self._release_schema_upgrade_lock(conn)
            else:
                logger.info(
                    "Another replica is currently holding the schema upgrade database lock and running the "
                    "upgrade, waiting for it to complete before continuing service startup."
                )
                while True:
                    result = conn.execute(text(f"SELECT IS_FREE_LOCK('{SCHEMA_UPGRADE_LOCK_NAME}');")).scalar()
                    if result == 1:
                        logger.info("Database schema upgrade is completed. Continuing service startup.")
                        break
                    time.sleep(SCHEMA_UPGRADE_WAIT_INTERVAL_SECS)

                current_revision, head_revision = self._database_revisions(conn, alembic_cfg, output_buffer)
                if not current_revision or current_revision not in head_revision:
                    raise RecoveryStorageException(
                        "Another replica released the schema upgrade lock, but the database is not at the Alembic head."
                    )

            return self.ready()

    def _database_revisions(self, conn, alembic_cfg: Config, output_buffer: StringIO) -> tuple[str, str]:
        """Read and validate the shared-storage Alembic state without mutating it."""
        table_names = set(inspect(conn).get_table_names())
        managed_tables = sorted(MANAGED_TABLES.intersection(table_names))
        has_version_table = VERSION_TABLE in table_names

        if not has_version_table and managed_tables:
            raise RecoveryStorageException(
                f"{FRESH_DATABASE_REQUIRED} Found managed tables without {VERSION_TABLE}: {', '.join(managed_tables)}."
            )

        try:
            current_revision = ""
            if has_version_table:
                command.current(alembic_cfg)
                current_revision = output_buffer.getvalue().rstrip("\n")
                output_buffer.truncate(0)
                output_buffer.seek(0)

            command.heads(alembic_cfg)
            head_revision = output_buffer.getvalue().rstrip("\n")
            output_buffer.truncate(0)
            output_buffer.seek(0)
        except Exception as exc:
            raise RecoveryStorageException(f"{FRESH_DATABASE_REQUIRED} Alembic reported: {exc}") from exc

        if has_version_table and not current_revision and managed_tables:
            raise RecoveryStorageException(f"{FRESH_DATABASE_REQUIRED} The {VERSION_TABLE} table has no revision.")

        logger.info(f"Current database schema revision is: {current_revision or '<none>'}")
        logger.info(f"Latest database schema revision is: {head_revision}")
        return current_revision, head_revision

    def _acquire_schema_upgrade_lock(self, connection) -> int:
        """
        Acquire a schema upgrade lock to prevent multiple instances from running schema upgrade at the same time.
        The lock is a per-connection lock and it will be automatically released when the connection is closed.

        Official documentation: https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html

        Args:
            connection: SQLAlchemy connection object.
        """
        return (
            connection.execute(
                text(f"SELECT GET_LOCK('{SCHEMA_UPGRADE_LOCK_NAME}', {SCHEMA_UPGRADE_TIMEOUT_SECS})")
            ).scalar()
            == 1
        )

    def _release_schema_upgrade_lock(self, connection) -> int | None:
        """
        Release the schema upgrade lock.

        Args:
            connection: SQLAlchemy connection object.
        """
        return connection.execute(text(f"SELECT RELEASE_LOCK('{SCHEMA_UPGRADE_LOCK_NAME}')")).scalar()

    def ready(self) -> bool:
        """
        Check if the MySQL connection is ready by pinging the database.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.debug("MySQL connection health check passed.")
            return True
        except Exception as e:
            logger.exception("MySQL connection health check failed", extra={"error": str(e)})
            return False
