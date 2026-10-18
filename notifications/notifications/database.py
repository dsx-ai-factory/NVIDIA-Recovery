# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pathlib
import time
from contextlib import asynccontextmanager

import alembic.command
import alembic.config
from fastapi import FastAPI
from sqlalchemy import Engine, event, text
from sqlalchemy import inspect as sa_inspect
from sqlmodel import create_engine

from notifications.prometheus.metrics import (
    DATABASE_INITIALIZATION_ERRORS,
    DATABASE_OPERATION_LATENCY,
)
from notifications.utils import config
from notifications.utils.logger import logger

ALEMBIC_CONFIG_PATH = pathlib.Path(__file__).parent / "_migrations/alembic.ini"
VERSION_TABLE = "alembic_versions__notifications"
MANAGED_TABLES = frozenset({"AuditEvents", "Notifications", "UserPreferences"})
FRESH_DATABASE_REQUIRED = (
    "The database contains an unsupported or unversioned Notifications schema. "
    "Provision a fresh empty database/schema; automatic migration, restamping, and data conversion are not supported."
)

_engine: Engine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle the database lifespan for FastAPI."""
    try:
        run_migrations()
    except Exception as e:
        DATABASE_INITIALIZATION_ERRORS.inc()
        logger.error(f"Error setting up database: {e}")
        raise
    yield


def get_ssl_connect_args() -> dict:
    """SQLAlchemy connect_args for TLS, shared by the app engine and Alembic's migration engine."""
    if not config.get_database_ssl_enabled():
        return {}
    ssl_ca_path = config.get_database_ssl_ca_path()
    if not pathlib.Path(ssl_ca_path).is_file():
        raise RuntimeError(f"DATABASE_SSL_ENABLED is true but ssl CA bundle does not exist at path: {ssl_ca_path}")
    # VERIFY_CA, not VERIFY_IDENTITY: check the cert chains to ssl_ca_path, not its hostname/SAN.
    return {"ssl": {"ca": ssl_ca_path, "check_hostname": False}}


def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    logger.info("Creating the sql engine.")
    connect_args = get_ssl_connect_args()
    _engine = create_engine(
        config.get_database_url(),
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args=connect_args,
    )

    # Add event listeners for SQL query timing
    @event.listens_for(_engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault("query_start_time", []).append(time.perf_counter_ns())

    @event.listens_for(_engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start_time = conn.info["query_start_time"].pop()
        operation = statement.split()[0].lower() if statement else "unknown"

        # Track database operation latency
        DATABASE_OPERATION_LATENCY.labels(operation=operation).observe(time.perf_counter_ns() - start_time)

    return _engine


def run_migrations():
    """Upgrade Notifications-owned tables exclusively through Alembic."""
    engine = get_engine()
    table_names = set(sa_inspect(engine).get_table_names())
    managed_tables = sorted(MANAGED_TABLES.intersection(table_names))
    if VERSION_TABLE not in table_names and managed_tables:
        raise RuntimeError(
            f"{FRESH_DATABASE_REQUIRED} Found managed tables without {VERSION_TABLE}: {', '.join(managed_tables)}."
        )
    if VERSION_TABLE in table_names and managed_tables:
        with engine.connect() as connection:
            current_revision = connection.execute(
                text(f"SELECT version_num FROM `{VERSION_TABLE}`")
            ).scalar_one_or_none()
        if current_revision is None:
            raise RuntimeError(f"{FRESH_DATABASE_REQUIRED} The {VERSION_TABLE} table has no revision.")

    logger.info("Running 'alembic upgrade head'")
    alembic_config = _get_alembic_config()
    try:
        alembic.command.upgrade(alembic_config, "head")
    except Exception as exc:
        raise RuntimeError(f"{FRESH_DATABASE_REQUIRED} Alembic reported: {exc}") from exc
    logger.info("Notifications database schema is at the Alembic head.")


def _get_alembic_config():
    return alembic.config.Config(ALEMBIC_CONFIG_PATH)
