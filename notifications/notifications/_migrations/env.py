# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from alembic import context
from notifications.database import get_ssl_connect_args
from notifications.utils import config as app_config
from sqlalchemy import engine_from_config, pool

from notifications.models.audit_events import AuditEventOrm  # noqa: F401, isort:skip
from notifications.models.notifications import NotificationOrm  # noqa: F401, isort:skip
from notifications.models.preferences import UserPreferencesOrm  # noqa: F401, isort:skip

###############################################################
# IMPORTANT: Import SQLModel models BEFORE importing SQLModel
from sqlmodel import SQLModel  # isort:skip

VERSION_TABLE = "alembic_versions__notifications"

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Need to replace the '%' with '%%' because the value is passed into `ConfigParser.set`, which supports variable
# interpolation. More info:
# https://alembic.sqlalchemy.org/en/latest/api/config.html#alembic.config.Config.set_main_option.params.value
_database_url = (os.getenv("DATABASE_URL") or app_config.get_database_url()).replace("%", "%%")

config.set_main_option("sqlalchemy.url", _database_url)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = SQLModel.metadata


def include_name(name, type_, _parent_names):
    """Limit autogeneration to tables owned by Notifications."""
    if type_ == "table":
        return name in target_metadata.tables
    return True


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        include_name=include_name,
        include_schemas=False,
        literal_binds=True,
        version_table=VERSION_TABLE,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section, {})

    # Overwrite the db connection url to be the one from the env var when available.
    url = os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    configuration["sqlalchemy.url"] = url  # type: ignore[assignment]

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=get_ssl_connect_args(),
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
            include_schemas=False,
            version_table=VERSION_TABLE,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
