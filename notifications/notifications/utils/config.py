# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from dynaconf import Dynaconf

Config = Dynaconf(root_path=os.environ.get("DEFAULT_CONFIG_PATH", "."), settings_files=["settings.*", ".secrets.*"])


def get_database_url() -> str:
    """Construct the database url from the config."""
    return os.getenv("DATABASE_URL") or (
        f"{Config.DATABASE_PROTOCOL}://{Config.DATABASE_USER}:{Config.DATABASE_PASSWORD}"
        f"@{Config.DATABASE_HOST}:{Config.DATABASE_PORT}/{Config.DATABASE_DATABASE}"
    )


def get_database_ssl_enabled() -> bool:
    """Whether to verify the database server certificate over TLS."""
    return Config.get("DATABASE_SSL_ENABLED", False)


def get_database_ssl_ca_path() -> str:
    """Path to a mounted PEM CA bundle to verify the database server certificate. Only used when
    get_database_ssl_enabled() is True."""
    return Config.get("DATABASE_SSL_CA_PATH", "/etc/mysql-ca/ca.pem")
