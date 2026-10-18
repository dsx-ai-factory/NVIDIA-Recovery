# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from job_manager.config import MySQLConnectionSettings


def test_ssl_defaults_to_disabled():
    settings = MySQLConnectionSettings(host="mysql", port=3306, username="u", password="p", database_name="db")

    assert settings.ssl_enabled is False
    assert settings.ssl_ca_path == "/etc/mysql-ca/ca.pem"


def test_ssl_enabled_and_custom_ca_path_from_config():
    settings = MySQLConnectionSettings(
        host="mysql",
        port=3306,
        username="u",
        password="p",
        database_name="db",
        ssl_enabled=True,
        ssl_ca_path="/custom/ca.pem",
    )

    assert settings.ssl_enabled is True
    assert settings.ssl_ca_path == "/custom/ca.pem"
