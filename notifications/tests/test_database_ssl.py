# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from sqlmodel import create_engine as real_create_engine

from notifications import database


@pytest.fixture(autouse=True)
def reset_engine_cache():
    """get_engine() caches its result in a module-level global; clear it around each test."""
    database._engine = None
    yield
    database._engine = None


def _spy_create_engine(recorded_kwargs, *args, **kwargs):
    """Record the connect_args passed in, but build a real (sqlite) engine so downstream
    event-listener registration in get_engine() still works."""
    recorded_kwargs.update(kwargs)
    return real_create_engine("sqlite://")


def test_get_engine_without_ssl_omits_ssl_connect_arg(config):
    config.DATABASE_SSL_ENABLED = False
    recorded_kwargs = {}

    with patch("notifications.database.create_engine", lambda *a, **kw: _spy_create_engine(recorded_kwargs, *a, **kw)):
        database.get_engine()

    assert "ssl" not in recorded_kwargs["connect_args"]


@patch("pathlib.Path.is_file", return_value=True)
def test_get_engine_with_ssl_enabled_disables_hostname_check(mock_is_file, config):
    config.DATABASE_SSL_ENABLED = True
    config.DATABASE_SSL_CA_PATH = "/etc/mysql-ca/ca.pem"
    recorded_kwargs = {}

    with patch("notifications.database.create_engine", lambda *a, **kw: _spy_create_engine(recorded_kwargs, *a, **kw)):
        database.get_engine()

    assert recorded_kwargs["connect_args"]["ssl"] == {"ca": "/etc/mysql-ca/ca.pem", "check_hostname": False}


@patch("pathlib.Path.is_file", return_value=False)
def test_get_engine_with_ssl_enabled_missing_ca_file_raises(mock_is_file, config):
    config.DATABASE_SSL_ENABLED = True
    config.DATABASE_SSL_CA_PATH = "/etc/mysql-ca/ca.pem"

    with pytest.raises(RuntimeError, match="ssl CA bundle does not exist"):
        database.get_engine()
