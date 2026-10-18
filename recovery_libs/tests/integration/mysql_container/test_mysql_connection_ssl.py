# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Standalone integration test proving MySQLConnection's ssl_enabled path performs real TLS
server-certificate verification against a TLS-enforcing MySQL server, not just that the
connect_args dict is shaped correctly (see the mocked unit tests in
tests/storage/test_mysql_connection.py).

This spins up its own throwaway MySQL container (see the mysql_container fixture registered
in this directory's conftest.py) rather than the externally-provisioned database most
integration tests assume, since it needs to extract the container's auto-generated
self-signed CA bundle.
"""

import io
import subprocess
import tarfile

import pytest
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.testing.storage.mysql import DB_NAME, ROOT_PASSWORD
from testcontainers.mysql import MySqlContainer


def _read_file_from_container(container: MySqlContainer, path: str) -> bytes:
    """Read a single file out of a running docker container via the docker archive API."""
    stream, _ = container.get_wrapped_container().get_archive(path)
    buf = io.BytesIO(b"".join(stream))
    with tarfile.open(fileobj=buf) as tar:
        member = tar.getmembers()[0]
        return tar.extractfile(member).read()


@pytest.fixture(scope="module")
def container_ca_path(mysql_container, tmp_path_factory):
    """The CA bundle the container generated for itself, extracted to a local file."""
    ca_bytes = _read_file_from_container(mysql_container, "/var/lib/mysql/ca.pem")
    ca_path = tmp_path_factory.mktemp("mysql-ca") / "ca.pem"
    ca_path.write_bytes(ca_bytes)
    return ca_path


@pytest.fixture(scope="module")
def mismatched_ca_path(tmp_path_factory):
    """A syntactically valid but unrelated self-signed CA that does not match the container's
    certificate, to prove verification actually fails closed rather than just failing to parse."""
    ca_path = tmp_path_factory.mktemp("mysql-ca-wrong") / "ca.pem"
    key_path = tmp_path_factory.mktemp("mysql-key-wrong") / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=not-the-mysql-container",
            "-keyout",
            str(key_path),
            "-out",
            str(ca_path),
        ],
        check=True,
        capture_output=True,
    )
    return ca_path


def _connection(mysql_container, **kwargs) -> MySQLConnection:
    return MySQLConnection(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        username="root",
        password=ROOT_PASSWORD,
        database_name=DB_NAME,
        **kwargs,
    )


@pytest.mark.integration
def test_ssl_enabled_connects_with_containers_self_signed_ca(mysql_container, container_ca_path):
    connection = _connection(mysql_container, ssl_enabled=True, ssl_ca=str(container_ca_path))

    assert connection.ready() is True


@pytest.mark.integration
def test_ssl_enabled_rejects_mismatched_ca(mysql_container, mismatched_ca_path):
    connection = _connection(mysql_container, ssl_enabled=True, ssl_ca=str(mismatched_ca_path))

    assert connection.ready() is False


@pytest.mark.integration
def test_ssl_disabled_connects_without_verification(mysql_container):
    """Baseline: the container still accepts a plain (unverified) connection when
    ssl_enabled is left at its default of False."""
    connection = _connection(mysql_container)

    assert connection.ready() is True
