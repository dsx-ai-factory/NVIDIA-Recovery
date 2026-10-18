# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the AttributionRequest-emission atomic claim.

These require a running MySQL configured via environment variables (see conftest).
They verify that ``claim_attribution_request_emission`` flips the marker exactly once and
is safe against the lost-update race between concurrent claimers.
"""

import threading
import uuid
from dataclasses import asdict
from datetime import UTC, datetime

import pytest
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from sqlalchemy import text


@pytest.fixture(scope="module")
def mysql_connection(db_config):
    connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=db_config["database_name"],
    )
    yield connection
    connection.engine.dispose()


@pytest.fixture(scope="module")
def mysql_storage(mysql_connection):
    storage = MySQLStorage(connection=mysql_connection)
    storage.bootstrap()
    assert storage.ready()
    return storage


@pytest.fixture(scope="function")
def clean_job_records(mysql_connection):
    yield
    with mysql_connection.session() as session:
        session.execute(text("DELETE FROM job_execution_records WHERE job_id LIKE 'attrreq_%'"))
        session.commit()


def _insert_job(storage: MySQLStorage, job_id: str, attempt_index: int) -> None:
    storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=job_id,
            job_attempt_index=attempt_index,
            workflow_id=job_id,
            heimdall_job_id=uuid.uuid4(),
            last_updated_at=datetime.now(UTC),
        )
    )


def test_claim_wins_once_then_noops(mysql_storage, clean_job_records):
    _insert_job(mysql_storage, "attrreq_seq", 0)

    assert mysql_storage.claim_attribution_request_emission("attrreq_seq", 0) is True
    # Second claim on the same attempt is a no-op.
    assert mysql_storage.claim_attribution_request_emission("attrreq_seq", 0) is False


def test_claim_returns_false_when_no_such_attempt(mysql_storage, clean_job_records):
    # No matching row -> nothing updated -> not claimed.
    assert mysql_storage.claim_attribution_request_emission("attrreq_missing", 0) is False


def test_claim_is_atomic_under_concurrency(mysql_storage, clean_job_records):
    _insert_job(mysql_storage, "attrreq_race", 0)

    results: list[bool] = []
    start = threading.Barrier(2)

    def claim() -> None:
        start.wait()
        results.append(mysql_storage.claim_attribution_request_emission("attrreq_race", 0))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Exactly one concurrent claimer wins the row.
    assert results.count(True) == 1
    assert results.count(False) == 1


def test_general_update_does_not_reset_claim(mysql_storage, clean_job_records):
    _insert_job(mysql_storage, "attrreq_reset", 0)
    # First terminal update claims the emission.
    assert mysql_storage.claim_attribution_request_emission("attrreq_reset", 0) is True

    # A later/duplicate terminal update: its record came from a job-update payload that lacks
    # the internal field, so from_json defaults attribution_request_emitted=False.
    incoming = SlurmJobExecutionRecord(
        job_id="attrreq_reset",
        job_attempt_index=0,
        workflow_id="attrreq_reset",
        heimdall_job_id=uuid.uuid4(),
        last_updated_at=datetime.now(UTC),
    )
    assert incoming.attribution_request_emitted is False
    mysql_storage.update_job_record(**asdict(incoming))

    # The claimed flag survives the full-record update, so a second claim (a duplicate publish) fails.
    assert mysql_storage.read_job_record("attrreq_reset", 0).attribution_request_emitted is True
    assert mysql_storage.claim_attribution_request_emission("attrreq_reset", 0) is False
