# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration coverage for partial Job update transition snapshots."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from recovery_libs.storage.base import UpdateJobResult
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from sqlalchemy import inspect, text


@pytest.fixture(scope="module")
def mysql_connection(db_config):
    """Create the module-scoped connection to the integration database."""
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
    """Return bootstrapped storage backed by the integration database."""
    storage = MySQLStorage(connection=mysql_connection)
    storage.bootstrap()
    assert storage.ready()
    return storage


@pytest.fixture
def clean_job_records(mysql_connection):
    """Remove Job records owned by this module after each test."""
    yield
    with mysql_connection.session() as session:
        session.execute(text("DELETE FROM job_execution_records WHERE job_id LIKE 'update_snapshot_%'"))
        session.commit()


def test_concurrent_partial_updates_return_a_serialized_transition_chain(mysql_storage, clean_job_records):
    """Concurrent result snapshots must describe consecutive committed writes."""
    job_id = "update_snapshot_race"
    mysql_storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=job_id,
            job_attempt_index=0,
            workflow_id=job_id,
            state="PENDING",
            last_updated_at=datetime.now(UTC),
        )
    )

    start = threading.Barrier(2)
    results: list[UpdateJobResult] = []
    results_lock = threading.Lock()

    def update_state(state: str) -> None:
        start.wait()
        result = mysql_storage.update_job_record(
            job_id=job_id,
            job_attempt_index=0,
            state=state,
            last_updated_at=datetime.now(UTC),
        )
        with results_lock:
            results.append(result)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(update_state, state) for state in ("RUNNING", "FAILED")]
        for future in futures:
            future.result()

    assert len(results) == 2
    for result in results:
        assert result.old_record is not None
        assert result.updated_record is not None
        assert inspect(result.old_record).session is None
        assert inspect(result.updated_record).session is None

    initial_transitions = [result for result in results if result.old_record.state == "PENDING"]
    assert len(initial_transitions) == 1
    first = initial_transitions[0]
    second = next(result for result in results if result is not first)
    assert second.old_record.state == first.updated_record.state

    persisted = mysql_storage.read_job_record(job_id, 0)
    assert persisted is not None
    assert persisted.state == second.updated_record.state
