# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

import pytest
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.storage.type_decorators import TZDateTimeType
from sqlalchemy import text


@pytest.fixture
def mysql_connection(db_config):
    """Isolate dialect timestamp processors from another test's fault injection."""
    connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=db_config["database_name"],
    )
    yield connection
    connection.write_engine.dispose()
    connection.engine.dispose()


@pytest.fixture
def mysql_storage(mysql_connection):
    """Return storage with a fresh connection for each timestamp integrity test."""
    storage = MySQLStorage(connection=mysql_connection)
    storage.bootstrap()
    assert storage.ready()
    return storage


@pytest.fixture
def clean_job_id(mysql_connection):
    job_id = "timestamp-atomicity-refresh-failure"
    with mysql_connection.session() as session:
        session.execute(text("DELETE FROM job_execution_records WHERE job_id = :job_id"), {"job_id": job_id})
        session.commit()
    yield job_id
    with mysql_connection.session() as session:
        session.execute(text("DELETE FROM job_execution_records WHERE job_id = :job_id"), {"job_id": job_id})
        session.commit()


def test_mysql_sessions_enable_strict_trans_tables(mysql_connection):
    with mysql_connection.session() as session:
        sql_modes = set(session.scalar(text("SELECT @@SESSION.sql_mode")).split(","))

    assert "STRICT_TRANS_TABLES" in sql_modes


def test_job_upsert_rolls_back_when_precommit_refresh_fails(
    mysql_storage,
    mysql_connection,
    clean_job_id,
    monkeypatch,
):
    def fail_timestamp_read(self, value, dialect):
        if value is not None:
            raise TypeError("forced unreadable TIMESTAMP")

    monkeypatch.setattr(TZDateTimeType, "process_result_value", fail_timestamp_read)
    record = SlurmJobExecutionRecord(
        job_id=clean_job_id,
        job_attempt_index=0,
        workflow_id=clean_job_id,
        last_updated_at=datetime.now(UTC),
    )

    with pytest.raises(RecoveryStorageException, match="forced unreadable TIMESTAMP"):
        mysql_storage.upsert_job_record(record)

    with mysql_connection.session() as session:
        persisted_count = session.scalar(
            text("SELECT COUNT(*) FROM job_execution_records WHERE job_id = :job_id"),
            {"job_id": clean_job_id},
        )
    assert persisted_count == 0


@pytest.mark.parametrize("bad_minute", [25, 34])
def test_completed_slurm_attempt_corrects_persisted_start(mysql_storage, clean_job_id, bad_minute):
    """Accounting corrects earlier or later starts without changing another attempt."""
    actual = datetime(2026, 10, 2, 19, 33, 20, tzinfo=UTC)
    end = datetime(2026, 10, 2, 19, 33, 21, tzinfo=UTC)
    initial_start = datetime(2026, 10, 2, 19, 23, 7, tzinfo=UTC)
    for attempt, start in (
        (0, initial_start),
        (1, datetime(2026, 10, 2, 19, bad_minute, 8, tzinfo=UTC)),
    ):
        mysql_storage.upsert_job_record(
            SlurmJobExecutionRecord(
                job_id=clean_job_id,
                job_attempt_index=attempt,
                workflow_id="retry-runtime",
                state="PENDING",
                start_time=start,
                last_updated_at=actual,
            )
        )
    mysql_storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=clean_job_id,
            job_attempt_index=1,
            workflow_id="retry-runtime",
            state="COMPLETED",
            start_time=actual,
            end_time=end,
            last_updated_at=end,
            scheduler_metadata={"scheduler": "slurm", "slurm_start_time": actual.isoformat()},
        )
    )
    # An absent timestamp on a subsequent observation must not erase it.
    mysql_storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=clean_job_id,
            job_attempt_index=1,
            workflow_id="retry-runtime",
            state="COMPLETED",
            last_updated_at=end,
            scheduler_metadata={"scheduler": "slurm"},
        )
    )
    retry = mysql_storage.read_job_record(clean_job_id, 1)
    assert retry.start_time == actual
    assert retry.end_time == end
    assert (retry.end_time - retry.start_time).total_seconds() == 1
    assert mysql_storage.read_job_record(clean_job_id, 0).start_time == initial_start
