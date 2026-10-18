# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression test: concurrent SlurmJobExecutionRecord upserts must not deadlock on real MySQL.

Root cause: MySQLStorage.upsert_job_record() does a `SELECT ... FOR UPDATE` and, when no row is
found, falls through to an INSERT. Under InnoDB's default REPEATABLE READ isolation, a locking
read that finds no matching row still takes a gap lock, so concurrent transactions probing
different not-yet-existing keys in the same gap can deadlock on each other's insert intention
lock -- independent of indexing, since `job_execution_records` already has a unique index on
(job_id, job_attempt_index).

Spins up its own throwaway MySQL container, following the pattern in
tests/integration/test_mysql_connection_ssl.py, since it needs an empty table to maximize gap
lock contention and doesn't want to share state with other integration tests.
"""

import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pytest
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import MySQLStorage
from sqlalchemy.exc import OperationalError

# MySQL error 1213: "Deadlock found when trying to get lock; try restarting transaction".
MYSQL_DEADLOCK_ERROR_CODE = 1213

WORKER_COUNT = 16
ITERATIONS_PER_WORKER = 20
EXISTING_JOB_POOL_SIZE = 30


def _is_mysql_deadlock(exc: BaseException) -> bool:
    cause = exc.__cause__ or exc.__context__
    while cause is not None:
        if (
            isinstance(cause, OperationalError)
            and cause.orig is not None
            and getattr(cause.orig, "args", None)
            and cause.orig.args[0] == MYSQL_DEADLOCK_ERROR_CODE
        ):
            return True
        cause = cause.__cause__ or cause.__context__
    return False


def _build_job_record(job_id: str, now: datetime) -> SlurmJobExecutionRecord:
    return SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=0,
        workflow_id=f"workflow-{job_id}",
        last_updated_at=now,
    )


class _ErrorCollector:
    """Thread-safe bucket for exceptions raised by concurrent upsert_job_record() calls,
    split by whether they're the real MySQL deadlock this module targets."""

    def __init__(self) -> None:
        self.deadlocks: list[BaseException] = []
        self.other_errors: list[BaseException] = []
        self._lock = threading.Lock()

    def record(self, exc: BaseException) -> None:
        with self._lock:
            if _is_mysql_deadlock(exc):
                self.deadlocks.append(exc)
            else:
                self.other_errors.append(exc)

    def assert_clean(self) -> None:
        assert self.other_errors == [], (
            f"Unexpected (non-deadlock) errors during concurrent upsert: {self.other_errors}"
        )
        assert self.deadlocks == [], (
            f"Expected concurrent job upserts to succeed without a MySQL deadlock (error "
            f"{MYSQL_DEADLOCK_ERROR_CODE}), but got {len(self.deadlocks)}: {self.deadlocks}"
        )


@pytest.mark.integration
@pytest.mark.regression
@pytest.mark.parametrize(
    "new_job_probability",
    [1.0, 0.5, 0.0],
    ids=["all-new-inserts", "mixed-insert-and-update", "all-existing-updates"],
)
def test_concurrent_job_upserts_succeed_without_deadlock(storage: MySQLStorage, new_job_probability: float) -> None:
    """Fires many worker threads that each repeatedly upsert a job_id at roughly the same
    time, and asserts every upsert succeeds cleanly.

    Covers the three shapes of concurrent load the controller can produce: all-new-inserts (the
    gap-lock deadlock scenario, see module docstring), all-existing-updates (ordinary record
    locks, not gap locks), and mixed-insert-and-update (closer to real reconciliation traffic).
    A recoverable MySQL deadlock must not propagate out of upsert_job_record() and abort the
    reconciliation tick.
    """
    now = datetime.now(UTC)
    existing_job_ids = [f"existing-{i}" for i in range(EXISTING_JOB_POOL_SIZE)]
    if new_job_probability < 1.0:
        for job_id in existing_job_ids:
            storage.upsert_job_record(_build_job_record(job_id, now))

    collector = _ErrorCollector()

    def upsert_jobs(worker_index: int) -> None:
        for i in range(ITERATIONS_PER_WORKER):
            if random.random() < new_job_probability:
                job_id = f"new-p{new_job_probability}-w{worker_index}-{i}"
            else:
                job_id = random.choice(existing_job_ids)
            try:
                storage.upsert_job_record(_build_job_record(job_id, datetime.now(UTC)))
            except RecoveryStorageException as exc:
                collector.record(exc)

    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as pool:
        futures = [pool.submit(upsert_jobs, w) for w in range(WORKER_COUNT)]
        for future in as_completed(futures):
            future.result()

    collector.assert_clean()


@pytest.mark.integration
@pytest.mark.regression
def test_concurrent_upserts_of_the_same_new_job_succeed_without_duplicate_key_error(
    storage: MySQLStorage,
) -> None:
    """Fires many worker threads that all upsert the identical brand-new job_id at roughly the
    same time, and asserts every upsert succeeds cleanly.

    READ COMMITTED (used above to avoid the gap-lock deadlock) trades it for a narrower race: a
    non-matching SELECT ... FOR UPDATE no longer blocks a concurrent transaction from also seeing
    "no row exists" for the same key, so multiple transactions can all attempt to INSERT it; all
    but the first collide on the unique index with a duplicate-key error (1062). That must be
    handled (rolled back and retried) rather than propagate as an unhandled
    RecoveryStorageException.
    """
    job_id = "duplicate-key-race"
    now = datetime.now(UTC)
    collector = _ErrorCollector()

    def upsert_same_job(_worker_index: int) -> None:
        try:
            storage.upsert_job_record(_build_job_record(job_id, now))
        except RecoveryStorageException as exc:
            collector.record(exc)

    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as pool:
        futures = [pool.submit(upsert_same_job, w) for w in range(WORKER_COUNT)]
        for future in as_completed(futures):
            future.result()

    collector.assert_clean()
