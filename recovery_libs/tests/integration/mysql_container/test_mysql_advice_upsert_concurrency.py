# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests: concurrent Advice writers on real MySQL must not fail or lose Issues.

Job Manager runs several replicas, job-update consumers, and a leader repair loop. All of
them can write Advice for the same job attempt at once. MySQLStorage.upsert_advice() takes a
`SELECT ... FOR UPDATE` lock on (job_id, attempt_index) and then runs an INSERT or an UPDATE.
When the row does not exist yet, concurrent first writers can hit a deadlock (1213) or a
duplicate-key error (1062). Writers that read Advice, append an Issue, and write the record
back can also overwrite each other's Issues unless the merge runs under the row lock.

Each test module gets its own throwaway MySQL container, which follows the pattern in
test_mysql_job_upsert_deadlock.py. Job IDs are unique per test because the module's tests
share the container.
"""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import AdviceState, Issue
from recovery_libs.storage.dataclass import AdviceRecord, SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import AdviceIssueMerge, MySQLStorage

pytestmark = [pytest.mark.integration, pytest.mark.regression]

WORKER_COUNT = 16
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _unique_job_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_job(storage: MySQLStorage, job_id: str) -> None:
    """Advice has a foreign key to its job attempt's heimdall_job_id."""
    storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=job_id,
            job_attempt_index=0,
            workflow_id=f"workflow-{job_id}",
            last_updated_at=datetime.now(UTC),
        )
    )


def _issue(issue_type: str, minute: int) -> Issue:
    return Issue(issue_type=issue_type, timestamp=BASE_TIME + timedelta(minutes=minute), detected_by="test")


def _advice(job_id: str, *, issues: list[Issue], state: AdviceState | None = None) -> AdviceRecord:
    now = datetime.now(UTC)
    return AdviceRecord(
        heimdall_id=generate_heimdall_job_id(job_id, 0),
        visible=state is not None,
        job_id=job_id,
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        advice_state=state.value if state is not None else None,
        issues=issues,
        created_at=now,
        updated_at=now,
    )


def _run_concurrently(task, count: int) -> list[BaseException]:
    """Run task(index) on count threads that start together, and return the errors raised."""
    barrier = threading.Barrier(count)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def run(index: int) -> None:
        barrier.wait()
        try:
            task(index)
        except BaseException as error:  # noqa: BLE001 - every failure is reported to the test.
            with errors_lock:
                errors.append(error)

    with ThreadPoolExecutor(max_workers=count) as pool:
        list(pool.map(run, range(count)))
    return errors


def test_concurrent_first_advice_upserts_succeed(storage: MySQLStorage) -> None:
    """Many writers that create Advice at the same moment must all succeed.

    Half the workers create Advice for distinct new attempts. Without a conflict retry, their
    gap locks can deadlock (1213). The other half race to create Advice for one shared new
    attempt, so all but one INSERT collide with a duplicate key (1062). Each conflict must be
    retried, so that no Advice write is reported as failed.
    """
    distinct_job_ids = [_unique_job_id("advice-new") for _ in range(WORKER_COUNT // 2)]
    shared_job_id = _unique_job_id("advice-shared")
    for job_id in [*distinct_job_ids, shared_job_id]:
        _create_job(storage, job_id)

    def upsert(index: int) -> None:
        job_id = distinct_job_ids[index] if index < len(distinct_job_ids) else shared_job_id
        storage.upsert_advice(_advice(job_id, issues=[_issue("CRASH", index)]))

    errors = _run_concurrently(upsert, WORKER_COUNT)

    assert errors == []
    for job_id in [*distinct_job_ids, shared_job_id]:
        assert storage.read_advice_by_job(job_id, 0) is not None


def test_concurrent_issue_appends_to_one_attempt_keep_every_issue(storage: MySQLStorage) -> None:
    """Concurrent watcher Issues for one job attempt must all be persisted.

    Each worker models a Job Manager replica that read the Advice before any other worker
    wrote it, so its record carries only its own Issue. Before the fix, every replica wrote
    its own list and the last commit won, which dropped the other Issues.
    """
    job_id = _unique_job_id("advice-append")
    _create_job(storage, job_id)
    issues = [_issue("CRASH", minute) for minute in range(WORKER_COUNT)]

    def append(index: int) -> None:
        storage.upsert_advice(_advice(job_id, issues=[issues[index]]), issue_merge=AdviceIssueMerge.APPEND_ONLY)

    errors = _run_concurrently(append, WORKER_COUNT)

    assert errors == []
    stored = storage.read_advice_by_job(job_id, 0)
    assert stored is not None
    assert sorted(issue.timestamp for issue in stored.issues) == [issue.timestamp for issue in issues]


def test_issue_append_after_stale_read_keeps_concurrently_committed_decision(storage: MySQLStorage) -> None:
    """An Issue writer with a stale read must not erase Advice committed after that read.

    The watcher path reads no Advice and builds hidden Advice with no desired state.
    Meanwhile, the job-update path commits Restarted Advice. When the watcher writes, the
    decision must stay as committed, and the watcher's Issue must be added next to the
    existing Issue.
    """
    job_id = _unique_job_id("advice-stale-append")
    _create_job(storage, job_id)
    watcher_record = _advice(job_id, issues=[_issue("HANG", 2)])  # Built from the stale (empty) read.
    storage.upsert_advice(_advice(job_id, issues=[_issue("CRASH", 1)], state=AdviceState.RESTARTED))

    storage.upsert_advice(watcher_record, issue_merge=AdviceIssueMerge.APPEND_ONLY)

    stored = storage.read_advice_by_job(job_id, 0)
    assert stored is not None
    assert stored.visible is True
    assert stored.advice_state == AdviceState.RESTARTED.value
    assert [issue.issue_type for issue in stored.issues] == ["CRASH", "HANG"]


def test_terminal_decision_after_stale_read_keeps_concurrently_committed_issue(storage: MySQLStorage) -> None:
    """A decision writer with a stale read must replace the decision but keep newer Issues.

    The terminal path reads Advice with one Issue and builds a Terminated record from that
    read. Meanwhile, the watcher commits a second Issue. The Terminated decision must win,
    and both Issues must stay alongside the terminal reason.
    """
    job_id = _unique_job_id("advice-stale-terminal")
    _create_job(storage, job_id)
    storage.upsert_advice(_advice(job_id, issues=[_issue("CRASH", 1)]))
    terminal_record = _advice(
        job_id, issues=[_issue("CRASH", 1), _issue("MAX_REQUEUE_REACHED", 3)], state=AdviceState.TERMINATED
    )
    storage.upsert_advice(_advice(job_id, issues=[_issue("HANG", 2)]), issue_merge=AdviceIssueMerge.APPEND_ONLY)

    storage.upsert_advice(terminal_record, issue_merge=AdviceIssueMerge.UNION)

    stored = storage.read_advice_by_job(job_id, 0)
    assert stored is not None
    assert stored.advice_state == AdviceState.TERMINATED.value
    assert [issue.issue_type for issue in stored.issues] == ["CRASH", "HANG", "MAX_REQUEUE_REACHED"]
