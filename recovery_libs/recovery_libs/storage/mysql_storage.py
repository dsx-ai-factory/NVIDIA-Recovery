# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import functools
import logging
import os
import random
import time
import uuid
from collections.abc import Callable, Sequence, Sized
from copy import deepcopy
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, NamedTuple

from sqlalchemy import Integer, and_, cast, desc, func, or_, select, tuple_, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import aliased, contains_eager, joinedload, make_transient, selectinload
from sqlalchemy.sql import Select

from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    AdviceState,
    JobTimeFilter,
    SortOrderEnum,
)
from recovery_libs.storage.base import (
    BaseStorage,
    RecoveryStorageException,
    RecoveryStorageInputValidationError,
    UpdateJobResult,
)
from recovery_libs.storage.dataclass import (
    AdviceReconciliationStatusRecord,
    AdviceRecord,
    ApplicationIntentionRecord,
    DagDependency,
    DagDependencyActionStatus,
    DataClassBase,
    JobEvent,
    RecoveryMilestoneRecord,
    RecoverySequenceRecord,
    SlurmJobExecutionRecord,
    WorkflowRecord,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.timestamps import normalize_mysql_timestamp

logger = logging.getLogger(__name__)

SCHEMA_UPGRADE_LOCK_NAME = "heimdall_mysql_schema_upgrade_lock"
SCHEMA_UPGRADE_TIMEOUT_SECS = 5
SCHEMA_UPGRADE_WAIT_INTERVAL_SECS = 5
# Maximum number of jobs to fetch in a single batch query.
# This limit prevents excessive memory usage and protects against malicious/accidental
# large requests while still supporting typical node analysis use cases (20-30 jobs).
# Value chosen based on common database batch operation conventions.
MAX_BATCH_READ_SIZE = 1000

# MySQL error 1213: "Deadlock found when trying to get lock; try restarting transaction".
MYSQL_DEADLOCK_ERROR_CODE = 1213
# MySQL error 1062: "Duplicate entry '...' for key '...'".
MYSQL_DUPLICATE_ENTRY_ERROR_CODE = 1062
UPSERT_CONFLICT_MAX_RETRIES = int(os.environ.get("UPSERT_CONFLICT_MAX_RETRIES", "3"))
UPSERT_CONFLICT_BASE_DELAY_SECS = float(os.environ.get("UPSERT_CONFLICT_BASE_DELAY_SECS", "0.05"))
UPSERT_CONFLICT_JITTER_SECS = float(os.environ.get("UPSERT_CONFLICT_JITTER_SECS", "0.05"))


def _is_mysql_deadlock(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) and exc.orig is not None and exc.orig.args[0] == MYSQL_DEADLOCK_ERROR_CODE


def _is_mysql_duplicate_entry(exc: BaseException) -> bool:
    return (
        isinstance(exc, IntegrityError)
        and exc.orig is not None
        and exc.orig.args[0] == MYSQL_DUPLICATE_ENTRY_ERROR_CODE
    )


def _is_retryable_mysql_conflict(exc: BaseException) -> bool:
    return _is_mysql_deadlock(exc) or _is_mysql_duplicate_entry(exc)


def _conflict_retry_delay(attempt: int) -> float:
    """Exponential backoff with jitter for a 1-indexed conflict retry attempt."""
    return UPSERT_CONFLICT_BASE_DELAY_SECS * (2 ** (attempt - 1)) + random.uniform(0, UPSERT_CONFLICT_JITTER_SECS)


def retry_on_conflict(describe: Callable[..., str]) -> Callable:
    """Decorator: retry a storage method with bounded exponential backoff and jitter when it
    hits a real MySQL write conflict (deadlock 1213 or duplicate-key 1062), instead of
    propagating it immediately.

    Meant for storage methods that do a locking `SELECT ... FOR UPDATE` followed by an
    INSERT/UPDATE. Both conflicts come from concurrent transactions racing on the same row and
    resolve the same way: the wrapped method rolls back, and the retry re-runs the locking read,
    converging on the correct INSERT or UPDATE.

    Retry budget and backoff are controlled by the UPSERT_CONFLICT_MAX_RETRIES,
    UPSERT_CONFLICT_BASE_DELAY_SECS, and UPSERT_CONFLICT_JITTER_SECS environment variables.

    Args:
        describe: Called with the decorated method's arguments (excluding self) to build a
            human-readable label for retry/exhaustion log messages.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            label = describe(*args, **kwargs)
            attempt = 1
            while True:
                try:
                    return func(self, *args, **kwargs)
                except (OperationalError, IntegrityError) as e:
                    if not _is_retryable_mysql_conflict(e):
                        raise
                    if attempt > UPSERT_CONFLICT_MAX_RETRIES:
                        logger.error(
                            f"Exhausted {UPSERT_CONFLICT_MAX_RETRIES} conflict retries in "
                            f"{func.__name__} for {label}: {e}"
                        )
                        raise
                    delay = _conflict_retry_delay(attempt)
                    logger.warning(
                        f"Conflict on attempt {attempt} in {func.__name__} for {label}, "
                        f"retrying in {delay:.3f}s ({UPSERT_CONFLICT_MAX_RETRIES - attempt + 1} retries left)."
                    )
                    time.sleep(delay)
                    attempt += 1

        return wrapper

    return decorator


_JOB_UPSERT_TIMESTAMP_FIELDS = ("submit_time", "start_time", "end_time")

_UPSERT_UPDATABLE_FIELDS = [
    "workflow_id",
    "job_name",
    "state",
    "state_reason",
    "submit_time",
    "start_time",
    "end_time",
    "cluster",
    "partition",
    "reservation",
    "namespace",
    "worker_cluster_name",
    "user",
    "command",
    "dependency",
    "node_count",
    "total_gpu_count",
    "total_cpu_count",
    "total_memory_mb",
    "allocated_node_list",
    "inclusion_node_list",
    "exclusion_node_list",
    "sticky_exclusion_node_list",
    "scheduler_metadata",
    "anomalies",
    "checkpoints",
    "aps_spec",
    "config_validation_failure_types",
    "retry_of",
    "log_file_paths",
]


def _normalize_job_upsert_timestamps(job_record: SlurmJobExecutionRecord) -> None:
    for field in _JOB_UPSERT_TIMESTAMP_FIELDS:
        value = getattr(job_record, field)
        if value is None:
            continue
        try:
            setattr(job_record, field, normalize_mysql_timestamp(value, field_name=field))
        except (TypeError, ValueError) as error:
            raise RecoveryStorageInputValidationError(str(error)) from error


def _commit_validated_record(session, record: DataClassBase) -> None:
    """Round-trip an ORM record before committing so failed reads cannot leave durable writes."""
    try:
        session.flush()
        session.refresh(record)
        # Commit expires attached objects by default. Detaching the already
        # refreshed record preserves the return value without doing fallible
        # database work after the transaction becomes durable.
        session.expunge(record)
        session.commit()
    except Exception:
        session.rollback()
        raise


class UpsertJobResult(NamedTuple):
    """Result of creating or updating a job execution record.

    ``old_record`` is a transient snapshot of the record before an update. It
    is ``None`` when the upsert created a new record.
    """

    inserted_record: SlurmJobExecutionRecord
    was_created: bool
    old_record: SlurmJobExecutionRecord | None


class UpsertAdviceResult(NamedTuple):
    """Result of creating or updating an advice record.

    ``old_record`` is a transient snapshot of the record before an update. It
    is ``None`` when the upsert created a new record.
    """

    inserted_record: AdviceRecord
    was_created: bool
    old_record: AdviceRecord | None


class AdviceIssueMerge(StrEnum):
    """How ``MySQLStorage.upsert_advice`` combines incoming Issues with the stored row's Issues.

    Writers that read Advice, add an Issue, and write the whole record back race with
    each other: the stored Issues can change between their read and their write. The merge
    modes run under the upsert's row lock, so an Issue committed by a concurrent
    writer is kept instead of being overwritten by a stale snapshot.
    """

    REPLACE = "replace"
    """The incoming Issues replace the stored Issues. This is the API write contract."""

    UNION = "union"
    """Keep the stored Issues and append the incoming Issues that are not stored yet.
    All other fields are replaced by the incoming record."""

    APPEND_ONLY = "append_only"
    """For an existing row, append the incoming Issues that are not stored yet and keep
    every other stored field. A new row is created from the incoming record. Use this
    for writers that only report evidence and must not change the advice decision."""


def _issue_identity(issue: Any) -> tuple[Any, Any, Any]:
    """Return the key that marks two Issues as the same observation.

    Matches the duplicate check Job Manager applies before it records a watcher Issue.
    """
    return (issue.issue_type, issue.timestamp, issue.detected_by)


def _union_issues(stored: list | None, incoming: list | None) -> list | None:
    """Return the stored Issues followed by incoming Issues that are not stored yet.

    Stored Issues come first so the list stays in commit order. ``dag_utils`` reads the
    newest Issue from the end of the list.
    """
    seen = {_issue_identity(issue) for issue in stored or []}
    additions = []
    for issue in incoming or []:
        identity = _issue_identity(issue)
        if identity not in seen:
            seen.add(identity)
            additions.append(issue)
    if not additions:
        return stored
    return [*(stored or []), *additions]


class SoftDeleteAdviceResult(NamedTuple):
    """Result of soft deleting an Advice record.

    ``advice_record`` is ``None`` when no record matched. ``was_deleted``
    indicates whether the record was soft deleted.
    """

    advice_record: AdviceRecord | None
    was_deleted: bool


def apply_sorting_condition(table: type[DataClassBase], select_query: Select, sort_by: list[tuple[str, str]]) -> Select:
    """
    Helper function to apply sorting to a SQLAlchemy select query based on the provided sort_by list.
    """
    for column_name, sort_order in sort_by:
        column = getattr(table, column_name, None)
        if not column:
            raise RecoveryStorageInputValidationError(
                f"Invalid sort_by expression, column '{column_name}' does not exist in SlurmJobExecutionRecord table."
            )

        # job_id needs a slightly different treatment because the column datatype is VARCHAR for future
        # compatibility considerations, but we want to sort it as an integer in Slurm.
        if sort_order == SortOrderEnum.ASC.value:
            if column_name == "job_id":
                select_query = select_query.order_by(cast(column, Integer).asc())
            else:
                select_query = select_query.order_by(column.asc())
        elif sort_order == SortOrderEnum.DESC.value:
            if column_name == "job_id":
                select_query = select_query.order_by(cast(column, Integer).desc())
            else:
                select_query = select_query.order_by(column.desc())
        else:
            raise RecoveryStorageInputValidationError(
                f"Invalid sort_by expression, '{sort_order}' is not a valid sort order, only 'asc' and "
                "'desc' are supported."
            )

    return select_query


class MySQLStorage(BaseStorage):
    """
    MySQL storage class to interact with the MySQL database job execution tables.

    Args:
        connection: MySQLConnection object.
    """

    def __init__(
        self,
        connection: MySQLConnection,
    ):
        super().__init__()
        self.connection = connection

    def bootstrap(self, schema_upgrade: bool = True) -> bool:
        return self.connection.bootstrap(schema_upgrade=schema_upgrade) and self.ready()

    def ready(self) -> bool:
        return self.connection.ready()

    def _fill_heimdall_job_id(self, job_record: SlurmJobExecutionRecord) -> None:
        if job_record.heimdall_job_id is None:
            job_record.heimdall_job_id = generate_heimdall_job_id(
                job_record.job_id,
                job_record.job_attempt_index,
            )

    def get_job_latest_attempt_index(self, job_id: str) -> int:
        try:
            with self.connection.session() as session:
                select_query = (
                    select(SlurmJobExecutionRecord.job_attempt_index)
                    .where(SlurmJobExecutionRecord.job_id == job_id)
                    .order_by(desc(SlurmJobExecutionRecord.job_attempt_index))
                    .limit(1)
                )
                result = session.scalars(select_query).first()
                if result is not None:
                    return result
                else:
                    return -1
        except Exception as e:
            logger.exception(
                f"Failed to get latest attempt index for job: {job_id}", extra={"job_id": job_id, "error": str(e)}
            )
            raise RecoveryStorageException(f"Failed to get latest attempt index for job: {job_id}, error: {e}") from e

    def create_job_record(self, job_record: SlurmJobExecutionRecord) -> bool:
        try:
            self._fill_heimdall_job_id(job_record)
            with self.connection.session() as session:
                session.add(job_record)
                session.commit()
                logger.debug(
                    f"Succesfully created a job record for job {job_record.job_id}, attempt index {job_record.job_attempt_index}."
                )
                return True
        except Exception as e:
            logger.exception("Failed to create job record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to create job record: {e}") from e

    def upsert_job_record(
        self,
        job_record: SlurmJobExecutionRecord,
        *,
        populate_heimdall_id_log_path: bool = False,
    ) -> UpsertJobResult:
        """
        Create or update a job execution record.

        If a record with the same job_id and job_attempt_index exists, it will be updated.
        Otherwise, a new record will be created.

        Args:
            job_record: The job record to upsert. Must have job_id and job_attempt_index set.
            populate_heimdall_id_log_path: Populate the legacy log-file field from
                the Heimdall job ID resolved under the upsert lock when no
                nonempty path list is already retained.

        Returns:
            The stored record, whether it was created, and the transient
            pre-update snapshot when an existing record was updated.

        Raises:
            RecoveryStorageException: If the upsert operation fails, including when conflict
                (deadlock or duplicate-key) retries are exhausted.
        """
        try:
            _normalize_job_upsert_timestamps(job_record)
            self._fill_heimdall_job_id(job_record)
            return self._upsert_job_record_once(job_record, populate_heimdall_id_log_path, job_record.log_file_paths)
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            logger.exception("Failed to upsert job record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to upsert job record: {e}") from e

    @retry_on_conflict(
        describe=lambda job_record, populate_heimdall_id_log_path, original_log_file_paths: (
            f"job {job_record.job_id}, attempt index {job_record.job_attempt_index}"
        )
    )
    def _upsert_job_record_once(
        self,
        job_record: SlurmJobExecutionRecord,
        populate_heimdall_id_log_path: bool,
        original_log_file_paths: list | None,
    ) -> UpsertJobResult:
        """Single-attempt body of upsert_job_record(), run fresh for each conflict retry.

        Runs under READ COMMITTED: REPEATABLE READ's gap lock on a non-matching
        SELECT ... FOR UPDATE is what causes the deadlock retry_on_conflict retries around (see
        its docstring). When self.connection.write_pool_enabled (the default), that isolation
        level is set once per physical connection on a dedicated write pool -- see
        MySQLConnection -- so this method doesn't pay a per-call SET/RESET round trip. When
        disabled, it falls back to the per-transaction override below on the shared pool.

        Restores job_record.log_file_paths to the caller's original value on every attempt: a
        failed create attempt (below) may have mutated it to a placeholder, and job_record is
        reused across retries, so that stale value would otherwise leak into a retry's UPDATE and
        overwrite real log paths the winning transaction already committed.
        """
        job_record.log_file_paths = original_log_file_paths
        with self.connection.write_session() as session:
            if not self.connection.write_pool_enabled:
                session.connection(execution_options={"isolation_level": "READ COMMITTED"})

            # Check if job already exists
            existing_job = session.scalars(
                select(SlurmJobExecutionRecord)
                .where(SlurmJobExecutionRecord.job_id == job_record.job_id)
                .where(SlurmJobExecutionRecord.job_attempt_index == job_record.job_attempt_index)
                .with_for_update()
            ).first()

            now = datetime.now(UTC)

            if existing_job:
                # Snapshot before mutating the ORM-managed instance. Making the
                # copy transient prevents accidental persistence by callers.
                old_record = deepcopy(existing_job)
                make_transient(old_record)

                if existing_job.heimdall_job_id is None:
                    existing_job.heimdall_job_id = job_record.heimdall_job_id
                if (
                    populate_heimdall_id_log_path
                    and job_record.log_file_paths is None
                    and not existing_job.log_file_paths
                ):
                    job_record.log_file_paths = [str(existing_job.heimdall_job_id)]
                for field in _UPSERT_UPDATABLE_FIELDS:
                    new_value = getattr(job_record, field)
                    if new_value is not None:
                        # Slurm execution observations can correct a prior queue estimate.
                        # Preserve the existing first-start rule for other producers.
                        if (
                            field == "start_time"
                            and existing_job.start_time is not None
                            and (job_record.scheduler_metadata or {}).get("scheduler") != "slurm"
                        ):
                            continue
                        setattr(existing_job, field, new_value)
                existing_job.last_updated_at = now

                _commit_validated_record(session, existing_job)
                logger.debug(
                    f"Successfully updated job record for job {job_record.job_id}, attempt index {job_record.job_attempt_index}."
                )
                return UpsertJobResult(
                    inserted_record=existing_job,
                    was_created=False,
                    old_record=old_record,
                )
            else:
                # Create new record
                if populate_heimdall_id_log_path:
                    job_record.log_file_paths = [str(job_record.heimdall_job_id)]
                job_record.last_updated_at = now
                session.add(job_record)
                _commit_validated_record(session, job_record)

                logger.debug(
                    f"Successfully created job record for job {job_record.job_id}, attempt index {job_record.job_attempt_index}."
                )
                return UpsertJobResult(
                    inserted_record=job_record,
                    was_created=True,
                    old_record=None,
                )

    def read_job_record(self, job_id: str, job_attempt_index: int | None = None) -> SlurmJobExecutionRecord | None:
        try:
            with self.connection.session() as session:
                if job_attempt_index is not None:
                    select_query = (
                        select(SlurmJobExecutionRecord)
                        .where(SlurmJobExecutionRecord.job_id == job_id)
                        .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
                    )
                else:
                    select_query = (
                        select(SlurmJobExecutionRecord)
                        .where(SlurmJobExecutionRecord.job_id == job_id)
                        .order_by(desc(SlurmJobExecutionRecord.job_attempt_index))
                        .limit(1)
                    )

                select_query = select_query.options(joinedload(SlurmJobExecutionRecord.events))

                return session.scalars(select_query).first() or None
        except Exception as e:
            logger.exception("Failed to read job record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to read job record: {e}") from e

    def get_allocated_nodes_batch(self, job_runs: list[tuple[str, int]]) -> dict[str, set[str]]:
        """
        Batch read allocated node lists for multiple jobs.

        Args:
            job_runs: List of (job_id, attempt_index) tuples to fetch.
                     Maximum batch size is MAX_BATCH_READ_SIZE (1000).

        Returns:
            Dictionary mapping "{job_id}/{attempt_index}" to set of allocated nodes.
            Jobs not found in database will have empty sets and be logged as warnings.

        Raises:
            RecoveryStorageInputValidationError: If batch size exceeds maximum or input is invalid.
            RecoveryStorageException: If database query fails.

        Example:
            >>> storage.get_allocated_nodes_batch([("12345", 0), ("12346", 0)])
            {"12345/0": {"node1", "node2"}, "12346/0": {"node3", "node4"}}
        """
        if not job_runs:
            return {}

        # Input validation
        if not all(
            isinstance(job_id, str) and job_id and isinstance(idx, int) and idx >= 0 for job_id, idx in job_runs
        ):
            raise RecoveryStorageInputValidationError(
                "Invalid job_runs format: expected list of (str, int>=0) tuples with non-empty job_id"
            )

        # Deduplicate while preserving order
        seen = set()
        unique_job_runs = []
        for job_run in job_runs:
            if job_run not in seen:
                seen.add(job_run)
                unique_job_runs.append(job_run)

        if len(unique_job_runs) != len(job_runs):
            logger.debug(f"Removed {len(job_runs) - len(unique_job_runs)} duplicate entries from batch request")
        job_runs = unique_job_runs

        # Batch size limit
        if len(job_runs) > MAX_BATCH_READ_SIZE:
            raise RecoveryStorageInputValidationError(
                f"Batch size {len(job_runs)} exceeds maximum {MAX_BATCH_READ_SIZE}"
            )

        try:
            with self.connection.session() as session:
                # Fetch only job_id, attempt_index, and allocated_node_list for efficiency
                select_query = select(
                    SlurmJobExecutionRecord.job_id,
                    SlurmJobExecutionRecord.job_attempt_index,
                    SlurmJobExecutionRecord.allocated_node_list,
                ).where(
                    tuple_(
                        SlurmJobExecutionRecord.job_id,
                        SlurmJobExecutionRecord.job_attempt_index,
                    ).in_(job_runs)
                )

                results = session.execute(select_query).all()

                # Build result dict
                result_dict: dict[str, set[str]] = {}
                found_jobs = set()

                for job_id, attempt_index, node_list in results:
                    key = f"{job_id}/{attempt_index}"
                    found_jobs.add((job_id, attempt_index))
                    # Filter out None/empty values from node list
                    result_dict[key] = {n for n in (node_list or []) if n}

                # Log missing jobs and add empty sets
                for job_id, attempt_index in job_runs:
                    if (job_id, attempt_index) not in found_jobs:
                        key = f"{job_id}/{attempt_index}"
                        logger.warning(f"Job record not found: {key}")
                        result_dict[key] = set()

                logger.debug(
                    f"Batch read {len(results)} job records (requested {len(job_runs)}, "
                    f"missing {len(job_runs) - len(results)})"
                )

                return result_dict

        except Exception as e:
            logger.exception("Failed to batch read job records", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to batch read job records: {e}") from e

    def read_workflows(
        self,
        limit: int = 50,
        offset: int = 0,
        workflow_id: str | None = None,
        workflow_id_match: str = "prefix",
    ) -> tuple[list[WorkflowRecord], int]:
        try:
            with self.connection.session() as session:
                workflow_start_time = func.min(SlurmJobExecutionRecord.start_time).label("workflow_start_time")
                workflow_end_time = func.max(SlurmJobExecutionRecord.end_time).label("workflow_end_time")

                # TODO: Index/cache/materialize this query since it likely is not performant for large amounts of jobs.
                select_query_base = select(
                    SlurmJobExecutionRecord.workflow_id,
                    workflow_start_time,
                    workflow_end_time,
                ).group_by(SlurmJobExecutionRecord.workflow_id)
                if workflow_id is not None:
                    if workflow_id_match == "prefix":
                        select_query_base = select_query_base.where(
                            SlurmJobExecutionRecord.workflow_id.istartswith(workflow_id, autoescape=True)
                        )
                    elif workflow_id_match == "fuzzy":
                        select_query_base = select_query_base.where(
                            SlurmJobExecutionRecord.workflow_id.icontains(workflow_id, autoescape=True)
                        )
                    else:
                        select_query_base = select_query_base.where(SlurmJobExecutionRecord.workflow_id == workflow_id)

                select_query = (
                    select_query_base.order_by(workflow_start_time, SlurmJobExecutionRecord.workflow_id)
                    .limit(limit)
                    .offset(offset)
                )

                base_subquery = select_query_base.subquery()
                select_count_query = select(func.count(base_subquery.c.workflow_id)).select_from(base_subquery)

                rows = session.execute(select_query).all()
                record_count = session.scalars(select_count_query).first() or 0

                records = [
                    WorkflowRecord(
                        id=row.workflow_id,
                        start_time=row.workflow_start_time,
                        end_time=row.workflow_end_time,
                    )
                    for row in rows
                ]

                return records, record_count
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            raise RecoveryStorageException(f"Unable to query the db with due to error: {e}.") from e

    def read_job_groups(
        self,
        job_group_id: str | None = None,
        user: str | None = None,
        sort_by: list[tuple[str, str]] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Reads aggregated job group metadata derived from the job execution records table.

        Each row represents a unique job_group_id with the count of unique job IDs and total attempts
        belonging to that group.

        Args:
            job_group_id (Optional[str]): Filter by a specific job group ID.
            user (Optional[str]): Filter by user.
            sort_by (Optional[list[tuple[str, str]]]): List of (column, order) tuples for sorting.
                Supported columns: job_group_id, unique_job_count, total_job_attempt_count.
            limit (int): Maximum number of records to return. Default is 50.
            offset (int): Number of records to skip. Default is 0.

        Returns:
            tuple[list[dict[str, Any]], int]: A tuple containing the list of job group metadata dicts
                and the total count of matching groups (ignoring pagination).
        """
        try:
            with self.connection.session() as session:
                unique_job_count = func.count(func.distinct(SlurmJobExecutionRecord.job_id)).label("unique_job_count")
                total_job_attempt_count = func.count(SlurmJobExecutionRecord.id).label("total_job_attempt_count")

                select_query_base = select(
                    SlurmJobExecutionRecord.job_group_id,
                    unique_job_count,
                    total_job_attempt_count,
                ).where(SlurmJobExecutionRecord.job_group_id.is_not(None))

                if job_group_id:
                    select_query_base = select_query_base.where(SlurmJobExecutionRecord.job_group_id == job_group_id)
                if user:
                    select_query_base = select_query_base.where(SlurmJobExecutionRecord.user == user)

                select_query_base = select_query_base.group_by(SlurmJobExecutionRecord.job_group_id)

                # Count total matching groups using a subquery
                base_subquery = select_query_base.subquery()
                select_count_query = select(func.count(base_subquery.c.job_group_id)).select_from(base_subquery)

                # Apply sorting
                select_query = select_query_base
                if sort_by:
                    valid_columns = {
                        "job_group_id": SlurmJobExecutionRecord.job_group_id,
                        "unique_job_count": unique_job_count,
                        "total_job_attempt_count": total_job_attempt_count,
                    }
                    for column_name, sort_order in sort_by:
                        column = valid_columns.get(column_name)
                        if column is None:
                            raise RecoveryStorageInputValidationError(
                                f"Invalid sort_by expression, column '{column_name}' is not a valid sort column "
                                f"for job groups. Valid columns: {list(valid_columns.keys())}."
                            )
                        if sort_order == SortOrderEnum.ASC.value:
                            if column_name == "job_group_id":
                                select_query = select_query.order_by(cast(column, Integer).asc())
                            else:
                                select_query = select_query.order_by(column.asc())
                        elif sort_order == SortOrderEnum.DESC.value:
                            if column_name == "job_group_id":
                                select_query = select_query.order_by(cast(column, Integer).desc())
                            else:
                                select_query = select_query.order_by(column.desc())
                        else:
                            raise RecoveryStorageInputValidationError(
                                f"Invalid sort_by expression, '{sort_order}' is not a valid sort order, "
                                "only 'asc' and 'desc' are supported."
                            )

                select_query = select_query.limit(limit).offset(offset)

                record_count = session.scalars(select_count_query).first() or 0
                records = session.execute(select_query).all()

                return [
                    {
                        "job_group_id": record.job_group_id,
                        "unique_job_count": record.unique_job_count,
                        "total_job_attempt_count": record.total_job_attempt_count,
                    }
                    for record in records
                ], record_count
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            raise RecoveryStorageException(
                f"Unable to query job groups with job_group_id={job_group_id}, user={user} due to error: {e}."
            ) from e

    def read_job_records(
        self,
        complete_data: bool = False,
        job_id: str | None = None,
        job_attempt_index: int | None = None,
        job_name: str | None = None,
        user: str | None = None,
        states: Sequence[str] = (),
        limit: int = 50,
        offset: int = 0,
        time_filter: JobTimeFilter | None = None,
        sort_by: list[tuple[str, str]] | None = None,
        *,
        join_job_events_table: bool = False,
        heimdall_job_id: uuid.UUID | None = None,
        job_name_match: str = "prefix",
        job_group_id: str | None = None,
        include_job_group_attempts: bool = True,
        workflow_id: str | None = None,
    ) -> tuple[list[SlurmJobExecutionRecord], int]:
        """
        Reads job execution records from the database based on provided filters, with support for pagination and sorting.

        Args:
            complete_data (bool): If True, retrieves complete job data including related events.
            job_id (Optional[str]): Filter by job ID.
            job_attempt_index (Optional[int]): Filter by job attempt index.
            job_name (Optional[str]): Filter by job name; see job_name_match for exact vs prefix.
            job_name_match (Optional[str]): "prefix" (default), "exact", or "fuzzy" (case-insensitive substring match) for job_name filter.
            workflow_id (Optional[str]): Filter by exact workflow ID, independently of job_name.
            user (Optional[str]): Filter by user.
            states (list[str]): Filter by job states.
            limit (int): Maximum number of records to return. Default is 50.
            offset (int): Number of records to skip before starting to return records. Default is 0.
            time_filter (Optional[JobTimeFilter]): Filter by job time attributes (e.g., created, updated).
            sort_by (Optional[list[tuple[str, str]]]): List of (column, order) tuples for sorting.
            join_job_events_table (bool): Whether to join with the job events table to retrieve related events.
            heimdall_job_id (Optional[uuid.UUID]): Filter by Heimdall job UUID. When provided, job_id and
                job_attempt_index filters are ignored.
            job_group_id (Optional[str]): Filter by job group ID. When provided, include_job_group_attempts is ignored.
            include_job_group_attempts (bool): When job_group_id is not provided, controls whether to include job
                attempts that belong to a job group. Default is True (include all). When False, only returns job
                attempts that are not part of any job group (job_group_id IS NULL).

        Returns:
            tuple[list[SlurmJobExecutionRecord], int]: A tuple containing the list of job records for the current page and the total count (ignoring pagination).

        Raises:
            RecoveryStorageException: If an error occurs during the database query.
            RecoveryStorageInputValidationError: If invalid input is provided.

        Example:
            >>> jobs, count = storage.read_job_records(user="alice", states=["COMPLETED"], limit=10, offset=0)
        """
        if job_attempt_index is not None and not job_id and not heimdall_job_id:
            raise RecoveryStorageException("job_id cannot be None or empty when job_attempt_index is provided.")

        try:
            with self.connection.session() as session:
                select_query = select(SlurmJobExecutionRecord)
                select_count_query = select(func.count(SlurmJobExecutionRecord.id))

                # If heimdall_job_id is provided, use it as the primary filter (ignores job_id/job_attempt_index)
                if heimdall_job_id:
                    select_query = select_query.where(SlurmJobExecutionRecord.heimdall_job_id == heimdall_job_id)
                    select_count_query = select_count_query.where(
                        SlurmJobExecutionRecord.heimdall_job_id == heimdall_job_id
                    )
                else:
                    if job_id:
                        select_query = select_query.where(SlurmJobExecutionRecord.job_id == job_id)
                        select_count_query = select_count_query.where(SlurmJobExecutionRecord.job_id == job_id)
                    if job_attempt_index is not None:
                        select_query = select_query.where(
                            SlurmJobExecutionRecord.job_attempt_index == job_attempt_index
                        )
                        select_count_query = select_count_query.where(
                            SlurmJobExecutionRecord.job_attempt_index == job_attempt_index
                        )
                if job_name:
                    if job_name_match == "prefix":
                        select_query = select_query.where(SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%"))
                        select_count_query = select_count_query.where(
                            SlurmJobExecutionRecord.job_name.ilike(f"{job_name}%")
                        )
                    elif job_name_match == "fuzzy":
                        select_query = select_query.where(SlurmJobExecutionRecord.job_name.ilike(f"%{job_name}%"))
                        select_count_query = select_count_query.where(
                            SlurmJobExecutionRecord.job_name.ilike(f"%{job_name}%")
                        )
                    else:
                        select_query = select_query.where(SlurmJobExecutionRecord.job_name == job_name)
                        select_count_query = select_count_query.where(SlurmJobExecutionRecord.job_name == job_name)
                if workflow_id is not None:
                    select_query = select_query.where(SlurmJobExecutionRecord.workflow_id == workflow_id)
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.workflow_id == workflow_id)
                if user:
                    select_query = select_query.where(SlurmJobExecutionRecord.user == user)
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.user == user)

                if job_group_id:
                    select_query = select_query.where(SlurmJobExecutionRecord.job_group_id == job_group_id)
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.job_group_id == job_group_id)
                elif not include_job_group_attempts:
                    # Use IS NULL which can leverage the index on job_group_id
                    select_query = select_query.where(SlurmJobExecutionRecord.job_group_id.is_(None))
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.job_group_id.is_(None))

                # Pre-existing code was querying with a single state, keep this for backward compatiblity
                # in case using IN clause changes index selection in mySQL
                if len(states) == 1:
                    select_query = select_query.where(SlurmJobExecutionRecord.state == states[0])
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.state == states[0])
                elif len(states) > 1:
                    select_query = select_query.where(SlurmJobExecutionRecord.state.in_(states))
                    select_count_query = select_count_query.where(SlurmJobExecutionRecord.state.in_(states))

                if time_filter:
                    job_time_attribute = SlurmJobExecutionRecord.get_time_attribute(time_filter.job_time_target)
                    if time_filter.from_time:
                        select_query = select_query.where(job_time_attribute >= time_filter.from_time)
                        select_count_query = select_count_query.where(job_time_attribute >= time_filter.from_time)
                    if time_filter.to_time:
                        select_query = select_query.where(job_time_attribute < time_filter.to_time)
                        select_count_query = select_count_query.where(job_time_attribute < time_filter.to_time)

                if sort_by:
                    select_query = apply_sorting_condition(SlurmJobExecutionRecord, select_query, sort_by)

                if join_job_events_table:
                    select_query_subquery = select_query.limit(limit).offset(offset).subquery()
                    logger.debug(f"Subquery to job_execution_records table: {select_query_subquery}")
                    job_subset_alias = aliased(SlurmJobExecutionRecord, select_query_subquery)
                    join_query = (
                        select(job_subset_alias)
                        .outerjoin(JobEvent, JobEvent.job_record_id == job_subset_alias.id)
                        .options(contains_eager(job_subset_alias.events))
                    )

                    # Apply the sorting condition again since the result after join is not guaranteed to have the same order
                    # as the subquery result.
                    if sort_by:
                        join_query = apply_sorting_condition(job_subset_alias, join_query, sort_by)

                    logger.debug(f"Query to join job_execution_records and job_events tables: {join_query}")
                    select_query = join_query
                else:
                    select_query = select_query.limit(limit).offset(offset)

                record_count = session.scalars(select_count_query).first() or 0
                records = session.execute(select_query).scalars().unique().all()

                return (list(records), record_count)
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            raise RecoveryStorageException(
                f"Unable to query the db with job_id={job_id}, "
                f"job_attempt_index={job_attempt_index}, job_name={job_name}, workflow_id={workflow_id}, "
                f"user={user}, states={states}, time_filter={time_filter}, "
                f"job_group_id={job_group_id}, include_job_group_attempts={include_job_group_attempts} due to error: {e}."
            ) from e

    def update_job_record(
        self,
        job_id: str,
        job_attempt_index: int | None,
        **kwargs: Any,
    ) -> UpdateJobResult:
        """Partially update a Job and return its committed transition.

        The target row remains locked from the pre-update snapshot through the
        write and commit. When the attempt index is omitted, the latest attempt
        is selected under that same lock so a concurrent writer cannot make the
        returned snapshots describe a different transition.
        """
        try:
            # Remove None and empty collections from the kwargs to avoid overriding existing values with empty values.
            update_values = {}
            for key, value in kwargs.items():
                if isinstance(value, Sized):
                    if len(value) != 0:
                        update_values[key] = value
                elif value is not None:
                    update_values[key] = value

            # Claim-owned: only claim_attribution_request_emission() sets it. A general update must
            # never reset the claimed flag, or a later terminal update would re-emit AttributionRequest.
            update_values.pop("attribution_request_emitted", None)
            with self.connection.session() as session:
                known_record_query = select(SlurmJobExecutionRecord).where(SlurmJobExecutionRecord.job_id == job_id)
                if job_attempt_index is None:
                    known_record_query = known_record_query.order_by(
                        desc(SlurmJobExecutionRecord.job_attempt_index)
                    ).limit(1)
                else:
                    known_record_query = known_record_query.where(
                        SlurmJobExecutionRecord.job_attempt_index == job_attempt_index
                    )
                known_record_query = known_record_query.with_for_update()
                known_record = session.scalars(known_record_query).first()

                if known_record is None:
                    session.commit()
                    logger.debug(f"No job record matched update for job {job_id}, attempt index {job_attempt_index}.")
                    return UpdateJobResult(updated_record=None, old_record=None)

                # Resolve the latest attempt and capture its exact pre-update state
                # while holding the same row lock used by the write below.
                job_attempt_index = known_record.job_attempt_index
                old_record = deepcopy(known_record)
                make_transient(old_record)

                if (
                    "allocated_node_list" in update_values
                    and known_record.allocated_node_list
                    and len(update_values["allocated_node_list"]) < len(known_record.allocated_node_list)
                ):
                    logger.info(
                        f"Updated allocated node list ({len(update_values['allocated_node_list'])}) "
                        f"is shorter than the previous known list ({len(known_record.allocated_node_list)}), "
                        "skip updating allocated_node_list field."
                    )
                    update_values.pop("allocated_node_list")

                if (
                    "node_count" in update_values
                    and known_record.node_count is not None
                    and int(str(update_values["node_count"])) < int(known_record.node_count)
                ):
                    logger.info(
                        f"Updated node count ({update_values['node_count']}) is less than the previous known count "
                        f"({known_record.node_count}), skip updating node_count field."
                    )
                    update_values.pop("node_count")

                session.execute(
                    update(SlurmJobExecutionRecord)
                    .where(SlurmJobExecutionRecord.job_id == job_id)
                    .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
                    .values(update_values)
                )
                _commit_validated_record(session, known_record)
                logger.debug(f"Succesfully updated the job record for job {job_id}, attempt index {job_attempt_index}.")
                return UpdateJobResult(
                    updated_record=known_record,
                    old_record=old_record,
                )
        except Exception as e:
            logger.exception("Failed to update job record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to update job record: {e}") from e

    def claim_attribution_request_emission(self, job_id: str, job_attempt_index: int) -> bool:
        """Atomically claim the right to emit a AttributionRequest event for this job attempt.

        Performs a single conditional UPDATE that flips attribution_request_emitted from
        false to true and returns whether this caller won the claim. Because the
        predicate is evaluated under the row lock, exactly one concurrent caller
        can win, so it is safe against the lost-update race that a read-modify-write
        via update_job_record would be subject to. This lets the AttributionRequest event
        be emitted at most once per attempt across the terminal-transition and
        anomaly-record paths.

        Returns:
            True if this call flipped the flag (caller should emit); False if it was
            already claimed by someone else (caller must not emit).
        """
        try:
            with self.connection.session() as session:
                result = session.connection().execute(
                    update(SlurmJobExecutionRecord)
                    .where(SlurmJobExecutionRecord.job_id == job_id)
                    .where(SlurmJobExecutionRecord.job_attempt_index == job_attempt_index)
                    .where(SlurmJobExecutionRecord.attribution_request_emitted.is_(False))
                    .values(attribution_request_emitted=True)
                )
                session.commit()
                return result.rowcount == 1
        except Exception as e:
            logger.exception(
                f"Failed to claim attribution_request emission for job {job_id} attempt {job_attempt_index}",
                extra={"job_id": job_id, "job_attempt_index": job_attempt_index, "error": str(e)},
            )
            raise RecoveryStorageException(f"Failed to claim attribution_request emission: {e}") from e

    def read_users(
        self,
        job_states: list[str] | None = None,
        limit: int = 25,
        offset: int = 0,
        descending: bool = False,
    ) -> list[str]:
        """
        Method to read a list of usernames from the storage.

        Parameters:
            job_states (Optional[list[str]]): When specified, only users with jobs in these states will be returned.
            limit (int): Number of users to return in one query.
            offset (int): Offset to start the query from.
            descending (bool): Sort the users in descending order.

        Returns:
            List of users.

        Raises:
            RecoveryStorageException when the operation fails.
        """
        try:
            with self.connection.session() as session:
                select_query = select(SlurmJobExecutionRecord.user)
                if job_states:
                    select_query = select_query.where(SlurmJobExecutionRecord.state.in_(job_states))

                if descending:
                    select_query = select_query.order_by(desc(SlurmJobExecutionRecord.user))
                else:
                    select_query = select_query.order_by(SlurmJobExecutionRecord.user)

                select_query = select_query.distinct().limit(limit).offset(offset)
                users = [user for user in session.scalars(select_query).all() if user is not None]
                return users
        except Exception as e:
            logger.exception("Failed to find users from the storage", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to get users from the storage: {e}") from e

    def create_job_event(self, job_event: JobEvent) -> bool:
        """
        Method to create a job event in the storage.

        Parameters:
            job_event: An instance of JobEvent.

        Returns:
            True if the creation succeeded.

        Raises:
            RecoveryStorageException when a new job event can't be created.
        """
        try:
            with self.connection.session() as session:
                session.add(job_event)
                session.commit()
                logger.debug(
                    f"Successfully created a job event for job {job_event.job_id}, attempt index {job_event.job_attempt_index}."
                )
                return True
        except Exception as e:
            logger.exception(f"Failed to create job event, job event details: {job_event}", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to create job event: {e}") from e

    # ============================================================================
    # Recovery Sequence and Milestone Operations
    # ============================================================================

    def begin_or_get_recovery_sequence(
        self,
        candidate: RecoverySequenceRecord,
    ) -> tuple[RecoverySequenceRecord, bool]:
        """Create a sequence or return the immutable concurrent winner.

        The database natural-key constraint is the concurrency authority. If
        another writer commits the same logical sequence first, this method
        rolls back the losing insert and returns the complete winning row
        without merging any fields from the loser.

        Args:
            candidate: The proposed recovery sequence record to persist.

        Returns:
            A tuple containing, in order:

            * The canonical stored sequence record. When another writer has
              already won the natural-key race, this is that writer's immutable
              record.
            * A boolean that is ``True`` only when this call created the record,
              and ``False`` when a record already existed.
        """

        natural_key = (
            candidate.scheduler_system,
            candidate.scheduler_scope,
            candidate.job_id,
            candidate.source_attempt_index,
            candidate.operation,
        )
        try:
            with self.connection.session() as session:
                existing = self._select_recovery_sequence_by_natural_key(session, natural_key)
                if existing is not None:
                    session.expunge(existing)
                    return existing, False

                session.add(candidate)
                try:
                    session.commit()
                except IntegrityError:
                    # A concurrent creator won the unique-key race. Roll back
                    # this transaction before re-reading its canonical record.
                    session.rollback()
                    existing = self._select_recovery_sequence_by_natural_key(
                        session,
                        natural_key,
                        for_update=True,
                    )
                    if existing is None:
                        raise
                    session.expunge(existing)
                    return existing, False

                session.refresh(candidate)
                session.expunge(candidate)
                return candidate, True
        except RecoveryStorageInputValidationError:
            raise
        except Exception as error:
            logger.error(f"Failed to begin or get recovery sequence: {error}")
            raise RecoveryStorageException(f"Failed to begin or get recovery sequence: {error}") from error

    @staticmethod
    def _select_recovery_sequence_by_natural_key(
        session,
        natural_key,
        *,
        for_update: bool = False,
    ) -> RecoverySequenceRecord | None:
        scheduler_system, scheduler_scope, job_id, source_attempt_index, operation = natural_key
        statement = select(RecoverySequenceRecord).where(
            RecoverySequenceRecord.scheduler_system == scheduler_system,
            RecoverySequenceRecord.scheduler_scope == scheduler_scope,
            RecoverySequenceRecord.job_id == job_id,
            RecoverySequenceRecord.source_attempt_index == source_attempt_index,
            RecoverySequenceRecord.operation == operation,
        )
        if for_update:
            statement = statement.with_for_update()
        return session.scalars(statement).first()

    def read_recovery_sequence(self, sequence_id: uuid.UUID) -> RecoverySequenceRecord | None:
        try:
            with self.connection.session() as session:
                record = session.get(RecoverySequenceRecord, sequence_id)
                if record is not None:
                    session.expunge(record)
                return record
        except Exception as error:
            logger.error(f"Failed to read recovery sequence {sequence_id}: {error}")
            raise RecoveryStorageException(f"Failed to read recovery sequence: {error}") from error

    def read_recovery_sequence_by_natural_key(
        self,
        *,
        scheduler_system: str,
        scheduler_scope: str,
        job_id: str,
        source_attempt_index: int,
        operation: str,
    ) -> RecoverySequenceRecord | None:
        natural_key = (scheduler_system, scheduler_scope, job_id, source_attempt_index, operation)
        try:
            with self.connection.session() as session:
                record = self._select_recovery_sequence_by_natural_key(session, natural_key)
                if record is not None:
                    session.expunge(record)
                return record
        except Exception as error:
            logger.error(f"Failed to read recovery sequence by natural key: {error}")
            raise RecoveryStorageException(f"Failed to read recovery sequence by natural key: {error}") from error

    def record_recovery_milestone(
        self,
        candidate: RecoveryMilestoneRecord,
    ) -> tuple[RecoveryMilestoneRecord, bool, bool]:
        """Persist one idempotent milestone and honor explicit completion intent.

        The scheduler adapter, not storage, decides whether an occurrence is a
        terminal milestone. Locking the owning sequence ensures only the first
        accepted ``completes_sequence`` report establishes its completion
        timestamp; later terminal reports remain immutable audit records.

        Args:
            candidate: The proposed recovery milestone record to persist.

        Returns:
            A tuple containing, in order:

            * The canonical stored milestone record.
            * A boolean that is ``True`` only when this call inserted the
              milestone, and ``False`` for an idempotent duplicate.
            * A boolean that is ``True`` only when this call newly completed
              the owning recovery sequence.
        """

        try:
            with self.connection.session() as session:
                # Serialize milestone acceptance for one sequence so concurrent
                # successful release reports cannot move the first completion.
                sequence = session.scalars(
                    select(RecoverySequenceRecord)
                    .where(RecoverySequenceRecord.sequence_id == candidate.sequence_id)
                    .with_for_update()
                ).first()
                if sequence is None:
                    raise RecoveryStorageInputValidationError(
                        f"Recovery sequence {candidate.sequence_id} does not exist."
                    )
                if candidate.operation != sequence.operation:
                    raise RecoveryStorageInputValidationError(
                        "Recovery milestone operation does not match its sequence."
                    )

                existing = self._select_recovery_milestone(session, candidate)
                if existing is not None:
                    self._validate_duplicate_recovery_milestone(existing, candidate)
                    session.expunge(existing)
                    return existing, False, False

                session.add(candidate)
                try:
                    session.flush()
                except IntegrityError:
                    session.rollback()
                    existing = self._select_recovery_milestone(session, candidate)
                    if existing is None:
                        raise
                    self._validate_duplicate_recovery_milestone(existing, candidate)
                    session.expunge(existing)
                    return existing, False, False

                completed_sequence = False
                if candidate.completes_sequence and sequence.completed_at is None:
                    sequence.status = "completed"
                    sequence.completed_at = candidate.completed_at
                    sequence.updated_at = candidate.created_at
                    completed_sequence = True

                session.commit()
                session.refresh(candidate)
                session.expunge(candidate)
                return candidate, True, completed_sequence
        except RecoveryStorageInputValidationError:
            raise
        except Exception as error:
            logger.error(f"Failed to record recovery milestone: {error}")
            raise RecoveryStorageException(f"Failed to record recovery milestone: {error}") from error

    @staticmethod
    def _select_recovery_milestone(session, candidate) -> RecoveryMilestoneRecord | None:
        return session.scalars(
            select(RecoveryMilestoneRecord).where(
                RecoveryMilestoneRecord.sequence_id == candidate.sequence_id,
                RecoveryMilestoneRecord.idempotency_key == candidate.idempotency_key,
            )
        ).first()

    @staticmethod
    def _validate_duplicate_recovery_milestone(
        existing: RecoveryMilestoneRecord,
        candidate: RecoveryMilestoneRecord,
    ) -> None:
        fields_match = (
            existing.operation == candidate.operation
            and existing.action == candidate.action
            and existing.outcome == candidate.outcome
            and existing.completes_sequence == candidate.completes_sequence
            and existing.completed_at == candidate.completed_at
        )
        if not fields_match:
            raise RecoveryStorageInputValidationError(
                "Recovery milestone idempotency key is already associated with a different action occurrence."
            )

    # ============================================================================
    # Application Intention CRUD Operations
    # ============================================================================

    def create_or_get_application_intention(
        self,
        intention: ApplicationIntentionRecord,
    ) -> tuple[ApplicationIntentionRecord, bool]:
        """Persist the first application intention for a scheduler job.

        The first committed row is the durable, absorbing decision for the job.
        Concurrent duplicate requests return that row rather than changing its
        original reason or timestamp.

        Returns:
            The stored intention and whether this call created it.
        """
        try:
            return self._create_or_get_application_intention_once(intention)
        except Exception as error:
            logger.exception(
                "Failed to persist application intention",
                extra={"job_id": intention.job_id, "error": str(error)},
            )
            raise RecoveryStorageException(f"Failed to persist application intention: {error}") from error

    @retry_on_conflict(describe=lambda intention: f"application intention for job {intention.job_id}")
    def _create_or_get_application_intention_once(
        self,
        intention: ApplicationIntentionRecord,
    ) -> tuple[ApplicationIntentionRecord, bool]:
        """Run one conflict-retryable create-or-get transaction."""
        with self.connection.write_session() as session:
            if not self.connection.write_pool_enabled:
                session.connection(execution_options={"isolation_level": "READ COMMITTED"})
            try:
                existing = session.scalars(
                    select(ApplicationIntentionRecord)
                    .where(ApplicationIntentionRecord.job_id == intention.job_id)
                    .with_for_update()
                ).first()
                if existing is not None:
                    session.expunge(existing)
                    return existing, False

                session.add(intention)
                session.commit()
                session.refresh(intention)
                session.expunge(intention)
                return intention, True
            except Exception:
                session.rollback()
                raise

    def read_application_intention(self, job_id: str) -> ApplicationIntentionRecord | None:
        """Return the durable application intention for a scheduler job, if any."""
        try:
            with self.connection.session() as session:
                return session.scalars(
                    select(ApplicationIntentionRecord).where(ApplicationIntentionRecord.job_id == job_id)
                ).first()
        except Exception as error:
            logger.exception(
                "Failed to read application intention",
                extra={"job_id": job_id, "error": str(error)},
            )
            raise RecoveryStorageException(f"Failed to read application intention: {error}") from error

    def claim_application_intention_audit_attempt(
        self,
        job_id: str,
        attempt_index: int,
    ) -> tuple[ApplicationIntentionRecord | None, bool]:
        """Associate a request audit event with the first latest known attempt.

        The row lock makes concurrent background reconciliation and scheduler
        updates agree on one attempt. Returning ``claimed=False`` suppresses
        duplicate request events on later polls and repeated API calls.
        """
        try:
            with self.connection.write_session() as session:
                if not self.connection.write_pool_enabled:
                    session.connection(execution_options={"isolation_level": "READ COMMITTED"})
                try:
                    intention = session.scalars(
                        select(ApplicationIntentionRecord)
                        .where(ApplicationIntentionRecord.job_id == job_id)
                        .with_for_update()
                    ).first()
                    if intention is None or intention.audit_attempt_index is not None:
                        if intention is not None:
                            session.expunge(intention)
                        return intention, False

                    intention.audit_attempt_index = attempt_index
                    session.commit()
                    session.refresh(intention)
                    session.expunge(intention)
                    return intention, True
                except Exception:
                    session.rollback()
                    raise
        except Exception as error:
            logger.exception(
                "Failed to claim application intention audit attempt",
                extra={"job_id": job_id, "attempt_index": attempt_index, "error": str(error)},
            )
            raise RecoveryStorageException(f"Failed to claim application intention audit attempt: {error}") from error

    # ============================================================================
    # Advice CRUD Operations
    # ============================================================================

    def create_advice(self, advice: AdviceRecord) -> AdviceRecord:
        """
        Create a new advice record in the database.

        Args:
            advice: The AdviceRecord to create.

        Returns:
            The created AdviceRecord.

        Raises:
            RecoveryStorageException: If the creation fails.
        """
        try:
            with self.connection.session() as session:
                session.add(advice)
                session.commit()
                logger.debug(f"Successfully created advice record with heimdall_id: {advice.heimdall_id}")
                return advice
        except Exception as e:
            logger.exception("Failed to create advice record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to create advice record: {e}") from e

    def read_advice(self, heimdall_id: uuid.UUID) -> AdviceRecord | None:
        """
        Read a single advice record by its heimdall_id.

        Args:
            heimdall_id: The UUID of the advice record.

        Returns:
            The AdviceRecord if found, None otherwise.

        Raises:
            RecoveryStorageException: If the query fails.
        """
        try:
            with self.connection.session() as session:
                select_query = (
                    select(AdviceRecord)
                    .options(selectinload(AdviceRecord.reconciliation_statuses))
                    .where(AdviceRecord.heimdall_id == heimdall_id)
                )
                return session.scalars(select_query).first()
        except Exception as e:
            logger.exception(
                f"Failed to read advice record with heimdall_id {heimdall_id}",
                extra={"heimdall_id": str(heimdall_id), "error": str(e)},
            )
            raise RecoveryStorageException(f"Failed to read advice record: {e}") from e

    def read_advice_list(
        self,
        job_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        include_hidden: bool = False,
        created_after: datetime | None = None,
        only_active: bool = False,
    ) -> tuple[list[AdviceRecord], int]:
        """
        Read a list of advice records with optional filtering and pagination.

        Args:
            job_id: Optional filter by job_id.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            include_hidden: If False (default), only returns visible advice records.
                           If True, returns all records regardless of visibility.
            created_after: Optional lower creation-time bound. Durable Advice
                states remain eligible before this bound.
            only_active: Return only Advice that can still require reconciliation.

        Returns:
            A tuple of (list of AdviceRecord, total count).

        Raises:
            RecoveryStorageException: If the query fails.
        """
        try:
            with self.connection.session() as session:
                select_query = select(AdviceRecord).options(selectinload(AdviceRecord.reconciliation_statuses))
                select_count_query = select(func.count(AdviceRecord.heimdall_id))

                # By default, only show visible advice
                if not include_hidden:
                    select_query = select_query.where(AdviceRecord.visible == True)
                    select_count_query = select_count_query.where(AdviceRecord.visible == True)

                if job_id:
                    select_query = select_query.where(AdviceRecord.job_id == job_id)
                    select_count_query = select_count_query.where(AdviceRecord.job_id == job_id)

                if created_after is not None:
                    freshness_filter = or_(
                        AdviceRecord.created_at >= created_after,
                        AdviceRecord.advice_state.in_([AdviceState.TERMINATED.value, AdviceState.RELEASED.value]),
                    )
                    select_query = select_query.where(freshness_filter)
                    select_count_query = select_count_query.where(freshness_filter)

                if only_active:
                    valid_recommendation = or_(
                        AdviceRecord.advice_state.in_([state.value for state in AdviceState]),
                        and_(
                            func.json_type(AdviceRecord.node_exclusions) == "ARRAY",
                            func.json_length(AdviceRecord.node_exclusions) > 0,
                        ),
                        and_(
                            func.json_type(AdviceRecord.node_inclusions) == "ARRAY",
                            func.json_length(AdviceRecord.node_inclusions) > 0,
                        ),
                    )
                    terminal_status_exists = (
                        select(AdviceReconciliationStatusRecord.id)
                        .where(
                            AdviceReconciliationStatusRecord.advice_id == AdviceRecord.heimdall_id,
                            AdviceReconciliationStatusRecord.observed_generation == AdviceRecord.generation,
                            AdviceReconciliationStatusRecord.disposition.in_(
                                [
                                    AdviceReconciliationDisposition.CONVERGED.value,
                                    AdviceReconciliationDisposition.ABANDONED.value,
                                ]
                            ),
                        )
                        .correlate(AdviceRecord)
                        .exists()
                    )
                    active_filter = and_(
                        AdviceRecord.visible == True,
                        valid_recommendation,
                        ~terminal_status_exists,
                    )
                    select_query = select_query.where(active_filter)
                    select_count_query = select_count_query.where(active_filter)

                # Order by created_at descending (newest first)
                select_query = select_query.order_by(desc(AdviceRecord.created_at))
                select_query = select_query.limit(limit).offset(offset)

                records = list(session.scalars(select_query).all())
                record_count = session.scalars(select_count_query).first() or 0

                logger.debug(f"Found {len(records)} advice records (total: {record_count})")
                return (records, record_count)
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            logger.exception("Failed to read advice records", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to read advice records: {e}") from e

    def read_advice_by_job(self, job_id: str, attempt_index: int) -> AdviceRecord | None:
        """
        Read a single advice record by its natural key (job_id, attempt_index).

        Args:
            job_id: The job identifier.
            attempt_index: The job attempt index.

        Returns:
            The AdviceRecord if found, None otherwise.

        Raises:
            RecoveryStorageException: If the query fails.
        """
        try:
            with self.connection.session() as session:
                return session.scalars(
                    select(AdviceRecord)
                    .options(selectinload(AdviceRecord.reconciliation_statuses))
                    .where(
                        AdviceRecord.job_id == job_id,
                        AdviceRecord.attempt_index == attempt_index,
                    )
                ).first()
        except Exception as e:
            logger.exception(
                f"Failed to read advice for job {job_id} attempt {attempt_index}",
                extra={"job_id": job_id, "attempt_index": attempt_index, "error": str(e)},
            )
            raise RecoveryStorageException(f"Failed to read advice by job: {e}") from e

    def upsert_advice(
        self,
        advice: AdviceRecord,
        *,
        issue_merge: AdviceIssueMerge = AdviceIssueMerge.REPLACE,
    ) -> UpsertAdviceResult:
        """
        Create or update an advice record.

        Matches on (job_id, attempt_index) to find existing records. This ensures
        that advice for the same job attempt is always merged, even if the caller
        provides a different heimdall_id.

        Args:
            advice: The AdviceRecord to upsert.
            issue_merge: How incoming Issues combine with the stored Issues. The
                stored row is locked while they are combined. See AdviceIssueMerge.

        Returns:
            The stored record, whether it was created, and the transient
            pre-update snapshot when an existing record was updated.

        Raises:
            RecoveryStorageInputValidationError: If the advice is invalid or would
                replace Terminated advice.
            RecoveryStorageException: If the upsert fails, including when conflict
                (deadlock or duplicate-key) retries are exhausted.
        """
        if advice.visible and not self._advice_has_reconciliation_path(advice):
            raise RecoveryStorageInputValidationError(
                f"Visible advice for job {advice.job_id} attempt {advice.attempt_index} "
                "must contain a desired state or non-empty node constraints."
            )

        try:
            return self._upsert_advice_once(advice, issue_merge)
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            logger.exception("Failed to upsert advice record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to upsert advice record: {e}") from e

    @retry_on_conflict(
        describe=lambda advice, issue_merge: f"advice for job {advice.job_id}, attempt index {advice.attempt_index}"
    )
    def _upsert_advice_once(self, advice: AdviceRecord, issue_merge: AdviceIssueMerge) -> UpsertAdviceResult:
        """Run one attempt of upsert_advice(). A conflict retry runs it again from the start.

        Job Manager replicas, its job-update consumers, and its leader repair loop can
        all write Advice for the same job attempt at once. This method uses the same
        READ COMMITTED write path as _upsert_job_record_once and for the same reason.
        Under REPEATABLE READ, a SELECT ... FOR UPDATE that finds no row takes a gap
        lock, so two first writers deadlock (1213) when both INSERT. Under READ
        COMMITTED, the losing INSERT gets a duplicate-key error (1062) instead.
        retry_on_conflict handles either error. The retry reads the row under lock again
        and then takes the update path.

        Raw SQLAlchemy errors must reach the decorator, so this method does not wrap
        them. It rolls back explicitly before the session closes. Rollback returns an
        ``advice`` object that was added in the failed transaction to the transient
        state, so the retry can INSERT it again. Each attempt sets every field this
        method writes on ``advice``, and merged Issues are written only to the locked
        row. As a result, a failed attempt leaves nothing behind for the retry.
        """
        with self.connection.write_session() as session:
            if not self.connection.write_pool_enabled:
                session.connection(execution_options={"isolation_level": "READ COMMITTED"})
            try:
                existing = session.scalars(
                    select(AdviceRecord)
                    .where(
                        AdviceRecord.job_id == advice.job_id,
                        AdviceRecord.attempt_index == advice.attempt_index,
                    )
                    .with_for_update()
                ).first()

                now = datetime.now(UTC)

                if existing is not None and issue_merge == AdviceIssueMerge.APPEND_ONLY:
                    # Evidence-only writers must not overwrite a decision that another writer
                    # committed after this writer read the advice, so only Issues change.
                    old_record = deepcopy(existing)
                    make_transient(old_record)
                    merged_issues = _union_issues(existing.issues, advice.issues)
                    if merged_issues is not existing.issues:
                        existing.issues = merged_issues
                        existing.updated_at = now
                    return self._commit_and_detach_advice(session, existing, was_created=False, old_record=old_record)

                if (
                    existing
                    and existing.advice_state == AdviceState.TERMINATED.value
                    and advice.advice_state != AdviceState.TERMINATED.value
                ):
                    raise RecoveryStorageInputValidationError(
                        f"Cannot replace Terminated advice for job {advice.job_id} "
                        f"attempt {advice.attempt_index} with {advice.advice_state}."
                    )

                self._attach_recovery_context_to_advice(session, advice)

                if existing is not None:
                    old_record = deepcopy(existing)
                    make_transient(old_record)
                    desired_state_changed = self._advice_desired_state_changed(existing, advice)
                    # Canonicalize heimdall_id to the job record's UUID
                    if existing.heimdall_id != advice.heimdall_id:
                        existing.heimdall_id = advice.heimdall_id
                    # Update existing record
                    existing.visible = advice.visible
                    existing.job_id = advice.job_id
                    existing.scheduler_metadata = advice.scheduler_metadata
                    existing.attempt_index = advice.attempt_index
                    existing.advice_state = advice.advice_state
                    existing.node_exclusions = advice.node_exclusions
                    existing.node_inclusions = advice.node_inclusions
                    if issue_merge == AdviceIssueMerge.REPLACE:
                        existing.issues = advice.issues
                    else:
                        existing.issues = _union_issues(existing.issues, advice.issues)
                    existing.fact_attribution = advice.fact_attribution
                    existing.recovery_sequence_id = advice.recovery_sequence_id
                    existing.recovery_traceparent = advice.recovery_traceparent
                    existing.recovery_tracestate = advice.recovery_tracestate
                    existing.recovery_triggered_at = advice.recovery_triggered_at
                    existing.recovery_operation = advice.recovery_operation
                    if desired_state_changed:
                        existing.generation += 1
                        existing.desired_state_changed_at = now
                    existing.updated_at = now
                    return self._commit_and_detach_advice(session, existing, was_created=False, old_record=old_record)

                # Create new record
                advice.generation = 1
                advice.desired_state_changed_at = now
                advice.created_at = now
                advice.updated_at = now
                session.add(advice)
                return self._commit_and_detach_advice(session, advice, was_created=True, old_record=None)
            except Exception:
                session.rollback()
                raise

    @staticmethod
    def _commit_and_detach_advice(
        session, record: AdviceRecord, *, was_created: bool, old_record: AdviceRecord | None
    ) -> UpsertAdviceResult:
        """Commit the upsert and return ``record`` detached from ``session``.

        The record is loaded and detached before the session closes, so callers can
        read its fields without a lazy refresh on a detached object.
        """
        session.commit()
        session.refresh(record)
        if not was_created:
            session.refresh(record, attribute_names=["reconciliation_statuses"])
        session.expunge(record)
        logger.debug(
            f"Successfully {'created' if was_created else 'updated'} advice record "
            f"with heimdall_id: {record.heimdall_id}"
        )
        return UpsertAdviceResult(inserted_record=record, was_created=was_created, old_record=old_record)

    @staticmethod
    def _advice_has_reconciliation_path(advice: AdviceRecord) -> bool:
        valid_states = {state.value for state in AdviceState}
        return (
            advice.advice_state in valid_states
            or any(advice.node_exclusions or [])
            or any(advice.node_inclusions or [])
        )

    @staticmethod
    def _advice_desired_state_changed(existing: AdviceRecord, incoming: AdviceRecord) -> bool:
        def normalized_nodes(values: list[str] | None) -> tuple[str, ...]:
            return tuple(sorted({value for value in values or [] if value}))

        def routing_scope(metadata: dict | None) -> tuple[object, object, object]:
            fields = metadata or {}
            return (fields.get("scheduler"), fields.get("managed_by"), fields.get("environment"))

        return (
            existing.advice_state != incoming.advice_state
            or normalized_nodes(existing.node_exclusions) != normalized_nodes(incoming.node_exclusions)
            or normalized_nodes(existing.node_inclusions) != normalized_nodes(incoming.node_inclusions)
            or routing_scope(existing.scheduler_metadata) != routing_scope(incoming.scheduler_metadata)
        )

    def upsert_advice_reconciliation_status(
        self,
        status_record: AdviceReconciliationStatusRecord,
    ) -> AdviceReconciliationStatusRecord:
        """Idempotently record one generation-scoped reconciliation observation."""
        terminal_dispositions = {
            AdviceReconciliationDisposition.CONVERGED.value,
            AdviceReconciliationDisposition.ABANDONED.value,
        }
        allowed_dispositions = {item.value for item in AdviceReconciliationDisposition}
        allowed_reasons = {item.value for item in AdviceReconciliationReason}
        if status_record.observed_generation < 1:
            raise RecoveryStorageInputValidationError("observed_generation must be positive")
        if status_record.disposition not in allowed_dispositions:
            raise RecoveryStorageInputValidationError(
                f"unsupported reconciliation disposition {status_record.disposition}"
            )
        if status_record.reason not in allowed_reasons:
            raise RecoveryStorageInputValidationError(f"unsupported reconciliation reason {status_record.reason}")
        if status_record.diagnostic_message is not None and len(status_record.diagnostic_message) > 1024:
            raise RecoveryStorageInputValidationError("diagnostic_message cannot exceed 1024 characters")

        try:
            with self.connection.session() as session:
                advice = session.scalars(
                    select(AdviceRecord).where(AdviceRecord.heimdall_id == status_record.advice_id).with_for_update()
                ).first()
                if advice is None:
                    raise RecoveryStorageInputValidationError(f"Advice {status_record.advice_id} does not exist.")
                if status_record.observed_generation > advice.generation:
                    raise RecoveryStorageInputValidationError(
                        f"Observed Advice generation {status_record.observed_generation} exceeds current generation "
                        f"{advice.generation}."
                    )

                existing = session.scalars(
                    select(AdviceReconciliationStatusRecord)
                    .where(
                        AdviceReconciliationStatusRecord.advice_id == status_record.advice_id,
                        AdviceReconciliationStatusRecord.observed_generation == status_record.observed_generation,
                    )
                    .with_for_update()
                ).first()

                if existing is not None:
                    if existing.disposition in terminal_dispositions and (
                        status_record.disposition != existing.disposition or status_record.reason != existing.reason
                    ):
                        raise RecoveryStorageInputValidationError(
                            "A terminal reconciliation status cannot be replaced for the same Advice generation."
                        )
                    if (
                        existing.disposition in terminal_dispositions
                        and status_record.disposition not in terminal_dispositions
                    ):
                        raise RecoveryStorageInputValidationError(
                            "A terminal reconciliation status cannot return to pending."
                        )

                    if existing.disposition != status_record.disposition or existing.reason != status_record.reason:
                        existing.transitioned_at = status_record.transitioned_at
                    existing.disposition = status_record.disposition
                    existing.reason = status_record.reason
                    existing.observed_at = status_record.observed_at
                    existing.diagnostic_message = status_record.diagnostic_message
                    persisted = existing
                else:
                    session.add(status_record)
                    persisted = status_record

                session.commit()
                session.refresh(persisted)
                session.expunge(persisted)
                return persisted
        except RecoveryStorageInputValidationError:
            raise
        except Exception as e:
            logger.exception("Failed to upsert advice reconciliation status", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to upsert advice reconciliation status: {e}") from e

    def _attach_recovery_context_to_advice(self, session, advice: AdviceRecord) -> None:
        operation_by_state = {
            "Restarted": "restart",
            "Terminated": "terminate",
            "Released": "dag_release",
        }
        operation = operation_by_state.get(advice.advice_state or "")
        if operation is None:
            self._clear_advice_recovery_context(advice)
            return

        metadata = advice.scheduler_metadata or {}
        scheduler_system = metadata.get("scheduler")
        if not isinstance(scheduler_system, str) or not scheduler_system:
            self._clear_advice_recovery_context(advice)
            return
        scheduler_scope = metadata.get("environment", "unspecified")
        if not isinstance(scheduler_scope, str) or not scheduler_scope:
            scheduler_scope = "unspecified"

        sequence = self._select_recovery_sequence_by_natural_key(
            session,
            (
                scheduler_system,
                scheduler_scope,
                advice.job_id,
                advice.attempt_index,
                operation,
            ),
        )
        if sequence is None:
            self._clear_advice_recovery_context(advice)
            return

        advice.recovery_sequence_id = sequence.sequence_id
        advice.recovery_traceparent = sequence.traceparent
        advice.recovery_tracestate = sequence.tracestate
        advice.recovery_triggered_at = sequence.triggered_at
        advice.recovery_operation = sequence.operation

    @staticmethod
    def _clear_advice_recovery_context(advice: AdviceRecord) -> None:
        advice.recovery_sequence_id = None
        advice.recovery_traceparent = None
        advice.recovery_tracestate = None
        advice.recovery_triggered_at = None
        advice.recovery_operation = None

    def delete_advice(self, heimdall_id: uuid.UUID) -> SoftDeleteAdviceResult:
        """
        Soft delete an advice record by setting visible=False.

        Args:
            heimdall_id: The UUID of the advice record to delete.

        Returns:
            The matched Advice record and whether this call changed it from
            visible to hidden. A missing record is represented by ``None``.

        Raises:
            RecoveryStorageException: If the deletion fails.
        """
        try:
            with self.connection.session() as session:
                advice_record = session.scalars(
                    select(AdviceRecord).where(AdviceRecord.heimdall_id == heimdall_id).with_for_update()
                ).first()
                if advice_record is None:
                    logger.debug(f"No advice record found with heimdall_id: {heimdall_id}")
                    return SoftDeleteAdviceResult(advice_record=None, was_deleted=False)

                if not advice_record.visible:
                    session.expunge(advice_record)
                    logger.debug(f"Advice record with heimdall_id {heimdall_id} is already hidden")
                    return SoftDeleteAdviceResult(advice_record=advice_record, was_deleted=False)

                advice_record.visible = False
                advice_record.updated_at = datetime.now(UTC)
                _commit_validated_record(session, advice_record)
                logger.debug(f"Successfully soft deleted advice record with heimdall_id: {heimdall_id}")
                return SoftDeleteAdviceResult(advice_record=advice_record, was_deleted=True)
        except Exception as e:
            logger.exception(
                f"Failed to soft delete advice record with heimdall_id {heimdall_id}",
                extra={"heimdall_id": str(heimdall_id), "error": str(e)},
            )
            raise RecoveryStorageException(f"Failed to soft delete advice record: {e}") from e

    # ============================================================================
    # DAG Dependency CRUD Operations
    # ============================================================================

    def upsert_dag_dependency(self, record: DagDependency) -> tuple[DagDependency, bool]:
        """
        Create or update a DAG dependency record. Matches on the unique constraint
        (dependent_job_id, dependency_job_identifier, dependency_job_identifier_type).

        If a matching record exists, only action_status and action_time can be
        updated from the incoming record. An initial ``NOT_ELIGIBLE``
        registration never regresses a dependency action that has already
        progressed. Other fields are not modified.

        Args:
            record: The DagDependency record to upsert.

        Returns:
            A tuple of (DagDependency, updated) where updated is True if the record was created
            or modified, False if the existing record was unchanged.

        Raises:
            RecoveryStorageException: If the operation fails.
        """
        try:
            with self.connection.session() as session:
                existing = session.scalars(
                    select(DagDependency).where(
                        DagDependency.dependent_job_id == record.dependent_job_id,
                        DagDependency.dependency_job_identifier == record.dependency_job_identifier,
                        DagDependency.dependency_job_identifier_type == record.dependency_job_identifier_type,
                    )
                ).first()

                if existing:
                    # DAG registration always supplies the initial NOT_ELIGIBLE
                    # state. Do not let a repeated scheduler observation regress
                    # an action that has already started, succeeded, failed, or
                    # was aborted because an upstream parent terminated.
                    if (
                        record.action_status == DagDependencyActionStatus.NOT_ELIGIBLE.value
                        and existing.action_status != DagDependencyActionStatus.NOT_ELIGIBLE.value
                    ):
                        session.expunge(existing)
                        return (existing, False)
                    changed = False
                    if record.action_status != existing.action_status:
                        existing.action_status = record.action_status
                        changed = True
                    if record.action_time != existing.action_time:
                        existing.action_time = record.action_time
                        changed = True

                    if changed:
                        session.commit()
                        session.refresh(existing)

                    session.expunge(existing)
                    return (existing, changed)

                session.add(record)
                session.commit()
                session.refresh(record)
                session.expunge(record)
                logger.debug(
                    f"Created DAG dependency record: dependent_job_id={record.dependent_job_id}, "
                    f"dependency={record.dependency_job_identifier}"
                )
                return (record, True)
        except Exception as e:
            logger.exception("Failed to upsert DAG dependency record", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to upsert DAG dependency record: {e}") from e

    def read_dag_dependencies(
        self,
        dependent_job_id: str | None = None,
        dependency_job_identifier: str | None = None,
        dependency_job_identifier_type: str | None = None,
        action_statuses: list[str] | None = None,
        action_time_before: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
        sort_by: list[tuple[str, str]] | None = None,
    ) -> tuple[list[DagDependency], int]:
        """
        Read DAG dependency records with flexible filtering, sorting, and pagination.

        Args:
            dependent_job_id: Filter by dependent (child) job ID.
            dependency_job_identifier: Filter by parent job identifier (job ID or job name).
            dependency_job_identifier_type: Filter by parent identifier type ("id" or "name").
                Required when dependency_job_identifier is provided.
            action_statuses: Filter by action status values.
            action_time_before: Filter for records with action_time before this threshold.
            limit: Maximum number of records to return. Default is 50.
            offset: Number of records to skip. Default is 0.
            sort_by: List of (column, order) tuples for sorting (e.g., [("id", "asc")]).

        Returns:
            Tuple of (list of matching DagDependency records, total count ignoring pagination).

        Raises:
            RecoveryStorageException: If the query fails.
            RecoveryStorageInputValidationError: If invalid input is provided.
        """
        if dependency_job_identifier and not dependency_job_identifier_type:
            raise RecoveryStorageInputValidationError(
                "dependency_job_identifier_type is required when dependency_job_identifier is provided."
            )

        try:
            with self.connection.session() as session:
                select_query = select(DagDependency)
                select_count_query = select(func.count(DagDependency.id))

                if dependent_job_id:
                    select_query = select_query.where(DagDependency.dependent_job_id == dependent_job_id)
                    select_count_query = select_count_query.where(DagDependency.dependent_job_id == dependent_job_id)

                if dependency_job_identifier:
                    select_query = select_query.where(
                        DagDependency.dependency_job_identifier == dependency_job_identifier,
                        DagDependency.dependency_job_identifier_type == dependency_job_identifier_type,
                    )
                    select_count_query = select_count_query.where(
                        DagDependency.dependency_job_identifier == dependency_job_identifier,
                        DagDependency.dependency_job_identifier_type == dependency_job_identifier_type,
                    )

                if action_statuses and len(action_statuses) == 1:
                    select_query = select_query.where(DagDependency.action_status == action_statuses[0])
                    select_count_query = select_count_query.where(DagDependency.action_status == action_statuses[0])
                elif action_statuses and len(action_statuses) > 1:
                    select_query = select_query.where(DagDependency.action_status.in_(action_statuses))
                    select_count_query = select_count_query.where(DagDependency.action_status.in_(action_statuses))

                if action_time_before is not None:
                    select_query = select_query.where(DagDependency.action_time < action_time_before)
                    select_count_query = select_count_query.where(DagDependency.action_time < action_time_before)

                if sort_by:
                    select_query = apply_sorting_condition(DagDependency, select_query, sort_by)

                total_count = session.execute(select_count_query).scalar() or 0
                results = list(session.scalars(select_query.limit(limit).offset(offset)).all())
                return results, total_count
        except Exception as e:
            logger.exception("Failed to read DAG dependencies", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to read DAG dependencies: {e}") from e

    def update_dag_dependency_status(
        self,
        id: int,
        action_status: str,
        action_time: datetime | None = None,
        reason_code: str | None = None,
        reason_message: str | None = None,
        reason_details: dict[str, Any] | None = None,
        last_evaluated_at: datetime | None = None,
    ) -> bool:
        """
        Atomically update a dependency's action state and evaluation explanation.

        Passing no reason values intentionally clears a previous waiting
        explanation when the action advances to ``IN_PROGRESS`` or
        ``SUCCESSFUL``. ``ABORTED`` is absorbing: later non-aborted updates use
        a guarded statement and return ``False`` instead of reopening a gate
        after terminal-parent propagation.

        Args:
            id: The primary key of the DAG dependency record.
            action_status: The new action status value.
            action_time: The timestamp of the action (optional).
            reason_code: Stable machine-readable explanation for the latest evaluation.
            reason_message: Human-readable explanation for the latest evaluation.
            reason_details: Structured context for the latest evaluation.
            last_evaluated_at: Timestamp of the latest evaluation.

        Returns:
            True if the guarded update matched a row; False when the row is
            absent or an aborted gate rejected a non-aborted transition.

        Raises:
            RecoveryStorageException: If the update fails.
        """
        try:
            update_values: dict[str, Any] = {
                "action_status": action_status,
                "reason_code": reason_code,
                "reason_message": reason_message,
                "reason_details": reason_details,
                "last_evaluated_at": last_evaluated_at,
            }
            if action_time is not None:
                update_values["action_time"] = action_time

            with self.connection.session() as session:
                update_stmt = update(DagDependency).where(DagDependency.id == id)
                if action_status != DagDependencyActionStatus.ABORTED.value:
                    update_stmt = update_stmt.where(
                        DagDependency.action_status != DagDependencyActionStatus.ABORTED.value
                    )
                result = session.connection().execute(update_stmt.values(update_values))
                session.commit()
                updated = result.rowcount != 0
                if updated:
                    logger.debug(f"Successfully updated DAG dependency {id} to status={action_status}")
                return updated
        except Exception as e:
            logger.exception(f"Failed to update DAG dependency {id}", extra={"dependency_id": id, "error": str(e)})
            raise RecoveryStorageException(f"Failed to update DAG dependency {id}: {e}") from e
