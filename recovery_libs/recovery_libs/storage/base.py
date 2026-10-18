# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, NamedTuple, Optional

from recovery_libs.schema.job_manager.models import (
    JobTimeFilter,
)
from recovery_libs.storage.dataclass import (
    DagDependency,
    JobEvent,
    SlurmJobExecutionRecord,
    WorkflowRecord,
)


class RecoveryStorageException(Exception):
    """
    Custom exception class when storage system error happens.
    """


class RecoveryStorageInputValidationError(Exception):
    """
    Custom exception class when storage error happened due to invalid input.
    """


class UpdateJobResult(NamedTuple):
    """Result of a successful partial Job update.

    ``old_record`` and ``updated_record`` are detached snapshots captured by
    the storage transaction. Both are ``None`` when no record matched the
    requested Job attempt.
    """

    updated_record: SlurmJobExecutionRecord | None
    old_record: SlurmJobExecutionRecord | None


class BaseStorage(ABC):
    def _set_update_time(self, job_record: SlurmJobExecutionRecord) -> None:
        """
        Method to set the last updated time if the last_updated_at is not set
        at the data source. It's a no-op if a value is already provided.
        """
        if not job_record.last_updated_at:
            job_record.last_updated_at = datetime.now(UTC)

    @abstractmethod
    def ready(self) -> bool:
        """
        Method to indicate whether the storage is ready for requests.

        Parameters:
            None.

        Returns:
            True is ready, False otherwise.

        Raises:
            None.
        """

    @abstractmethod
    def bootstrap(self, schema_upgrade: bool = True) -> bool:
        """
        Method to initialize the storage to start serving traffic.

        Bootstrap the storage by either upgrading the schema or checking if the schema is up-to-date.

        Args:
            schema_upgrade (bool): Run schema upgrade (if needed) through Alembic during bootstrap, default is True.

        Returns:
            Ture if succeeded. When bootstrap fails, it should prefer throwing
            an exception rather than return False to provide more details on
            the reason of the failuer.

        Raises:
            RecoveryStorageException when storage setup fails.
        """

    @abstractmethod
    def get_job_latest_attempt_index(self, job_id: str) -> int:
        """
        Method to retrive the latest job attempt index given a job id.

        Parameters:
            job_id: Slurm job id.

        Returns:
            latest job attempt index as int.

        Raises:
            None
        """

    @abstractmethod
    def create_job_record(self, job_record: SlurmJobExecutionRecord) -> bool:
        """
        Method to create a job record in the storage.

        Parameters:
            job_record: SlurmJobExecutionRecord object.

        Returns:
            True if the creation succeeded.

        Raises:
            RecoveryStorageException when new job record can't be created.
        """

    @abstractmethod
    def read_job_record(self, job_id: str, job_attempt_index: int | None = None) -> SlurmJobExecutionRecord | None:
        """
        Method to read a job record from the storage.

        Parameters:
            job_id: Slurm job id.
            job_attempt_index: The attempt index of the Slurm job, if not
            provided it will default to the latest attempt.

        Returns:
            SlurmJobExecutionRecord when the target record is found.

        Raises:
            RecoveryStorageException when the job record cannot be found due to
            storage read errors.
        """

    @abstractmethod
    def read_job_records(
        self,
        complete_data: bool,
        job_id: str | None,
        job_attempt_index: int | None,
        job_name: str | None,
        user: str | None,
        states: list[str],
        limit: int,
        offset: int,
        time_filter: JobTimeFilter | None,
        sort_by: list[tuple[str, str]] | None,
        *,
        heimdall_job_id: Optional["uuid.UUID"] = None,
        job_name_match: str = "prefix",
        workflow_id: str | None = None,
    ) -> tuple[list[SlurmJobExecutionRecord], int]:
        """
        Method to read a list of job records from recovery_libs.storage with given filters.
        Currently the supported filtering options are: job_id, job_attempt_index,
        job_name, workflow_id, and retry_of. At least one of them need to be provided with the
        exception of job_attempt_index and state which cannot be given alone.

        Parameters:
            complete_data: boolean option that controls whether complete job records
                should be returned. By default only a subset of the fields will be
                returned when using secondary indices.
            job_id: Optional Slurm job id.
            job_attempt_index: Optional job attmept index, when provided, the
                job_id must exist too.
            job_name: Optional job name filter.
            job_name_match: How to match job_name: "prefix" (default), "exact", or "fuzzy" (case-insensitive substring match).
            workflow_id: Optional exact workflow ID filter, independent of job_name.
            user: Optional job username filter.
            states: Optional list of job states filter.
            limit: Number of records to return.
            offset: Offset to start the query from.
            time_filter: When specified, filter the query by time, refer to JobTimeFilter for details.
            sort_by: Optional list of sort_by tuples of format (field_name, sort_direction) that controls the order of
                the records returned.
            heimdall_job_id: Optional UUID filter. If provided, returns the job with this heimdall_job_id.

        Returns:
            List of SlurmJobExecutionRecord and the total number of records that match the query in a tuple.

        Raises:
            RecoveryStorageException when an exception is raised during communication
            with the storage provider.
        """

    @abstractmethod
    def update_job_record(
        self,
        job_id: str,
        job_attempt_index: int | None,
        **kwargs: Any,
    ) -> UpdateJobResult:
        """
        Method to partially update a job record with values in the kwargs.

        Parameter:
            job_id: Slurm job id.
            job_attempt_index: The attempt index of the Slurm job, if not
            provided it will default to the latest attempt.

        Returns:
            Detached snapshots of the record before and after the successful
            update. Both snapshots are ``None`` when no record matched.

        Raises:
            RecoveryStorageException when the update fails due to reasons like
            invalid job id, invalid job id, wrong field name, disallowed values, etc.
        """

    @abstractmethod
    def read_workflows(
        self,
        limit: int = 50,
        offset: int = 0,
        workflow_id: str | None = None,
        workflow_id_match: str = "prefix",
    ) -> tuple[list[WorkflowRecord], int]:
        """
        Retrieves a list of workflows.

        Args:
            limit (int): The maximum number of records to return.
            offset (int): The offset to start the query from.
            workflow_id (Optional[str]): The workflow ID to filter by.
            workflow_id_match (str): How to match workflow_id: "prefix" (default), "exact", or "fuzzy".

        Returns:
            tuple[list[WorkflowRecord], int]: A tuple containing a list of `WorkflowRecord` objects and the total number of records.

        Raises:
            RecoveryStorageException: If there is an error during the database query.
        """

    @abstractmethod
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

        Raises:
            RecoveryStorageException: If there is an error during the database query.
        """

    @abstractmethod
    def read_users(self, job_states: list[str] | None, limit: int, offset: int, descending: bool) -> list[str]:
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

    @abstractmethod
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

    # ============================================================================
    # DAG Dependency Operations
    # ============================================================================

    @abstractmethod
    def upsert_dag_dependency(self, record: DagDependency) -> tuple[DagDependency, bool]:
        """Create or update a DAG dependency record."""

    @abstractmethod
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
        """

    @abstractmethod
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
        """Atomically update a DAG dependency action and its latest evaluation explanation."""
