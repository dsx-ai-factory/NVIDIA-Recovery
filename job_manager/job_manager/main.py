# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import base64
import binascii
import json
import os
import signal
import sys
import threading
import uuid
from datetime import UTC, datetime
from typing import Any, Literal, cast

import uvicorn
import yaml
from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Query, Response, status
from fastapi.openapi.utils import get_openapi
from fastapi_mcp import FastApiMCP
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import ValidationError
from recovery_libs.audit import (
    AuditBuffer,
    AuditTrailSettings,
    ProducerEvent,
    create_audit_buffer,
)
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceReconciliationStatus,
    ApplicationIntention,
    ApplicationIntentionInput,
    GetRecoverySequenceRequest,
    GetRecoverySequenceResponse,
    GetWorkflowsResponse,
    JobAnomaly,
    JobExecutionStatuses,
    JobGroupStatus,
    JobGroupStatuses,
    JobTimeFilter,
    JobTimeFilterTarget,
    ListAdviceResponse,
    ListDagDependenciesResponse,
    NodeAnalysisResponse,
    NodeAnalysisResult,
    NodeEvents,
    RecordRecoveryMilestoneRequest,
    RecordRecoveryMilestoneResponse,
    RecoveryContext,
    RecoveryOperation,
    RecoveryTriggerType,
    SlurmJobExecutionStatus,
    SortOrderEnum,
    SystemRequeueConfig,
    Workflow,
)
from recovery_libs.schema.workflow_identity import CanonicalWorkflowID
from recovery_libs.storage.base import (
    RecoveryStorageException,
    RecoveryStorageInputValidationError,
)
from recovery_libs.storage.dataclass import SlurmJobFinalStates, SlurmJobNonFinalStates
from recovery_libs.tracing.traces import set_up_trace_provider
from recovery_libs.utils.nodeset import operations, parser, topology
from recovery_libs.utils.nodeset.operations import EvaluationError
from recovery_libs.utils.nodeset.parser import ParseError

from job_manager.advice_mapping import (
    http_to_storage_advice,
    http_to_storage_advice_reconciliation_status,
    storage_to_http_advice,
    storage_to_http_advice_reconciliation_status,
)
from job_manager.audit.advice_changes import audit_advice_delete_if_needed
from job_manager.constants import INFINITE_SYSTEM_REQUEUE_LIMIT
from job_manager.job_lifecycle_times import (
    INVALID_SLURM_LIFECYCLE_TIMES_DETAIL,
    SlurmLifecycleTimesValidationError,
    project_job_lifecycle_times,
)
from job_manager.job_log_file_paths import (
    INVALID_SLURM_LOG_FILE_PATHS_DETAIL,
    SlurmLogFilePathsValidationError,
    needs_heimdall_job_id_placeholder,
    project_job_log_file_paths,
)
from job_manager.job_mapping import http_to_storage_job_status, storage_to_http_job_status
from job_manager.job_scheduler_metadata import (
    INVALID_SLURM_JOB_METADATA_DETAIL,
    SlurmJobMetadataValidationError,
    project_job_scheduler_fields,
)
from job_manager.logger import logger
from job_manager.service import JobManager, resolve_effective_final_state
from job_manager.storage_utils import (
    maybe_persist_requeue_terminal_advice,
    sanitize_log_file_paths_for_retry,
    storage_attribution_to_summary,
    storage_to_http_attributions,
    storage_to_http_dag_dependency,
)

CONFIG_FILE_ENV_VAR_NAME = "JOB_MANAGER_CONFIG_PATH"
DEFAULT_CONFIG_FILE_PATH = "./job_manager/job_manager_config.yaml"


def _decode_advice_page_token(page_token: str) -> tuple[int, dict[str, Any] | None]:
    """Decode a legacy offset token or a filter-bound Advice page token."""
    try:
        decoded = base64.b64decode(page_token).decode("utf-8")
        try:
            offset = int(decoded)
        except ValueError:
            payload = json.loads(decoded)
            if not isinstance(payload, dict):
                raise ValueError("unsupported Advice page token")  # noqa: TRY004
            if payload.get("version") != 1 or not isinstance(payload.get("offset"), int) or payload["offset"] < 0:
                raise ValueError("unsupported Advice page token")
            return payload["offset"], payload
        if offset < 0:
            raise ValueError("negative Advice page offset")
        return offset, None
    except (binascii.Error, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid page_token format.",
        ) from error


def _encode_advice_page_token(offset: int, filter_context: dict[str, Any] | None) -> str:
    """Encode pagination state without exposing filter internals as query state."""
    if filter_context is None:
        payload = str(offset)
    else:
        payload = json.dumps({**filter_context, "version": 1, "offset": offset}, separators=(",", ":"))
    return base64.b64encode(payload.encode("utf-8")).decode("utf-8")


job_manager: JobManager | None = None
app = FastAPI()
shutdown_event = threading.Event()
PENDING_LIKE_JOB_STATES = {
    SlurmJobNonFinalStates.PENDING.value,
    SlurmJobNonFinalStates.CONFIGURING.value,
    SlurmJobNonFinalStates.POWER_UP_NODE.value,
    SlurmJobNonFinalStates.RESV_DEL_HOLD.value,
}
SLURM_JOB_FINAL_STATE_SET = {state.value for state in SlurmJobFinalStates}
# COMPLETED and REQUEUED recovery is driven by exact-attempt RESTARTED
# Advice. Keeping them out of broad discovery avoids repeatedly reading
# historical terminal jobs while preserving the scheduler action path.
# NODE_FAIL and PREEMPTED are also excluded because Slurm requeues them
# without a Heimdall scheduler mutation after their state is persisted.
SLURM_RECONCILIATION_ELIGIBLE_JOB_STATES = [
    *(state.value for state in SlurmJobNonFinalStates),
    SlurmJobFinalStates.BOOT_FAIL.value,
    SlurmJobFinalStates.DEADLINE.value,
    SlurmJobFinalStates.FAILED.value,
    SlurmJobFinalStates.OUT_OF_MEMORY.value,
    SlurmJobFinalStates.TIMEOUT.value,
    SlurmJobFinalStates.REVOKED.value,
    SlurmJobFinalStates.SPECIAL_EXIT.value,
]

# Expose metrics endpoint
Instrumentator().instrument(app).expose(app)


@app.get("/status", operation_id="get_status")
def get_status():
    """
    Endpoint to indicate if job manager is properly functioning or not.
    """
    if not job_manager.storage or not job_manager.event_bus_consumers:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    else:
        return (
            Response(status_code=status.HTTP_200_OK)
            if job_manager.storage.ready()
            and all(consumer.is_healthy() for consumer in job_manager.event_bus_consumers.values())
            else Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        )


@app.get("/job_status/{job_id}", operation_id="get_job_status")
def get_job_status(
    job_id: str,
    job_attempt_index: int | None = Query(
        default=None,
        description="The job_attempt_index query parameter.",
    ),
) -> SlurmJobExecutionStatus:
    """
    Retrieve the execution status of a specific job.

    Parameters:
        job_id (str):
            The unique identifier of the job to retrieve the status for.
        job_attempt_index (Optional[int], optional):
            The specific attempt index of the job, if applicable. If not provided,
            the latest or most relevant attempt is used by default.

    Returns:
        SlurmJobExecutionStatus:
            An object of SlurmJobExecutionStatus representing the current execution status of the specified job.
    """
    logger.debug(f"Query storage with job id: {job_id}, attempt index: {job_attempt_index}")
    try:
        job_record = job_manager.storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)  # type: ignore[union-attr]
        if not job_record:
            logger.info(f"Unable to find matching job for job id: {job_id}, attempt index {job_attempt_index}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Unable to find matching job for job id: {job_id}, attempt index {job_attempt_index}",
            )
        else:
            return storage_to_http_job_status(job_record)
    except RecoveryStorageException as wse:
        logger.exception(
            f"Error obtaining job status with job id ({job_id}), attempt index ({job_attempt_index})",
            extra={"job_id": job_id, "error": str(wse)},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/job_anomalies/{job_id}", operation_id="get_job_anomalies")
def get_job_anomalies(job_id: str, job_attempt_index: int | None = Query(None)) -> list[JobAnomaly]:
    """
    Retrieve a list of anomalies associated with a specific job.

    Parameters:
        job_id (str):
            The unique identifier of the job.
        job_attempt_index (Optional[int], optional):
            The index of the job attempt to retrieve anomalies for.
            If not provided, anomalies for the most relevant or latest attempt are returned.

    Returns:
        list[JobAnomaly]:
            A list of anomalies detected for the specified job attempt.
    """
    logger.info(f"Query storage with job id: {job_id} and job attempt index {job_attempt_index}")
    try:
        job_record = job_manager.storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)  # type: ignore[union-attr]
        if not job_record:
            message = f"Unable to find job id: {job_id}, attempt index {job_attempt_index}."
            logger.info(message)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=message,
            )
        else:
            return job_record.anomalies or []
    except RecoveryStorageException as wse:
        logger.exception("Error obtaining job anomalies", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/workflows", operation_id="get_workflows")
def get_workflows(
    limit: int = Query(default=50, description="The maximum number of records to return."),
    offset: int = Query(default=0, description="The offset to start the records from."),
    workflow_id: CanonicalWorkflowID | None = Query(
        default=None,
        alias="workflow-id",
        description="The workflow ID filter.",
    ),
    workflow_id_match: Literal["prefix", "exact", "fuzzy"] = Query(
        default="prefix",
        description=(
            "How to match workflow_id: 'prefix' (default, case-insensitive starts with), "
            "'exact' (case-sensitive exact match), or 'fuzzy' (case-insensitive substring match)."
        ),
    ),
) -> GetWorkflowsResponse:
    """
    Retrieve workflows grouped by their canonical persisted workflow ID.
    """
    # SELECT workflow_id, min(start_time), max(end_time) FROM job_execution_records GROUP BY workflow_id;

    try:
        records, count = job_manager.storage.read_workflows(  # type: ignore[union-attr]
            limit=limit,
            offset=offset,
            workflow_id=workflow_id,
            workflow_id_match=workflow_id_match,
        )
        return GetWorkflowsResponse(
            workflows=[
                Workflow(id=record.id, start_time=record.start_time, end_time=record.end_time) for record in records
            ],
            item_count=count,
        )
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error listing workflows", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as e:
        logger.exception("Storage error listing workflows", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@app.get("/job_groups", operation_id="get_job_groups")
def get_job_groups(
    job_group_id: str | None = Query(
        default=None,
        description="Filter by a specific job group ID.",
    ),
    user: str | None = Query(default=None, description="The user filter."),
    limit: int = Query(default=50, description="The maximum number of records to return."),
    offset: int = Query(default=0, description="The offset to start the records from."),
    sort_by: str | None = Query(
        default=None,
        description=(
            "Comma separated list of fields to sort by. Valid fields: job_group_id, unique_job_count, "
            "total_job_attempt_count. Format: 'field:order' pairs, e.g., 'total_job_attempt_count:desc'."
        ),
    ),
) -> JobGroupStatuses:
    """
    Query aggregated job group metadata.

    GET /job_groups
    Retrieves a list of job group metadata derived from job execution records, with optional filtering,
    pagination, and sorting.

    Query Parameters:
    job_group_id (str, optional):
    Filter by a specific job group ID.

    user (str, optional):
    Filter by the username who submitted the jobs.

    limit (int, default=50):
    Maximum number of records to return.

    offset (int, default=0):
    Number of records to skip (used for pagination).

    sort_by (str, optional):
    Comma-separated list of fields to sort by. Each field can specify order as asc or desc.
    Valid fields: job_group_id, unique_job_count, total_job_attempt_count.
    Example: total_job_attempt_count:desc

    Returns:
    A JobGroupStatuses object containing the list of job group metadata and total count.
    """
    try:
        sort_by_fields = []
        if sort_by:
            elements = [element.strip() for element in sort_by.split(",")]
            for element in elements:
                parts = element.split(":", 1)
                if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid sort_by expression '{element}': expected 'field:asc|desc'.",
                    )
                field = parts[0].strip()
                order = parts[1].strip().lower()
                if order != SortOrderEnum.ASC.value and order != SortOrderEnum.DESC.value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid sort_by expression, sort order must be either 'asc' or 'desc' for field {field}.",
                    )
                sort_by_fields.append((field, order))

        records, count = job_manager.storage.read_job_groups(  # type: ignore[union-attr]
            job_group_id=job_group_id,
            user=user,
            sort_by=sort_by_fields if sort_by_fields else None,
            limit=limit,
            offset=offset,
        )

        return JobGroupStatuses(
            job_groups=[JobGroupStatus(**record) for record in records],
            item_count=count,
        )
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error listing job groups", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as e:
        logger.exception("Storage error listing job groups", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@app.post(
    "/application-intentions",
    operation_id="post_application_intention",
    response_model=ApplicationIntention,
)
def post_application_intention(
    background_tasks: BackgroundTasks,
    intention: ApplicationIntentionInput = Body(
        ...,
        description="A durable application request for the scheduler job.",
    ),
) -> ApplicationIntention:
    """Record a job-level cancellation request from an application.

    A successful response means that Job Manager durably accepted the
    idempotent request. It does not wait for Advice creation or scheduler
    cancellation. Job Manager starts best-effort reconciliation after sending
    the response and retries from normal job and attribution updates.
    """
    try:
        accepted = job_manager.create_application_intention(intention)  # type: ignore[union-attr]
        background_tasks.add_task(
            job_manager.reconcile_latest_application_intention,  # type: ignore[union-attr]
            accepted.job_id,
        )
        return accepted
    except RecoveryStorageException as error:
        logger.exception(
            "Failed to persist application intention for job %s",
            intention.job_id,
            extra={"error": str(error)},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)) from error


@app.post("/jobs", operation_id="post_jobs")
def post_jobs(
    job: SlurmJobExecutionStatus = Body(..., description="The job to upsert."),
) -> SlurmJobExecutionStatus:
    """
    Create or update a job execution record.

    This endpoint accepts a job execution status and either creates a new record
    if one doesn't exist, or updates the existing record if it does. The job is
    identified by the combination of job_id and job_attempt_index.

    This endpoint is idempotent - calling it multiple times with the same data
    will update the existing record rather than failing or creating duplicates.

    Args:
        job: The job execution status to upsert.

    Returns:
        SlurmJobExecutionStatus: The created or updated job execution status.

    Raises:
        HTTPException: If validation fails or a database error occurs.
    """
    logger.info(f"Upserting job: job_id={job.job_id}, attempt_index={job.job_attempt_index}")

    try:
        (
            job.cluster,
            job.partition,
            job.user,
            job.command,
            job.state_reason,
            job.node_count,
            job.total_gpu_count,
            job.total_cpu_count,
            job.total_memory_mb,
        ) = project_job_scheduler_fields(
            scheduler_metadata=job.scheduler_metadata,
            cluster=job.cluster,
            partition=job.partition,
            user=job.user,
            command=job.command,
            state_reason=job.state_reason,
            node_count=job.node_count,
            total_gpu_count=job.total_gpu_count,
            total_cpu_count=job.total_cpu_count,
            total_memory_mb=job.total_memory_mb,
        )

        # Defaults are for scheduler-neutral operator ingress. A partial Slurm
        # accounting update must leave absent values unset so storage preserves
        # richer fields previously learned from squeue.
        if job.scheduler_metadata.get("scheduler") != "slurm":
            if not job.user:
                job.user = "unknown"
            if not job.cluster:
                job.cluster = "unknown"
            if not job.command:
                job.command = "not_applicable"
        if not job.aps_spec:
            job.aps_spec = ApsSlurmJobSpec(auto_resume_mode=AutoResumeModeEnum.REQUEUE)
        elif not job.aps_spec.auto_resume_mode:
            job.aps_spec.auto_resume_mode = AutoResumeModeEnum.REQUEUE

        job.submit_time, job.start_time, job.end_time = project_job_lifecycle_times(
            scheduler_metadata=job.scheduler_metadata,
            state=job.state or "",
            submit_time=job.submit_time,
            start_time=job.start_time,
            end_time=job.end_time,
        )
        populate_heimdall_id_log_path = needs_heimdall_job_id_placeholder(
            scheduler_metadata=job.scheduler_metadata,
            log_file_paths=job.log_file_paths,
        )
        job.log_file_paths = project_job_log_file_paths(
            scheduler_metadata=job.scheduler_metadata,
            log_file_paths=job.log_file_paths,
        )

        # Convert HTTP model to storage model
        job_record = http_to_storage_job_status(job)

        # Advisor API upserts are the production scheduler ingress. Resolve the
        # terminal outcome from Slurm metadata as well as the scheduler-neutral
        # state so an older Slurm job can be acted on while it is still
        # COMPLETING, then de-duplicate against the previously stored outcome so
        # the idempotent polling API cannot emit the CRASH twice.
        #
        # Only the state-based CRASH is emitted here. A COMPLETED or TIMEOUT
        # outcome is a level-based scheduler classification that must not carry
        # recovery evidence on this path; emitting an END anomaly, terminal
        # Advice, or requeue notification for it would turn healthy completions
        # and scheduler timeouts into false-positive recoveries (see the healthy
        # negative-control and scheduler-timeout e2e contracts). The controller
        # drives the requeue for a failed job from its own reconcile loop.
        effective_final_state = resolve_effective_final_state(job_record)
        known_record = None
        if effective_final_state is not None:
            known_record = job_manager.storage.read_job_record(  # type: ignore[union-attr]
                job_id=job_record.job_id,
                job_attempt_index=job_record.job_attempt_index,
            )
        known_effective_final_state = resolve_effective_final_state(known_record) if known_record is not None else None

        anomaly_record = known_record or job_record
        anomaly_config = job_record.aps_spec.anomaly_detection_config if job_record.aps_spec else None
        state_based_crash_recovery_enabled = (
            effective_final_state == SlurmJobFinalStates.FAILED.value
            and anomaly_config is not None
            and anomaly_config.log_based_crash_enabled is False
        )
        if (
            effective_final_state == SlurmJobFinalStates.FAILED.value
            and known_effective_final_state is None
            and job_manager.should_send_crash_anomaly(  # type: ignore[union-attr]
                record=anomaly_record, state=effective_final_state
            )
        ):
            job_manager._generate_crash_event(  # type: ignore[union-attr]
                record=job_record,
                base_anomalies=known_record.anomalies if known_record is not None else None,
            )

        # Slurm can briefly report a previous attempt's path after requeue.
        # Preserve a reused path only when the workload explicitly declares
        # that its attempts intentionally share log files.
        if job.scheduler_metadata.get("scheduler") == "slurm":
            sanitize_log_file_paths_for_retry(job_record, job_manager.storage)  # type: ignore[union-attr]

        # Upsert the job record
        updated_record, created, _ = job_manager.upsert_job_record(  # type: ignore[union-attr]
            job_record,
            populate_heimdall_id_log_path=populate_heimdall_id_log_path,
        )
        job_manager.reconcile_anomaly_advice(updated_record)  # type: ignore[union-attr]
        # Dependency registration is creation-only: scheduler polling submits
        # the same record repeatedly and must not reset an action that has
        # already been released or aborted.
        if created:
            job_manager.register_dag_dependencies_for_new_job(updated_record)  # type: ignore[union-attr]
            if updated_record.state != SlurmJobFinalStates.COMPLETED.value:
                job_manager.refresh_dag_dependency_observability_for_observed_job(  # type: ignore[union-attr]
                    updated_record
                )
        if updated_record.state == SlurmJobFinalStates.COMPLETED.value:
            job_manager._evaluate_dag_dependencies_for_completed_job(updated_record)  # type: ignore[union-attr]

        # An application cancellation is a durable job-level intent. Reconcile
        # it after creation-time DAG registration so a terminal parent can
        # propagate to its descendants, and before attribution can generate a
        # competing Restarted decision. Future attempts repeat this check when
        # they are first observed.
        application_intention_result = job_manager.reconcile_application_intention(updated_record)  # type: ignore[union-attr]

        # Persist terminal cancellation/limit Advice at the scheduler-state
        # boundary so recovery outcomes remain visible. State-based anomaly handling
        # remains transition-triggered, but Advice reconciliation is level-based:
        # a failed write is retried on the next scheduler upsert and only an
        # existing terminal record satisfies the helper.
        dag_support_enabled = job_manager._is_dag_support_enabled()  # type: ignore[union-attr]
        is_explicitly_cancelled = updated_record.state == SlurmJobFinalStates.CANCELLED.value
        is_dag_parent_stopped = dag_support_enabled and updated_record.state == SlurmJobFinalStates.STOPPED.value
        should_persist_terminal_advice = (
            is_explicitly_cancelled or is_dag_parent_stopped or state_based_crash_recovery_enabled
        )
        if should_persist_terminal_advice and job_manager._is_advice_management_enabled():  # type: ignore[union-attr]
            maybe_persist_requeue_terminal_advice(
                job_manager.storage,  # type: ignore[union-attr]
                updated_record,
                audit_buffer=job_manager.audit_buffer,  # type: ignore[union-attr]
                system_requeue_config=job_manager._build_system_requeue_config(),  # type: ignore[union-attr]
                get_jobs=job_manager._get_jobs_for_requeue_check,  # type: ignore[union-attr]
                on_persisted=job_manager._maybe_send_recovery_notification,  # type: ignore[union-attr]
                on_advice_persisted=job_manager.propagate_terminated_advice_for_dag_dependents,  # type: ignore[union-attr]
                allow_explicit_dag_abort=dag_support_enabled,
                on_advice_ready=job_manager._begin_sequence_for_advice,  # type: ignore[union-attr]
            )

        # Attribution produces the terminal Restarted/Terminated Advice that a
        # scheduler adapter is allowed to act on. Emit only after the anomaly is
        # durable so ADI and the controller cannot observe a partial attempt.
        if updated_record.anomalies and not application_intention_result.blocks_recovery:
            job_manager._maybe_emit_attribution_request(updated_record)  # type: ignore[union-attr]

        action = "created" if created else "updated"
        logger.info(
            f"Successfully {action} job: job_id={updated_record.job_id}, "
            f"attempt_index={updated_record.job_attempt_index}"
        )

        # Convert back to HTTP model and return (matching stub behavior)
        return storage_to_http_job_status(updated_record)

    except SlurmLogFilePathsValidationError as error:
        logger.warning(f"Rejected malformed Slurm log file paths metadata: {error}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=INVALID_SLURM_LOG_FILE_PATHS_DETAIL,
        ) from error
    except SlurmLifecycleTimesValidationError as error:
        logger.warning(f"Rejected malformed Slurm lifecycle time metadata: {error}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=INVALID_SLURM_LIFECYCLE_TIMES_DETAIL,
        ) from error
    except SlurmJobMetadataValidationError as error:
        logger.warning(f"Rejected malformed Slurm job metadata: {error}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=INVALID_SLURM_JOB_METADATA_DETAIL,
        ) from error
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error upserting job", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as e:
        logger.exception("Storage error upserting job", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@app.get("/advice", operation_id="get_advice")
def get_advice(
    page_size: int = Query(default=50, ge=1, le=1000, description="Number of items per page."),
    page_token: str | None = Query(default=None, description="Token for the next page of results."),
    job_id: str | None = Query(default=None, description="Filter advice by job_id."),
    include_hidden: bool = Query(default=False, description="If true, include hidden/non-visible advice records."),
    created_after: datetime | None = Query(
        default=None,
        description="Return ordinary advice created at or after this time while retaining durable states.",
    ),
    only_active: bool = Query(default=False, description="Return only advice that can still require reconciliation."),
) -> ListAdviceResponse:
    """
    Retrieve a list of advice for jobs.

    This endpoint returns advice recommendations from the NVIDIA Recovery system for jobs
    that have been analyzed for issues such as crashes, hangs, or stragglers.

    Parameters:
        page_size (int):
            The maximum number of advice items to return per page. Must be between 1 and 1000.
            Default is 50.
        page_token (str, optional):
            A token representing the starting point for the next page of results.
            If not provided, the first page is returned. The token is a base64-encoded offset value.
        job_id (str, optional):
            Filter advice by job_id.
        include_hidden (bool):
            If True, include advice records that are not yet visible (soft-deleted or under construction).
            Default is False (only visible advice is returned).

    Returns:
        ListAdviceResponse:
            A response containing a list of advice items and an optional nextPageToken
            for pagination.
    """
    logger.info(
        f"Query advice with page_size: {page_size}, page_token: {page_token}, job_id: {job_id}, include_hidden: {include_hidden}"
    )

    try:
        # Decode page_token and recover the filter context used by every page.
        offset = 0
        token_context = None
        if page_token:
            offset, token_context = _decode_advice_page_token(page_token)

        filtered_query = only_active or created_after is not None
        if token_context is not None:
            expected_context = {
                "only_active": only_active,
            }
            for key, expected_value in expected_context.items():
                if token_context.get(key) != expected_value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Advice page token does not match the requested filters.",
                    )
            if "created_after" not in token_context:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Advice page token is missing its filter context.",
                )
            token_created_after = token_context["created_after"]
            try:
                effective_created_after = (
                    datetime.fromisoformat(token_created_after) if isinstance(token_created_after, str) else None
                )
            except ValueError as error:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Advice page token has an invalid created_after boundary.",
                ) from error
            if token_created_after is not None and effective_created_after is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Advice page token has an invalid created_after boundary.",
                )
        else:
            if page_token and filtered_query:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="A legacy page token cannot be used with Advice filters.",
                )
            effective_created_after = created_after

        if effective_created_after is not None and effective_created_after.tzinfo is None:
            effective_created_after = effective_created_after.replace(tzinfo=UTC)

        records, total_count = job_manager.storage.read_advice_list(  # type: ignore[union-attr]
            job_id=job_id,
            limit=page_size,
            offset=offset,
            include_hidden=include_hidden,
            created_after=effective_created_after,
            only_active=only_active,
        )

        # Convert storage records to HTTP models
        advice_list = [storage_to_http_advice(record) for record in records]

        # Generate next_page_token if there are more results
        next_page_token = None
        if offset + len(records) < total_count:
            next_offset = offset + page_size
            filter_context = None
            if filtered_query:
                filter_context = {
                    "only_active": only_active,
                    "created_after": (
                        effective_created_after.isoformat() if effective_created_after is not None else None
                    ),
                }
            next_page_token = _encode_advice_page_token(next_offset, filter_context)

        return ListAdviceResponse(
            item_count=total_count,
            advice=advice_list,
            next_page_token=next_page_token,
        )
    except RecoveryStorageException as wse:
        logger.exception("Error retrieving advice", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.put(
    "/advice/{heimdall_id}/reconciliation-status",
    operation_id="put_advice_reconciliation_status",
)
def put_advice_reconciliation_status(
    heimdall_id: str,
    reconciliation_status: AdviceReconciliationStatus = Body(
        ...,
        description="Generation-scoped reconciliation observation to record idempotently.",
    ),
) -> AdviceReconciliationStatus:
    """Record one generation-scoped reconciliation observation for Advice."""
    try:
        advice_id = uuid.UUID(heimdall_id)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid heimdall_id format: {heimdall_id}. Must be a valid UUID.",
        ) from error

    try:
        stored = job_manager.storage.upsert_advice_reconciliation_status(  # type: ignore[union-attr]
            http_to_storage_advice_reconciliation_status(advice_id, reconciliation_status)
        )
        return storage_to_http_advice_reconciliation_status(stored)
    except RecoveryStorageInputValidationError as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
    except RecoveryStorageException as error:
        logger.exception("Storage error updating Advice reconciliation status", extra={"error": str(error)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)) from error


@app.get("/recovery/sequences", operation_id="get_recovery_sequence")
def get_recovery_sequence(
    scheduler_system: str = Query(min_length=1, max_length=32),
    scheduler_scope: str = Query(min_length=1, max_length=256),
    job_id: str = Query(min_length=1, max_length=256),
    source_attempt_index: int = Query(ge=0),
    operation: RecoveryOperation = Query(),
) -> GetRecoverySequenceResponse:
    """Read canonical context for an exact scoped operation without creating it."""
    try:
        result = job_manager.recovery_service.get(  # type: ignore[union-attr]
            GetRecoverySequenceRequest(
                scheduler_system=scheduler_system,
                scheduler_scope=scheduler_scope,
                job_id=job_id,
                source_attempt_index=source_attempt_index,
                operation=operation,
            )
        )
        if result is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recovery sequence not found.")
        return result
    except RecoveryStorageInputValidationError as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
    except RecoveryStorageException as error:
        logger.exception("Failed to get a recovery sequence.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)) from error


@app.post(
    "/recovery/milestones",
    operation_id="record_recovery_milestone",
)
def record_recovery_milestone(
    request: RecordRecoveryMilestoneRequest,
) -> RecordRecoveryMilestoneResponse:
    """Idempotently record one scheduler-action completion occurrence."""

    try:
        return job_manager.recovery_service.record_milestone(request)  # type: ignore[union-attr]
    except RecoveryStorageInputValidationError as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
    except RecoveryStorageException as error:
        logger.exception("Failed to record a recovery milestone.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)) from error


@app.get("/advice/{heimdall_id}", operation_id="get_advice_by_id")
def get_advice_by_id(
    heimdall_id: str,
) -> Advice:
    """
    Retrieve a single advice record by its NVIDIA Recovery ID.

    Parameters:
        heimdall_id (str):
            The UUID of the advice record assigned by NVIDIA Recovery.

    Returns:
        Advice:
            The advice record if found.

    Raises:
        HTTPException: 404 if not found, 400 if invalid ID format, 500 on server error.
    """
    logger.info(f"Query advice with heimdall_id: {heimdall_id}")

    try:
        advice_uuid = uuid.UUID(heimdall_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid heimdall_id format: {heimdall_id}. Must be a valid UUID.",
        )

    try:
        record = job_manager.storage.read_advice(heimdall_id=advice_uuid)  # type: ignore[union-attr]
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Advice with heimdall_id {heimdall_id} not found.",
            )
        return storage_to_http_advice(record)
    except RecoveryStorageException as wse:
        logger.exception(
            f"Error retrieving advice {heimdall_id}", extra={"heimdall_id": heimdall_id, "error": str(wse)}
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.post("/advice", operation_id="post_advice")
def post_advice(
    advice: Advice = Body(..., description="The advice record to create or update."),
) -> Advice:
    """
    Create or update an advice record.

    This endpoint accepts an advice object and either creates a new record
    if one doesn't exist, or updates the existing record if it does. The advice
    is identified by its id (heimdall_id).

    This endpoint is idempotent - calling it multiple times with the same data
    will update the existing record rather than failing or creating duplicates.

    Parameters:
        advice (Advice):
            The advice record to upsert.

    Returns:
        Advice:
            The created or updated advice record.

    Raises:
        HTTPException: 409 if advice ID doesn't match job record's canonical heimdall_job_id,
            400 on validation error, 500 on server error.
    """
    logger.info(f"Upserting advice: id={advice.id}, job_id={advice.job_id}")

    # Look up the job record once for both ID validation and scheduler_metadata backfill.
    job_record = None
    try:
        job_record = job_manager.storage.read_job_record(  # type: ignore[union-attr]
            job_id=advice.job_id,
            job_attempt_index=advice.restart_count,
        )
    except RecoveryStorageException:
        pass  # If job record lookup fails, proceed with caller-supplied ID

    if job_record:
        if job_record.heimdall_job_id is None:
            canonical_heimdall_job_id = generate_heimdall_job_id(advice.job_id, advice.restart_count)
            job_manager.update_job_record(  # type: ignore[union-attr]
                job_id=advice.job_id,
                job_attempt_index=advice.restart_count,
                heimdall_job_id=canonical_heimdall_job_id,
            )
            advice.id = str(canonical_heimdall_job_id)
        elif advice.id != str(job_record.heimdall_job_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Advice ID mismatch: caller sent {advice.id}, "
                    f"but job record for {advice.job_id} attempt {advice.restart_count} "
                    f"has canonical heimdall_job_id {job_record.heimdall_job_id}"
                ),
            )

    try:
        # Backend storage alone chooses canonical recovery identity. Never
        # parent Advice work from caller-supplied correlation, even on a lookup miss.
        advice.recovery_context = None
        # Backfill scheduler metadata from the job record when callers omit it.
        if not advice.scheduler_metadata and job_record and job_record.scheduler_metadata:
            advice.scheduler_metadata = job_record.scheduler_metadata

        if job_record is not None:
            recovery_context = job_manager._begin_sequence_for_advice(  # type: ignore[union-attr]
                advice,
                trigger_type=RecoveryTriggerType.OPERATOR,
            )
            advice.recovery_context = recovery_context if isinstance(recovery_context, RecoveryContext) else None

        advice_record = http_to_storage_advice(advice)

        # Upsert the advice record
        updated_record, created, _ = cast(JobManager, job_manager).upsert_advice_record(advice_record)
        job_manager.propagate_terminated_advice_for_dag_dependents(updated_record)  # type: ignore[union-attr]

        action = "created" if created else "updated"
        logger.info(f"Successfully {action} advice: id={updated_record.heimdall_id}")

        return storage_to_http_advice(updated_record)
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error upserting advice", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as wse:
        logger.exception("Storage error upserting advice", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse
    except Exception as e:
        logger.exception("Unexpected error upserting advice", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error: {e!s}") from e


@app.get("/advice/by-job/{job_id}/{attempt_index}", operation_id="get_advice_by_job")
def get_advice_by_job(
    job_id: str,
    attempt_index: int,
) -> Advice:
    """
    Retrieve a single advice record by its natural key (job_id + attempt_index).

    Parameters:
        job_id (str): The job identifier.
        attempt_index (int): The job attempt index.

    Returns:
        Advice: The matching advice record.

    Raises:
        HTTPException: 404 if not found, 500 on server error.
    """
    logger.info(f"Query advice by job_id={job_id}, attempt_index={attempt_index}")

    try:
        record = job_manager.storage.read_advice_by_job(job_id=job_id, attempt_index=attempt_index)  # type: ignore[union-attr]
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No advice found for job {job_id} attempt {attempt_index}",
            )
        return storage_to_http_advice(record)
    except RecoveryStorageException as wse:
        logger.exception(
            f"Error retrieving advice for job {job_id} attempt {attempt_index}",
            extra={"job_id": job_id, "error": str(wse)},
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.delete("/advice/{heimdall_id}", operation_id="delete_advice")
def delete_advice(
    heimdall_id: str,
) -> Response:
    """
    Delete an advice record by its NVIDIA Recovery ID.

    Parameters:
        heimdall_id (str):
            The UUID of the advice record to delete.

    Returns:
        Response:
            204 No Content on successful deletion.

    Raises:
        HTTPException: 404 if not found, 400 if invalid ID format, 500 on server error.
    """
    logger.info(f"Deleting advice with heimdall_id: {heimdall_id}")

    try:
        advice_uuid = uuid.UUID(heimdall_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid heimdall_id format: {heimdall_id}. Must be a valid UUID.",
        )

    try:
        result = job_manager.storage.delete_advice(heimdall_id=advice_uuid)  # type: ignore[union-attr]
        if result.advice_record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Advice with heimdall_id {heimdall_id} not found.",
            )
        audit_advice_delete_if_needed(result, cast(JobManager, job_manager).audit_buffer)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except RecoveryStorageException as wse:
        logger.exception(f"Error deleting advice {heimdall_id}", extra={"heimdall_id": heimdall_id, "error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/jobs", operation_id="get_jobs")
def get_jobs(
    complete_data: bool = Query(
        default=False,
        deprecated=True,
        description=(
            "Deprecated query parameter, it used to control whether full job records will be returned or not, now it "
            "always returns full job records."
        ),
    ),
    job_id: str | None = Query(default=None, description="The job_id filter."),
    job_attempt_index: str | None = Query(
        default=None,
        description="The job_attempt_index filter. (Cannot be used by itself)",
    ),
    heimdall_job_id: str | None = Query(
        default=None,
        description="The heimdall_job_id (UUID) filter. If provided, other job_id/job_attempt_index filters are ignored.",
    ),
    user: str | None = Query(default=None, description="The user filter."),
    job_name: str | None = Query(default=None, description="The job_name filter."),
    workflow_id: CanonicalWorkflowID | None = Query(
        default=None,
        description="The exact workflow_id filter.",
    ),
    job_name_match: Literal["prefix", "exact", "fuzzy"] = Query(
        default="prefix",
        description="How to match job_name: 'prefix' (default, case-insensitive starts with), 'exact' (case-sensitive exact match), or 'fuzzy' (case-insensitive substring match).",
    ),
    state: str | None = Query(
        default=None,
        description="The slurm job_state filter, cannot be specified with completed_jobs",
    ),
    limit: int = Query(default=50, description="The maximum number of records to return."),
    offset: int = Query(default=0, description="The offset to start the records from."),
    completed_jobs: bool | None = Query(
        default=None,
        description="If specified, either return completed or running jobs, cannot be specified together with state",
    ),
    reconciliation_eligible_jobs: bool = Query(
        default=False,
        description=(
            "Return jobs that can seed scheduler reconciliation: all non-final jobs plus failure-like terminal "
            "jobs. COMPLETED and REQUEUED jobs are excluded because their recovery is driven by exact-attempt "
            "Advice. Cannot be specified together with state or completed_jobs."
        ),
    ),
    time_filter_target: JobTimeFilterTarget | None = Query(
        default=None,
        description="Filter jobs by this job time, requires either from or to time filter times",
    ),
    time_filter_from: datetime | None = Query(default=None, description="The job time filter start time"),
    time_filter_to: datetime | None = Query(default=None, description="The job time filter end time"),
    sort_by: str | None = Query(
        default=None,
        description=(
            "Comma separated list of fields to sort by. The sort will happen in the same order as the order of the "
            "fields. Valid input is a comma-separated list of 'field:order' pairs."
            "e.g., 'job_id:asc, job_attempt_index:desc'."
        ),
    ),
    job_group_id: str | None = Query(
        default=None,
        description="Filter by job group ID. Returns all job attempts belonging to this job group.",
    ),
    include_job_group_attempts: bool = Query(
        default=True,
        description=(
            "When job_group_id is not provided, controls whether to include job attempts that belong to a job group. "
            "Defaults to True. When False, only returns standalone job attempts. Ignored when job_group_id is provided."
        ),
    ),
) -> JobExecutionStatuses:
    """
    Query all available jobs.

    GET /jobs
    Retrieves a list of job execution records with optional filtering, pagination, and sorting.

    This endpoint returns job execution data based on various query parameters such as job ID, user, job name, state, and time-based filters. The response includes full job records by default.

    Query Parameters:
    complete_data (bool, deprecated):
    Deprecated. Previously used to toggle between full and partial job records. Now always returns full job records.

    job_id (str, optional):
    Filter results by a specific job ID.

    job_attempt_index (str, optional):
    Filter by job attempt index. Note: Cannot be used on its own—must be combined with job_id.

    user (str, optional):
    Filter results by the username who submitted the job.

    job_name (str, optional):
    Filter results by the job name.

    workflow_id (str, optional):
    Filter results by the exact canonical workflow ID.

    job_name_match (str, optional):
    How to match job_name: "prefix" (default, starts with) or "exact".

    state (str, optional):
    Filter by job state (e.g., Slurm job states).
    Cannot be used together with completed_jobs.

    completed_jobs (bool, optional):
    Filter by job completion status.
    If True, returns completed jobs; if False, returns running jobs.
    Cannot be used with state.

    reconciliation_eligible_jobs (bool, default=False):
    Return the state-based candidate set used to seed scheduler reconciliation:
    every non-final job plus failure-like terminal jobs. COMPLETED and REQUEUED
    are excluded because their recovery is driven by exact-attempt Advice.
    Cannot be used with state or completed_jobs.

    offset (int, default=0):
    Number of records to skip (used for pagination).

    time_filter_target (JobTimeFilterTarget, optional):
    Specifies which job time field (created, started, etc.) to filter by.
    Requires either time_filter_from or time_filter_to.

    time_filter_from (datetime, optional):
    Start of time range for the selected time filter target.

    time_filter_to (datetime, optional):
    End of time range for the selected time filter target.

    sort_by (str, optional):
    Comma-separated list of fields to sort by. Each field can specify order as asc or desc.
    Example: job_id:asc,job_attempt_index:desc

    job_group_id (str, optional):
    Filter by job group ID. Returns all job attempts belonging to this job group.

    include_job_group_attempts (bool, default=True):
    When job_group_id is not provided, controls whether to include job attempts that belong to a job group.
    When False, only returns standalone job attempts (not part of any job group).
    Ignored when job_group_id is provided.

    Returns:
    A JobExecutionStatuses object containing the list of job execution records that match the filter criteria.
    """
    # Parse and validate heimdall_job_id if provided
    heimdall_job_uuid: uuid.UUID | None = None
    if heimdall_job_id:
        try:
            heimdall_job_uuid = uuid.UUID(heimdall_job_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid heimdall_job_id format: {heimdall_job_id}. Must be a valid UUID.",
            )

    if job_attempt_index is not None and not job_attempt_index.isdigit():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="job_attempt_index must be a valid number.",
        )

    if job_attempt_index is not None and not job_id and not heimdall_job_uuid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="job_id cannot be None or empty when job_attempt_index is provided.",
        )

    time_filter = _get_time_filter(time_filter_target, time_filter_from, time_filter_to)
    states = _get_states(state, completed_jobs, reconciliation_eligible_jobs)

    try:
        sort_by_fields = []
        if sort_by:
            elements = [element.strip() for element in sort_by.split(",")]
            for element in elements:
                field, order = element.split(":")
                order = order.strip().lower()
                field = field.strip()
                if order != SortOrderEnum.ASC.value and order != SortOrderEnum.DESC.value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid sort_by expression, sort order must be either 'asc' or 'desc' for field {field}.",
                    )
                sort_by_fields.append((field, order))
        else:
            # This is to be compatible with the previous behavior where the jobs were sorted by job_id in ascending
            # order by default.
            sort_by_fields.append(("job_id", SortOrderEnum.ASC.value))

        records, count = job_manager.storage.read_job_records(  # type: ignore[union-attr]
            complete_data=complete_data,
            job_id=job_id,
            job_attempt_index=job_attempt_index,  # type: ignore[arg-type]
            heimdall_job_id=heimdall_job_uuid,
            job_name=job_name,
            workflow_id=workflow_id,
            user=user,
            states=states,
            time_filter=time_filter,
            limit=limit,
            offset=offset,
            sort_by=sort_by_fields,
            job_name_match=job_name_match,
            job_group_id=job_group_id,
            include_job_group_attempts=include_job_group_attempts,
        )

        return JobExecutionStatuses(
            statuses=[storage_to_http_job_status(record) for record in records],
            item_count=count,
        )
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error listing jobs", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as e:
        logger.exception("Storage error listing jobs", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


def _get_time_filter(
    job_time_target: JobTimeFilterTarget | None,
    from_time: datetime | None,
    to_time: datetime | None,
) -> JobTimeFilter | None:
    try:
        if job_time_target:
            return JobTimeFilter(job_time_target=job_time_target, from_time=from_time, to_time=to_time)
        else:
            return None
    except ValidationError as e:
        logger.exception("Invalid job time filter", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


def _get_states(
    state: str | None,
    completed_jobs: bool | None,
    reconciliation_eligible_jobs: bool = False,
) -> list[str]:
    if sum((state is not None, completed_jobs is not None, reconciliation_eligible_jobs)) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="state, completed_jobs, and reconciliation_eligible_jobs are mutually exclusive.",
        )

    if reconciliation_eligible_jobs:
        return list(SLURM_RECONCILIATION_ELIGIBLE_JOB_STATES)
    elif state is not None:
        return [state]
    elif completed_jobs is not None:
        return [s.value for s in SlurmJobFinalStates] if completed_jobs else [s.value for s in SlurmJobNonFinalStates]
    else:
        return []


@app.get("/dag_dependencies", operation_id="get_dag_dependencies")
def get_dag_dependencies(
    dependent_job_id: str | None = Query(default=None, description="Filter by dependent (child) job ID."),
    dependency_job_identifier: str | None = Query(
        default=None, description="Filter by parent job identifier (job ID or job name)."
    ),
    dependency_job_identifier_type: str | None = Query(
        default=None,
        description="Filter by parent identifier type ('id' or 'name'). Required when dependency_job_identifier is provided.",
    ),
    action_status: list[str] | None = Query(
        default=None,
        description=(
            "Filter by action status values: 'not_eligible', 'in_progress', 'successful', 'failed', or 'aborted'."
        ),
    ),
    action_time_before: datetime | None = Query(
        default=None, description="Filter for records with action_time before this timestamp."
    ),
    limit: int = Query(default=50, ge=1, le=1000, description="Maximum number of records to return."),
    offset: int = Query(default=0, ge=0, description="Number of records to skip for pagination."),
    sort_by: str | None = Query(
        default=None,
        description=("Comma-separated list of 'field:order' pairs for sorting. e.g., 'id:asc,dependent_job_id:desc'."),
    ),
) -> ListDagDependenciesResponse:
    """
    Query DAG dependency records.

    GET /dag_dependencies
    Retrieves a list of DAG dependency records with optional filtering, pagination, and sorting.
    Each record represents a dependency relationship between a dependent (child) job and a
    dependency (parent) job in a DAG workflow.
    """
    try:
        sort_by_fields = []
        if sort_by:
            elements = [element.strip() for element in sort_by.split(",")]
            for element in elements:
                field, order = element.split(":")
                order = order.strip().lower()
                field = field.strip()
                if order != SortOrderEnum.ASC.value and order != SortOrderEnum.DESC.value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid sort_by expression, sort order must be either 'asc' or 'desc' for field {field}.",
                    )
                sort_by_fields.append((field, order))

        records, count = job_manager.storage.read_dag_dependencies(  # type: ignore[union-attr]
            dependent_job_id=dependent_job_id,
            dependency_job_identifier=dependency_job_identifier,
            dependency_job_identifier_type=dependency_job_identifier_type,
            action_statuses=action_status,
            action_time_before=action_time_before,
            limit=limit,
            offset=offset,
            sort_by=sort_by_fields if sort_by_fields else None,
        )

        return ListDagDependenciesResponse(
            item_count=count,
            dag_dependencies=[storage_to_http_dag_dependency(record) for record in records],
        )
    except RecoveryStorageInputValidationError as ve:
        logger.exception("Validation error listing DAG dependencies", extra={"error": str(ve)})
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as e:
        logger.exception("Storage error listing DAG dependencies", extra={"error": str(e)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@app.get("/jobs/node-analysis", operation_id="get_node_analysis")
def get_node_analysis(
    expr: str = Query(..., description="Set algebra expression (e.g., '12345/0 U 12346/0 - 12347/0')"),
    group_by: str = Query(
        default="none",
        description="Group results by 'rack' or 'none'. Default is 'none'.",
    ),
) -> NodeAnalysisResponse:
    """
    Analyze node sets across multiple jobs using set algebra expressions.

    This endpoint evaluates set algebra expressions over job node lists to identify
    commonalities and differences between jobs. It supports union (U), intersection (&),
    and difference (-) operations.

    Query Parameters:
        expr (str, required):
            Set algebra expression using job IDs in format 'job_id/attempt_index'.
            Supported operators:
            - U: Union (nodes in either set)
            - &: Intersection (nodes in both sets)
            - -: Difference (nodes in first set but not second)
            - ( ): Parentheses for grouping
            Examples:
            - "12345/0 U 12346/0" (union of two jobs)
            - "(12345/0 U 12346/0) - 12347/0" (union then subtract)
            - "12345/0 & 12346/0 & 12347/0" (common nodes across three jobs)

        group_by (str, optional):
            How to group the result nodes. Options: "rack" or "none".
            Default is "none". When set to "rack", nodes are grouped by their
            rack identifier (only applicable for clusters with rack-based node naming).

    Returns:
        NodeAnalysisResponse:
            Contains the computed node set, optionally grouped by rack, along with
            metadata about the expression and jobs queried.

    Raises:
        400 Bad Request:
            - Invalid expression syntax
            - Invalid group_by value
            - Expression references too many jobs (max 100)
        404 Not Found:
            - One or more job IDs not found in database
        500 Internal Server Error:
            - Database errors
            - Unexpected failures
    """
    # Validate group_by parameter
    if group_by not in ["rack", "none"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid group_by value: '{group_by}'. Must be 'rack' or 'none'.",
        )

    try:
        # Parse the expression
        ast = parser.parse_expression(expr)
        job_ids = parser.extract_job_ids_from_ast(ast)

        logger.info(f"Node analysis request: expr='{expr}', group_by='{group_by}', jobs_count={len(job_ids)}")

        # Convert job_id/attempt_index strings to tuples for storage query
        job_runs = []
        for job_str in job_ids:
            try:
                job_id, attempt_index_str = job_str.split("/")
                job_runs.append((job_id, int(attempt_index_str)))
            except (ValueError, IndexError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid job attempt ID format: '{job_str}'. Expected 'job_id/attempt_index'.",
                )

        # Fetch job node lists from storage
        try:
            job_nodes = job_manager.storage.get_allocated_nodes_batch(job_runs)  # type: ignore[union-attr]
        except RecoveryStorageInputValidationError as e:
            logger.warning(f"Storage input validation error: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            )
        except RecoveryStorageException as e:
            logger.exception("Storage error during node analysis", extra={"error": str(e)})
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch job records from storage.",
            ) from e

        # Check if any jobs are missing
        missing_jobs = [job_str for job_str in job_ids if not job_nodes.get(job_str)]
        if missing_jobs:
            logger.warning(f"Jobs not found: {missing_jobs}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job(s) not found: {', '.join(missing_jobs)}",
            )

        # Evaluate the expression
        result_nodes = operations.evaluate(ast, job_nodes)

        logger.debug(f"Node analysis result: {len(result_nodes)} nodes")

        # Optionally group by rack
        grouped_by_rack = None
        if group_by == "rack":
            grouped_by_rack = topology.group_by_rack(list(result_nodes))
            logger.debug(f"Grouped into {len(grouped_by_rack)} racks")

        return NodeAnalysisResponse(
            expression=expr,
            result=NodeAnalysisResult(
                nodes=sorted(result_nodes),
                count=len(result_nodes),
            ),
            grouped_by_rack=grouped_by_rack,
            jobs_queried=job_ids,
        )

    except HTTPException:
        # Re-raise HTTPExceptions (400/404 from validation/not found checks)
        raise
    except ParseError as e:
        logger.warning(f"Expression parse error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid expression: {e!s}",
        )
    except EvaluationError as e:
        logger.exception("Expression evaluation error", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to evaluate expression: {e!s}",
        ) from e
    except Exception as e:
        logger.exception("Unexpected error in node analysis", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during node analysis.",
        ) from e


@app.get("/error_attributions", operation_id="get_error_attributions")
def get_error_attributions(
    node_names: list[str] = Query(None),
    job_ids: list[str] = Query(None),
    start_timestamp: datetime | None = Query(None),
    end_timestamp: datetime | None = Query(None),
    anomaly_uuid: str | None = Query(None),
    limit: int | None = Query(None),
    offset: int | None = Query(None),
) -> NodeEvents:
    """
    Retrieve error attributions (both node-level and global/job-level) for specified nodes or jobs within a given time range.

    Parameters:
        node_names (List[str], optional):
            List of node names to filter node-level error events by. If not provided, includes all nodes.
            Note: This filter only applies to node-level events, not global events.
        job_ids (List[str], optional):
            List of job IDs to filter error events by. If not provided, includes all jobs.
        start_timestamp (Optional[datetime], optional):
            Start of the time range for filtering error events. If not provided, defaults to earliest available data.
        end_timestamp (Optional[datetime], optional):
            End of the time range for filtering error events. If not provided, defaults to latest available data.
        anomaly_uuid(Optional[str], optional):
            The job anomaly id to filter events by. If not provided, does not filter by anomaly id.
        limit (Optional[int], optional):
            Maximum number of error events to return for each type (node and global).
        offset (Optional[int], optional):
            Number of records to skip for pagination for each type (node and global).

    Returns:
        NodeEvents:
            A structured collection containing both node-level and global/job-level error events matching
            the specified filters. The item_count field reflects the total count of both types of events.
    """
    if not node_names and not job_ids and not start_timestamp and not end_timestamp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one filtering criteria must be provided.",
        )
    if not job_manager.attribution_storage:  # type: ignore[union-attr]
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Attribution storage is not initialized.",
        )
    try:
        node_events, item_count = job_manager.attribution_storage.read_attributions(  # type: ignore[union-attr]
            node_names=node_names,
            job_ids=job_ids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            anomaly_uuid=uuid.UUID(anomaly_uuid) if anomaly_uuid else None,
            limit=limit,  # type: ignore[arg-type]
            offset=offset,  # type: ignore[arg-type]
        )
        # Also fetch global events with same filters (except node_names since global events don't have nodes)
        global_events, global_item_count = job_manager.attribution_storage.read_global_attributions(  # type: ignore[union-attr]
            job_ids=job_ids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            anomaly_uuid=uuid.UUID(anomaly_uuid) if anomaly_uuid else None,
            limit=limit,  # type: ignore[arg-type]
            offset=offset,  # type: ignore[arg-type]
        )
        node_events_http, global_events_http = storage_to_http_attributions(node_events, global_events)
        return NodeEvents(
            item_count=item_count + global_item_count,
            events=node_events_http,
            global_events=global_events_http,
            attribution_insight="",
            attribution_summary="",
        )
    except ValueError as ve:
        logger.info(f"ValueError obtaining node error attributions: {ve!s}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as wse:
        logger.exception("Error obtaining node error attributions", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/job_error_attributions", operation_id="get_job_error_attributions")
def get_job_error_attributions(
    job_id: str,
    job_attempt_index: int | None = Query(None),
    anomaly_uuid: str | None = Query(None),
) -> NodeEvents:
    """
    Retrieve error attributions (both node-level and global/job-level) for a specific job.

    Parameters:
        job_id (str):
            The unique identifier of the job to inspect.
        job_attempt_index (Optional[int], optional):
            The specific attempt index of the job. If omitted, the latest (or most relevant)
            attempt is used.
        anomaly_uuid(Optional[str], optional):
            The job anomaly id to filter events by. If not provided, does not filter by anomaly id.

    Returns:
        NodeEvents:
            A structured collection containing both node-level and global/job-level error events
            attributed to the given job (and attempt, if provided). The item_count field reflects
            the total count of both types of events.
    """
    if not job_manager.attribution_storage:  # type: ignore[union-attr]
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Attribution storage is not initialized.",
        )
    try:
        # Fetch job record to get job state
        job_record = job_manager.storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)  # type: ignore[union-attr]
        job_state = job_record.state if job_record else None

        node_attributions = job_manager.attribution_storage.read_node_attributions_for_job(  # type: ignore[union-attr]
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            anomaly_uuid=uuid.UUID(anomaly_uuid) if anomaly_uuid else None,
        )
        node_item_count = len(node_attributions)

        # Also fetch global events for the same job
        global_attributions = job_manager.attribution_storage.read_global_attributions_for_job(  # type: ignore[union-attr]
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            anomaly_uuid=uuid.UUID(anomaly_uuid) if anomaly_uuid else None,
        )
        global_item_count = len(global_attributions)

        attribution_summary_config = job_manager.config["service"]["attribution_summary_config"]  # type: ignore[union-attr]
        attribution_insight_config = job_manager.config["service"]["attribution_insight_config"]  # type: ignore[union-attr]
        summary, insight = storage_attribution_to_summary(
            attribution_summary_config=attribution_summary_config,
            attribution_insight_config=attribution_insight_config,
            node_events=node_attributions,
            global_events=global_attributions,
            job_state=job_state,
        )

        logger.debug(f"Job {job_id} error attributions: summary {summary}, insight {insight}")

        node_events_http, global_events_http = storage_to_http_attributions(node_attributions, global_attributions)
        return NodeEvents(
            item_count=node_item_count + global_item_count,
            events=node_events_http,
            global_events=global_events_http,
            attribution_summary=summary,
            attribution_insight=insight,
        )
    except ValueError as ve:
        logger.info(f"ValueError obtaining node error attributions: {ve!s}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except RecoveryStorageException as wse:
        logger.exception("Error obtaining node error attributions", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/admin/users", operation_id="get_admin_users")
def get_users(
    job_states: list[str] | None = Query(default=None),
    limit: int = Query(default=25),
    offset: int = Query(default=0),
    desc: bool = Query(default=False),
) -> list[str]:
    """
    Retrieve a list of users filtered by optional job states, with pagination and sorting.

    Parameters:
        job_states (Optional[list[str]], optional):
            A list of job states to filter users by. If provided, only users with jobs in the specified states will be included.
        limit (int, default=25):
            The maximum number of users to return.
        offset (int, default=0):
            The number of users to skip, used for pagination.
        desc (bool, default=False):
            If True, the list is sorted in descending order.

    Returns:
        List[str]:
            A list of usernames matching the provided filters.
    """
    # TODO: Extract JWT token from the request header and validate requesters' admin status.
    try:
        users = job_manager.storage.read_users(job_states=job_states, limit=limit, offset=offset, descending=desc)  # type: ignore[union-attr]
        logger.debug(f"Users read from the database: {users}")
        return users
    except RecoveryStorageException as wse:
        logger.exception("Error obtaining users", extra={"error": str(wse)})
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(wse)) from wse


@app.get("/config/requeue", operation_id="get_requeue_job")
def get_system_requeue_config() -> SystemRequeueConfig:
    """
    Retrieve the system-wide requeue configuration.

    Returns:
        SystemRequeueConfig:
            An object containing configuration settings that define how and when jobs are automatically requeued
            by the system.
    """
    return SystemRequeueConfig(
        max_requeue_times=job_manager.get_system_max_requeue_times(),  # type: ignore[union-attr]
        checkpoint_based_requeue_stop_threshold=job_manager.get_checkpoint_based_requeue_stop_threshold(),  # type: ignore[union-attr]
        requeue_on_completion=job_manager.get_requeue_on_completion_config(),  # type: ignore[union-attr]
    )


@app.get("/guardrail_rules", operation_id="get_guardrail_rules")
def get_guardrail_rules() -> list[str]:
    """
    Retrieve the list of guardrail rules.
    :return:
    :rtype:
    """
    if not job_manager.config["service"].get("guardrail_rules"):  # type: ignore[union-attr]
        return []
    return job_manager.config["service"]["guardrail_rules"]  # type: ignore[union-attr]


def start_http_server(config: dict[str, Any]) -> None:
    uvicorn_log_config = uvicorn.config.LOGGING_CONFIG
    uvicorn_log_config["loggers"]["uvicorn.access"]["level"] = "WARNING"
    uvicorn.run(
        app,
        host=config["http"]["host"],
        port=config["http"]["port"],
        log_config=uvicorn_log_config,
    )


def load_service_config_from_path(path: str) -> dict[str, Any]:
    config = {}
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise RuntimeError(f"Cannot parse config file from '{path}'")

    return config


def validate_service_config(config: dict[str, Any]) -> None:
    if "service" not in config:
        raise RuntimeError("'Service' section must exist in the config. ")

    service_config = config["service"]
    try:
        AuditTrailSettings.model_validate(service_config.get("audit_trail", {}))
    except ValidationError as error:
        raise RuntimeError(f"Invalid audit trail configuration: {error}") from error

    if "guardrail_rules" not in service_config or service_config["guardrail_rules"] is None:
        raise RuntimeError("'Guardrail rules' section must exist in the config. ")

    if "event_bus" not in service_config or "storage" not in service_config or "http" not in service_config:
        raise RuntimeError("'storage', 'event_bus' and 'http' are mandatory sections in the service config.'")

    if service_config["storage"].get("sql"):
        sql_config = service_config["storage"]["sql"]
        mandatory_fields = ["host", "port", "username", "database_name"]
        all_present = all(key in sql_config for key in mandatory_fields)
        if not all_present:
            raise RuntimeError(
                f"'{', '.join(mandatory_fields)}' are mandatory fields in the 'sql' section of the storage config, "
                "one or more are missing."
            )

    if service_config.get("anomaly_queue_delivery_retry_delay_secs", 0) > 0 and (
        not service_config.get("anomaly_dead_letter_routing_key")
        or not service_config.get("anomaly_dead_letter_redelivery_routing_key")
    ):
        raise RuntimeError(
            "'anomaly_dead_letter_routing_key' and 'anomaly_dead_letter_redelivery_routing_key' must be specified "
            "when 'anomaly_queue_delivery_retry_delay_secs' is greater than 0."
        )

    if (
        (system_max_requeue_times := service_config.get("system_max_requeue_times"))
        and system_max_requeue_times <= 0
        and system_max_requeue_times != INFINITE_SYSTEM_REQUEUE_LIMIT
    ):
        raise RuntimeError(
            f"{system_max_requeue_times} is not a valid value for system_max_requeue_times field, it must be "
            "greater than 0 for finite requeue times or -1 for infinite requeue."
        )

    if service_config.get("job_state_based_auto_resume_time_window_secs") or service_config.get(
        "job_state_based_auto_resume_threshold_secs"
    ):
        time_window = service_config.get("job_state_based_auto_resume_time_window_secs")
        threshold = service_config.get("job_state_based_auto_resume_threshold_secs")

        if not time_window or not threshold:
            raise RuntimeError(
                "'job_state_based_auto_resume_time_window_secs' and 'job_state_based_auto_resume_threshold_secs' "
                "must be both set for the state based auto resume feature to work."
            )

        time_window = service_config["job_state_based_auto_resume_time_window_secs"]
        threshold = service_config["job_state_based_auto_resume_threshold_secs"]

        if time_window <= 0:
            raise RuntimeError("'job_state_based_auto_resume_time_window_secs' must be greater than 0.")

        if threshold <= 0:
            raise RuntimeError("'job_state_based_auto_resume_threshold_secs' must be greater than 0.")

        if time_window <= threshold:
            raise RuntimeError(
                "'job_state_based_auto_resume_time_window_secs' must be greater than "
                "'job_state_based_auto_resume_threshold_secs'."
            )


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}. Stopping job manager.")
    shutdown_event.set()
    job_manager.stop()
    logger.info("Job manager stopped.")
    sys.exit(0)


@app.get("/.well-known/manifest.json")
def manifest():
    return {
        "name": "Heimdall Job Manager",
        "short_name": "Job Manager",
        "version": "1.0.0",
        "description": "Job manager",
    }


mcp = FastApiMCP(
    app,
    name="Job Manager MCP Server",
    description="Job Manager MCP Server",
    include_operations=[
        "get_job_status",
        "get_job_anomalies",
        "get_jobs",
        "get_node_analysis",
        "get_error_attributions",
        "get_job_error_attributions",
        "get_workflows",
        "get_advice",
        "get_admin_users",
        "get_guardrail_rules",
    ],
)

mcp.mount_http()
mcp.mount_sse()  # SSE is going to be deprecated, leave it here for some legacy tools
mcp.setup_server()


def main():
    global job_manager

    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame))
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame))

    config_path = os.environ.get(CONFIG_FILE_ENV_VAR_NAME, DEFAULT_CONFIG_FILE_PATH)
    config = load_service_config_from_path(config_path)
    logger.info(f"Service config: {config}.")
    validate_service_config(config)

    set_up_trace_provider(service_name="nvidia-recovery-job-manager")

    audit_settings = AuditTrailSettings.model_validate(config["service"].get("audit_trail", {}))
    audit_buffer: AuditBuffer[ProducerEvent] = create_audit_buffer(
        settings=audit_settings,
        logger=logger.getChild("audit"),
    )

    job_manager = JobManager(config, shutdown_event, audit_buffer=audit_buffer)

    try:
        audit_buffer.start()

        job_manager.start_consuming()
        job_manager.start_background_jobs()
        logger.info("Starting http server.")
        http_thread = threading.Thread(target=lambda: start_http_server(config["service"]))
        http_thread.start()
        http_thread.join()
    finally:
        audit_buffer.stop()


def dump_schema():
    json.dump(
        get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
            openapi_version="3.1.0",
        ),
        sys.stdout,
        indent=4,
    )


if __name__ == "__main__":
    main()
