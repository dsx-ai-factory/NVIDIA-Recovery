# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError
from recovery_libs.advice import build_terminal_advice, check_max_requeue_reached, ensure_slurm_scheduler_metadata
from recovery_libs.audit import AuditBuffer, ProducerEvent
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.aps_spec.spec import AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceRecommendation,
    AdviceState,
    DagDependencyReasonCodeEnum,
    DagDependencyResponse,
    GlobalEvent,
    Issue,
    JobExecutionError,
    JobTerminalAdvice,
    NodeEvent,
    RecoveryContext,
    RecoveryOperation,
    RecoveryTriggerType,
    SlurmJobExecutionStatus,
    SortOrderEnum,
    SystemRequeueConfig,
)
from recovery_libs.schema.watcher.model import Event
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.base import BaseStorage, RecoveryStorageException
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    DagDependency,
    SlurmJobExecutionRecord,
    SlurmJobFinalStates,
    SlurmJobNonFinalStates,
)
from recovery_libs.storage.global_events import GlobalEvent as GlobalEventStorage
from recovery_libs.storage.mysql_storage import AdviceIssueMerge, MySQLStorage, UpsertAdviceResult, UpsertJobResult
from recovery_libs.storage.node_events import ErrorAttribution, ErrorAttributionType, ExclusionType
from recovery_libs.storage.node_events import NodeEvent as NodeEventStorage
from recovery_libs.tracing.traces import restore_trace_context
from recovery_libs.utils.time import parse_event_timestamp

from job_manager.advice_mapping import (
    http_to_storage_advice,
    storage_to_http_advice,
)
from job_manager.audit.advice_changes import audit_advice_upsert
from job_manager.audit.job_changes import (
    audit_job_update,
    audit_job_upsert,
)
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.logger import logger
from job_manager.recovery import RecoveryService

# Set of non-final job states for efficient lookup
NON_FINAL_JOB_STATES = {s.value for s in SlurmJobNonFinalStates}

_DAG_READ_PAGE_SIZE = 50
_DAG_RELEASE_AUDIT_REASON = "DAG_DEPENDENCIES_SATISFIED"
_TRACER = trace.get_tracer(__name__)


def persist_advice_record(
    storage: MySQLStorage,
    audit_buffer: AuditBuffer[ProducerEvent],
    advice_record: AdviceRecord,
    *,
    audit_reason: str | None = None,
    issue_merge: AdviceIssueMerge = AdviceIssueMerge.REPLACE,
) -> UpsertAdviceResult:
    """Persist and audit Advice under its durable recovery trace context.

    Args:
        storage: Storage backend that commits the Advice record.
        audit_buffer: Buffer that receives the committed semantic transition.
        advice_record: Advice storage model, optionally carrying recovery context.
        audit_reason: Optional additional reason attached to the audit event.
        issue_merge: How storage combines the record's Issues with the stored Issues
            while it holds the row lock. Internal writers that build the record from an
            earlier read must merge, so they keep Issues written by concurrent writers.

    Returns:
        The persisted record, whether it was created, and its prior value.
    """
    # All audited upserts share this fallback, including DAG descendant
    # termination. Correlation failure must never prevent committing Advice.
    operation_by_state: dict[str | None, RecoveryOperation] = {
        "Restarted": RecoveryOperation.RESTART,
        "Terminated": RecoveryOperation.TERMINATE,
        "Released": RecoveryOperation.DAG_RELEASE,
    }
    operation = operation_by_state.get(advice_record.advice_state)
    if operation is None or advice_record.recovery_operation != operation.value:
        # An Advice generation can change operation. Its old context must not
        # parent the new operation span before storage selects the new sequence.
        advice_record.recovery_sequence_id = None
        advice_record.recovery_traceparent = None
        advice_record.recovery_tracestate = None
        advice_record.recovery_operation = None
        advice_record.recovery_triggered_at = None
    if advice_record.recovery_sequence_id is None and operation is not None:
        try:
            job = storage.read_job_record(job_id=advice_record.job_id, job_attempt_index=advice_record.attempt_index)
            if isinstance(job, SlurmJobExecutionRecord):
                dependency_decision = (
                    operation == RecoveryOperation.DAG_RELEASE or audit_reason == "DAG_PARENT_TERMINATED"
                )
                anomalies = job.anomalies or []
                trigger = anomalies[0] if anomalies and not dependency_decision else None
                recovery = RecoveryService(storage).begin_for_job(
                    job,
                    operation=operation,
                    scheduler_metadata=advice_record.scheduler_metadata,
                    trigger_type=(
                        RecoveryTriggerType.DEPENDENCY
                        if dependency_decision
                        else RecoveryTriggerType.ANOMALY
                        if trigger is not None
                        else RecoveryTriggerType.JOB_STATE
                    ),
                    trigger_identifier=str(trigger.uuid) if trigger is not None and trigger.uuid else None,
                    triggered_at=(
                        None if dependency_decision else trigger.timestamp if trigger is not None else job.end_time
                    ),
                )
                advice_record.recovery_sequence_id = uuid.UUID(recovery.sequence_id)
                advice_record.recovery_traceparent = recovery.traceparent
                advice_record.recovery_tracestate = recovery.tracestate
                advice_record.recovery_operation = recovery.operation.value
                advice_record.recovery_triggered_at = recovery.triggered_at
        except Exception:
            logger.exception("Failed to obtain recovery context; persisting Advice without correlation.")
    parent_context = (
        restore_trace_context(advice_record.recovery_traceparent, advice_record.recovery_tracestate)
        if advice_record.recovery_traceparent is not None
        else None
    )
    attributes: dict[str, Any] = {
        "job.id": advice_record.job_id,
        "job.attempt.source": advice_record.attempt_index,
        "advice.id": str(advice_record.heimdall_id),
    }
    if advice_record.recovery_sequence_id is not None:
        attributes["recovery.sequence.id"] = str(advice_record.recovery_sequence_id)
    if advice_record.recovery_operation is not None:
        attributes["recovery.operation"] = advice_record.recovery_operation

    with _TRACER.start_as_current_span(
        "job.recovery.advice",
        context=parent_context,
        attributes=attributes,
    ) as span:
        try:
            result = storage.upsert_advice(advice_record, issue_merge=issue_merge)
            audit_advice_upsert(
                result,
                audit_buffer,
                additional_reason=audit_reason,
                operation_span=span,
            )
        except Exception as error:
            span.set_attribute("recovery.outcome", "failed")
            span.set_status(Status(StatusCode.ERROR, str(error)))
            raise
        span.set_attribute("advice.id", str(result[0].heimdall_id))
        span.set_attribute("advice.generation", result[0].generation)
        span.set_attribute("recovery.outcome", "applied" if result[1] else "skipped")
        return result


def persist_job_record(
    storage: MySQLStorage,
    audit_buffer: AuditBuffer[ProducerEvent],
    job_record: SlurmJobExecutionRecord,
    *,
    populate_heimdall_id_log_path: bool = False,
) -> UpsertJobResult:
    """Persist a Job attempt and audit its committed approved projection."""
    result = storage.upsert_job_record(
        job_record,
        populate_heimdall_id_log_path=populate_heimdall_id_log_path,
    )
    audit_job_upsert(result, audit_buffer)
    return result


def update_job_record(
    storage: MySQLStorage,
    audit_buffer: AuditBuffer[ProducerEvent],
    job_id: str,
    job_attempt_index: int | None,
    **updates: Any,
) -> bool:
    """Apply a partial Job update and audit its committed transition."""
    result = storage.update_job_record(
        job_id=job_id,
        job_attempt_index=job_attempt_index,
        **updates,
    )
    audit_job_update(result, audit_buffer)
    # Preserve the existing Job Manager contract: a completed storage call is
    # successful even when its UPDATE matched no record.
    return True


def storage_to_http_attributions(
    node_events: Sequence[NodeEventStorage], global_events: Sequence[GlobalEventStorage]
) -> tuple[Sequence[NodeEvent], Sequence[GlobalEvent]]:
    """
    Convert storage attributions (NodeEvents and GlobalEvents) to HTTP models.

    Args:
        node_events: Sequence of NodeEventStorage objects
        global_events: Sequence of GlobalEventStorage objects

    Returns:
        Tuple of (node_events_http, global_events_http)
    """
    node_events_http = []
    for event in node_events:
        error = event.job_execution_error

        attribution = NodeEvent(
            timestamp=event.timestamp,
            job_id=event.job_id,
            job_attempt_index=event.job_attempt_index,
            job_start_time=event.job_start_time,
            job_end_time=event.job_end_time,
            job_execution_error=JobExecutionError(
                code=error.code, message=error.message, description=error.description, signature=error.signature
            ),
            cluster=event.cluster,
            node=event.node,
            exclusion_type=event.exclusion_type.name,
            original_exclusion_type=event.original_exclusion_type.name,
            error_attribution=event.error_attribution.name,
            error_attribution_type=event.error_attribution_type.name,
            error_attribution_origin=event.error_attribution_origin.name,
            error_attribution_sources=[source.name for source in event.error_attribution_sources],
            fact_status=event.fact_status.name,
        )

        node_events_http.append(attribution)

    global_events_http = []
    for event in global_events:  # type: ignore[assignment]
        error = event.job_execution_error

        attribution = GlobalEvent(  # type: ignore[assignment]
            timestamp=event.timestamp,
            job_id=event.job_id,
            job_attempt_index=event.job_attempt_index,
            job_execution_error=JobExecutionError(
                code=error.code, message=error.message, description=error.description, signature=error.signature
            ),
            error_attribution=event.error_attribution.name,
            error_attribution_type=event.error_attribution_type.name,
            error_attribution_origin=event.error_attribution_origin.name,
            error_attribution_sources=[source.name for source in event.error_attribution_sources],
            fact_status=event.fact_status.name,
        )

        global_events_http.append(attribution)

    return (node_events_http, global_events_http)  # type: ignore[return-value]


def storage_attribution_to_summary(
    attribution_summary_config: dict[str, Any],
    attribution_insight_config: dict[str, Any],
    node_events: Sequence[NodeEventStorage],
    global_events: Sequence[GlobalEventStorage],
    job_state: str | None = None,
) -> tuple[str, str]:
    """
    Convert a sequence of storage attributions to a summary and insight string.

    Args:
        attribution_summary_config: Configuration for summary generation
        attribution_insight_config: Configuration for insight generation
        node_events: Sequence of node-level attribution events
        global_events: Sequence of global/job-level attribution events
        job_state: Optional job state (e.g., "RUNNING", "COMPLETED"). If the job is in a
                   non-final state, returns "Unknown" for both summary and insight.

    Returns:
        Tuple of (summary, insight) strings
    """
    summary = "Unknown"
    insight = "Unknown"

    # Early return for running jobs
    if job_state and job_state in NON_FINAL_JOB_STATES:
        logger.debug(f"Job state '{job_state}' is non-final, returning default summary and insight")
        return summary, insight

    if not node_events and not global_events:
        logger.debug("No node or global events found for job in database")
        return summary, insight

    # Find node event with maximum exclusion type value (if node events exist)
    max_exclusion_event = None
    if node_events:
        max_exclusion_event = max(node_events, key=lambda x: x.exclusion_type.value if x.exclusion_type else 0)

    is_hardware_error = any(node_event.error_attribution == ErrorAttribution.HARDWARE for node_event in node_events)

    # TODO: validate this logic
    is_potential_hardware_error = any(
        node_event.error_attribution != ErrorAttribution.APPLICATION for node_event in node_events
    ) or any(global_event.error_attribution != ErrorAttribution.APPLICATION for global_event in global_events)

    if is_hardware_error:
        summary = attribution_summary_config["infra"]["summary"]
    elif is_potential_hardware_error:
        summary = attribution_summary_config["potential_infra"]["summary"]
    else:
        summary = attribution_summary_config["not_infra"]["summary"]

    # Insight is based on exclusion type (only applicable for node events)
    if max_exclusion_event:
        if max_exclusion_event.exclusion_type == ExclusionType.CONFIRMED:
            insight = attribution_insight_config["confirmed"]["insight"]
        elif max_exclusion_event.exclusion_type == ExclusionType.TEMPORARY:
            insight = attribution_insight_config["temporary"]["insight"]
        elif max_exclusion_event.exclusion_type == ExclusionType.NONE:
            insight = attribution_insight_config["none"]["insight"]
        else:
            logger.warning(f"Unknown exclusion type: {max_exclusion_event.exclusion_type}")
    else:
        # No node events, only global events - use "none" insight as default
        insight = attribution_insight_config["none"]["insight"]

    return summary, insight


def populate_sticky_exclusion_node_list(
    job_record_storage: MySQLStorage,
    attribution_storage: AttributionStorage,
    job_update: SlurmJobExecutionRecord,
    known_job_record: SlurmJobExecutionRecord,
) -> None:
    """
    This function populates the sticky exclusion node list for a job attempt from the most recent finished job attempt
    and the confirmed bad nodes identified in the anomaly attribution results of such attempt.

    When there's no previous finished job attempt, the exclusion node list from the current job attempt will be
    preserved in the sticky exclusion node list to ensure that the exclusion node list from user input is not lost.
    """

    # Job managed by requeue. It's guaranteed to only have 1 job attempt active at any given time.
    if known_job_record.aps_spec and known_job_record.aps_spec.auto_resume_mode == AutoResumeModeEnum.REQUEUE:
        if job_update.state == SlurmJobNonFinalStates.PENDING:
            logger.debug(
                f"Not populating sticky exclusion node list for job {job_update.job_id}, attempt "
                f"{job_update.job_attempt_index} because it is still in PENDING state."
            )
            return

        # When we are in this function, current attempt is already persisted in the database and will be returned in the
        # query result. So we need to query the last 2 attempts ordered by attempt index in descending order to find the
        # most recent earlier attempt and use its sticky exclusion node list.
        job_attempts, _ = job_record_storage.read_job_records(
            job_id=job_update.job_id,
            sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
            limit=2,
        )
        earlier_job_attempts = [
            attempt for attempt in job_attempts if attempt.job_attempt_index < job_update.job_attempt_index
        ]

        if not earlier_job_attempts:
            # When no earlier job attempts are found, the current job is the head of the workflow, the
            # exclusion_node_list came from user input and should be preserved in the sticky exclusion node list.
            logger.info(
                f"No earlier job attempts found for job {job_update.job_id}, attempt {job_update.job_attempt_index}, "
                "populating sticky exclusion node list with the current exclusion node list "
                f"'{job_update.exclusion_node_list}'."
            )
            job_update.sticky_exclusion_node_list = job_update.exclusion_node_list
            return

        # Carry over the sticky node list from the previous attempt.
        job_update.sticky_exclusion_node_list = earlier_job_attempts[0].sticky_exclusion_node_list or []
        # Find the confirmed bad nodes from the attribution result of the preivous attempt and include them as well.
        node_events = attribution_storage.read_node_attributions_for_job(
            job_id=job_update.job_id, job_attempt_index=earlier_job_attempts[0].job_attempt_index
        )
        job_update.sticky_exclusion_node_list.extend(
            [node_event.node for node_event in node_events if node_event.exclusion_type == ExclusionType.CONFIRMED]
        )
    else:  # Job managed by singleton dependency. Mulitple jobs with the same name can exist at the same time.
        if job_update.state == SlurmJobNonFinalStates.PENDING:
            logger.debug(
                f"Not populating sticky exclusion node list for job {job_update.job_id}, attempt {job_update.job_attempt_index} "
                "because it is still in PENDING state."
            )
            return

        # Query the last 5 job attempts that are finished and sort them by their end_time in descending order.
        finished_job_attempts, _ = job_record_storage.read_job_records(
            job_name=job_update.job_name,
            states=[s.value for s in SlurmJobFinalStates],
            sort_by=[("end_time", SortOrderEnum.DESC)],
            limit=1,
        )

        if not finished_job_attempts:
            # When no finished job attempts are found, the current job is the head of the workflow, the
            # exclusion_node_list came from user input and should be preserved in the sticky exclusion node list.
            job_update.sticky_exclusion_node_list = job_update.exclusion_node_list
            return

        # Carry over the sticky node list from the previous attempt.
        job_update.sticky_exclusion_node_list = finished_job_attempts[0].sticky_exclusion_node_list or []
        # Find the confirmed bad nodes from the attribution result of the preivous attempt and include them as well.
        node_events = attribution_storage.read_node_attributions_for_job(
            job_id=finished_job_attempts[0].job_id, job_attempt_index=finished_job_attempts[0].job_attempt_index
        )
        job_update.sticky_exclusion_node_list.extend(
            [node_event.node for node_event in node_events if node_event.exclusion_type == ExclusionType.CONFIRMED]
        )

        return


def sanitize_log_file_paths_for_retry(job_record: SlurmJobExecutionRecord, storage: MySQLStorage) -> None:
    """
    Determine whether to persist log file paths for the job record. By default, only persist paths that are not already
    associated with the previous attempt. This prevents a requeue race where Slurm returns paths from the prior run.
    Preserve reused paths when the workload explicitly declares that its attempts share log files.

    Args:
        job_record: The current job record to be updated.
        storage: The MySQLStorage instance for database operations.

    Returns:
        None, the update is done in place.
    """
    if job_record.job_attempt_index == 0:
        # First attempt, no need to validate
        return

    if job_record.aps_spec and job_record.aps_spec.log_file_paths_shared_across_attempts:
        # The workload explicitly declares that the same file belongs to every
        # retry. Retain it for display, but do not use its name as attempt
        # identity when correlating events.
        return

    previous_attempt_index = job_record.job_attempt_index - 1
    previous_job_attempt = storage.read_job_record(
        job_id=job_record.job_id,
        job_attempt_index=previous_attempt_index,
    )

    if not previous_job_attempt:
        logger.info(f"No previous job attempt found for job {job_record.job_id} ({job_record.job_attempt_index}).")
        return

    # Drop only paths shared with the previous attempt (e.g. the stable batch
    # StdOut slurm-<jobid>.out), keeping unique per-attempt paths.
    previous_paths = set(previous_job_attempt.log_file_paths or [])
    current_paths = job_record.log_file_paths or []
    deduped = [path for path in current_paths if path not in previous_paths]

    if len(deduped) != len(current_paths):
        dropped = [path for path in current_paths if path in previous_paths]
        logger.warning(
            f"Dropping log file path(s) {dropped} carried over from previous job attempt "
            f"({previous_attempt_index}) for job {job_record.job_id} ({job_record.job_attempt_index}) "
            "to prevent a requeue race condition; keeping the per-attempt path(s)."
        )
    job_record.log_file_paths = deduped or None

    return


def create_advice_issue_from_event(
    storage: MySQLStorage,
    event: Event,
    job_record: SlurmJobExecutionRecord,
    timestamp_parser: Callable[[Event], datetime | None] | None = None,
    detected_by: str = "Heimdall Watcher",
    on_advice_persisted: Callable[[AdviceRecord, str | None], None] | None = None,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Persist one causal Issue, propagating storage failures so existing processing can retry."""
    try:
        issue_timestamp = timestamp_parser(event) if timestamp_parser else None

        if issue_timestamp is None:
            logger.warning(
                f"Skipping advice issue due to missing timestamp for job {job_record.job_id} "
                f"attempt {job_record.job_attempt_index}"
            )
            return

        # The in-memory job_record may not have heimdall_job_id (e.g., from_json() RabbitMQ path).
        # Read from DB to get the canonical value.
        heimdall_job_id = job_record.heimdall_job_id
        if heimdall_job_id is None:
            db_record = storage.read_job_record(
                job_id=job_record.job_id,
                job_attempt_index=job_record.job_attempt_index,
            )
            heimdall_job_id = db_record.heimdall_job_id if db_record else None

        if heimdall_job_id is None:
            raise ValueError(
                f"Cannot create advice for job {job_record.job_id} attempt {job_record.job_attempt_index}: "
                f"heimdall_job_id is not set on the job record"
            )
        advice_id = heimdall_job_id
        existing = storage.read_advice(heimdall_id=advice_id)
        existing_issues = existing.issues if existing else None

        issue = Issue(
            issue_type=event.type.value,
            timestamp=issue_timestamp,
            detected_by=detected_by,
            relevant_log_lines=[event.body] if event.body else None,
        )

        if existing_issues:
            for existing_issue in existing_issues:
                if (
                    existing_issue.issue_type == issue.issue_type
                    and existing_issue.timestamp == issue.timestamp
                    and existing_issue.detected_by == issue.detected_by
                ):
                    return

        if existing:
            advice = storage_to_http_advice(existing)
            advice.issues = (existing_issues or []) + [issue]
        else:
            advice = Advice(
                id=str(advice_id),
                visible=False,
                job_id=job_record.job_id,
                restart_count=job_record.job_attempt_index,
                scheduler_metadata=job_record.scheduler_metadata or {},
                advice=AdviceRecommendation(state=None, node_constraints=None),
                issues=[issue],
            )

        advice_record = http_to_storage_advice(advice)
        # The read above is not locked. Job Manager replicas and the repair loop can add
        # Issues or decide the advice concurrently, so storage appends this Issue under
        # the row lock and leaves the rest of any committed advice unchanged.
        upsert_result = persist_advice_record(
            storage, audit_buffer, advice_record, issue_merge=AdviceIssueMerge.APPEND_ONLY
        )
        if on_advice_persisted is not None:
            on_advice_persisted(upsert_result[0], None)
    except Exception as e:
        logger.exception(
            f"Failed to persist advice issue for event job {job_record.job_id}",
            extra={"job_id": job_record.job_id, "error": str(e)},
        )
        if isinstance(e, RecoveryStorageException):
            raise


def create_advice_issues_from_anomalies(
    storage: MySQLStorage,
    job_record: SlurmJobExecutionRecord,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    on_advice_persisted: Callable[[AdviceRecord, str | None], None] | None = None,
) -> None:
    """Repair missing causal Issues from committed anomalies without regenerating their timestamps."""
    for anomaly in job_record.anomalies or []:
        event = None
        generated = not anomaly.original_watcher_anomaly_event
        if anomaly.original_watcher_anomaly_event:
            try:
                event = Event.model_validate_json(anomaly.original_watcher_anomaly_event)
                generated = bool(
                    event.attributes and (event.attributes.model_extra or {}).get("job_manager_generated") is True
                )
            except ValidationError:
                logger.warning(
                    "Invalid stored event for anomaly %s; using its persisted type and timestamp.", anomaly.uuid
                )
        if event is None:
            try:
                event = Event(type=EventType(anomaly.anomaly_type), timestamp=anomaly.timestamp.isoformat())
            except ValueError:
                logger.warning(
                    "Skipping unsupported stored anomaly type %s for job %s.", anomaly.anomaly_type, job_record.job_id
                )
                continue
        create_advice_issue_from_event(
            storage,
            event,
            job_record,
            timestamp_parser=parse_event_timestamp,
            detected_by="Heimdall Job Manager" if generated else "Heimdall Watcher",
            audit_buffer=audit_buffer,
            on_advice_persisted=on_advice_persisted,
        )


GetPreviousAttemptStatus = Callable[[str, int], SlurmJobExecutionStatus | None]
GetJobsBatch = Callable[..., list[SlurmJobExecutionStatus]]
# Invoked after a terminal advice is persisted, so callers (the service) can emit the
# matching dashboard notification. Kept as a callback so this storage layer stays free of
# the notifications client.
TerminalAdviceCallback = Callable[[SlurmJobExecutionRecord, JobTerminalAdvice], None]
AdvicePersistedCallback = Callable[[AdviceRecord, str | None], None]
AdviceReadyCallback = Callable[[Advice], RecoveryContext | None]


def _read_existing_advice_for_terminal_event(
    storage: MySQLStorage,
    *,
    job_id: str,
    job_attempt_index: int,
    status: SlurmJobExecutionStatus | None,
) -> Advice | None:
    effective_job_id = status.job_id if status is not None else job_id
    effective_attempt_index = status.job_attempt_index if status is not None else job_attempt_index

    existing_record = storage.read_advice_by_job(job_id=effective_job_id, attempt_index=effective_attempt_index)
    if existing_record is None:
        return None
    return storage_to_http_advice(existing_record)


def _read_terminated_advice(storage: MySQLStorage, *, job_id: str, job_attempt_index: int) -> AdviceRecord | None:
    """Return the attempt's Advice record only when it is already Terminated.

    Terminated Advice is sticky (storage rejects replacing it with any other
    state), so terminal-advice writers check this first. A late or out-of-order
    signal for an attempt that was already terminated is then an expected no-op
    instead of a rejected write.
    """
    existing_record = storage.read_advice_by_job(job_id=job_id, attempt_index=job_attempt_index)
    if existing_record is not None and existing_record.advice_state == AdviceState.TERMINATED.value:
        return existing_record
    return None


def create_terminal_advice_for_job(
    storage: MySQLStorage,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    job_id: str,
    job_attempt_index: int,
    terminal_advice: JobTerminalAdvice,
    audit_reason: str | None = None,
    status: SlurmJobExecutionStatus | None = None,
    anomaly_attribution: AnomalyAttributionResult | None = None,
    exclusion_stickiness_enabled: bool = False,
    get_previous_attempt: GetPreviousAttemptStatus | None = None,
    on_advice_persisted: AdvicePersistedCallback | None = None,
    on_advice_ready: AdviceReadyCallback | None = None,
) -> JobTerminalAdvice | None:
    """Build and persist terminal advice, recording its cause once per job attempt.

    The causal Issue is intentionally retained even for a scheduler-originated
    terminal decision: DAG descendants use it as durable provenance when they
    are cancelled because of this parent.
    """
    try:
        existing_advice = _read_existing_advice_for_terminal_event(
            storage,
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            status=status,
        )
        advice_payload = build_terminal_advice(
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            terminal_advice=terminal_advice,
            status=status,
            anomaly_attribution=anomaly_attribution,
            existing_advice=existing_advice,
            exclusion_stickiness_enabled=exclusion_stickiness_enabled,
            get_previous_attempt=get_previous_attempt,
        )
        # Stamp scheduler=slurm so the controller treats JM-authored advice as owned
        # even before it registers the job. TODO(multi-scheduler): derive per-job / drop
        # once ordering is fixed -- see TD-20260706-advice-scheduler-metadata-hardcoded-slurm.
        advice_payload.scheduler_metadata = ensure_slurm_scheduler_metadata(advice_payload.scheduler_metadata)
        if advice_payload.advice and advice_payload.advice.state == AdviceState.TERMINATED:
            terminal_reason_issue = Issue(
                issue_type=terminal_advice.value,
                timestamp=datetime.now(UTC),
                detected_by="Heimdall Job Manager",
            )
            if not any(
                issue.issue_type == terminal_reason_issue.issue_type
                and issue.detected_by == terminal_reason_issue.detected_by
                for issue in advice_payload.issues or []
            ):
                advice_payload.issues = [*(advice_payload.issues or []), terminal_reason_issue]
        exclusion_nodes = (
            set(advice_payload.advice.node_constraints.exclusions)
            if advice_payload.advice.node_constraints and advice_payload.advice.node_constraints.exclusions
            else set()
        )
        logger.info(
            f"Persisting terminal advice for job {advice_payload.job_id} attempt {advice_payload.restart_count}: "
            f"advice={terminal_advice}, exclusion_nodes={sorted(exclusion_nodes)}"
        )
        if on_advice_ready is not None:
            try:
                recovery_context = on_advice_ready(advice_payload)
                advice_payload.recovery_context = (
                    recovery_context if isinstance(recovery_context, RecoveryContext) else None
                )
            except Exception as error:
                logger.exception(
                    "Failed to attach recovery context to Advice %s; persisting without correlation",
                    advice_payload.id,
                    extra={"advice_id": advice_payload.id, "error": str(error)},
                )
        upsert_result = persist_advice_record(
            storage,
            audit_buffer,
            http_to_storage_advice(advice_payload),
            audit_reason=audit_reason,
            # The payload's Issues come from an unlocked read. Keep Issues that a
            # concurrent watcher event committed after that read.
            issue_merge=AdviceIssueMerge.UNION,
        )
        if on_advice_persisted is not None:
            on_advice_persisted(upsert_result[0], terminal_advice.value)
        return terminal_advice
    except Exception as error:
        logger.exception(
            f"Failed to persist terminal advice for job {job_id} attempt {job_attempt_index}",
            extra={"job_id": job_id, "job_attempt_index": job_attempt_index, "error": str(error)},
        )
        return None


def maybe_persist_requeue_terminal_advice(
    storage: MySQLStorage,
    job_record: SlurmJobExecutionRecord,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    system_requeue_config: SystemRequeueConfig,
    get_jobs: GetJobsBatch,
    on_persisted: TerminalAdviceCallback | None = None,
    on_advice_persisted: AdvicePersistedCallback | None = None,
    allow_explicit_dag_abort: bool = False,
    on_advice_ready: AdviceReadyCallback | None = None,
) -> None:
    """Write USER_CANCELED or MAX_REQUEUE_REACHED Advice from observed job state.

    ``allow_explicit_dag_abort`` preserves cancellation/stop intent for a DAG
    parent that may be referenced by a child registered after this observation.
    """
    status = storage_to_http_job_status(job_record)
    existing_record = _read_terminated_advice(
        storage,
        job_id=status.job_id,
        job_attempt_index=status.job_attempt_index,
    )
    if existing_record is not None:
        if on_advice_persisted is not None:
            on_advice_persisted(existing_record, None)
        return

    is_requeue_managed = bool(
        job_record.aps_spec and job_record.aps_spec.auto_resume_mode == AutoResumeModeEnum.REQUEUE
    )
    is_explicit_abort = status.state == SlurmJobFinalStates.CANCELLED.value or (
        allow_explicit_dag_abort and status.state == SlurmJobFinalStates.STOPPED.value
    )
    if is_explicit_abort and (is_requeue_managed or allow_explicit_dag_abort):
        written = create_terminal_advice_for_job(
            storage,
            audit_buffer=audit_buffer,
            job_id=job_record.job_id,
            job_attempt_index=job_record.job_attempt_index,
            terminal_advice=JobTerminalAdvice.USER_CANCELED,
            status=status,
            on_advice_persisted=on_advice_persisted,
            on_advice_ready=on_advice_ready,
        )
        if written is not None and on_persisted is not None:
            on_persisted(job_record, written)
        return

    if not is_requeue_managed:
        return

    requeue_limit_reached, requeue_reason = check_max_requeue_reached(
        status,
        system_requeue_config=system_requeue_config,
        get_jobs=get_jobs,
    )
    if requeue_limit_reached:
        written = create_terminal_advice_for_job(
            storage,
            audit_buffer=audit_buffer,
            job_id=job_record.job_id,
            job_attempt_index=job_record.job_attempt_index,
            terminal_advice=JobTerminalAdvice.MAX_REQUEUE_REACHED,
            audit_reason=requeue_reason,
            status=status,
            on_advice_persisted=on_advice_persisted,
            on_advice_ready=on_advice_ready,
        )
        if written is not None and on_persisted is not None:
            on_persisted(job_record, written)


def maybe_persist_job_released_terminal_advice_from_attribution(
    storage: MySQLStorage,
    attribution: AnomalyAttributionResult,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    exclusion_stickiness_enabled: bool,
    get_previous_attempt: GetPreviousAttemptStatus,
    system_requeue_config: SystemRequeueConfig,
    get_jobs: GetJobsBatch,
    on_persisted: TerminalAdviceCallback | None = None,
    on_advice_persisted: AdvicePersistedCallback | None = None,
    on_advice_ready: AdviceReadyCallback | None = None,
) -> None:
    """Write requeue terminal advice when attribution results are persisted.

    Attempts that already hold Terminated Advice are left unchanged.
    """
    if attribution.attribution_type == ErrorAttributionType.SLOW:
        logger.info("Anomaly attribution is produced with slow signals, skipping terminal advice persistence.")
        return

    job_record = storage.read_job_record(
        job_id=attribution.job_id,
        job_attempt_index=attribution.job_attempt_index,
    )
    if job_record is None:
        logger.warning(
            f"Cannot persist JOB_RELEASED advice: job record not found for job {attribution.job_id} "
            f"attempt {attribution.job_attempt_index}."
        )
        return

    if not job_record.aps_spec or job_record.aps_spec.auto_resume_mode != AutoResumeModeEnum.REQUEUE:
        return

    # Attribution can arrive after the attempt was already terminated (user
    # cancel, requeue limit, DAG-parent termination). Terminated Advice is
    # sticky, so writing JOB_RELEASED (Restarted) would only be rejected by
    # storage; skip it, matching the requeue path.
    existing_record = _read_terminated_advice(
        storage,
        job_id=attribution.job_id,
        job_attempt_index=attribution.job_attempt_index,
    )
    if existing_record is not None:
        logger.info(
            "Skipping attribution-driven terminal advice: attempt already has Terminated advice",
            extra={"job_id": attribution.job_id, "job_attempt_index": attribution.job_attempt_index},
        )
        if on_advice_persisted is not None:
            on_advice_persisted(existing_record, None)
        return

    status = storage_to_http_job_status(job_record)
    requeue_limit_reached, requeue_reason = check_max_requeue_reached(
        status,
        system_requeue_config=system_requeue_config,
        get_jobs=get_jobs,
    )
    terminal_advice = JobTerminalAdvice.MAX_REQUEUE_REACHED if requeue_limit_reached else JobTerminalAdvice.JOB_RELEASED
    written = create_terminal_advice_for_job(
        storage,
        audit_buffer=audit_buffer,
        job_id=attribution.job_id,
        job_attempt_index=attribution.job_attempt_index,
        terminal_advice=terminal_advice,
        audit_reason=requeue_reason,
        status=status,
        anomaly_attribution=attribution,
        exclusion_stickiness_enabled=exclusion_stickiness_enabled,
        get_previous_attempt=get_previous_attempt,
        on_advice_persisted=on_advice_persisted,
        on_advice_ready=on_advice_ready,
    )
    if written is not None and on_persisted is not None:
        on_persisted(job_record, written)


def persist_dag_release_advice(
    storage: MySQLStorage,
    job_record: SlurmJobExecutionRecord,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    on_advice_ready: AdviceReadyCallback | None = None,
) -> Advice:
    """Persist and audit exact-attempt RELEASED Advice for a dependency-ready child."""
    existing_record = storage.read_advice_by_job(
        job_id=job_record.job_id,
        attempt_index=job_record.job_attempt_index,
    )
    if existing_record is not None:
        advice = storage_to_http_advice(existing_record)
        existing_state = advice.advice.state if advice.advice else None
        if existing_state not in (None, AdviceState.RELEASED):
            assert existing_state is not None
            raise ValueError(
                f"Cannot replace {existing_state.value} advice with RELEASED for job "
                f"{job_record.job_id} attempt {job_record.job_attempt_index}"
            )
        advice.visible = True
        advice.advice = AdviceRecommendation(state=AdviceState.RELEASED, node_constraints=None)
    else:
        advice = Advice(
            id=str(
                job_record.heimdall_job_id or generate_heimdall_job_id(job_record.job_id, job_record.job_attempt_index)
            ),
            visible=True,
            job_id=job_record.job_id,
            restart_count=job_record.job_attempt_index,
            scheduler_metadata=job_record.scheduler_metadata or {},
            advice=AdviceRecommendation(state=AdviceState.RELEASED, node_constraints=None),
        )

    advice.scheduler_metadata = ensure_slurm_scheduler_metadata(advice.scheduler_metadata)
    if on_advice_ready is not None:
        try:
            recovery_context = on_advice_ready(advice)
            advice.recovery_context = recovery_context if isinstance(recovery_context, RecoveryContext) else None
        except Exception as error:
            logger.exception(
                "Failed to attach recovery context to DAG Advice %s; persisting without correlation",
                advice.id,
                extra={"advice_id": advice.id, "error": str(error)},
            )
    persisted_advice, _, _ = persist_advice_record(
        storage,
        audit_buffer,
        http_to_storage_advice(advice),
        audit_reason=_DAG_RELEASE_AUDIT_REASON,
    )
    logger.info(f"Persisted DAG release advice for job {job_record.job_id} attempt {job_record.job_attempt_index}.")
    return storage_to_http_advice(persisted_advice)


def read_all_dag_dependencies(
    storage: BaseStorage,
    dependent_job_id: str | None = None,
    dependency_job_identifier: str | None = None,
    dependency_job_identifier_type: str | None = None,
    action_statuses: list[str] | None = None,
    action_time_before: datetime | None = None,
    sort_by: list[tuple[str, str]] | None = None,
) -> list[DagDependency]:
    """
    Read all DAG dependency records matching the given filters by paginating through the storage layer.

    Accepts the same filter parameters as ``storage.read_dag_dependencies`` but without ``limit``/``offset``.
    Internally pages through the results so callers don't silently stop at the default page size.

    Args:
        storage: The storage instance.
        dependent_job_id: Filter by dependent (child) job ID.
        dependency_job_identifier: Filter by parent job identifier (job ID or job name).
        dependency_job_identifier_type: Filter by parent identifier type ("id" or "name").
        action_statuses: Filter by action status values.
        action_time_before: Filter for records with action_time before this threshold.
        sort_by: List of (column, order) tuples for sorting.

    Returns:
        List of all matching DagDependency records.
    """
    all_records: list[DagDependency] = []
    offset = 0
    while True:
        page, total_count = storage.read_dag_dependencies(
            dependent_job_id=dependent_job_id,
            dependency_job_identifier=dependency_job_identifier,
            dependency_job_identifier_type=dependency_job_identifier_type,
            action_statuses=action_statuses,
            action_time_before=action_time_before,
            limit=_DAG_READ_PAGE_SIZE,
            offset=offset,
            sort_by=sort_by,
        )
        all_records.extend(page)
        if len(all_records) >= total_count or len(page) == 0:
            break
        offset += _DAG_READ_PAGE_SIZE
    if len(all_records) > 0 and len(all_records) > _DAG_READ_PAGE_SIZE:
        logger.info(f"read_all_dag_dependencies fetched {len(all_records)} records across multiple pages")
    return all_records


def storage_to_http_dag_dependency(record: DagDependency) -> DagDependencyResponse:
    """
    Convert a storage DagDependency record to the public-facing DagDependencyResponse model.

    Args:
        record: The internal DagDependency ORM record.

    Returns:
        The external-facing DagDependencyResponse.
    """
    return DagDependencyResponse(
        dependent_job_id=record.dependent_job_id,
        dependent_job_action=record.dependent_job_action,
        dependency_job_identifier=record.dependency_job_identifier,
        dependency_job_identifier_type=record.dependency_job_identifier_type,
        dependency_trigger_condition=record.dependency_trigger_condition,
        action_status=record.action_status,
        action_time=record.action_time,
        reason_code=DagDependencyReasonCodeEnum(record.reason_code) if record.reason_code is not None else None,
        reason_message=record.reason_message,
        reason_details=record.reason_details,
        last_evaluated_at=record.last_evaluated_at,
    )
