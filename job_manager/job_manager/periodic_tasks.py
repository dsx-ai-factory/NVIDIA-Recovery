# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Periodic tasks run as part of the job manager to detect conditions of different criteria and take actions accordingly.
"""

import time as time_module
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import uuid6
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from recovery_libs.audit import AuditBuffer, ProducerEvent
from recovery_libs.schema.aps_spec.spec import DagActionEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    JobAnomalyHandledBy,
    JobTimeFilter,
    JobTimeFilterTarget,
    SortOrderEnum,
    filter_anomalies,
)
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import (
    DagDependencyActionStatus,
    SlurmJobExecutionRecord,
    SlurmJobFinalStates,
)
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.tracing.traces import (
    JobAnomalySpanName,
    extract_trace_id_from_span,
)
from recovery_libs.utils.time import parse_event_timestamp

from job_manager.logger import logger
from job_manager.metrics import CRASH_ANOMALY_GENERATED_COUNTER
from job_manager.storage_utils import (
    create_advice_issue_from_event,
    create_advice_issues_from_anomalies,
    persist_dag_release_advice,
    read_all_dag_dependencies,
    update_job_record,
)

tracer = trace.get_tracer(__name__)


def generate_anomaly_event_for_failed_jobs(
    storage: MySQLStorage,
    audit_buffer: AuditBuffer[ProducerEvent],
    time_window_secs: int,
    threshold_secs: int,
    on_anomaly_persisted: Callable[[SlurmJobExecutionRecord], object] | None = None,
    *,
    advice_enabled: bool = False,
) -> None:
    """
    This function is run periodically to find the jobs that failed but were not automatically resumed due to missing
    anomaly events, then generate crash anomalies for them.

    Persist each crash anomaly before recording its Advice Issue when advice is enabled and
    notifying the direct attribution callback. Storage failures skip both so attribution never
    starts for an uncommitted anomaly.
    """

    now = datetime.now(UTC)
    offset = 0
    lastest_attempt_index_evaluated_for_jobs: dict[str, int] = {}

    while failed_job_attempts := storage.read_job_records(
        states=[SlurmJobFinalStates.FAILED],
        time_filter=JobTimeFilter(
            job_time_target=JobTimeFilterTarget.END_TIME,
            from_time=now - timedelta(seconds=(time_window_secs)),
            to_time=now,
        ),
        # Sort the result by job_id in ascending order and then job_attempt_index in descending order.
        sort_by=[
            ("job_id", SortOrderEnum.ASC),
            ("job_attempt_index", SortOrderEnum.DESC),
        ],
        offset=offset,
    )[0]:
        offset += len(failed_job_attempts)
        for job in failed_job_attempts:
            try:
                # Use the lastest_attempt_index_evaluated_for_jobs dictionary to optimize the processing.
                # Skip the job attempt if a higher job attempt index of the same job id has been evaluated
                # in the previous iteration.
                if (
                    job.job_id not in lastest_attempt_index_evaluated_for_jobs
                    or job.job_attempt_index > lastest_attempt_index_evaluated_for_jobs[job.job_id]
                ):
                    lastest_attempt_index_evaluated_for_jobs[job.job_id] = job.job_attempt_index

                if job.job_attempt_index < lastest_attempt_index_evaluated_for_jobs[job.job_id]:
                    logger.debug(
                        f"Skipping job {job.job_id}, attempt {job.job_attempt_index} because a higher job attempt index "
                        f"{lastest_attempt_index_evaluated_for_jobs[job.job_id]} has been evaluated in the previous iteration."
                    )
                    continue

                # A prior anomaly write may have committed before its Advice write failed.
                # Repair that original cause before skipping generation of a new event.
                if filter_anomalies(job.anomalies):
                    if advice_enabled:
                        create_advice_issues_from_anomalies(storage, job, audit_buffer=audit_buffer)
                        if on_anomaly_persisted is not None and not job.attribution_request_emitted:
                            on_anomaly_persisted(job)
                    logger.debug(
                        f"Skipping job {job.job_id}, attempt {job.job_attempt_index} because it already has an anomaly."
                    )
                    continue

                # Scheduler accounting may lag the observed failure; wait for the
                # Advisor Controller to publish an end time before aging the job.
                if job.end_time is None:
                    logger.warning(
                        f"Job {job.job_id}, attempt {job.job_attempt_index} is in FAILED state but has no end_time "
                        "associated with it. This is unexpected behavior, skipping this job attempt for state-based anomaly "
                        "generation."
                    )
                    continue

                # If the job end time is older than the threshold, generate an end event for it.
                if job.end_time < now - timedelta(seconds=threshold_secs):
                    logger.info(
                        f"The job {job.job_id}, attempt {job.job_attempt_index} failed with no anomaly for longer than"
                        f" {threshold_secs} seconds, generating a CRASH event for it to continue the auto resume process."
                    )

                    detection_time_ns = time_module.time_ns()
                    job_crash_event = Event(
                        body="Job CRASH event for failed job with no anomaly.",
                        type=EventType.CRASH,
                        timestamp=str(detection_time_ns),
                        attributes=Attributes(
                            job_id=job.job_id,
                            detection_time=str(detection_time_ns),
                            uuid=str(uuid6.uuid7()),
                            slurm_job_attempt=job.job_attempt_index,
                            job_manager_generated=True,
                        ),
                    )

                    # Create an anomaly detection span with start as job end_time and end as anomaly creation time to
                    # capture the latency of anomaly detection.
                    detection_span = tracer.start_span(
                        name=f"job_anomaly.{JobAnomalySpanName.DETECTION}",
                        kind=SpanKind.PRODUCER,
                        attributes={
                            "job_id": job.job_id,
                            "job_attempt_index": job.job_attempt_index,
                            "job_anomaly.type": job_crash_event.type,
                            "job_anomaly.uuid": job_crash_event.attributes.uuid,  # type: ignore[union-attr]
                        },
                        start_time=int(job.end_time.timestamp() * 1_000_000_000),
                    )
                    detection_span.set_status(StatusCode.OK)
                    detection_span.end(end_time=detection_time_ns)

                    job.anomalies = job.anomalies or []
                    job.anomalies.append(
                        JobAnomaly(
                            anomaly_type=job_crash_event.type,
                            timestamp=datetime.fromtimestamp(int(job_crash_event.timestamp) / 1e9, UTC),  # type: ignore[arg-type]
                            uuid=job_crash_event.attributes.uuid,  # type: ignore[union-attr]
                            handled_by=JobAnomalyHandledBy.HEIMDALL,
                            processed=True,
                            trace_id=extract_trace_id_from_span(detection_span),
                            original_watcher_anomaly_event=job_crash_event.model_dump_json(),
                        )
                    )

                    with tracer.start_as_current_span(
                        name=f"job_anomaly.{JobAnomalySpanName.PERSISTENCE}",
                        kind=SpanKind.INTERNAL,
                        context=trace.set_span_in_context(detection_span),
                        attributes={
                            "job_id": job.job_id,
                            "job_attempt_index": job.job_attempt_index,
                            "job_anomaly.type": job_crash_event.type,
                            "job_anomaly.uuid": job_crash_event.attributes.uuid,  # type: ignore[union-attr]
                        },
                    ) as persistence_span:
                        # Persist the anomaly to the job record.
                        logger.debug(f"Persisting job crash event to the database: {job_crash_event.model_dump_json()}")
                        update_job_record(
                            storage,
                            audit_buffer,
                            job_id=job.job_id,
                            job_attempt_index=job.job_attempt_index,
                            anomalies=job.anomalies,
                        )
                        persistence_span.set_status(StatusCode.OK)

                        CRASH_ANOMALY_GENERATED_COUNTER.inc(1)

                        if advice_enabled:
                            create_advice_issue_from_event(
                                storage=storage,
                                event=job_crash_event,
                                job_record=job,
                                timestamp_parser=parse_event_timestamp,
                                detected_by="Heimdall Job Manager",
                                audit_buffer=audit_buffer,
                            )

                        # Emit only after persistence succeeds, while its span is current,
                        # so the direct ADI request inherits the anomaly's trace context.
                        if on_anomaly_persisted is not None:
                            on_anomaly_persisted(job)

            except RecoveryStorageException as e:
                # Capturing the storage exception here to avoid the entire periodic task failing due to a single job
                # processing error.
                logger.error(
                    f"Failed to process job {job.job_id}, attempt {job.job_attempt_index} due to storage error: {e}."
                )


def reconcile_timed_out_dag_actions(
    storage: MySQLStorage,
    timeout_secs: int,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """
    Find IN_PROGRESS DAG dependency actions that have been pending longer than the timeout threshold
    and reconcile their exact attempt-0 RELEASED advice.

    Args:
        storage: The MySQL storage instance.
        timeout_secs: The timeout in seconds after which an IN_PROGRESS action is considered stale.
        audit_buffer: Buffer that records each committed release Advice transition.
    """
    threshold = datetime.now(UTC) - timedelta(seconds=timeout_secs)
    timed_out = read_all_dag_dependencies(
        storage,
        action_statuses=[DagDependencyActionStatus.IN_PROGRESS.value],
        action_time_before=threshold,
    )
    if not timed_out:
        return

    # Group by dependent_job_id
    groups: dict[str, list] = {}
    for dep in timed_out:
        groups.setdefault(dep.dependent_job_id, []).append(dep)

    for dependent_job_id, deps in groups.items():
        action = deps[0].dependent_job_action
        try:
            if action == DagActionEnum.RELEASE.value:
                dependent_record = storage.read_job_record(
                    job_id=dependent_job_id,
                    job_attempt_index=0,
                )
                if dependent_record is None:
                    logger.warning(
                        f"Cannot reconcile timed-out DAG action for job {dependent_job_id}: attempt 0 not found."
                    )
                    continue
            else:
                logger.warning(f"Unsupported DAG action for reconciliation: {action}")
                continue

            now = datetime.now(UTC)
            for dep in deps:
                storage.update_dag_dependency_status(
                    id=dep.id,
                    action_status=DagDependencyActionStatus.IN_PROGRESS.value,
                    action_time=now,
                )

            persist_dag_release_advice(
                storage,
                dependent_record,
                audit_buffer=audit_buffer,
            )

            for dep in deps:
                storage.update_dag_dependency_status(
                    id=dep.id,
                    action_status=DagDependencyActionStatus.SUCCESSFUL.value,
                    action_time=datetime.now(UTC),
                )
            logger.info(f"Reconciled timed-out DAG release advice for job {dependent_job_id} attempt 0")
        except Exception as e:
            logger.exception(
                f"Failed to reconcile timed-out DAG action for job {dependent_job_id}",
                extra={"job_id": dependent_job_id, "error": str(e)},
            )
