# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Final

from recovery_libs.audit import AuditBuffer, AuditEventLevel, ProducerEvent
from recovery_libs.storage.base import UpdateJobResult
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import UpsertJobResult

from job_manager.audit.events import (
    JobAttemptFirstSeenEvent,
    JobAttemptFirstSeenPayload,
    JobAttemptUpdatedEvent,
    JobAttemptUpdatedPayload,
    JobAuditSnapshot,
    current_unix_microseconds,
)
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.logger import logger

# Keep this projection explicit: adding a field to the public Job response must
# not silently add it to the audit trail. It omits user-entered labels, raw
# anomaly events, scheduler user/command/dependency data, log paths, checkpoints,
# and scheduler metadata. The complete aps_spec remains because it is Heimdall's
# recovery specification for the Job.
_APPROVED_JOB_AUDIT_FIELDS: Final = {
    "job_id",
    "heimdall_job_id",
    "job_attempt_index",
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
    "node_count",
    "total_gpu_count",
    "total_cpu_count",
    "total_memory_mb",
    "allocated_node_list",
    "inclusion_node_list",
    "exclusion_node_list",
    "failed_node_list",
    "sticky_exclusion_node_list",
    "aps_spec",
    "config_validation_failure_types",
}


def _snapshot_job(record: SlurmJobExecutionRecord) -> JobAuditSnapshot:
    """Return the approved audit projection of the public Job representation."""
    return storage_to_http_job_status(record).model_dump(
        mode="json",
        by_alias=False,
        include=_APPROVED_JOB_AUDIT_FIELDS,
    )


def build_job_attempt_first_seen_event(record: SlurmJobExecutionRecord) -> JobAttemptFirstSeenEvent | None:
    """Build the initial Job event, or return ``None`` when event validation fails."""
    try:
        return JobAttemptFirstSeenEvent(
            heimdall_job_id=str(record.heimdall_job_id),
            event_level=AuditEventLevel.OPERATIONAL,
            payload=JobAttemptFirstSeenPayload(
                job_id=record.job_id,
                attempt_index=record.job_attempt_index,
                job=_snapshot_job(record),
            ),
            event_produced_at=current_unix_microseconds(),
        )
    except Exception:
        logger.exception("Failed to validate Job first-seen audit event.")
        return None


def build_job_attempt_updated_event(
    old_record: SlurmJobExecutionRecord,
    updated_record: SlurmJobExecutionRecord,
) -> JobAttemptUpdatedEvent | None:
    """Build a changed-Job event, or return ``None`` for no change or failed validation."""
    try:
        old_job = _snapshot_job(old_record)
        new_job = _snapshot_job(updated_record)
        if old_job == new_job:
            return None

        return JobAttemptUpdatedEvent(
            heimdall_job_id=str(updated_record.heimdall_job_id),
            event_level=AuditEventLevel.OPERATIONAL,
            payload=JobAttemptUpdatedPayload(
                job_id=updated_record.job_id,
                attempt_index=updated_record.job_attempt_index,
                old_job=old_job,
                new_job=new_job,
            ),
            event_produced_at=current_unix_microseconds(),
        )
    except Exception:
        logger.exception("Failed to validate Job update audit event.")
        return None


def audit_job_upsert(
    result: UpsertJobResult,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Add the lifecycle event for a successful Job upsert, logging audit failures."""
    try:
        updated_record, created, old_record = result
        if created:
            created_event = build_job_attempt_first_seen_event(updated_record)
            if created_event is not None:
                audit_buffer.add(created_event)
        elif old_record is not None:
            updated_event = build_job_attempt_updated_event(old_record, updated_record)
            if updated_event is not None:
                audit_buffer.add(updated_event)
    except Exception:
        # Catch-all exception block to prevent audit exceptions from impacting operational work.
        logger.exception("Failed to create audit event.")


def audit_job_update(
    result: UpdateJobResult,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Add the lifecycle event for a successful partial Job update, logging audit failures."""
    try:
        old_record, updated_record = result.old_record, result.updated_record

        if isinstance(old_record, SlurmJobExecutionRecord) and isinstance(updated_record, SlurmJobExecutionRecord):
            updated_event = build_job_attempt_updated_event(old_record, updated_record)
            if updated_event is not None:
                audit_buffer.add(updated_event)
    except Exception:
        # Catch-all exception block to prevent audit exceptions from impacting operational work.
        logger.exception("Failed to create audit event.")
