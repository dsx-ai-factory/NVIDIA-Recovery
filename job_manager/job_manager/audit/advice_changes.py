# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from opentelemetry.trace import Span
from pydantic import ValidationError
from recovery_libs.audit import AuditBuffer, AuditEventLevel, ProducerEvent
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.dataclass import AdviceRecord
from recovery_libs.storage.mysql_storage import SoftDeleteAdviceResult, UpsertAdviceResult
from recovery_libs.tracing.recovery import operation_correlation

from job_manager.advice_mapping import storage_to_http_advice
from job_manager.audit.events import (
    AdviceAuditSnapshot,
    AdviceCreatedEvent,
    AdviceCreatedPayload,
    AdviceUnpublishedEvent,
    AdviceUnpublishedPayload,
    AdviceUpdatedEvent,
    AdviceUpdatedPayload,
    current_unix_microseconds,
)
from job_manager.logger import logger


def _snapshot_advice(record: AdviceRecord) -> AdviceAuditSnapshot:
    """Return the public Advice representation without its storage-managed update timestamp."""
    return storage_to_http_advice(record).model_dump(
        mode="json",
        exclude={"updated_at"},
    )


def _without_checkpoint_issues(snapshot: AdviceAuditSnapshot) -> AdviceAuditSnapshot:
    """
    Ignore checkpoint activity for comparison while retaining complete audit payloads.

    Checkpoint activity is not entirely relevant to recovery operations. Furthermore,
    the entire old and new advice fields are stored, and frequent checkpointing can
    cause the payloads to explode in sizes.
    """

    return {
        **snapshot,
        "issues": [
            issue
            for issue in snapshot["issues"] or []
            if issue["issue_type"] not in (EventType.CHECKPOINTLOAD.value, EventType.CHECKPOINTSAVE.value)
        ],
    }


def _recovery_correlation_uuid(record: AdviceRecord) -> str:
    """Return the recovery sequence ID, falling back to the Advice ID for legacy records."""
    if record.recovery_sequence_id is not None:
        return str(record.recovery_sequence_id)
    return str(record.heimdall_id)


def _append_additional_reason(reasons: list[str], additional_reason: str | None) -> list[str]:
    """Append one caller-known reason when provided."""
    if additional_reason is not None:
        reasons.append(additional_reason)
    return reasons


def _new_issue_types(old_record: AdviceRecord, updated_record: AdviceRecord) -> list[str]:
    """Return Issue types introduced by this update, preserving their persisted order."""
    unmatched_old_issues = list(old_record.issues or [])
    new_issue_types: list[str] = []
    for issue in updated_record.issues or []:
        if issue in unmatched_old_issues:
            unmatched_old_issues.remove(issue)
        else:
            new_issue_types.append(issue.issue_type)
    return new_issue_types


def build_advice_created_event(
    record: AdviceRecord,
    *,
    additional_reason: str | None = None,
    operation_span: Span | None = None,
) -> AdviceCreatedEvent | None:
    """Build the initial Advice event, or return ``None`` when event validation fails."""
    try:
        advice = storage_to_http_advice(record)
        creation_reasons = _append_additional_reason(
            [issue.issue_type for issue in advice.issues or []],
            additional_reason,
        )
        if not creation_reasons:
            creation_reasons = [advice.advice.state.value] if advice.advice.state is not None else ["unspecified"]

        return AdviceCreatedEvent(
            heimdall_job_id=str(record.heimdall_id),
            event_level=AuditEventLevel.RECOVERY,
            payload=AdviceCreatedPayload(
                advice_id=str(record.heimdall_id),
                job_id=record.job_id,
                attempt_index=record.attempt_index,
                advice=_snapshot_advice(record),
                creation_reasons=creation_reasons,
                correlation_uuid=_recovery_correlation_uuid(record),
                **operation_correlation(
                    operation_span, str(record.recovery_sequence_id) if record.recovery_sequence_id else None
                ),
            ),
            event_produced_at=current_unix_microseconds(),
        )
    except ValidationError:
        logger.exception("Failed to validate Advice creation audit event.")
        return None


def build_advice_updated_event(
    old_record: AdviceRecord,
    updated_record: AdviceRecord,
    *,
    additional_reason: str | None = None,
    operation_span: Span | None = None,
) -> AdviceUpdatedEvent | None:
    """Build an Advice update event, suppressing no-op and checkpoint-only changes."""
    try:
        old_advice = _snapshot_advice(old_record)
        new_advice = _snapshot_advice(updated_record)
        if _without_checkpoint_issues(old_advice) == _without_checkpoint_issues(new_advice):
            return None

        change_reasons = _append_additional_reason(
            _new_issue_types(old_record, updated_record),
            additional_reason,
        )

        return AdviceUpdatedEvent(
            heimdall_job_id=str(updated_record.heimdall_id),
            event_level=AuditEventLevel.RECOVERY,
            payload=AdviceUpdatedPayload(
                advice_id=str(updated_record.heimdall_id),
                job_id=updated_record.job_id,
                attempt_index=updated_record.attempt_index,
                old_advice=old_advice,
                new_advice=new_advice,
                change_reasons=change_reasons,
                correlation_uuid=_recovery_correlation_uuid(updated_record),
                **operation_correlation(
                    operation_span,
                    str(updated_record.recovery_sequence_id) if updated_record.recovery_sequence_id else None,
                ),
            ),
            event_produced_at=current_unix_microseconds(),
        )
    except ValidationError:
        logger.exception("Failed to validate Advice update audit event.")
        return None


def build_advice_unpublished_event(
    record: AdviceRecord,
    *,
    reason: str | None = None,
) -> AdviceUnpublishedEvent | None:
    """Build an Advice-unpublished event, or ``None`` when validation fails."""
    try:
        return AdviceUnpublishedEvent(
            heimdall_job_id=str(record.heimdall_id),
            event_level=AuditEventLevel.RECOVERY,
            payload=AdviceUnpublishedPayload(
                advice_id=str(record.heimdall_id),
                job_id=record.job_id,
                attempt_index=record.attempt_index,
                reason=reason,
                correlation_uuid=_recovery_correlation_uuid(record),
                **operation_correlation(
                    None, str(record.recovery_sequence_id) if record.recovery_sequence_id else None
                ),
            ),
            event_produced_at=current_unix_microseconds(),
        )
    except ValidationError:
        logger.exception("Failed to validate Advice unpublished audit event.")
        return None


def audit_advice_upsert(
    result: UpsertAdviceResult,
    audit_buffer: AuditBuffer[ProducerEvent],
    *,
    additional_reason: str | None = None,
    operation_span: Span | None = None,
) -> None:
    """Add the lifecycle event for a successful Advice upsert."""

    try:
        updated_record, created, old_record = result
        if created:
            created_event = build_advice_created_event(
                updated_record,
                additional_reason=additional_reason,
                operation_span=operation_span,
            )
            if created_event is not None:
                audit_buffer.add(created_event)
        elif old_record is not None:
            updated_event = build_advice_updated_event(
                old_record,
                updated_record,
                additional_reason=additional_reason,
                operation_span=operation_span,
            )
            if updated_event is not None:
                audit_buffer.add(updated_event)
    except Exception:
        # Catch-all exception block to prevent audit exceptions from impacting operational work.
        logger.exception("Failed to create audit event.")


def audit_advice_delete_if_needed(
    result: SoftDeleteAdviceResult,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Produce audit lifecycle event for a successful Advice deletion."""
    if result.advice_record is None:
        logger.warning("Soft delete requires a matching Advice record.")
        return

    if not result.was_deleted:
        return

    try:
        event = build_advice_unpublished_event(result.advice_record)
        if event is not None:
            audit_buffer.add(event)
    except Exception:
        # Catch-all exception block to prevent audit exceptions from impacting operational work.
        logger.exception("Failed to create audit event.")
