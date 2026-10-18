# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, SpanKind
from pydantic import Field
from recovery_libs.schema.job_manager.models import (
    GetRecoverySequenceRequest,
    GetRecoverySequenceResponse,
    RecordRecoveryMilestoneRequest,
    RecordRecoveryMilestoneResponse,
    RecoveryContext,
    RecoveryOperation,
    RecoverySchedulerActionOutcome,
    RecoveryTriggerType,
)
from recovery_libs.storage.base import RecoveryStorageInputValidationError
from recovery_libs.storage.dataclass import (
    RecoveryMilestoneRecord,
    RecoverySequenceRecord,
    SlurmJobExecutionRecord,
)
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.tracing.traces import restore_trace_context, serialize_trace_context

from job_manager.metrics import (
    RECOVERY_END_TO_END_DURATION,
    RECOVERY_MILESTONES_RECORDED_COUNTER,
    RECOVERY_SEQUENCES_COUNTER,
)

_MAX_FUTURE_CLOCK_SKEW = timedelta(minutes=5)
_TRACER = trace.get_tracer(__name__)


class RecoverySequenceTrigger(GetRecoverySequenceRequest):
    """Backend-only creation input; trigger evidence is not a public API."""

    trigger_type: RecoveryTriggerType
    trigger_identifier: str | None = Field(default=None, max_length=256)
    triggered_at: datetime | None = None


class RecoveryService:
    """Own durable recovery identity, trace context, and completion semantics.

    A recovery sequence is canonical for one scheduler/job attempt/operation
    natural key. This service ensures retries and concurrent triggers reuse the
    stored W3C context, while only the first accepted milestone explicitly
    marked terminal by its scheduler adapter establishes the end-to-end
    completion boundary.
    """

    def __init__(self, storage: MySQLStorage):
        self._storage = storage

    def get(self, request: GetRecoverySequenceRequest) -> GetRecoverySequenceResponse | None:
        """Look up canonical context without creating a row or recovery root span."""
        record = self._storage.read_recovery_sequence_by_natural_key(
            scheduler_system=request.scheduler_system,
            scheduler_scope=request.scheduler_scope,
            job_id=request.job_id,
            source_attempt_index=request.source_attempt_index,
            operation=request.operation.value,
        )
        return (
            None if record is None else GetRecoverySequenceResponse(recovery_context=self._context_from_record(record))
        )

    def begin_or_get(
        self,
        request: RecoverySequenceTrigger,
    ) -> GetRecoverySequenceResponse:
        """Return the canonical recovery sequence for an exact attempt.

        Existing sequences retain their original trigger time and W3C parent.
        For a new sequence, Job Manager creates a root trigger span linked to
        the incoming producer context and asks storage to atomically arbitrate
        concurrent creators. Validation and storage failures propagate to the
        API boundary; callers that treat correlation as best effort decide how
        to degrade without changing recovery behavior.

        Args:
            request: The scheduler identity, exact job attempt, operation, and
                trigger evidence used to identify the recovery sequence.

        Returns:
            The canonical durable recovery context selected for the exact job
            attempt and operation.
        """
        triggered_at = self._validated_timestamp(request.triggered_at or datetime.now(UTC), "triggered_at")
        existing = self._storage.read_recovery_sequence_by_natural_key(
            scheduler_system=request.scheduler_system,
            scheduler_scope=request.scheduler_scope,
            job_id=request.job_id,
            source_attempt_index=request.source_attempt_index,
            operation=request.operation.value,
        )
        if existing is not None:
            candidate = self._candidate_from_existing(existing, request)
            sequence, _ = self._storage.begin_or_get_recovery_sequence(candidate)
            RECOVERY_SEQUENCES_COUNTER.labels(
                operation=request.operation.value,
                trigger_type=request.trigger_type.value,
                outcome="existing",
            ).inc()
            return GetRecoverySequenceResponse(recovery_context=self._context_from_record(sequence))

        incoming_span_context = trace.get_current_span().get_span_context()
        links = [Link(incoming_span_context)] if incoming_span_context.is_valid else None
        attributes = {
            "recovery.operation": request.operation.value,
            "recovery.trigger.type": request.trigger_type.value,
            "recovery.slo.eligible": request.operation == RecoveryOperation.RESTART,
            "job.id": request.job_id,
            "job.attempt.source": request.source_attempt_index,
        }
        if request.trigger_identifier:
            attributes["recovery.trigger.identifier"] = request.trigger_identifier

        with _TRACER.start_as_current_span(
            "job.recovery.trigger",
            context=Context(),
            kind=SpanKind.INTERNAL,
            links=links,
            attributes=attributes,
            start_time=int(triggered_at.timestamp() * 1_000_000_000),
        ) as span:
            carrier = serialize_trace_context(span=span)
            if "traceparent" not in carrier:
                raise RuntimeError("OpenTelemetry did not create a valid recovery trigger span context")
            now = datetime.now(UTC)
            candidate = RecoverySequenceRecord(
                sequence_id=uuid.uuid4(),
                scheduler_system=request.scheduler_system,
                scheduler_scope=request.scheduler_scope,
                job_id=request.job_id,
                source_attempt_index=request.source_attempt_index,
                operation=request.operation.value,
                traceparent=carrier["traceparent"],
                tracestate=carrier.get("tracestate"),
                trigger_type=request.trigger_type.value,
                trigger_identifier=request.trigger_identifier,
                triggered_at=triggered_at,
                status="active",
                completed_at=None,
                created_at=now,
                updated_at=now,
            )
            sequence, created = self._storage.begin_or_get_recovery_sequence(candidate)
            span.set_attribute("recovery.sequence.id", str(sequence.sequence_id))
            span.set_attribute("recovery.outcome", "applied" if created else "skipped")

        RECOVERY_SEQUENCES_COUNTER.labels(
            operation=request.operation.value,
            trigger_type=request.trigger_type.value,
            outcome="created" if created else "existing",
        ).inc()
        return GetRecoverySequenceResponse(recovery_context=self._context_from_record(sequence))

    def begin_for_job(
        self,
        record: SlurmJobExecutionRecord,
        *,
        operation: RecoveryOperation = RecoveryOperation.RESTART,
        trigger_type: RecoveryTriggerType = RecoveryTriggerType.JOB_STATE,
        trigger_identifier: str | None = None,
        triggered_at: datetime | None = None,
        scheduler_metadata: dict | None = None,
    ) -> RecoveryContext:
        """Begin or retrieve recovery using identity persisted with a job.

        Scheduler metadata is the trusted source of scheduler system and scope.
        Missing or malformed values use the public compatibility defaults, and
        the returned context does not itself mutate Advice or scheduler state.

        Args:
            record: The persisted job execution whose scheduler and attempt
                identity define the recovery sequence.
            operation: The scheduler-agnostic recovery operation being started.
            trigger_type: The category of evidence that triggered recovery.
            trigger_identifier: An optional identifier for the triggering
                evidence, such as an anomaly identifier.
            scheduler_metadata: The exact Advice scheduler identity when already known.
            triggered_at: The optional trigger timestamp. The current UTC time
                is used when this value is absent.

        Returns:
            The canonical durable recovery context for the job attempt.
        """
        metadata = scheduler_metadata if scheduler_metadata is not None else record.scheduler_metadata or {}
        scheduler_system = metadata.get("scheduler", "slurm")
        scheduler_scope = metadata.get("environment", "unspecified")
        if not isinstance(scheduler_system, str) or not scheduler_system:
            scheduler_system = "slurm"
        if not isinstance(scheduler_scope, str) or not scheduler_scope:
            scheduler_scope = "unspecified"
        response = self.begin_or_get(
            RecoverySequenceTrigger(
                scheduler_system=scheduler_system,
                scheduler_scope=scheduler_scope,
                job_id=record.job_id,
                source_attempt_index=record.job_attempt_index,
                operation=operation,
                trigger_type=trigger_type,
                trigger_identifier=trigger_identifier,
                triggered_at=triggered_at,
            )
        )
        return response.recovery_context

    def record_milestone(
        self,
        request: RecordRecoveryMilestoneRequest,
    ) -> RecordRecoveryMilestoneResponse:
        """Idempotently record one completed scheduler-action occurrence.

        Storage deduplicates the caller's occurrence key and serializes updates
        to the owning sequence. The scheduler adapter supplies completion
        intent explicitly because Job Manager must not interpret scheduler-
        specific action names. Only the first accepted terminal milestone sets
        the immutable completion timestamp; later reports remain audit records.

        Args:
            request: The milestone occurrence, explicit sequence-completion
                intent, and idempotency key to record.

        Returns:
            The record result, including whether the milestone was a duplicate,
            whether the sequence is complete after accepting it, and the
            end-to-end duration only when this report newly completed the
            sequence.
        """
        try:
            sequence_id = uuid.UUID(request.sequence_id)
        except ValueError as error:
            raise RecoveryStorageInputValidationError("Recovery sequence_id must be a UUID.") from error
        if request.completes_sequence and request.outcome != RecoverySchedulerActionOutcome.SUCCESS:
            raise RecoveryStorageInputValidationError("Only a successful action can complete a recovery sequence.")
        completed_at = self._validated_timestamp(request.completed_at, "completed_at")
        sequence_before = self._storage.read_recovery_sequence(sequence_id)
        if sequence_before is None:
            raise RecoveryStorageInputValidationError(f"Recovery sequence {sequence_id} does not exist.")
        if completed_at < sequence_before.triggered_at:
            raise RecoveryStorageInputValidationError(
                "Recovery milestone completed_at precedes the persisted trigger timestamp."
            )

        now = datetime.now(UTC)
        milestone = RecoveryMilestoneRecord(
            sequence_id=sequence_id,
            idempotency_key=request.idempotency_key,
            operation=request.operation.value,
            action=request.action.value,
            outcome=request.outcome.value,
            completes_sequence=request.completes_sequence,
            completed_at=completed_at,
            created_at=now,
        )
        _stored, created, completed_sequence = self._storage.record_recovery_milestone(milestone)
        sequence_after = self._storage.read_recovery_sequence(sequence_id)
        assert sequence_after is not None

        record_result = "created" if created else "duplicate"
        RECOVERY_MILESTONES_RECORDED_COUNTER.labels(
            operation=request.operation.value,
            action=request.action.value,
            outcome=request.outcome.value,
            record_result=record_result,
        ).inc()

        stored_completed_at = sequence_after.completed_at
        duration_seconds = None
        if completed_sequence and stored_completed_at is not None:
            duration_seconds = (stored_completed_at - sequence_after.triggered_at).total_seconds()
            RECOVERY_END_TO_END_DURATION.labels(operation=request.operation.value).observe(duration_seconds)

        return RecordRecoveryMilestoneResponse(
            duplicate=not created,
            sequence_completed=stored_completed_at is not None,
            end_to_end_duration_seconds=duration_seconds,
        )

    @staticmethod
    def _validated_timestamp(value: datetime, field_name: str) -> datetime:
        """Normalize an aware timestamp and reject unsafe future clock skew.

        Args:
            value: The timezone-aware timestamp to normalize.
            field_name: The public field name used in validation errors.

        Returns:
            The timestamp converted to UTC.
        """
        if value.tzinfo is None:
            raise RecoveryStorageInputValidationError(f"Recovery {field_name} must include a timezone.")
        value = value.astimezone(UTC)
        if value > datetime.now(UTC) + _MAX_FUTURE_CLOCK_SKEW:
            raise RecoveryStorageInputValidationError(
                f"Recovery {field_name} exceeds the allowed five-minute clock-skew window."
            )
        return value

    @staticmethod
    def _candidate_from_existing(
        existing: RecoverySequenceRecord,
        request: RecoverySequenceTrigger,
    ) -> RecoverySequenceRecord:
        """Build a storage candidate without replacing canonical trace fields.

        Args:
            existing: The canonical stored recovery sequence.
            request: The repeated request whose non-canonical trigger evidence
                may be represented by the candidate.

        Returns:
            A candidate preserving all canonical sequence identity, trace, and
            timing fields from the stored record.
        """
        return RecoverySequenceRecord(
            sequence_id=existing.sequence_id,
            scheduler_system=existing.scheduler_system,
            scheduler_scope=existing.scheduler_scope,
            job_id=existing.job_id,
            source_attempt_index=existing.source_attempt_index,
            operation=existing.operation,
            traceparent=existing.traceparent,
            tracestate=existing.tracestate,
            trigger_type=existing.trigger_type,
            trigger_identifier=request.trigger_identifier,
            triggered_at=existing.triggered_at,
            status=existing.status,
            completed_at=existing.completed_at,
            created_at=existing.created_at,
            updated_at=datetime.now(UTC),
        )

    @staticmethod
    def _context_from_record(record: RecoverySequenceRecord) -> RecoveryContext:
        """Validate and expose the durable W3C context through the API model.

        Args:
            record: The stored recovery sequence carrying the durable W3C
                context.

        Returns:
            The validated public recovery context represented by the record.
        """
        if restore_trace_context(record.traceparent, record.tracestate) is None:
            raise RuntimeError(f"Recovery sequence {record.sequence_id} contains invalid W3C context")
        return RecoveryContext(
            sequence_id=str(record.sequence_id),
            traceparent=record.traceparent,
            tracestate=record.tracestate,
            triggered_at=record.triggered_at,
            operation=RecoveryOperation(record.operation),
        )
