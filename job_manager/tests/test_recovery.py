# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from job_manager.recovery import RecoverySequenceTrigger, RecoveryService
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from recovery_libs.schema.job_manager.models import (
    GetRecoverySequenceRequest,
    RecordRecoveryMilestoneRequest,
    RecoveryOperation,
    RecoverySchedulerAction,
    RecoverySchedulerActionOutcome,
    RecoveryTriggerType,
)
from recovery_libs.storage.base import RecoveryStorageInputValidationError
from recovery_libs.storage.dataclass import RecoverySequenceRecord
from recovery_libs.storage.mysql_storage import MySQLStorage


def _sequence(*, triggered_at: datetime, completed_at: datetime | None = None) -> RecoverySequenceRecord:
    now = datetime.now(UTC)
    return RecoverySequenceRecord(
        sequence_id=uuid.uuid4(),
        scheduler_system="slurm",
        scheduler_scope="cluster-a",
        job_id="42",
        source_attempt_index=3,
        operation="restart",
        traceparent="00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        tracestate=None,
        trigger_type="job_state",
        trigger_identifier=None,
        triggered_at=triggered_at,
        status="completed" if completed_at is not None else "active",
        completed_at=completed_at,
        created_at=now,
        updated_at=now,
    )


def test_begin_or_get_creates_root_span_link_and_durable_context():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    storage = MagicMock(spec=MySQLStorage)
    storage.read_recovery_sequence_by_natural_key.return_value = None
    storage.begin_or_get_recovery_sequence.side_effect = lambda candidate: (candidate, True)
    service = RecoveryService(storage)
    triggered_at = datetime.now(UTC) - timedelta(seconds=2)

    with tracer.start_as_current_span("producer") as producer:
        producer_context = producer.get_span_context()
        with patch("job_manager.recovery._TRACER", tracer):
            response = service.begin_or_get(
                RecoverySequenceTrigger(
                    schedulerSystem="slurm",
                    schedulerScope="cluster-a",
                    jobId="42",
                    sourceAttemptIndex=3,
                    operation="restart",
                    trigger_type="job_state",
                    triggered_at=triggered_at,
                )
            )

    spans = exporter.get_finished_spans()
    trigger_span = next(span for span in spans if span.name == "job.recovery.trigger")
    assert trigger_span.parent is None
    assert [link.context.trace_id for link in trigger_span.links] == [producer_context.trace_id]
    assert trigger_span.attributes["recovery.operation"] == "restart"
    assert response.recovery_context.sequence_id
    assert response.recovery_context.traceparent.startswith("00-")
    assert response.recovery_context.triggered_at == triggered_at


def test_record_milestone_uses_persisted_trigger_and_explicit_completion():
    storage = MagicMock(spec=MySQLStorage)
    triggered_at = datetime.now(UTC) - timedelta(seconds=12)
    completed_at = triggered_at + timedelta(seconds=7)
    before = _sequence(triggered_at=triggered_at)
    after = _sequence(triggered_at=triggered_at, completed_at=completed_at)
    after.sequence_id = before.sequence_id
    storage.read_recovery_sequence.side_effect = [before, after]
    storage.record_recovery_milestone.side_effect = lambda milestone: (milestone, True, True)

    response = RecoveryService(storage).record_milestone(
        RecordRecoveryMilestoneRequest(
            sequenceId=str(before.sequence_id),
            idempotencyKey="release-attempt-1",
            operation=RecoveryOperation.RESTART,
            action=RecoverySchedulerAction.SLURM_RELEASE,
            outcome=RecoverySchedulerActionOutcome.SUCCESS,
            completesSequence=True,
            completedAt=completed_at,
        )
    )

    assert response.duplicate is False
    assert response.sequence_completed is True
    assert response.end_to_end_duration_seconds == 7.0
    milestone = storage.record_recovery_milestone.call_args.args[0]
    assert milestone.completed_at == completed_at
    assert milestone.idempotency_key == "release-attempt-1"
    assert milestone.completes_sequence is True


def test_record_milestone_does_not_double_observe_another_writers_completion():
    storage = MagicMock(spec=MySQLStorage)
    triggered_at = datetime.now(UTC) - timedelta(seconds=12)
    completed_at = triggered_at + timedelta(seconds=7)
    before = _sequence(triggered_at=triggered_at)
    after = _sequence(triggered_at=triggered_at, completed_at=completed_at)
    after.sequence_id = before.sequence_id
    storage.read_recovery_sequence.side_effect = [before, after]
    storage.record_recovery_milestone.side_effect = lambda milestone: (milestone, True, False)

    response = RecoveryService(storage).record_milestone(
        RecordRecoveryMilestoneRequest(
            sequenceId=str(before.sequence_id),
            idempotencyKey="release-attempt-2",
            operation=RecoveryOperation.RESTART,
            action=RecoverySchedulerAction.SLURM_RELEASE,
            outcome=RecoverySchedulerActionOutcome.SUCCESS,
            completesSequence=True,
            completedAt=completed_at + timedelta(seconds=1),
        )
    )

    assert response.sequence_completed is True
    assert response.end_to_end_duration_seconds is None


def test_begin_or_get_reuses_existing_context_without_emitting_duplicate_root():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    storage = MagicMock(spec=MySQLStorage)
    existing = _sequence(triggered_at=datetime.now(UTC) - timedelta(seconds=1))
    storage.read_recovery_sequence_by_natural_key.return_value = existing
    storage.begin_or_get_recovery_sequence.return_value = (existing, False)

    with patch("job_manager.recovery._TRACER", tracer):
        response = RecoveryService(storage).begin_or_get(
            RecoverySequenceTrigger(
                schedulerSystem="slurm",
                schedulerScope="cluster-a",
                jobId="42",
                sourceAttemptIndex=3,
                operation=RecoveryOperation.RESTART,
                trigger_type=RecoveryTriggerType.ANOMALY,
                trigger_identifier="anomaly-1",
                triggered_at=datetime.now(UTC),
            )
        )

    assert response.recovery_context.sequence_id == str(existing.sequence_id)
    assert exporter.get_finished_spans() == ()


@pytest.mark.parametrize("exists", [False, True])
def test_lookup_never_creates_sequence_or_root(exists):
    """Diagnostic reads cannot initiate recovery, including lookup misses."""
    storage = MagicMock(spec=MySQLStorage)
    record = _sequence(triggered_at=datetime.now(UTC))
    storage.read_recovery_sequence_by_natural_key.return_value = record if exists else None
    with patch("job_manager.recovery._TRACER") as tracer:
        result = RecoveryService(storage).get(
            GetRecoverySequenceRequest(
                scheduler_system="slurm",
                scheduler_scope="cluster-a",
                job_id="42",
                source_attempt_index=3,
                operation=RecoveryOperation.RESTART,
            )
        )
    assert (result is not None) == exists
    if result is not None:
        assert result.recovery_context.sequence_id == str(record.sequence_id)
    storage.begin_or_get_recovery_sequence.assert_not_called()
    tracer.start_as_current_span.assert_not_called()


@pytest.mark.parametrize(
    "outcome",
    [
        RecoverySchedulerActionOutcome.ERROR,
        RecoverySchedulerActionOutcome.PLANNED,
        RecoverySchedulerActionOutcome.ALREADY_SATISFIED,
    ],
)
def test_only_successful_commands_can_complete_sequence(outcome):
    """A planned, failed, or observed action cannot create a latency sample."""
    storage = MagicMock(spec=MySQLStorage)
    with pytest.raises(RecoveryStorageInputValidationError):
        RecoveryService(storage).record_milestone(
            RecordRecoveryMilestoneRequest(
                sequence_id=str(uuid.uuid4()),
                idempotency_key=str(uuid.uuid4()),
                operation=RecoveryOperation.RESTART,
                action=RecoverySchedulerAction.SLURM_RELEASE,
                outcome=outcome,
                completes_sequence=True,
                completed_at=datetime.now(UTC),
            )
        )
    storage.record_recovery_milestone.assert_not_called()
