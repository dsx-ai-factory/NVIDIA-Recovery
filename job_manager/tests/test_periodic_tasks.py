# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import uuid6
from job_manager.attribution_request import emit_attribution_request_if_needed
from job_manager.periodic_tasks import (
    EventType,
    JobAnomaly,
    MySQLStorage,
    generate_anomaly_event_for_failed_jobs,
)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from recovery_libs.event_bus.event_bus_utils import RabbitMQPublisher
from recovery_libs.storage.base import RecoveryStorageException, UpdateJobResult
from test_utils import create_example_slurm_job_record


@pytest.mark.parametrize("advice_enabled", [False, True])
def test_crash_event_generated_for_failed_jobs(advice_enabled, monkeypatch):
    # Test that a crash anomaly is generated for a failed job without any anomalies.
    monkeypatch.setattr("job_manager.periodic_tasks.time_module.time_ns", lambda: 1791423450467741568)
    mock_storage = MagicMock(spec=MySQLStorage)
    mock_audit_buffer = MagicMock()
    on_anomaly_persisted = MagicMock()

    now = datetime.now(UTC)

    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=2,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        end_time=now - timedelta(minutes=15),
    )
    mock_storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]
    job_record.heimdall_job_id = uuid6.uuid7()
    mock_storage.update_job_record.return_value = UpdateJobResult(
        updated_record=job_record,
        old_record=deepcopy(job_record),
    )
    mock_storage.read_advice.return_value = None
    mock_storage.upsert_advice.side_effect = lambda record, **_: (record, True, None)

    def verify_persistence_before_attribution(record):
        mock_storage.update_job_record.assert_called_once_with(
            job_id=record.job_id,
            job_attempt_index=record.job_attempt_index,
            anomalies=record.anomalies,
        )
        if advice_enabled:
            mock_storage.upsert_advice.assert_called_once()
            mock_audit_buffer.add.assert_called_once()
        else:
            mock_storage.upsert_advice.assert_not_called()

    on_anomaly_persisted.side_effect = verify_persistence_before_attribution

    with (
        patch("job_manager.periodic_tasks.tracer") as mock_tracer,
        patch("job_manager.periodic_tasks.CRASH_ANOMALY_GENERATED_COUNTER") as generated_counter,
    ):
        mock_detection_span = MagicMock()
        mock_tracer.start_span.return_value = mock_detection_span
        mock_persistence_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_persistence_span

        generate_anomaly_event_for_failed_jobs(
            storage=mock_storage,
            audit_buffer=mock_audit_buffer,
            time_window_secs=1800,
            threshold_secs=300,
            on_anomaly_persisted=on_anomaly_persisted,
            advice_enabled=advice_enabled,
        )

        # Verify OpenTelemetry spans were created
        mock_tracer.start_span.assert_called_once_with(
            name="job_anomaly.detection",
            kind=SpanKind.PRODUCER,
            attributes={
                "job_id": job_record.job_id,
                "job_attempt_index": job_record.job_attempt_index,
                "job_anomaly.type": EventType.CRASH,
                "job_anomaly.uuid": job_record.anomalies[0].uuid,
            },
            start_time=int(job_record.end_time.timestamp() * 1_000_000_000),
        )
        mock_detection_span.end.assert_called_once()
        mock_detection_span.set_status.assert_called_once_with(StatusCode.OK)

        mock_tracer.start_as_current_span.assert_called_once_with(
            name="job_anomaly.persistence",
            kind=SpanKind.INTERNAL,
            context=trace.set_span_in_context(mock_detection_span),
            attributes={
                "job_id": job_record.job_id,
                "job_attempt_index": job_record.job_attempt_index,
                "job_anomaly.type": EventType.CRASH,
                "job_anomaly.uuid": job_record.anomalies[0].uuid,
            },
        )
        mock_persistence_span.set_status.assert_called_once_with(StatusCode.OK)
        generated_counter.inc.assert_called_once_with(1)

    on_anomaly_persisted.assert_called_once_with(job_record)

    assert len(job_record.anomalies) == 1
    assert job_record.anomalies[0].anomaly_type == EventType.CRASH.value
    mock_storage.update_job_record.assert_called_once_with(
        job_id=job_record.job_id,
        job_attempt_index=job_record.job_attempt_index,
        anomalies=job_record.anomalies,
    )
    if advice_enabled:
        audit_event = mock_audit_buffer.add.call_args.args[0]
        assert audit_event.event_type == "advice_created"
        assert audit_event.heimdall_job_id == str(job_record.heimdall_job_id)
        assert audit_event.payload.attempt_index == 2
        assert audit_event.payload.creation_reasons == ["CRASH"]
        issue = mock_storage.upsert_advice.call_args.args[0].issues[0]
        assert abs(issue.timestamp - job_record.anomalies[0].timestamp) <= timedelta(microseconds=1)
        assert issue.detected_by == "Heimdall Job Manager"
    else:
        mock_audit_buffer.add.assert_not_called()


def test_crash_event_not_generated_for_failed_jobs_with_anomaly():
    # Test that a crash anomaly is not generated for a failed job that already has an anomaly.
    mock_storage = MagicMock(spec=MySQLStorage)
    mock_audit_buffer = MagicMock()
    on_anomaly_persisted = MagicMock()

    now = datetime.now(UTC)

    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=0,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        end_time=now - timedelta(minutes=15),
    )
    job_record.anomalies = [
        JobAnomaly(
            uuid=str(uuid6.uuid7()),
            anomaly_type=EventType.CRASH,
            timestamp=now - timedelta(minutes=20),
        )
    ]

    mock_storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]

    generate_anomaly_event_for_failed_jobs(
        storage=mock_storage,
        audit_buffer=mock_audit_buffer,
        time_window_secs=1800,
        threshold_secs=300,
        on_anomaly_persisted=on_anomaly_persisted,
    )

    on_anomaly_persisted.assert_not_called()
    mock_storage.update_job_record.assert_not_called()
    mock_audit_buffer.add.assert_not_called()


def test_crash_event_not_generated_for_failed_jobs_threshold_not_reached():
    # Test that a crash anomaly is not generated for a failed job without any anomalies but hasn't failed for long enough.
    mock_storage = MagicMock(spec=MySQLStorage)
    mock_audit_buffer = MagicMock()
    on_anomaly_persisted = MagicMock()

    now = datetime.now(UTC)

    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=0,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        # 3 minutes (180 seconds) since the job ended, which is less than the threshold of 300 seconds.
        end_time=now - timedelta(minutes=3),
    )

    mock_storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]

    generate_anomaly_event_for_failed_jobs(
        storage=mock_storage,
        audit_buffer=mock_audit_buffer,
        time_window_secs=1800,
        threshold_secs=300,
        on_anomaly_persisted=on_anomaly_persisted,
    )

    on_anomaly_persisted.assert_not_called()
    mock_storage.update_job_record.assert_not_called()
    mock_audit_buffer.add.assert_not_called()


def test_crash_event_not_generated_for_failed_jobs_without_end_time():
    # Test that a crash anomaly is not generated for a failed job without an end_time.
    mock_storage = MagicMock(spec=MySQLStorage)
    mock_audit_buffer = MagicMock()
    on_anomaly_persisted = MagicMock()

    now = datetime.now(UTC)

    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=0,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        end_time=None,
    )

    mock_storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]

    generate_anomaly_event_for_failed_jobs(
        storage=mock_storage,
        audit_buffer=mock_audit_buffer,
        time_window_secs=1800,
        threshold_secs=300,
        on_anomaly_persisted=on_anomaly_persisted,
    )

    on_anomaly_persisted.assert_not_called()
    mock_storage.update_job_record.assert_not_called()
    mock_audit_buffer.add.assert_not_called()


def test_failed_crash_anomaly_persistence_does_not_trigger_attribution():
    """Storage failure must not start attribution or count an uncommitted anomaly."""
    storage = MagicMock(spec=MySQLStorage)
    audit_buffer = MagicMock()
    on_anomaly_persisted = MagicMock()
    now = datetime.now(UTC)
    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=0,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        end_time=now - timedelta(minutes=15),
    )
    storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]
    storage.update_job_record.side_effect = RecoveryStorageException("Persistence failed")

    with patch("job_manager.periodic_tasks.CRASH_ANOMALY_GENERATED_COUNTER") as generated_counter:
        generate_anomaly_event_for_failed_jobs(
            storage=storage,
            audit_buffer=audit_buffer,
            time_window_secs=1800,
            threshold_secs=300,
            on_anomaly_persisted=on_anomaly_persisted,
            advice_enabled=True,
        )

        generated_counter.inc.assert_not_called()
    storage.update_job_record.assert_called_once()
    storage.upsert_advice.assert_not_called()
    on_anomaly_persisted.assert_not_called()
    audit_buffer.add.assert_not_called()


def test_generated_crash_anomaly_trace_propagates_to_direct_attribution():
    """The persisted anomaly and direct ADI message stay on the same detection trace."""
    storage = MagicMock(spec=MySQLStorage)
    publisher = MagicMock(spec=RabbitMQPublisher)
    now = datetime.now(UTC)
    job_record = create_example_slurm_job_record(
        job_id="123456",
        attempt_index=0,
        state="FAILED",
        submit_time=now - timedelta(minutes=90),
        end_time=now - timedelta(minutes=15),
    )
    storage.read_job_records.side_effect = [([job_record], 1), ([], 0)]
    storage.update_job_record.return_value = UpdateJobResult(
        updated_record=job_record,
        old_record=deepcopy(job_record),
    )
    storage.claim_attribution_request_emission.return_value = True

    def emit_attribution(record):
        assert emit_attribution_request_if_needed(
            record,
            storage=storage,
            publisher=publisher,
            routing_key="job.attribution",
            enabled=True,
            straggler_enabled=False,
        )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    try:
        with patch("job_manager.periodic_tasks.tracer", provider.get_tracer(__name__)):
            generate_anomaly_event_for_failed_jobs(
                storage=storage,
                audit_buffer=MagicMock(),
                time_window_secs=1800,
                threshold_secs=300,
                on_anomaly_persisted=emit_attribution,
            )

        publisher.publish_message.assert_called_once()
        headers = publisher.publish_message.call_args.kwargs["properties"].headers
        published_context = trace.get_current_span(TraceContextTextMapPropagator().extract(headers)).get_span_context()
        assert published_context.is_valid
        persisted_anomaly = storage.update_job_record.call_args.kwargs["anomalies"][0]
        assert format(published_context.trace_id, "032x") == persisted_anomaly.trace_id

        spans = {span.name: span for span in exporter.get_finished_spans()}
        detection = spans["job_anomaly.detection"]
        persistence = spans["job_anomaly.persistence"]
        assert persistence.parent.span_id == detection.context.span_id
        assert published_context.span_id == persistence.context.span_id
    finally:
        provider.shutdown()


def test_failed_advice_write_is_repaired_on_next_scan_before_attribution(monkeypatch):
    """A durable CRASH keeps its original cause until Advice succeeds on a later scan."""
    monkeypatch.setattr("job_manager.periodic_tasks.time_module.time_ns", lambda: 1791423450467741568)
    storage = MagicMock(spec=MySQLStorage)
    audit_buffer = MagicMock()
    now = datetime.now(UTC)
    durable_job = create_example_slurm_job_record(
        job_id="123456", attempt_index=2, state="FAILED", end_time=now - timedelta(minutes=15)
    )
    durable_job.heimdall_job_id = uuid6.uuid7()
    storage.read_advice.return_value = None

    def persist_job(**updates):
        old_record = deepcopy(durable_job)
        durable_job.anomalies = deepcopy(updates["anomalies"])
        return UpdateJobResult(updated_record=deepcopy(durable_job), old_record=old_record)

    storage.update_job_record.side_effect = persist_job
    storage.upsert_advice.side_effect = RecoveryStorageException("Advice storage unavailable")
    on_anomaly_persisted = MagicMock()

    def scan():
        storage.read_job_records.side_effect = [([deepcopy(durable_job)], 1), ([], 0)]
        generate_anomaly_event_for_failed_jobs(
            storage, audit_buffer, 1800, 300, on_anomaly_persisted, advice_enabled=True
        )

    scan()
    storage.update_job_record.assert_called_once()
    on_anomaly_persisted.assert_not_called()
    audit_buffer.add.assert_not_called()
    original = durable_job.anomalies[0]

    def persist_advice(advice, **_):
        storage.read_advice.return_value = deepcopy(advice)
        return advice, True, None

    def dispatch(record):
        audit_buffer.add.assert_called_once()
        assert record.anomalies[0].uuid == original.uuid
        durable_job.attribution_request_emitted = True

    storage.upsert_advice.side_effect = persist_advice
    on_anomaly_persisted.side_effect = dispatch
    scan()
    scan()

    storage.update_job_record.assert_called_once()
    assert storage.upsert_advice.call_count == 2  # Failed write, then successful repair.
    on_anomaly_persisted.assert_called_once()
    audit_buffer.add.assert_called_once()
    issue = storage.read_advice.return_value.issues[0]
    assert abs(issue.timestamp - original.timestamp) <= timedelta(microseconds=1)
    assert issue.detected_by == "Heimdall Job Manager"
    assert issue.relevant_log_lines == ["Job CRASH event for failed job with no anomaly."]
    assert audit_buffer.add.call_args.args[0].payload.creation_reasons == ["CRASH"]
