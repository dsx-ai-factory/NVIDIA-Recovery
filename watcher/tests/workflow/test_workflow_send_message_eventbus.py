# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, call

from opentelemetry.trace import StatusCode
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.watcher.model import Attributes, Event


def test_send_message_eventbus_dedup_event_by_body(rules_engine):
    rules_engine.publisher.publish_message = MagicMock()
    events = [
        Event(
            type=EventType.HANGNOOUTPUT,
            body="same body content",
            attributes=Attributes(uuid="uuid-1", job_id="12345", slurm_job_attempt=0),
        ),
        Event(
            type=EventType.HANGNOOUTPUT,
            body="same body content",
            attributes=Attributes(uuid="uuid-2", job_id="12345", slurm_job_attempt=0),
        ),
    ]

    rules_engine.send_message_eventbus(events=events, routing_suffix="TEST", dedup=True)

    assert rules_engine.publisher.publish_message.call_count == 1


def test_send_message_eventbus_no_dedup_publishes_all(rules_engine):
    rules_engine.publisher.publish_message = MagicMock()
    events = [
        Event(
            type=EventType.HANGNOOUTPUT,
            body="same body",
            attributes=Attributes(uuid="uuid-1", job_id="12345", slurm_job_attempt=0),
        ),
        Event(
            type=EventType.HANGNOOUTPUT,
            body="same body",
            attributes=Attributes(uuid="uuid-2", job_id="12345", slurm_job_attempt=0),
        ),
    ]

    rules_engine.send_message_eventbus(events=events, routing_suffix="TEST", dedup=False)

    assert rules_engine.publisher.publish_message.call_count == 2


def test_send_message_eventbus_dedup_different_jobs_not_deduplicated(rules_engine):
    """Same body content from different jobs should NOT be deduplicated."""
    rules_engine.publisher.publish_message = MagicMock()
    events = [
        Event(type=EventType.HANGNOOUTPUT, body="same body", attributes=Attributes(uuid="uuid-1", job_id="job-111")),
        Event(type=EventType.HANGNOOUTPUT, body="same body", attributes=Attributes(uuid="uuid-2", job_id="job-222")),
    ]

    rules_engine.send_message_eventbus(events=events, routing_suffix="TEST", dedup=True)

    assert rules_engine.publisher.publish_message.call_count == 2


def test_send_message_eventbus_sets_span_status_before_end(mocker, rules_engine):
    """set_status must be called before end; calling it after produces an OTel warning."""
    mock_tracer = mocker.patch("watcher.workflow.tracer", autospec=True)
    mocker.patch("watcher.workflow.populate_headers_with_span_context", return_value={})
    mock_span = MagicMock()
    mock_tracer.start_span.return_value = mock_span

    event = Event(type=EventType.CRASH, body="segfault", attributes=Attributes(uuid="u1", job_id="1"))
    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH")

    calls = [c for c in mock_span.method_calls if c[0] in ("set_status", "end")]
    assert calls[-2] == call.set_status(StatusCode.OK)
    assert calls[-1][0] == "end"
