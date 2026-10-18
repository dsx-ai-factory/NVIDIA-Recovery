# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from recovery_libs.testing.http.tracing import TracedTransport, traced


@pytest.fixture(scope="module")
def spans() -> Iterator[InMemorySpanExporter]:
    # OpenTelemetry accepts one global provider per process, so join the one already installed when there is one.
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    exporter.shutdown()


@traced("advice.read")
def read_advice(*, job_id: str, attempt: int, verbose: bool = False, filters: tuple[str, ...] = ()) -> str:
    return f"{job_id}:{attempt}"


@traced("advice.fail")
def fail_reading(*, job_id: str) -> None:
    raise RuntimeError("unavailable")


def test_traced_operation_runs_in_a_named_span_with_identity_attributes(spans: InMemorySpanExporter) -> None:
    spans.clear()

    assert read_advice(job_id="6", attempt=0, verbose=True, filters=("a",)) == "6:0"

    [span] = spans.get_finished_spans()
    assert span.name == "advice.read"
    assert dict(span.attributes) == {"job_id": "6", "attempt": 0}


def test_traced_operation_error_marks_its_span_and_propagates(spans: InMemorySpanExporter) -> None:
    spans.clear()

    with pytest.raises(RuntimeError, match="unavailable"):
        fail_reading(job_id="6")

    [span] = spans.get_finished_spans()
    assert span.status.status_code is trace.StatusCode.ERROR


def _transport(status_code: int) -> tuple[TracedTransport, list[httpx.Request]]:
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(status_code)

    return TracedTransport(httpx.MockTransport(respond)), sent


def test_request_span_carries_trace_context_without_changing_the_callers_request(
    spans: InMemorySpanExporter,
) -> None:
    spans.clear()
    transport, sent = _transport(200)
    original = httpx.Request("GET", "http://service.test/advice/by-job/6/0")

    transport.handle_request(original)

    [span] = spans.get_finished_spans()
    assert span.name == "GET /advice/by-job/6/0"
    assert span.kind is trace.SpanKind.CLIENT
    assert span.attributes["http.response.status_code"] == 200
    assert sent[0].headers["traceparent"].split("-")[1] == format(span.context.trace_id, "032x")
    assert "traceparent" not in original.headers


@pytest.mark.parametrize(("status_code", "error"), [(200, False), (404, True), (503, True)])
def test_request_span_status_reflects_the_response(spans: InMemorySpanExporter, status_code: int, error: bool) -> None:
    spans.clear()
    transport, _ = _transport(status_code)

    transport.handle_request(httpx.Request("GET", "http://service.test/jobs"))

    [span] = spans.get_finished_spans()
    assert (span.status.status_code is trace.StatusCode.ERROR) is error
