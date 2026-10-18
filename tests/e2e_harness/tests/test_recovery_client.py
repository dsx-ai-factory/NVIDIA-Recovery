# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Callable, Iterator

import httpx
import pytest
from e2e_harness.recovery_client import (
    GatewayApi,
    JobManagerApi,
    NotificationsApi,
    RecoveryApiError,
    traced_http,
)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from recovery_libs.testing.http.retry import RetryPolicy

BASE_URL = "http://service.test"


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


def _recording_client(status_code: int = 200, text: str = "{}") -> tuple[httpx.Client, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code, text=text)

    http = traced_http(BASE_URL, timeout=5.0, transport=httpx.MockTransport(respond), retry=RetryPolicy(attempts=1))
    return http, requests


@pytest.mark.parametrize(
    ("call", "path", "query"),
    [
        (lambda http: JobManagerApi(http).job_status(job_id="6"), "/job_status/6", {}),
        (
            lambda http: JobManagerApi(http).job_status(job_id="6", attempt=1),
            "/job_status/6",
            {"job_attempt_index": "1"},
        ),
        (lambda http: JobManagerApi(http).advice(job_id="6", attempt=0), "/advice/by-job/6/0", {}),
        (lambda http: JobManagerApi(http).jobs(job_id="6"), "/jobs", {"job_id": "6"}),
        (
            lambda http: JobManagerApi(http).error_attributions(job_id="6", attempt=0),
            "/job_error_attributions",
            {"job_id": "6", "job_attempt_index": "0"},
        ),
        (
            lambda http: JobManagerApi(http).dag_dependencies(dependent_job_id="7"),
            "/dag_dependencies",
            {"dependent_job_id": "7"},
        ),
        (lambda http: JobManagerApi(http).requeue_config(), "/config/requeue", {}),
        (
            lambda http: NotificationsApi(http).notifications(job_id="6", created_after="2026-10-08T16:19:06Z"),
            "/notifications",
            {"job_id": "6", "created_after": "2026-10-08T16:19:06Z"},
        ),
        (
            lambda http: NotificationsApi(http).audit_events(
                heimdall_job_id="c1b5", min_event_produced_at=100, limit=100, cursor="next"
            ),
            "/v1/audit/events",
            {"heimdall_job_id": "c1b5", "min_event_produced_at": "100", "limit": "100", "cursor": "next"},
        ),
        (
            lambda http: GatewayApi(http).audit_events(job_id="6", limit=100, attempt=0, event_type="anomaly_detected"),
            "/v1/audit/events",
            {"jobId": "6", "limit": "100", "attemptIndex": "0", "eventType": "anomaly_detected"},
        ),
    ],
)
def test_operations_request_their_service_endpoints(
    spans: InMemorySpanExporter,
    call: Callable[[httpx.Client], object],
    path: str,
    query: dict[str, str],
) -> None:
    http, requests = _recording_client()

    call(http)

    [request] = requests
    assert request.method == "GET"
    assert request.url.path == path
    assert dict(request.url.params) == query


def test_path_identities_are_escaped(spans: InMemorySpanExporter) -> None:
    http, requests = _recording_client()

    JobManagerApi(http).advice(job_id="a/b c", attempt=0)

    assert requests[0].url.raw_path == b"/advice/by-job/a%2Fb%20c/0"


def test_error_status_raises_with_status_url_and_body(spans: InMemorySpanExporter) -> None:
    http, _ = _recording_client(status_code=503, text="downstream audit returned HTTP 500")

    with pytest.raises(RecoveryApiError) as error:
        GatewayApi(http).audit_events(job_id="62", limit=100, event_type="anomaly_detected")

    assert error.value.status_code == 503
    assert error.value.url.startswith(f"{BASE_URL}/v1/audit/events")
    assert error.value.text == "downstream audit returned HTTP 500"


def test_successful_response_is_returned_as_an_immutable_value(spans: InMemorySpanExporter) -> None:
    http, _ = _recording_client(text='{"state": "Restarted"}')

    response = JobManagerApi(http).advice(job_id="6", attempt=0)

    assert response.json() == {"state": "Restarted"}
    with pytest.raises(AttributeError):
        response.text = "changed"


def test_operation_span_wraps_request_span_and_propagates_trace_context(spans: InMemorySpanExporter) -> None:
    spans.clear()
    http, requests = _recording_client()

    JobManagerApi(http).advice(job_id="6", attempt=0)

    request_span, operation_span = spans.get_finished_spans()
    assert operation_span.name == "job_manager.advice"
    assert dict(operation_span.attributes) == {"job_id": "6", "attempt": 0}
    assert request_span.name == "GET /advice/by-job/6/0"
    assert request_span.kind is trace.SpanKind.CLIENT
    assert request_span.parent.span_id == operation_span.context.span_id
    assert request_span.attributes["http.response.status_code"] == 200
    trace_id = format(operation_span.context.trace_id, "032x")
    assert requests[0].headers["traceparent"].split("-")[1] == trace_id
