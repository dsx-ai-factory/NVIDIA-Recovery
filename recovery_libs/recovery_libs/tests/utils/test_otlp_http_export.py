# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""OTLP/HTTP export against a stub collector.

A collector serves each signal at ``<base>/v1/<signal>``. These tests drive
the real exporters with the shared environment contract Helm injects and
assert where requests land, so a regression to posting at the bare base URL
(which a collector rejects) fails here.
"""

import logging
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Self
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from recovery_libs.tracing.traces import extract_trace_id_from_span, serialize_trace_context, set_up_trace_provider
from recovery_libs.utils.otlp_handler import OTLPHandler, _reset_shared_provider
from recovery_libs.utils.types import OTLPProtocol


class _StubCollector:
    """Records the path and Authorization header of every POST."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str | None]] = []
        collector = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                collector.requests.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def collector() -> Iterator[_StubCollector]:
    with _StubCollector() as stub:
        yield stub


@pytest.fixture
def otlp_env(monkeypatch: pytest.MonkeyPatch, collector: _StubCollector) -> str:
    """Set the shared OTLP environment with a base path and return that base."""
    base = f"{collector.base_url}/otlp"
    # The exporters use requests, which would route loopback traffic through a developer's proxy.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    for name in ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", base)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "secret-token")
    return base


def _install_trace_provider(service_name: str) -> TracerProvider:
    """Run production setup and return its provider without touching the global one."""
    with patch("recovery_libs.tracing.traces.trace.set_tracer_provider") as set_provider:
        set_up_trace_provider(service_name=service_name)
    return set_provider.call_args.args[0]


def test_trace_export_uses_trace_path_and_bearer_token(otlp_env: str, collector: _StubCollector) -> None:
    provider = _install_trace_provider("test-service")
    try:
        with provider.get_tracer(__name__).start_as_current_span("span"):
            pass
        assert provider.force_flush(timeout_millis=5000)
    finally:
        provider.shutdown()

    assert collector.requests == [("/otlp/v1/traces", "Bearer secret-token")]


def test_log_export_uses_log_path_and_bearer_token(otlp_env: str, collector: _StubCollector) -> None:
    _reset_shared_provider()
    try:
        handler = OTLPHandler(endpoint=otlp_env, protocol=OTLPProtocol.HTTP, token="secret-token")
        handler.emit(logging.LogRecord("test", logging.INFO, __file__, 1, "message", (), None))
        handler.flush()
        handler._logger_provider.shutdown()
    finally:
        _reset_shared_provider()

    assert collector.requests == [("/otlp/v1/logs", "Bearer secret-token")]


def test_signal_specific_trace_endpoint_is_used_verbatim(
    otlp_env: str, collector: _StubCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", f"{collector.base_url}/custom/traces")
    provider = _install_trace_provider("test-service")
    try:
        with provider.get_tracer(__name__).start_as_current_span("span"):
            pass
        assert provider.force_flush(timeout_millis=5000)
    finally:
        provider.shutdown()

    assert collector.requests == [("/custom/traces", "Bearer secret-token")]


def test_spans_without_exporter_propagate_context_but_yield_no_trace_id(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"):
        monkeypatch.delenv(name, raising=False)
    provider = _install_trace_provider("test-service")

    with provider.get_tracer(__name__).start_as_current_span("span") as span:
        # A trace ID recorded for audit or recovery context would point at a trace that is never exported.
        assert extract_trace_id_from_span(span) is None
        # Downstream services still receive a valid, unsampled traceparent.
        assert serialize_trace_context(span=span)["traceparent"].endswith("-00")
