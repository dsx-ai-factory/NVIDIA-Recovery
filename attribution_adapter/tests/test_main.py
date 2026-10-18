# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Tests for the attribution_adapter FastAPI app (main.py).

Uses a FastAPI ``TestClient``. Per correction C13 the POST tests patch
``attribution_adapter.main.get_attribution_service`` (the endpoint reads the service via
that getter), returning a Mock whose ``.attribute`` returns a canned response. Covers
``/healthz``, FAST success, SLOW continue_session threading, and the error-response shape.
"""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionErrorType, AttributionPhase
from recovery_libs.schema.attribution_interface.response import (
    AttributionError,
    WorkloadAttributionResponse,
)


def make_request_payload(phase="fast", continue_session=None):
    now = datetime.now(UTC).isoformat()
    return {
        "schema_version": ATTRIBUTION_SCHEMA_VERSION,
        "workload_id": "12345",
        "scope": {"nodes": ["node1"], "ranks": [], "start_time": now, "end_time": now},
        "failure_context": {"detected_at": now, "detection_signal": "CRASHNAN"},
        "timeout_ms": 90000,
        "workload_context": {"workflow_id": "workflow-test", "attempt_index": 0},
        "continue_session": continue_session,
        "phase": phase,
    }


@pytest.fixture
def client():
    from attribution_adapter.main import app

    return TestClient(app)


def _stub_response(session_id="sess-1", error=None):
    return WorkloadAttributionResponse(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        service_identifier="attribution-adapter",
        service_version="0.1.0",
        request_id="12345",
        attribution_session_id=session_id,
        error=error,
    )


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_lifespan_closes_and_releases_singleton(monkeypatch):
    """FastAPI shutdown closes the lazily constructed service exactly once."""
    import attribution_adapter.main as main_module

    service = Mock()
    monkeypatch.setattr(main_module, "_attribution_service", service)

    with TestClient(main_module.app):
        assert main_module._attribution_service is service

    service.close.assert_called_once_with()
    assert main_module._attribution_service is None


def test_post_attribution_fast_success(client):
    svc = Mock()
    svc.attribute.return_value = _stub_response("sess-1")
    with patch("attribution_adapter.main.get_attribution_service", return_value=svc):
        r = client.post("/v1/attribution", json=make_request_payload("fast"))
    assert r.status_code == 200
    body = r.json()
    assert body["service_identifier"] == "attribution-adapter"
    assert body["attribution_session_id"] == "sess-1"
    assert svc.attribute.call_args.args[0].phase == AttributionPhase.FAST


def test_post_attribution_slow_threads_continue_session(client):
    svc = Mock()
    svc.attribute.return_value = _stub_response("prev-sess")
    with patch("attribution_adapter.main.get_attribution_service", return_value=svc):
        r = client.post("/v1/attribution", json=make_request_payload("slow", continue_session="prev-sess"))
    assert r.status_code == 200
    called = svc.attribute.call_args.args[0]
    assert called.phase == AttributionPhase.SLOW
    assert called.continue_session == "prev-sess"


def test_post_attribution_error_response_shape(client):
    err = AttributionError(type=AttributionErrorType.SERVICE_ERROR, message="boom")
    svc = Mock()
    svc.attribute.return_value = _stub_response(error=err)
    with patch("attribution_adapter.main.get_attribution_service", return_value=svc):
        r = client.post("/v1/attribution", json=make_request_payload("fast"))
    assert r.status_code == 200
    assert r.json()["error"] == {"type": "service_error", "message": "boom"}


def test_public_app_import_health_and_schema_without_provider():
    """A fresh process cannot import the private client during public API use."""
    import subprocess
    import sys

    code = """
import importlib.abc
import sys

class RejectProvider(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "nvidia_attribution_client" or fullname.startswith("nvidia_attribution_client."):
            raise ModuleNotFoundError("optional provider is absent", name=fullname)

sys.meta_path.insert(0, RejectProvider())
import attribution_adapter.main as main_module
from attribution_adapter.main import app, get_attribution_service
from attribution_adapter.settings.settings import Settings
from fastapi.testclient import TestClient
assert "attribution_adapter.attribution_service" not in sys.modules
assert "attribution_adapter.response_builder" not in sys.modules
with TestClient(app) as client:
    assert client.get("/healthz").json() == {"status": "ok"}
    schema = client.get("/openapi.json").json()
    assert "/v1/attribution" in schema["paths"]
    assert "WorkloadAttributionRequest" in schema["components"]["schemas"]
    assert "WorkloadAttributionResponse" in schema["components"]["schemas"]
assert "attribution_adapter.attribution_service" not in sys.modules
main_module.Settings = lambda: Settings.model_construct(fact_url="http://provider:8000")
try:
    get_attribution_service()
except RuntimeError as exc:
    assert isinstance(exc.__cause__, ModuleNotFoundError)
    assert exc.__cause__.name.startswith("nvidia_attribution_client")
else:
    raise AssertionError("explicit provider use must fail when its client is absent")
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stdout + result.stderr


def test_provider_initialization_failure_prevents_server_start():
    """Explicitly launching the provider cannot serve healthy with invalid setup."""
    import attribution_adapter.main as main_module

    with (
        patch.object(main_module, "get_attribution_service", side_effect=RuntimeError("invalid provider")),
        patch.object(main_module, "start_http_server") as metrics,
        patch.object(main_module.uvicorn, "run") as server,
        pytest.raises(RuntimeError, match="invalid provider"),
    ):
        main_module.main()
    metrics.assert_not_called()
    server.assert_not_called()


def test_invalid_request_is_rejected_without_provider(client):
    """Request validation belongs to the public interface, before provider use."""
    with patch("attribution_adapter.main.get_attribution_service") as provider:
        response = client.post("/v1/attribution", json={"workload_id": "12345"})
    assert response.status_code == 422
    provider.assert_not_called()


@pytest.mark.parametrize("url", [None, "", "relative/path", "ftp://provider", "http://"])
def test_startup_rejects_invalid_provider_url_without_private_imports(monkeypatch, url):
    """Run real endpoint validation before optional imports or server startup."""
    import attribution_adapter.main as main_module
    from attribution_adapter.settings.settings import Settings

    monkeypatch.setattr(main_module, "_attribution_service", None)
    monkeypatch.setattr(main_module, "Settings", lambda: Settings.model_construct(fact_url=url))
    with patch.object(main_module.uvicorn, "run") as server, pytest.raises(ValueError, match="fact_url"):
        main_module.main()
    server.assert_not_called()


@pytest.mark.parametrize("correlated", [False, True])
def test_attribution_span_restores_optional_backend_identity(client, correlated):
    """Public HTTP attribution retains canonical context without inventing a sequence."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    service = Mock()
    service.attribute.return_value = _stub_response()
    headers = {}
    if correlated:
        headers = {
            "traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
            "recovery-sequence-id": "12345678-1234-4321-8123-123456789abc",
            "recovery-operation": "restart",
        }
    with (
        patch("attribution_adapter.main.get_attribution_service", return_value=service),
        patch("attribution_adapter.main.tracer", provider.get_tracer("test")),
    ):
        response = client.post("/v1/attribution", json=make_request_payload(), headers=headers)
    assert response.status_code == 200
    (span,) = exporter.get_finished_spans()
    assert span.name == "job.recovery.attribution"
    assert span.attributes["job.id"] == "12345"
    if correlated:
        assert span.parent.span_id == 0x1234567890ABCDEF
        assert span.attributes["recovery.sequence.id"] == headers["recovery-sequence-id"]
        assert span.attributes["recovery.operation"] == "restart"
    else:
        assert "recovery.sequence.id" not in span.attributes
        assert "recovery.operation" not in span.attributes
