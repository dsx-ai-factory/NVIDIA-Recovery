# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Cross-service HTTP integration test for the attribution_adapter FACT v2 migration (design §11).

Exercises the REAL HTTP boundary end to end:

    Policy ``AttributionClient`` (POST /v1/attribution, requests.post)
        -> NV FastAPI ``app`` (via ``fastapi.testclient.TestClient``)
        -> mocked FACT / log-query / Prometheus collaborators inside the NV service.

``requests.post`` (used by ``attribution_client.attribute``) is monkeypatched to forward
the raw ``data=request.model_dump_json()`` body to the TestClient via ``content=`` (NOT
``json=``), and the TestClient ``Response`` already exposes
``.text``/``.status_code``/``.raise_for_status()``/``.json()`` so it is used directly.

Invocation — run from the workspace root with::

    uv run --package attribution-adapter pytest attribution_adapter/tests/provider/test_attribution_roundtrip.py -q
"""

import json
from pathlib import Path

import attribution_adapter.main as nv_main

# golden_fixtures is in the same tests/ directory; conftest.py adds it to sys.path.
import golden_fixtures
import pytest
import requests
from anomaly_detection_isolator.attribution_client import AttributionClient
from anomaly_detection_isolator.result_builder import build_attribution_result
from attribution_adapter.attribution_service import AttributionService
from attribution_adapter.settings.settings import AttributionQueries, LogQueryProviderSettings
from fastapi.testclient import TestClient
from recovery_libs.storage.node_events import ErrorAttributionType

GOLDEN_PATH = Path(__file__).resolve().parent / "golden_messages.json"
GOLDEN_MESSAGES = json.loads(GOLDEN_PATH.read_text())


# --------------------------------------------------------------------------- #
# Fixtures: a stubbed NV service wired into the FastAPI app + a routed client
# --------------------------------------------------------------------------- #
def _make_stub_service() -> AttributionService:
    """Build an AttributionService without its real __init__ and stub every collaborator.

    The fetch loops call log_fetcher / metrics_fetcher and the FACT collaborators; we stub
    all of them so attribute() reaches build_response without touching the log backend,
    Prometheus, or FACT for real.
    """
    from unittest.mock import MagicMock

    svc = AttributionService.__new__(AttributionService)
    svc.settings = MagicMock()
    # Knobs read by the ported fetch loops.
    svc.max_entries_per_request_syslogs = 1000
    svc.max_entries_per_request_applogs = 700
    svc.log_fetch_direction = "forward"
    svc.log_fetch_timeout_syslogs = None
    svc.log_fetch_timeout_applogs = None
    svc.log_fetch_timeout_slow_signals = None
    # Query getters read these off settings; give them concrete strings.
    svc.settings.log_query_backend = "loki"
    svc.settings.log_query_providers = {
        "loki": LogQueryProviderSettings(
            queries=AttributionQueries(
                system="syslog_q",
                application="applog_q_{}",
                epilogue="epilog_q_{}",
            )
        )
    }

    svc.fact_client = MagicMock()
    svc.fact_client.create_request.return_value = "fact-session-id"
    svc.fact_client.supply_logs.return_value = "ok"
    svc.fact_client.supply_metrics.return_value = "ok"

    svc.log_fetcher = MagicMock()
    svc.log_fetcher.get_sys_telemetry_logs.return_value = ("[]", None)
    svc.log_fetcher.get_app_telemetry_logs.return_value = ("[]", None)

    svc.metrics_fetcher = MagicMock()
    svc.metrics_fetcher.get_metrics.return_value = {}
    return svc


@pytest.fixture
def nv_service(monkeypatch):
    """A stubbed AttributionService patched in as attribution_adapter.main.get_attribution_service."""
    svc = _make_stub_service()
    monkeypatch.setattr(nv_main, "get_attribution_service", lambda: svc)
    return svc


@pytest.fixture
def routed_client(nv_service, monkeypatch):
    """A Policy AttributionClient whose requests.post is routed at the NV TestClient.

    attribute() posts with ``data=request.model_dump_json()``; the shim forwards that raw
    body via ``content=`` so FastAPI parses an identical JSON body, then returns the
    TestClient Response unchanged (it already satisfies the .text/.status_code/.json/
    .raise_for_status contract attribute() relies on).
    """
    test_client = TestClient(nv_main.app)

    def fake_post(url, data=None, headers=None, timeout=None, **kwargs):
        path = "/v1/attribution"
        return test_client.post(path, content=data, headers=headers)

    monkeypatch.setattr("anomaly_detection_isolator.attribution_client.requests.post", fake_post)

    client = AttributionClient.__new__(AttributionClient)
    client.settings = type(
        "S",
        (),
        {
            "attribution_enabled": True,
            "attribution_service_url": "http://attribution-adapter",
            "attribution_fast_http_timeout_seconds": 90,
            "attribution_slow_http_timeout_seconds": 180,
        },
    )()
    return client


# --------------------------------------------------------------------------- #
# Case 1 — FAST round-trip is byte-identical to the frozen golden snapshot.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("case", golden_fixtures.CASE_NAMES)
def test_fast_roundtrip_byte_identical(nv_service, routed_client, case):
    """Driving FACT's get_attribution with a golden fixture reproduces the legacy bytes over HTTP."""
    nv_service.fact_client.get_attribution.return_value = golden_fixtures.build_cases()[case]

    job_status = golden_fixtures.make_job_status()
    anomaly = golden_fixtures.make_anomaly()

    req = routed_client.build_fast_request(job_status, anomaly)
    resp = routed_client.attribute(req)

    assert resp.error is None

    result = build_attribution_result(
        resp,
        job_status,
        golden_fixtures.ANOMALY_TYPE,
        golden_fixtures.ANOMALY_UUID,
        ErrorAttributionType.FAST,
        golden_fixtures.capture_settings(),
    )

    from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor

    proc = AnomalyProcessor.__new__(AnomalyProcessor)
    proc.settings = golden_fixtures.capture_settings()
    result = proc._sanitize_attribution_result(result, job_status)

    assert json.loads(result.to_json()) == GOLDEN_MESSAGES[case]


# --------------------------------------------------------------------------- #
# Case 2 — SLOW continue_session threading: NV uses it as the FACT id.
# --------------------------------------------------------------------------- #
def test_slow_continue_session_threading(nv_service, routed_client):
    """A SLOW request's continue_session reaches NV and is used as the FACT attribution id."""
    nv_service.fact_client.get_attribution.return_value = golden_fixtures.build_cases()["node_single"]

    job_status = golden_fixtures.make_job_status()  # has start_time and end_time set
    epilog_end = golden_fixtures.FROZEN_JOB_END  # non-None so the SLOW pass is not aborted

    req = routed_client.build_slow_request(
        job_status,
        golden_fixtures.ANOMALY_TYPE,
        golden_fixtures.ANOMALY_UUID,
        continue_session="sess-123",
        epilog_end_time=epilog_end,
    )
    resp = routed_client.attribute(req)

    assert resp.error is None
    # NV used the continuation token as the FACT session id ...
    nv_service.fact_client.get_attribution.assert_called_once_with("sess-123")
    # ... and did NOT start a brand-new FACT request for the SLOW pass.
    nv_service.fact_client.create_request.assert_not_called()


# --------------------------------------------------------------------------- #
# Case 3 — Failure modes all yield an empty (non-None) result (design §10 / D7).
# --------------------------------------------------------------------------- #
def _empty_result_from(resp, job_status):
    """Reconstruct the Policy result from a response and assert it is empty but non-None."""
    result = build_attribution_result(
        resp,
        job_status,
        golden_fixtures.ANOMALY_TYPE,
        golden_fixtures.ANOMALY_UUID,
        ErrorAttributionType.FAST,
        golden_fixtures.capture_settings(),
    )
    assert result is not None
    assert result.node_attributions == {}
    assert result.global_attributions == {}
    return result


def test_failure_nv_raises_yields_empty(nv_service, routed_client):
    """(a) NV raises internally -> response carries an error + empty attributions; Policy result is empty non-None."""
    nv_service.fact_client.get_attribution.side_effect = RuntimeError("boom inside NV")

    job_status = golden_fixtures.make_job_status()
    anomaly = golden_fixtures.make_anomaly()

    req = routed_client.build_fast_request(job_status, anomaly)
    resp = routed_client.attribute(req)

    assert resp.error is not None
    assert resp.attributions == []
    _empty_result_from(resp, job_status)


def test_failure_attribution_disabled_no_http(monkeypatch):
    """(b) attribution_enabled=False -> normal empty response with NO HTTP call; result is empty non-None."""

    # Any HTTP attempt should blow up: prove no request is made.
    def explode(*args, **kwargs):
        raise AssertionError("requests.post must not be called when attribution is disabled")

    monkeypatch.setattr("anomaly_detection_isolator.attribution_client.requests.post", explode)

    client = AttributionClient.__new__(AttributionClient)
    client.settings = type(
        "S",
        (),
        {
            "attribution_enabled": False,
            "attribution_service_url": "http://attribution-adapter",
            "attribution_fast_http_timeout_seconds": 90,
            "attribution_slow_http_timeout_seconds": 180,
        },
    )()

    job_status = golden_fixtures.make_job_status()
    anomaly = golden_fixtures.make_anomaly()

    req = client.build_fast_request(job_status, anomaly)
    resp = client.attribute(req)

    assert resp.error is None
    assert resp.attributions == []
    _empty_result_from(resp, job_status)


def test_failure_timeout_yields_empty_never_raises(nv_service, monkeypatch):
    """(c) a transport timeout -> empty result, attribute() never raises."""

    def raise_timeout(*args, **kwargs):
        raise requests.Timeout("connection timed out")

    monkeypatch.setattr("anomaly_detection_isolator.attribution_client.requests.post", raise_timeout)

    client = AttributionClient.__new__(AttributionClient)
    client.settings = type(
        "S",
        (),
        {
            "attribution_enabled": True,
            "attribution_service_url": "http://attribution-adapter",
            "attribution_fast_http_timeout_seconds": 90,
            "attribution_slow_http_timeout_seconds": 180,
        },
    )()

    job_status = golden_fixtures.make_job_status()
    anomaly = golden_fixtures.make_anomaly()

    req = client.build_fast_request(job_status, anomaly)
    resp = client.attribute(req)  # must not raise

    assert resp.error is not None
    assert resp.attributions == []
    _empty_result_from(resp, job_status)
