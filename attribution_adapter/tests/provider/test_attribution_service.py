# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Tests for attribution_adapter.attribution_service.AttributionService.

Covers the FAST path (create_request -> fetch/supply loops -> get_attribution -> build_response),
the SLOW path (continue_session used as the FACT id, no create_request), and a top-level FACT
failure producing a SERVICE_ERROR response with empty attributions. The instance-level mocking
style mirrors the legacy ``test_anomaly_processor.setUp``.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from attribution_adapter.attribution_service import AttributionService
from attribution_adapter.settings.settings import AttributionQueries, LogQueryProviderSettings, Settings
from nvidia_attribution_client.models.source import Source
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionErrorType, AttributionPhase
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from requests import RequestException


def make_request(
    phase=AttributionPhase.FAST,
    continue_session=None,
    end_time="__now__",
    start_time="__now__",
):
    """Build a WorkloadAttributionRequest with overridable phase/continue_session/start_time/end_time.

    For SLOW requests the contract is: scope.start_time = epilog window START (real job end),
    scope.end_time = epilog window END (epilog_end_time, distinct and later).
    """
    now = datetime.now(UTC)
    scope_start_time = now if start_time == "__now__" else start_time
    scope_end_time = now if end_time == "__now__" else end_time
    return WorkloadAttributionRequest(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        workload_id="12345",
        scope=WorkloadScope(nodes=["node1"], start_time=scope_start_time, end_time=scope_end_time),
        failure_context=FailureContext(detected_at=now, detection_signal="CRASHNAN"),
        timeout_ms=90000,
        workload_context=WorkloadContext(
            workflow_id="experiment-42",
            attempt_index=0,
            command="srun",
            cluster="c",
            workload_name="train",
            node_count=2,
            total_gpu_count=16,
        ),
        continue_session=continue_session,
        phase=phase,
    )


def make_service():
    """Construct an AttributionService against Mock settings with all I/O mocked out.

    Mirrors test_anomaly_processor.setUp's intent: the collaborator constructors
    (ADILogQueryFetcher / ADIMetricsFetcher / FACTClient) read many Settings attributes
    during __init__, so they are patched to no-ops here; builtins.open is patched for the
    prometheus-auth file read; then log_fetcher/metrics_fetcher/fact_client are replaced on
    the instance with Mocks (replacing them with the canned return values used in the loops).
    """
    settings = Mock(spec=Settings)
    settings.attribution_engines = "FACT"
    settings.is_lepton_enabled = False
    settings.log_query_backend = "loki"
    settings.log_fetch_optimization_enabled = False
    settings.log_query_providers = {
        "loki": LogQueryProviderSettings(
            queries=AttributionQueries(
                system="{job='syslog'}",
                application='{{job="applog", job_id="_{}_"}}',
                epilogue='{{job="epilogue"}}',
            )
        ),
        "victorialogs": LogQueryProviderSettings(
            queries=AttributionQueries(
                system="system-logsql",
                application='_stream:{appName="slurm-app-log"} AND job_id:={job_id}',
                epilogue='_stream:{component="journal"} AND job_id:={job_id}',
            )
        ),
    }
    settings.prometheus_url = "http://prometheus:9090"
    settings.prometheus_hostname_label = "hostname"
    with (
        patch("builtins.open", new_callable=MagicMock),
        patch("attribution_adapter.attribution_service.create_attribution_log_fetcher"),
        patch("attribution_adapter.attribution_service.ADIMetricsFetcher.__init__", return_value=None),
        patch("attribution_adapter.attribution_service.FACTClient.__init__", return_value=None),
    ):
        svc = AttributionService(settings)
    svc.log_fetcher = Mock()
    svc.metrics_fetcher = Mock()
    svc.fact_client = Mock()
    svc.log_fetcher.get_sys_telemetry_logs.return_value = ("[]", None)
    svc.log_fetcher.get_app_telemetry_logs.return_value = ("[]", None)
    svc.metrics_fetcher.get_metrics.return_value = {"result": "[]"}
    return svc


def _stub_response(session_id):
    return WorkloadAttributionResponse(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        service_identifier="attribution-adapter",
        service_version="0.1.0",
        request_id="12345",
        attribution_session_id=session_id,
    )


def test_request_adapter_preserves_workflow_id_independently_from_job_name():
    svc = make_service()

    job_status = svc._job_status_from_request(make_request())

    assert job_status.workflow_id == "experiment-42"
    assert job_status.job_name == "train"


def test_query_getters_select_victorialogs_native_variants():
    """The selected backend chooses native LogsQL variants without translating LogQL."""
    svc = make_service()
    svc.settings.log_query_backend = "victorialogs"
    job_status = svc._job_status_from_request(make_request())

    assert svc._get_log_queries_syslogs(job_status) == {"system-logsql": Source.SYSLOG}
    assert svc._get_log_queries_app_logs(job_status) == {
        '_stream:{appName="slurm-app-log"} AND job_id:=12345': Source.APPLOG
    }
    assert svc._get_slow_signals_log_queries(job_status) == {
        '_stream:{component="journal"} AND job_id:=12345': Source.HEALTH_CHECK
    }


def test_close_releases_log_fetcher_once():
    """Service cleanup is idempotent across explicit and lifespan shutdown calls."""
    svc = make_service()

    svc.close()
    svc.close()

    svc.log_fetcher.close.assert_called_once_with()


def test_victorialogs_records_neutral_line_metric():
    """VictoriaLogs pages are accounted under the selected backend name."""
    svc = make_service()
    svc.settings.log_query_backend = "victorialogs"
    request = make_request()
    anomaly = svc._anomaly_from_request(request)
    job_status = svc._job_status_from_request(request)
    svc.fact_client.create_request.return_value = "fact-id"
    svc.fact_client.get_attribution.return_value = MagicMock()

    with (
        patch.object(svc, "_get_log_queries_syslogs", return_value={"system-logsql": Source.SYSLOG}),
        patch.object(svc, "_get_log_queries_app_logs", return_value={}),
        patch.object(svc, "_get_prom_queries", return_value={}),
        patch("attribution_adapter.attribution_service._record_log_lines_metrics") as record_metric,
    ):
        svc._process_fast_signals_with_fact(anomaly, job_status, run_checkers=True)

    record_metric.assert_called_once_with(
        svc.log_fetcher.get_sys_telemetry_logs.return_value[0],
        "victorialogs",
        "syslogs",
        job_status.job_id,
        job_status.job_attempt_index,
    )


def test_constructor_closes_log_fetcher_when_later_dependency_fails():
    """A partially built service does not leak its already-created log backends."""
    settings = Mock(spec=Settings)
    log_fetcher = Mock()

    with (
        patch(
            "attribution_adapter.attribution_service.create_attribution_log_fetcher",
            return_value=log_fetcher,
        ),
        patch(
            "attribution_adapter.attribution_service.FACTClient",
            side_effect=RuntimeError("FACT construction failed"),
        ),
        pytest.raises(RuntimeError, match="FACT construction failed"),
    ):
        AttributionService(settings)

    log_fetcher.close.assert_called_once_with()


def test_opaque_page_token_keeps_window_fixed_and_repeats_after_fact_413():
    """Shared backend tokens are replayed unchanged, including FACT payload-size retries."""
    svc = make_service()
    request = make_request()
    anomaly = svc._anomaly_from_request(request)
    job_status = svc._job_status_from_request(request)
    svc.log_fetcher.get_sys_telemetry_logs.side_effect = [
        ("first-page", "opaque-token"),
        ("second-page", None),
        ("second-page", None),
    ]
    too_large_response = Mock(status_code=413, text="too large")
    svc.fact_client.supply_logs.side_effect = [
        None,
        RequestException(response=too_large_response),
        None,
    ]
    svc.fact_client.create_request.return_value = "fact-id"
    svc.fact_client.get_attribution.return_value = MagicMock()

    with (
        patch.object(svc, "_get_log_queries_app_logs", return_value={}),
        patch.object(svc, "_get_prom_queries", return_value={}),
    ):
        svc._process_fast_signals_with_fact(anomaly, job_status, run_checkers=True)

    first, second, retry = svc.log_fetcher.get_sys_telemetry_logs.call_args_list
    assert first.kwargs["page_token"] is None
    assert second.kwargs["page_token"] == "opaque-token"
    assert retry.kwargs["page_token"] == "opaque-token"
    assert second.kwargs["start_time"] == first.kwargs["start_time"]
    assert second.kwargs["end_time"] == first.kwargs["end_time"]
    assert retry.kwargs["start_time"] == first.kwargs["start_time"]
    assert retry.kwargs["end_time"] == first.kwargs["end_time"]
    assert retry.kwargs["limit"] == 900


def test_syslog_pages_share_one_monotonic_fetch_budget():
    """Every page receives the remaining budget and no page starts after expiry."""
    svc = make_service()
    svc.log_fetch_timeout_syslogs = 5
    request = make_request()
    anomaly = svc._anomaly_from_request(request)
    job_status = svc._job_status_from_request(request)
    svc.log_fetcher.get_sys_telemetry_logs.side_effect = [("first-page", "opaque-token")]
    svc.fact_client.create_request.return_value = "fact-id"
    svc.fact_client.get_attribution.return_value = MagicMock()

    with (
        patch.object(svc, "_get_log_queries_app_logs", return_value={}),
        patch.object(svc, "_get_prom_queries", return_value={}),
        patch("attribution_adapter.attribution_service.time.monotonic", side_effect=[100.0, 101.0, 106.0]),
        patch("attribution_adapter.attribution_service.LOG_FETCH_TIMEOUT_TOTAL") as timeout_metric,
    ):
        svc._process_fast_signals_with_fact(anomaly, job_status, run_checkers=True)

    svc.log_fetcher.get_sys_telemetry_logs.assert_called_once()
    fetch_call = svc.log_fetcher.get_sys_telemetry_logs.call_args
    assert fetch_call.kwargs["deadline_monotonic"] == 105.0
    timeout_metric.labels.assert_called_once_with(
        log_type="syslogs",
        job_id=job_status.job_id,
        job_attempt_index=job_status.job_attempt_index,
    )
    timeout_metric.labels.return_value.inc.assert_called_once_with()


def test_fast_branch_creates_request_and_builds_response():
    """FAST: create_request supplies the FACT id, get_attribution is fetched, build_response is called."""
    svc = make_service()
    svc.fact_client.create_request.return_value = "fact-id-1"
    svc.fact_client.get_attribution.return_value = MagicMock()
    with patch("attribution_adapter.attribution_service.build_response") as mock_build:
        mock_build.return_value = _stub_response("fact-id-1")
        resp = svc.attribute(make_request(AttributionPhase.FAST))

    svc.fact_client.create_request.assert_called_once()
    svc.fact_client.get_attribution.assert_called_once_with("fact-id-1")
    assert mock_build.call_args.kwargs["attribution_session_id"] == "fact-id-1"
    assert resp.attribution_session_id == "fact-id-1"
    assert resp.error is None


def test_slow_branch_uses_continue_session_as_fact_id():
    """SLOW: continue_session is used as the FACT id; create_request is NOT called.

    Also verifies that get_sys_telemetry_logs is called with the DISTINCT epilog window
    (scope.start_time .. scope.end_time), proving the window is non-degenerate.
    """
    from datetime import timedelta

    svc = make_service()
    svc.fact_client.get_attribution.return_value = MagicMock()

    now = datetime.now(UTC)
    epilog_start = now  # real job end = scope.start_time
    epilog_end = now + timedelta(minutes=10)  # epilog_end_time = scope.end_time (distinct, later)

    with patch("attribution_adapter.attribution_service.build_response") as mock_build:
        mock_build.return_value = _stub_response("prev-sess")
        svc.attribute(
            make_request(
                AttributionPhase.SLOW,
                continue_session="prev-sess",
                start_time=epilog_start,
                end_time=epilog_end,
            )
        )

    svc.fact_client.create_request.assert_not_called()
    svc.fact_client.get_attribution.assert_called_once_with("prev-sess")
    assert mock_build.call_args.kwargs["attribution_session_id"] == "prev-sess"

    # Confirm the Loki call used the distinct epilog window (not the degenerate end..end window).
    call_kwargs = svc.log_fetcher.get_sys_telemetry_logs.call_args.kwargs
    assert call_kwargs["start_time"] == epilog_start, (
        f"Expected epilog start {epilog_start}, got {call_kwargs['start_time']}"
    )
    assert call_kwargs["end_time"] == epilog_end, f"Expected epilog end {epilog_end}, got {call_kwargs['end_time']}"
    assert call_kwargs["start_time"] != call_kwargs["end_time"], "Epilog window must be non-degenerate"


def test_slow_branch_without_end_time_skips_attribution():
    """SLOW: a request with no epilog window (scope.start_time=None) aborts before FACT.

    Mirrors the legacy ``return None`` abort: get_attribution must NOT be called and the
    response carries a NO_ATTRIBUTION_AVAILABLE error with empty attributions.
    Per the SLOW contract, scope.start_time is the epilog window start (real job end);
    when it is absent the whole slow pass is skipped.
    """
    svc = make_service()
    resp = svc.attribute(
        make_request(AttributionPhase.SLOW, continue_session="prev-sess", start_time=None, end_time=None)
    )

    svc.fact_client.get_attribution.assert_not_called()
    svc.fact_client.create_request.assert_not_called()
    assert resp.error is not None
    assert resp.error.type == AttributionErrorType.NO_ATTRIBUTION_AVAILABLE
    assert resp.error.type.value == "no_attribution_available"
    assert resp.attributions == []
    assert resp.attribution_session_id == "prev-sess"
    assert resp.request_id == "12345"


def test_top_level_fact_error_returns_service_error_response():
    """A top-level FACT failure yields a SERVICE_ERROR response with empty attributions."""
    svc = make_service()
    svc.fact_client.create_request.side_effect = RuntimeError("FACT down")
    resp = svc.attribute(make_request(AttributionPhase.FAST))

    assert resp.error is not None
    assert resp.error.type == AttributionErrorType.SERVICE_ERROR
    assert resp.error.message == "FACT down"
    assert resp.attributions == []
    assert resp.service_identifier == "attribution-adapter"
    assert resp.request_id == "12345"


def test_attribute_fast_projects_v2():
    """FAST: attribute() with a real AttributionResponseV2 produces the correct WorkloadAttributionResponse.

    Verifies end-to-end that the v2 response (verdict + symptoms dict) is projected by
    build_response into a WorkloadAttributionResponse with the expected NODE attribution and symptom.
    """
    from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2

    svc = make_service()
    svc.fact_client.create_request.return_value = "att-1"
    v2 = AttributionResponseV2.from_dict(
        {
            "model_type": "AttributionResponseV2",
            "verdict": {
                "attribution_outcome": "Fail InfraHW",
                "terminal_states": ["Fail"],
                "detail": "Known HW Cause - Node",
                "root_symptom": {
                    "symptom": "XID-79",
                    "source": {
                        "observation_id": 1,
                        "source": "syslog",
                        "pos": None,
                        "line": "xid 79",
                        "rule": "r",
                        "time": None,
                    },
                },
            },
            "symptoms": {
                "XID-79": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": {
                            "observation_id": 1,
                            "source": "syslog",
                            "pos": None,
                            "line": "xid 79",
                            "rule": "r",
                            "time": None,
                        },
                    }
                ]
            },
            "observation_context": [1],
        }
    )
    svc.fact_client.get_attribution.return_value = v2

    resp = svc.attribute(make_request(AttributionPhase.FAST))

    assert isinstance(resp, WorkloadAttributionResponse)
    assert resp.error is None
    assert len(resp.attributions) == 1
    a = resp.attributions[0]
    assert a.resource_set.value == "node1"
    assert a.symptom == "XID-79"
