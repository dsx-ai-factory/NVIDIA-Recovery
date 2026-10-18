# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Tests for anomaly_detection_isolator.attribution_client (design §7.1, §10, §12.1, §12.2)."""

import unittest
import uuid
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import requests
from anomaly_detection_isolator.attribution_client import AttributionClient
from anomaly_detection_isolator.models.audit import (
    ADIAttributionRequestForwardedAuditEvent,
    ADIAttributionResponseReceivedAuditEvent,
)
from anomaly_detection_isolator.settings.settings import Settings
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionErrorType, AttributionPhase
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus

HEIMDALL_JOB_ID = "018f89d8-7550-7df1-a59b-5a5a20d80ef6"


def _make_settings(**overrides) -> Mock:
    """Return a minimal Mock<Settings> with sensible attribution defaults."""
    s = Mock(spec=Settings)
    s.attribution_enabled = True
    s.attribution_service_url = "http://attribution-adapter.svc"
    s.attribution_fast_http_timeout_seconds = 90
    s.attribution_slow_http_timeout_seconds = 180
    for key, value in overrides.items():
        setattr(s, key, value)
    return s


def _make_job_status(**overrides) -> SlurmJobExecutionStatus:
    """Return a representative SlurmJobExecutionStatus."""
    base = {
        "job_id": "job-42",
        "workflow_id": "test-workflow",
        "job_attempt_index": 2,
        "cluster": "test-cluster",
        "job_name": "my-workload",
        "user": "alice",
        "state": "FAILED",
        "command": "srun python train.py",
        "node_count": 8,
        "total_gpu_count": 64,
        "allocated_node_list": ["node1", "node2"],
        "start_time": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        "end_time": datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC),
    }
    base.update(overrides)
    return SlurmJobExecutionStatus(**base)


def _make_anomaly(**overrides) -> JobAnomaly:
    """Return a representative JobAnomaly."""
    base = {
        "anomaly_type": "CRASH",
        "timestamp": datetime(2026, 1, 1, 0, 30, 0, tzinfo=UTC),
        "uuid": "aaaa-bbbb-cccc",
    }
    base.update(overrides)
    return JobAnomaly(**base)


def _make_attribution_response(**overrides) -> WorkloadAttributionResponse:
    """Return a minimal valid WorkloadAttributionResponse for mocking HTTP responses."""
    base = {
        "schema_version": ATTRIBUTION_SCHEMA_VERSION,
        "service_identifier": "attribution-adapter",
        "service_version": "0.1.0",
        "request_id": "job-42",
        "attribution_session_id": "session-xyz",
        "attributions": [],
    }
    base.update(overrides)
    return WorkloadAttributionResponse(**base)


class TestBuildFastRequest(unittest.TestCase):
    """Tests for AttributionClient.build_fast_request field mapping."""

    def setUp(self):
        self.settings = _make_settings()
        self.client = AttributionClient(self.settings)
        self.job_status = _make_job_status()
        self.anomaly = _make_anomaly()

    def test_phase_is_fast(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.phase, AttributionPhase.FAST)

    def test_workload_id_from_job_id(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_id, "job-42")

    def test_user_id_from_user(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.user_id, "alice")

    def test_schema_version_pinned(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.schema_version, ATTRIBUTION_SCHEMA_VERSION)

    def test_continue_session_is_none(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertIsNone(req.continue_session)

    def test_timeout_ms_is_fast(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.timeout_ms, 90 * 1000)

    def test_scope_nodes_from_allocated_node_list(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.scope.nodes, ["node1", "node2"])

    def test_scope_ranks_empty(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.scope.ranks, [])

    def test_scope_start_time_from_job_start(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.scope.start_time, self.job_status.start_time)

    def test_scope_end_time_from_job_end(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.scope.end_time, self.job_status.end_time)

    def test_failure_context_detected_at_from_anomaly_timestamp(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.failure_context.detected_at, self.anomaly.timestamp)

    def test_failure_context_detection_signal_from_anomaly_type(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.failure_context.detection_signal, "CRASH")

    def test_workload_context_attempt_index(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.attempt_index, 2)

    def test_workload_context_command(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.command, "srun python train.py")

    def test_workload_context_cluster(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.cluster, "test-cluster")

    def test_workload_context_workload_name(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.workload_name, "my-workload")

    def test_workload_context_preserves_workflow_id_independently_from_job_name(self):
        job = _make_job_status(job_name="train", workflow_id="experiment-42")

        req = self.client.build_fast_request(job, self.anomaly)

        self.assertEqual(req.workload_context.workflow_id, "experiment-42")
        self.assertEqual(req.workload_context.workload_name, "train")

    def test_workload_context_state(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.state, "FAILED")

    def test_workload_context_node_count(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.node_count, 8)

    def test_workload_context_total_gpu_count(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.total_gpu_count, 64)

    def test_workload_context_job_end_time_matches_job_status_end_time(self):
        """job_end_time is the real job end, distinct from scope.end_time (design §6)."""
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.job_end_time, self.job_status.end_time)

    def test_workload_context_anomaly_uuid(self):
        req = self.client.build_fast_request(self.job_status, self.anomaly)
        self.assertEqual(req.workload_context.anomaly_uuid, "aaaa-bbbb-cccc")

    def test_none_start_end_time_allowed(self):
        """C12: Optional datetimes must pass through as None without raising."""
        job = _make_job_status(start_time=None, end_time=None)
        req = self.client.build_fast_request(job, self.anomaly)
        self.assertIsNone(req.scope.start_time)
        self.assertIsNone(req.scope.end_time)
        self.assertIsNone(req.workload_context.job_end_time)

    def test_none_optional_fields_coerced_to_empty_string(self):
        """None user/command/cluster/job_name/state/anomaly_uuid → empty string."""
        job = _make_job_status(user=None, command=None, cluster=None, job_name=None, state=None)
        anomaly = _make_anomaly(uuid=None)
        req = self.client.build_fast_request(job, anomaly)
        self.assertEqual(req.user_id, "")
        self.assertEqual(req.workload_context.command, "")
        self.assertEqual(req.workload_context.cluster, "")
        self.assertEqual(req.workload_context.workload_name, "")
        self.assertEqual(req.workload_context.state, "")
        self.assertEqual(req.workload_context.anomaly_uuid, "")

    def test_none_allocated_node_list_becomes_empty_list(self):
        job = _make_job_status(allocated_node_list=None)
        req = self.client.build_fast_request(job, self.anomaly)
        self.assertEqual(req.scope.nodes, [])


class TestBuildSlowRequest(unittest.TestCase):
    """Tests for AttributionClient.build_slow_request field mapping (design §7.1)."""

    def setUp(self):
        self.settings = _make_settings()
        self.client = AttributionClient(self.settings)
        self.job_status = _make_job_status()
        self.epilog_end = datetime(2026, 1, 1, 1, 10, 0, tzinfo=UTC)
        self.continue_session = "session-abc"

    def _build(self, job_status=None, epilog_end=None):
        return self.client.build_slow_request(
            job_status=job_status or self.job_status,
            anomaly_type="CRASH",
            anomaly_uuid="slow-uuid",
            continue_session=self.continue_session,
            epilog_end_time=epilog_end or self.epilog_end,
        )

    def test_phase_is_slow(self):
        req = self._build()
        self.assertEqual(req.phase, AttributionPhase.SLOW)

    def test_continue_session_set(self):
        req = self._build()
        self.assertEqual(req.continue_session, "session-abc")

    def test_timeout_ms_is_slow(self):
        req = self._build()
        self.assertEqual(req.timeout_ms, 180 * 1000)

    def test_scope_start_time_is_job_end(self):
        """The epilog window start = job end (real job end, not anomaly_time)."""
        req = self._build()
        self.assertEqual(req.scope.start_time, self.job_status.end_time)

    def test_scope_end_time_is_epilog_end(self):
        req = self._build()
        self.assertEqual(req.scope.end_time, self.epilog_end)

    def test_scope_nodes_from_allocated_node_list(self):
        req = self._build()
        self.assertEqual(req.scope.nodes, ["node1", "node2"])

    def test_workload_context_job_end_time_is_real_job_end(self):
        """job_end_time = job_status.end_time, distinct from scope.end_time."""
        req = self._build()
        self.assertEqual(req.workload_context.job_end_time, self.job_status.end_time)

    def test_workload_context_anomaly_uuid(self):
        req = self._build()
        self.assertEqual(req.workload_context.anomaly_uuid, "slow-uuid")

    def test_workload_context_preserves_workflow_id_independently_from_job_name(self):
        job = _make_job_status(job_name="train", workflow_id="experiment-42")

        req = self._build(job_status=job)

        self.assertEqual(req.workload_context.workflow_id, "experiment-42")
        self.assertEqual(req.workload_context.workload_name, "train")

    def test_failure_context_detection_signal(self):
        req = self._build()
        self.assertEqual(req.failure_context.detection_signal, "CRASH")

    def test_failure_context_detected_at_uses_job_end_time(self):
        req = self._build()
        self.assertEqual(req.failure_context.detected_at, self.job_status.end_time)

    def test_failure_context_detected_at_fallback_when_no_end_time(self):
        """When job_status.end_time is None, detected_at falls back to datetime.now(utc)."""
        job = _make_job_status(end_time=None)
        before = datetime.now(tz=UTC)
        req = self._build(job_status=job)
        after = datetime.now(tz=UTC)
        self.assertIsNone(req.scope.start_time)  # same as job.end_time
        self.assertIsNotNone(req.failure_context.detected_at)
        # detected_at should be between before and after (within the test's timespan)
        self.assertGreaterEqual(req.failure_context.detected_at, before)
        self.assertLessEqual(req.failure_context.detected_at, after)

    def test_workload_id_from_job_id(self):
        req = self._build()
        self.assertEqual(req.workload_id, "job-42")

    def test_schema_version_pinned(self):
        req = self._build()
        self.assertEqual(req.schema_version, ATTRIBUTION_SCHEMA_VERSION)


class TestAttribute(unittest.TestCase):
    """Tests for AttributionClient.attribute (HTTP call behaviour)."""

    def setUp(self):
        self.job_status = _make_job_status()
        self.anomaly = _make_anomaly()

    def test_disabled_returns_empty_response_no_http_call(self):
        """Intentional disablement returns empty without HTTP or a failure signal."""
        settings = _make_settings(attribution_enabled=False)
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        for phase, session in [(AttributionPhase.FAST, None), (AttributionPhase.SLOW, "prior-session")]:
            request = req.model_copy(update={"phase": phase, "continue_session": session})
            with patch("requests.post") as mock_post:
                resp = client.attribute(request)

            mock_post.assert_not_called()
            self.assertEqual(resp.attributions, [])
            self.assertIsNone(resp.error)
            self.assertEqual(resp.request_id, req.workload_id)
            self.assertEqual(resp.attribution_session_id, session or "")

    def test_no_url_returns_empty_response_no_http_call(self):
        """When attribution_service_url is None the client returns empty without HTTP."""
        settings = _make_settings(attribution_enabled=True, attribution_service_url=None)
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with patch("requests.post") as mock_post:
            resp = client.attribute(req)

        mock_post.assert_not_called()
        self.assertEqual(resp.attributions, [])
        self.assertEqual(resp.error.type, AttributionErrorType.SERVICE_ERROR)

    def test_disabled_does_not_emit_audit_event(self):
        settings = _make_settings(attribution_enabled=False)
        audit_event_buffer = Mock()
        client = AttributionClient(settings, audit_event_buffer=audit_event_buffer)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with patch("requests.post"):
            client.attribute(req, heimdall_job_id=HEIMDALL_JOB_ID)

        audit_event_buffer.add.assert_not_called()

    def test_missing_heimdall_job_id_warns_and_does_not_emit_audit_event(self):
        settings = _make_settings(attribution_enabled=False)
        audit_event_buffer = Mock()
        client = AttributionClient(settings, audit_event_buffer=audit_event_buffer)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with patch("anomaly_detection_isolator.attribution_client.logger.warning") as warning:
            client.attribute(req)

        warning.assert_called_once()
        self.assertIn("heimdall_job_id is missing", warning.call_args.args[0])
        audit_event_buffer.add.assert_not_called()

    def test_missing_heimdall_job_id_warns_with_noop_audit_buffer(self):
        settings = _make_settings(attribution_enabled=False)
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with patch("anomaly_detection_isolator.attribution_client.logger.warning") as warning:
            client.attribute(req)

        warning.assert_called_once()

    def test_success_fast_uses_fast_timeout(self):
        """On a 2xx response the POST uses the FAST timeout."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        response_body = _make_attribution_response()

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = response_body.model_dump_json()
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp) as mock_post,
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        mock_post.assert_called_once()
        _, call_kwargs = mock_post.call_args
        self.assertEqual(call_kwargs["timeout"], 90)  # FAST timeout in seconds
        self.assertIsInstance(resp, WorkloadAttributionResponse)
        self.assertEqual(resp.service_identifier, "attribution-adapter")

    def test_success_slow_uses_slow_timeout(self):
        """On a 2xx response the POST uses the SLOW timeout."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_slow_request(
            job_status=self.job_status,
            anomaly_type="CRASH",
            anomaly_uuid="uuid-slow",
            continue_session="session-1",
            epilog_end_time=datetime(2026, 1, 1, 1, 10, tzinfo=UTC),
        )

        response_body = _make_attribution_response()

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = response_body.model_dump_json()
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp) as mock_post,
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        mock_post.assert_called_once()
        _, call_kwargs = mock_post.call_args
        self.assertEqual(call_kwargs["timeout"], 180)  # SLOW timeout in seconds
        self.assertIsInstance(resp, WorkloadAttributionResponse)

    def test_success_url_uses_v1_attribution_endpoint(self):
        """The POST URL is attribution_service_url + '/v1/attribution' (trailing slash stripped)."""
        settings = _make_settings(attribution_service_url="http://attribution-adapter.svc/")
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        response_body = _make_attribution_response()
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = response_body.model_dump_json()
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp) as mock_post,
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            client.attribute(req)

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["url"], "http://attribution-adapter.svc/v1/attribution")

    def test_success_trace_headers_injected(self):
        """W3C trace context headers are injected into the POST request (design §12.2)."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        response_body = _make_attribution_response()
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = response_body.model_dump_json()
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp) as mock_post,
            patch(
                "anomaly_detection_isolator.attribution_client.populate_headers_with_span_context",
                return_value={"Content-Type": "application/json", "traceparent": "00-abc-def-01"},
            ) as mock_trace_inject,
        ):
            client.attribute(req)

        mock_trace_inject.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        self.assertIn("Content-Type", call_kwargs["headers"])

    def test_success_response_parsed_correctly(self):
        """A 2xx response body is parsed into a WorkloadAttributionResponse."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        response_body = _make_attribution_response(attribution_session_id="parsed-session")
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.text = response_body.model_dump_json()
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        self.assertEqual(resp.attribution_session_id, "parsed-session")
        self.assertIsNone(resp.error)

    def test_success_emits_paired_request_and_response_audit_events(self):
        settings = _make_settings()
        audit_event_buffer = Mock()
        client = AttributionClient(settings, audit_event_buffer=audit_event_buffer)
        req = client.build_fast_request(self.job_status, self.anomaly)

        response_body = _make_attribution_response()
        mock_resp = Mock(status_code=200, text=response_body.model_dump_json())
        mock_resp.raise_for_status = Mock()

        with (
            patch("requests.post", return_value=mock_resp),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
            patch(
                "anomaly_detection_isolator.attribution_client.time.time_ns",
                return_value=1_700_000_000_123_456_789,
            ),
        ):
            client.attribute(req, heimdall_job_id=HEIMDALL_JOB_ID)

        self.assertEqual(audit_event_buffer.add.call_count, 2)
        request_event = audit_event_buffer.add.call_args_list[0].args[0]
        response_event = audit_event_buffer.add.call_args_list[1].args[0]
        self.assertIsInstance(request_event, ADIAttributionRequestForwardedAuditEvent)
        self.assertIsInstance(response_event, ADIAttributionResponseReceivedAuditEvent)
        self.assertEqual(request_event.heimdall_job_id, HEIMDALL_JOB_ID)
        self.assertEqual(response_event.heimdall_job_id, HEIMDALL_JOB_ID)
        self.assertEqual(request_event.payload.advice_id, HEIMDALL_JOB_ID)
        self.assertEqual(response_event.payload.advice_id, HEIMDALL_JOB_ID)
        self.assertEqual(request_event.payload.job_id, req.workload_id)
        self.assertEqual(response_event.payload.job_id, req.workload_id)
        self.assertEqual(request_event.payload.attempt_index, req.workload_context.attempt_index)
        self.assertEqual(response_event.payload.attempt_index, req.workload_context.attempt_index)
        self.assertEqual(request_event.payload.request_uuid, response_event.payload.request_uuid)
        self.assertEqual(request_event.event_produced_at, 1_700_000_000_123_456)
        self.assertEqual(response_event.event_produced_at, 1_700_000_000_123_456)
        self.assertNotEqual(request_event.idempotency_key, response_event.idempotency_key)
        uuid.UUID(request_event.payload.request_uuid)
        uuid.UUID(request_event.idempotency_key)
        uuid.UUID(response_event.idempotency_key)

    def test_transport_failure_returns_empty_response_never_raises(self):
        """A RequestException yields an empty response; never propagates to the caller."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with (
            patch("requests.post", side_effect=requests.exceptions.ConnectionError("refused")),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        self.assertIsNotNone(resp)
        self.assertEqual(resp.attributions, [])
        self.assertIsNotNone(resp.error)
        self.assertEqual(resp.error.type, AttributionErrorType.SERVICE_ERROR)

    def test_transport_failure_emits_only_request_forwarded_audit_event(self):
        settings = _make_settings()
        audit_event_buffer = Mock()
        client = AttributionClient(settings, audit_event_buffer=audit_event_buffer)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with (
            patch("requests.post", side_effect=requests.exceptions.ConnectionError("refused")),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            client.attribute(req, heimdall_job_id=HEIMDALL_JOB_ID)

        audit_event_buffer.add.assert_called_once()
        event = audit_event_buffer.add.call_args.args[0]
        self.assertIsInstance(event, ADIAttributionRequestForwardedAuditEvent)

    def test_timeout_returns_empty_response_with_timeout_error_type(self):
        """A Timeout exception sets error.type = TIMEOUT (not SERVICE_ERROR)."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with (
            patch("requests.post", side_effect=requests.exceptions.Timeout("timed out")),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        self.assertEqual(resp.error.type, AttributionErrorType.TIMEOUT)
        self.assertIn("timed out", resp.error.message)

    def test_non_2xx_raises_for_status_returns_empty_response(self):
        """A non-2xx response (via raise_for_status) returns empty response, not raises."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        mock_resp = Mock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")

        with (
            patch("requests.post", return_value=mock_resp),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        self.assertEqual(resp.attributions, [])
        self.assertEqual(resp.error.type, AttributionErrorType.SERVICE_ERROR)

    def test_empty_response_mirrors_request_id(self):
        """The empty response's request_id mirrors the request's workload_id."""
        settings = _make_settings(attribution_enabled=False)
        client = AttributionClient(settings)
        req = client.build_fast_request(self.job_status, self.anomaly)

        with patch("requests.post"):
            resp = client.attribute(req)

        self.assertEqual(resp.request_id, req.workload_id)

    def test_empty_response_mirrors_continue_session(self):
        """The empty response's attribution_session_id mirrors the request's continue_session."""
        settings = _make_settings()
        client = AttributionClient(settings)
        req = client.build_slow_request(
            job_status=self.job_status,
            anomaly_type="CRASH",
            anomaly_uuid="u",
            continue_session="my-session",
            epilog_end_time=None,
        )

        with (
            patch("requests.post", side_effect=requests.exceptions.ConnectionError("err")),
            patch("anomaly_detection_isolator.attribution_client.populate_headers_with_span_context"),
        ):
            resp = client.attribute(req)

        self.assertEqual(resp.attribution_session_id, "my-session")


if __name__ == "__main__":
    unittest.main()


def test_attribution_audit_points_to_actual_request_span_under_canonical_recovery():
    """Request/result events must link the measured call, not a consumer transport span."""
    from opentelemetry import context
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from recovery_libs.tracing.recovery import with_recovery_identity
    from recovery_libs.tracing.traces import restore_trace_context

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    sequence_id = str(uuid.uuid4())
    parent = with_recovery_identity(
        restore_trace_context("00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"), sequence_id, "restart"
    )
    token = context.attach(parent)
    buffer = Mock()
    client = AttributionClient(_make_settings(), audit_event_buffer=buffer)
    request = client.build_fast_request(_make_job_status(), _make_anomaly())
    response = Mock(status_code=200, text=_make_attribution_response().model_dump_json())
    try:
        with (
            patch("requests.post", return_value=response),
            patch(
                "anomaly_detection_isolator.attribution_client.trace.get_tracer",
                return_value=provider.get_tracer("test"),
            ),
        ):
            client.attribute(request, heimdall_job_id=HEIMDALL_JOB_ID)
    finally:
        context.detach(token)
    (span,) = exporter.get_finished_spans()
    assert span.parent.span_id == 0x1234567890ABCDEF
    assert span.attributes["recovery.sequence.id"] == sequence_id
    assert span.attributes["job.attempt.source"] == 2
    assert buffer.add.call_count == 2
    for call in buffer.add.call_args_list:
        assert call.args[0].payload.recovery_sequence_id == sequence_id
        assert call.args[0].payload.trace_id == format(span.context.trace_id, "032x")
        assert call.args[0].payload.span_id == format(span.context.span_id, "016x")
