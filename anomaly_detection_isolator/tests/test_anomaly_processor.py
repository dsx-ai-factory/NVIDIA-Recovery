# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor
from anomaly_detection_isolator.settings.settings import Settings
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)


def create_mock_settings(**overrides):
    """
    Helper function to create mock settings with sensible defaults.

    Args:
        **overrides: Any setting attributes to override from defaults

    Returns:
        Mock settings object
    """
    settings = Mock(spec=Settings)

    # Default settings (Policy Engine only — FACT/Loki/Prom config now lives in attribution-adapter)
    settings.fact_non_isolated_errors = None
    settings.fact_isolated_errors = None
    settings.fact_ambiguous_errors = None
    settings.fact_non_isolated_errors_env = None
    settings.fact_isolated_errors_env = None
    settings.fact_ambiguous_errors_env = None
    settings.max_isolated_nodes = 1
    settings.rack_aware_isolation_enabled = False
    settings.max_isolated_nodes_per_rack = None
    settings.max_racks_to_isolate = 9
    settings.rack_extraction_pattern = None
    settings.jobmanager_http_timeout_seconds = 30
    settings.attribution_service_url = "http://attribution:8000"
    settings.attribution_enabled = True
    settings.attribution_fast_http_timeout_seconds = 90
    settings.attribution_slow_http_timeout_seconds = 180

    # Apply any overrides
    for key, value in overrides.items():
        setattr(settings, key, value)

    return settings


def _empty_workload_response() -> WorkloadAttributionResponse:
    """Build an empty (zero-attribution) WorkloadAttributionResponse for tests."""
    return WorkloadAttributionResponse(
        schema_version="1.0",
        service_identifier="attribution-adapter",
        service_version="test",
        request_id="req-1",
        attribution_session_id="session-123",
        attributions=[],
        error=None,
    )


def _build_anomaly_processor(settings) -> AnomalyProcessor:
    """Construct an AnomalyProcessor with a patched AttributionClient.

    The Policy Engine no longer reads any token files or instantiates FACT/Loki/Prom
    clients, so we only stub the attribution collaborator the processor talks to.
    """
    processor = AnomalyProcessor(settings)
    processor.attribution_client = Mock()
    return processor


class TestAnomalyProcessor(unittest.TestCase):
    def setUp(self):
        # Create mock settings using helper
        self.settings = create_mock_settings()

        # Create AnomalyProcessor with patched collaborators
        self.anomaly_processor = _build_anomaly_processor(self.settings)

        # Create test data
        self.job_status = SlurmJobExecutionStatus(
            job_id="12345",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            cluster="test-cluster",
        )

        self.anomaly = JobAnomaly(
            job_id="12345", timestamp=datetime.now(), anomaly_type=EventType.CRASHNAN.value, uuid=str(uuid.uuid4())
        )

    @patch("anomaly_detection_isolator.anomaly_processor.build_attribution_result")
    def test_process_anomaly_with_fast_signals_returns_result_and_session(self, mock_build):
        """A successful FAST pass returns a non-None result and the session token."""
        resp = _empty_workload_response()
        self.anomaly_processor.attribution_client.build_fast_request.return_value = "fast-req"
        self.anomaly_processor.attribution_client.attribute.return_value = resp

        built = AnomalyAttributionResult(
            job_id=self.job_status.job_id,
            job_attempt_index=0,
            anomaly_type=EventType.CRASHNAN,
            anomaly_uuid=self.anomaly.uuid,
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )
        mock_build.return_value = built

        result, session_id = self.anomaly_processor.process_anomaly_with_fast_signals(
            self.anomaly, self.job_status, run_checkers=True
        )

        self.assertIsInstance(result, AnomalyAttributionResult)
        self.assertEqual(session_id, "session-123")
        self.anomaly_processor.attribution_client.build_fast_request.assert_called_once_with(
            self.job_status, self.anomaly
        )
        self.anomaly_processor.attribution_client.attribute.assert_called_once_with("fast-req", heimdall_job_id=None)
        # build_attribution_result called with FAST type and the response
        call_args = mock_build.call_args[0]
        self.assertIs(call_args[0], resp)
        self.assertEqual(call_args[4], ErrorAttributionType.FAST)

    @patch("anomaly_detection_isolator.anomaly_processor.build_attribution_result")
    def test_fast_attribution_receives_heimdall_job_id_when_identity_is_available(self, mock_build):
        resp = _empty_workload_response()
        self.job_status.heimdall_job_id = uuid.UUID("018f89d8-7550-7df1-a59b-5a5a20d80ef6")
        self.anomaly_processor.attribution_client.build_fast_request.return_value = "fast-req"
        self.anomaly_processor.attribution_client.attribute.return_value = resp
        mock_build.return_value = AnomalyAttributionResult(
            job_id=self.job_status.job_id,
            job_attempt_index=0,
            anomaly_type=EventType.CRASHNAN,
            anomaly_uuid=self.anomaly.uuid,
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )

        self.anomaly_processor.process_anomaly_with_fast_signals(
            self.anomaly,
            self.job_status,
            run_checkers=True,
        )

        self.anomaly_processor.attribution_client.attribute.assert_called_once_with(
            "fast-req",
            heimdall_job_id=str(self.job_status.heimdall_job_id),
        )

    @patch("anomaly_detection_isolator.anomaly_processor.build_attribution_result")
    def test_process_anomaly_with_fast_signals_never_none_on_empty_response(self, mock_build):
        """Even when attribute() returns an empty/error response, the result is never None."""
        resp = _empty_workload_response()
        resp.attribution_session_id = ""
        self.anomaly_processor.attribution_client.attribute.return_value = resp

        built = AnomalyAttributionResult(
            job_id=self.job_status.job_id,
            job_attempt_index=0,
            anomaly_type=EventType.CRASHNAN,
            anomaly_uuid=self.anomaly.uuid,
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )
        mock_build.return_value = built

        result, session_id = self.anomaly_processor.process_anomaly_with_fast_signals(
            self.anomaly, self.job_status, run_checkers=True
        )

        self.assertIsNotNone(result)
        self.assertIsInstance(result, AnomalyAttributionResult)
        # Empty session id is normalized to None
        self.assertIsNone(session_id)

    @patch("anomaly_detection_isolator.anomaly_processor.build_attribution_result")
    def test_process_anomaly_with_slow_signals_success(self, mock_build):
        """A SLOW pass with a session token and job end time returns a sanitized result."""
        resp = _empty_workload_response()
        self.anomaly_processor.attribution_client.build_slow_request.return_value = "slow-req"
        self.anomaly_processor.attribution_client.attribute.return_value = resp

        built = AnomalyAttributionResult(
            job_id=self.job_status.job_id,
            job_attempt_index=0,
            anomaly_type=EventType.CRASHNAN,
            anomaly_uuid=self.anomaly.uuid,
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={},
            global_attributions={},
        )
        mock_build.return_value = built

        epilog_end_time = self.job_status.end_time + timedelta(minutes=3)
        result = self.anomaly_processor.process_anomaly_with_slow_signals(
            self.job_status, "session-123", self.anomaly.anomaly_type, self.anomaly.uuid, epilog_end_time
        )

        self.assertIsInstance(result, AnomalyAttributionResult)
        self.anomaly_processor.attribution_client.build_slow_request.assert_called_once_with(
            self.job_status, self.anomaly.anomaly_type, self.anomaly.uuid, "session-123", epilog_end_time
        )
        self.anomaly_processor.attribution_client.attribute.assert_called_once_with("slow-req", heimdall_job_id=None)

    def test_process_anomaly_with_slow_signals_skips_when_no_session(self):
        """SLOW pass returns None (skip) when there is no session token to continue."""
        epilog_end_time = self.job_status.end_time + timedelta(minutes=3)
        result = self.anomaly_processor.process_anomaly_with_slow_signals(
            self.job_status, "", self.anomaly.anomaly_type, self.anomaly.uuid, epilog_end_time
        )
        self.assertIsNone(result)
        self.anomaly_processor.attribution_client.attribute.assert_not_called()

    def test_process_anomaly_with_slow_signals_skips_when_no_end_time(self):
        """SLOW pass returns None (skip) when the job has no end time."""
        self.job_status.end_time = None
        epilog_end_time = datetime.now(UTC) + timedelta(minutes=3)
        result = self.anomaly_processor.process_anomaly_with_slow_signals(
            self.job_status, "session-123", self.anomaly.anomaly_type, self.anomaly.uuid, epilog_end_time
        )
        self.assertIsNone(result)
        self.anomaly_processor.attribution_client.attribute.assert_not_called()

    def test_sanitize_attribution_result_below_threshold(self):
        """Test that _sanitize_attribution_result doesn't change exclusion types when below threshold."""
        # Setup
        self.settings.max_isolated_nodes = 3

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents with different exclusion types
        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2]},
            global_attributions={},
        )

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that exclusion types were not changed
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.TEMPORARY)

    def test_sanitize_attribution_result_unique_nodes_below_threshold(self):
        """Test that _sanitize_attribution_result doesn't change exclusion types when below threshold."""
        # Setup
        self.settings.max_isolated_nodes = 2

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents with different exclusion types
        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        node_event3 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        node_event4 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            # Even though there are 4 node events, there are only 2 unique nodes, so their exclusion type should not be changed.
            node_attributions={job_execution_error: [node_event1, node_event2, node_event3, node_event4]},
            global_attributions={},
        )

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that exclusion types were not changed
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.TEMPORARY)
        self.assertEqual(result.node_attributions[job_execution_error][2].exclusion_type, ExclusionType.TEMPORARY)
        self.assertEqual(result.node_attributions[job_execution_error][3].exclusion_type, ExclusionType.TEMPORARY)

    def test_sanitize_attribution_result_above_threshold(self):
        """Test that _sanitize_attribution_result preserves CONFIRMED nodes when they're within threshold."""
        # Setup: threshold=2, 2 CONFIRMED nodes (node1, node3), 1 TEMPORARY node (node2), total=3
        self.settings.max_isolated_nodes = 2

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents with different exclusion types
        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        node_event3 = NodeEvent(
            node="node3",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event4 = NodeEvent(
            node="node3",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2, node_event3, node_event4]},
            global_attributions={},
        )

        self.job_status.allocated_node_list = ["node1", "node2", "node3", "node4"]
        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that CONFIRMED nodes are preserved and TEMPORARY is changed to NONE
        # Also verify original_exclusion_type is set
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(
            result.node_attributions[job_execution_error][0].original_exclusion_type, ExclusionType.CONFIRMED
        )
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.NONE)
        self.assertEqual(
            result.node_attributions[job_execution_error][1].original_exclusion_type, ExclusionType.TEMPORARY
        )
        self.assertEqual(result.node_attributions[job_execution_error][2].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(
            result.node_attributions[job_execution_error][2].original_exclusion_type, ExclusionType.CONFIRMED
        )
        self.assertEqual(result.node_attributions[job_execution_error][3].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(
            result.node_attributions[job_execution_error][3].original_exclusion_type, ExclusionType.CONFIRMED
        )

    def test_sanitize_attribution_result_confirmed_above_threshold(self):
        """Test that _sanitize_attribution_result changes all exclusion types to NONE when CONFIRMED nodes exceed threshold."""
        # Setup: threshold=2, 3 CONFIRMED nodes, 1 TEMPORARY node
        self.settings.max_isolated_nodes = 2

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents - all CONFIRMED nodes on different nodes
        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event3 = NodeEvent(
            node="node3",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event4 = NodeEvent(
            node="node4",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2, node_event3, node_event4]},
            global_attributions={},
        )

        self.job_status.allocated_node_list = ["node1", "node2", "node3", "node4"]
        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that all exclusion types were changed to NONE because CONFIRMED nodes (3) > threshold (2)
        # But original_exclusion_type is preserved
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.NONE)
        self.assertEqual(
            result.node_attributions[job_execution_error][0].original_exclusion_type, ExclusionType.CONFIRMED
        )
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.NONE)
        self.assertEqual(
            result.node_attributions[job_execution_error][1].original_exclusion_type, ExclusionType.CONFIRMED
        )
        self.assertEqual(result.node_attributions[job_execution_error][2].exclusion_type, ExclusionType.NONE)
        self.assertEqual(
            result.node_attributions[job_execution_error][2].original_exclusion_type, ExclusionType.CONFIRMED
        )
        self.assertEqual(result.node_attributions[job_execution_error][3].exclusion_type, ExclusionType.NONE)
        self.assertEqual(
            result.node_attributions[job_execution_error][3].original_exclusion_type, ExclusionType.TEMPORARY
        )

    def test_sanitize_attribution_result_same_node_confirmed_and_temporary(self):
        """Test that _sanitize_attribution_result preserves TEMPORARY attributions for nodes that also have CONFIRMED attributions.

        Bug scenario: When a node has both CONFIRMED and TEMPORARY attributions, and confirmed nodes are within
        threshold but total nodes exceed threshold, the TEMPORARY attributions for CONFIRMED nodes should be
        preserved, not changed to NONE.
        """
        # Setup: threshold=2, node1 has CONFIRMED + TEMPORARY, node2 has CONFIRMED + TEMPORARY, node3 has only TEMPORARY
        # Total nodes = 3 (exceeds threshold), Confirmed nodes = 2 (within threshold)
        self.settings.max_isolated_nodes = 2

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Node1: CONFIRMED attribution
        node_event1_confirmed = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        # Node1: TEMPORARY attribution (same node as above)
        node_event1_temporary = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Node2: CONFIRMED attribution
        node_event2_confirmed = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        # Node2: TEMPORARY attribution (same node as above)
        node_event2_temporary = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Node3: Only TEMPORARY attribution (no CONFIRMED)
        node_event3_temporary = NodeEvent(
            node="node3",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.APPLICATION,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={
                job_execution_error: [
                    node_event1_confirmed,
                    node_event1_temporary,
                    node_event2_confirmed,
                    node_event2_temporary,
                    node_event3_temporary,
                ]
            },
            global_attributions={},
        )

        self.job_status.allocated_node_list = ["node1", "node2", "node3"]
        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify results:
        # - node1 CONFIRMED should stay CONFIRMED
        # - node1 TEMPORARY should stay TEMPORARY (not changed to NONE because node1 has CONFIRMED)
        # - node2 CONFIRMED should stay CONFIRMED
        # - node2 TEMPORARY should stay TEMPORARY (not changed to NONE because node2 has CONFIRMED)
        # - node3 TEMPORARY should be changed to NONE (only TEMPORARY, no CONFIRMED)

        events = result.node_attributions[job_execution_error]

        # Node1 CONFIRMED - should remain CONFIRMED
        self.assertEqual(events[0].node, "node1")
        self.assertEqual(events[0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(events[0].original_exclusion_type, ExclusionType.CONFIRMED)

        # Node1 TEMPORARY - should remain TEMPORARY (bug was changing this to NONE)
        self.assertEqual(events[1].node, "node1")
        self.assertEqual(events[1].exclusion_type, ExclusionType.TEMPORARY)
        self.assertEqual(events[1].original_exclusion_type, ExclusionType.TEMPORARY)

        # Node2 CONFIRMED - should remain CONFIRMED
        self.assertEqual(events[2].node, "node2")
        self.assertEqual(events[2].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(events[2].original_exclusion_type, ExclusionType.CONFIRMED)

        # Node2 TEMPORARY - should remain TEMPORARY (bug was changing this to NONE)
        self.assertEqual(events[3].node, "node2")
        self.assertEqual(events[3].exclusion_type, ExclusionType.TEMPORARY)
        self.assertEqual(events[3].original_exclusion_type, ExclusionType.TEMPORARY)

        # Node3 TEMPORARY - should be changed to NONE (no CONFIRMED attribution for node3)
        self.assertEqual(events[4].node, "node3")
        self.assertEqual(events[4].exclusion_type, ExclusionType.NONE)
        self.assertEqual(events[4].original_exclusion_type, ExclusionType.TEMPORARY)

    def test_sanitize_attribution_result_empty_error_attributions(self):
        """Test that _sanitize_attribution_result handles empty error_attributions."""
        # Setup
        self.settings.max_isolated_nodes = 3

        # Create the AnomalyAttributionResult with empty error_attributions
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that the result is unchanged
        self.assertEqual(result.node_attributions, {})

    def test_sanitize_attribution_result_all_none_exclusions(self):
        """Test that _sanitize_attribution_result handles all NONE exclusion types."""
        # Setup
        self.settings.max_isolated_nodes = 3

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents with all NONE exclusion types
        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.NONE,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.NONE,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2]},
            global_attributions={},
        )

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that exclusion types remain NONE
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.NONE)
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.NONE)

    def test_sanitize_attribution_result_with_none_result(self):
        """Test that _sanitize_attribution_result handles a None result."""
        # Create the AnomalyAttributionResult
        anomaly_attribution_result = None

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that the result is unchanged
        self.assertIsNone(result)

    def test_sanitize_attribution_result_with_allocated_nodes(self):
        """Test that _sanitize_attribution_result handles allocated nodes."""
        # Setup
        self.settings.max_isolated_nodes = 3
        self.job_status.allocated_node_list = ["node1", "node2", "node3"]

        # Create a JobExecutionError
        job_execution_error = JobExecutionError(
            code=0, message="Test error message", description="Test error description", signature="test-signature"
        )

        # Create NodeEvents with different exclusion types

        node_event1 = NodeEvent(
            node="node1",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="node2",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        node_event3 = NodeEvent(
            node="node3",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event4 = NodeEvent(
            node="node4",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        # Create the AnomalyAttributionResult
        anomaly_attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_uuid="test-anomaly-uuid",
            anomaly_type=EventType.HANG,
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2, node_event3, node_event4]},
            global_attributions={},
        )

        # Call the method
        result = self.anomaly_processor._sanitize_attribution_result(anomaly_attribution_result, self.job_status)

        # Verify that the result is unchanged
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.TEMPORARY)
        self.assertEqual(result.node_attributions[job_execution_error][2].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(result.node_attributions[job_execution_error][3].exclusion_type, ExclusionType.NONE)


class TestAnomalyProcessorPerRackLimits(unittest.TestCase):
    """Test cases for per-rack isolation limits."""

    def setUp(self):
        # Create mock settings with per-rack limits enabled
        self.settings = create_mock_settings(
            max_isolated_nodes=10,  # Global limit (not used when per-rack is set)
            rack_aware_isolation_enabled=True,  # Enable rack-aware isolation
            max_isolated_nodes_per_rack=2,  # Per-rack limit
            max_racks_to_isolate=9,  # Maximum number of racks that can have isolated nodes
            rack_extraction_pattern=None,  # Use default patterns
        )

        # Create AnomalyProcessor with patched collaborators
        self.anomaly_processor = _build_anomaly_processor(self.settings)

        # Create mock job status
        self.job_status = SlurmJobExecutionStatus(
            job_id="test-job-123",
            workflow_id="test-workflow",
            job_attempt_index=0,
            allocated_node_list=[
                "nvl72010-T01",
                "nvl72010-T02",
                "nvl72010-T03",
                "nvl72010-T04",
                "nvl72020-T01",
                "nvl72020-T02",
                "nvl72020-T03",
                "nvl72020-T04",
            ],
        )

    def test_per_rack_limits_single_rack_within_limit(self):
        """Test that nodes within per-rack limit are kept."""
        # Create attribution result with 2 confirmed nodes in rack 010 (within limit of 2)
        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        node_event1 = NodeEvent(
            node="nvl72010-T01",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="nvl72010-T02",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2]},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Both nodes should remain CONFIRMED (within limit)
        self.assertEqual(result.node_attributions[job_execution_error][0].exclusion_type, ExclusionType.CONFIRMED)
        self.assertEqual(result.node_attributions[job_execution_error][1].exclusion_type, ExclusionType.CONFIRMED)

    def test_per_rack_limits_single_rack_exceeds_limit_with_temporary(self):
        """Test that temporary nodes are changed to NONE when rack exceeds limit."""
        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # 2 confirmed + 1 temporary in rack 010 (exceeds limit of 2)
        node_event1 = NodeEvent(
            node="nvl72010-T01",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event2 = NodeEvent(
            node="nvl72010-T02",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )

        node_event3 = NodeEvent(
            node="nvl72010-T03",
            timestamp=datetime.now(UTC),
            job_id="test-job-123",
            job_attempt_index=0,
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.ADI,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: [node_event1, node_event2, node_event3]},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Confirmed nodes should remain, temporary should be changed to NONE
        events = result.node_attributions[job_execution_error]
        confirmed_count = sum(1 for e in events if e.exclusion_type == ExclusionType.CONFIRMED)
        none_count = sum(1 for e in events if e.exclusion_type == ExclusionType.NONE)

        self.assertEqual(confirmed_count, 2)
        self.assertEqual(none_count, 1)

    def test_per_rack_limits_single_rack_too_many_confirmed(self):
        """Test that excess nodes beyond per-rack limit are changed to NONE."""
        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # 3 confirmed nodes in rack 010 (exceeds limit of 2)
        # Expected: 2 nodes kept as CONFIRMED, 1 node changed to NONE
        node_events = []
        for i in range(3):
            node_events.append(
                NodeEvent(
                    node=f"nvl72010-T0{i + 1}",
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Should keep 2 nodes as CONFIRMED (up to limit), change 1 to NONE
        events = result.node_attributions[job_execution_error]
        confirmed_count = sum(1 for e in events if e.exclusion_type == ExclusionType.CONFIRMED)
        none_count = sum(1 for e in events if e.exclusion_type == ExclusionType.NONE)

        self.assertEqual(confirmed_count, 2, "Should keep 2 nodes as CONFIRMED (per-rack limit)")
        self.assertEqual(none_count, 1, "Should change 1 excess node to NONE")

    def test_per_rack_limits_multiple_racks_independent(self):
        """Test that rack limits are applied independently per rack."""
        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # Rack 010: 3 confirmed (exceeds limit) - should all remain CONFIRMED (rack becomes unusable)
        # Rack 020: 2 confirmed (within limit) - should remain CONFIRMED (rack stays usable)
        node_events = []

        # Rack 010 nodes
        for i in range(3):
            node_events.append(
                NodeEvent(
                    node=f"nvl72010-T0{i + 1}",
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        # Rack 020 nodes
        for i in range(2):
            node_events.append(
                NodeEvent(
                    node=f"nvl72020-T0{i + 1}",
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Check rack1 nodes - should all remain CONFIRMED (rack unusable by design)
        events = result.node_attributions[job_execution_error]
        rack1_events = [e for e in events if e.node.startswith("rack1")]
        rack2_events = [e for e in events if e.node.startswith("rack2")]

        for event in rack1_events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.CONFIRMED,
                f"Rack1 node {event.node} should remain CONFIRMED (rack unusable)",
            )

        for event in rack2_events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.CONFIRMED,
                f"Rack2 node {event.node} should remain CONFIRMED (rack usable)",
            )

    def test_per_rack_limits_with_nvl72_pattern(self):
        """Test per-rack limits with nvl72 node naming pattern (default pattern 1)."""
        # Update job status with nvl72 pattern nodes
        self.job_status.allocated_node_list = [
            "nvl72010-T01",
            "nvl72010-T02",
            "nvl72010-T03",
            "nvl72d020-T01",
            "nvl72D020-T02",
            "nvl72020-T03",
        ]

        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # Rack 010: 3 confirmed (exceeds limit)
        # Rack 020: 1 confirmed (within limit)
        node_events = [
            NodeEvent(
                node="nvl72010-T01",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="nvl72010-T02",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="nvl72010-T03",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="nvl72d020-T01",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
        ]

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Check results - rack 010 should have 2 CONFIRMED (limit), 1 NONE (excess)
        events = result.node_attributions[job_execution_error]
        rack010_events = [
            e
            for e in events
            if e.node.startswith("nvl72010") or e.node.startswith("nvl72d010") or e.node.startswith("nvl72D010")
        ]
        rack020_events = [
            e
            for e in events
            if e.node.startswith("nvl72020") or e.node.startswith("nvl72d020") or e.node.startswith("nvl72D020")
        ]

        # Rack 010 should have 2 CONFIRMED (per-rack limit) and 1 NONE (excess)
        rack010_confirmed = sum(1 for e in rack010_events if e.exclusion_type == ExclusionType.CONFIRMED)
        rack010_none = sum(1 for e in rack010_events if e.exclusion_type == ExclusionType.NONE)
        self.assertEqual(rack010_confirmed, 2, "Rack 010 should keep 2 nodes as CONFIRMED (per-rack limit)")
        self.assertEqual(rack010_none, 1, "Rack 010 should change 1 excess node to NONE")

        # Rack 020 should remain CONFIRMED (within limit)
        for event in rack020_events:
            self.assertEqual(
                event.exclusion_type, ExclusionType.CONFIRMED, f"Rack 020 node {event.node} should remain CONFIRMED"
            )

    def test_per_rack_limits_with_custom_pattern(self):
        """Test per-rack limits with custom rack extraction pattern."""
        # Use custom pattern
        self.settings.rack_extraction_pattern = r"(r\d+)"

        # Update job status with r-pattern nodes
        self.job_status.allocated_node_list = [
            "r1n001",
            "r1n002",
            "r1n003",
            "r2n001",
            "r2n002",
            "r2n003",
        ]

        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # r1: 3 confirmed (exceeds limit)
        # r2: 1 confirmed (within limit)
        node_events = [
            NodeEvent(
                node="r1n001",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="r1n002",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="r1n003",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
            NodeEvent(
                node="r2n001",
                timestamp=datetime.now(UTC),
                job_id="test-job-123",
                job_attempt_index=0,
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.ADI,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.CONFIRMED,
                fact_status=FactStatus.OK,
            ),
        ]

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Check results
        events = result.node_attributions[job_execution_error]
        r1_events = [e for e in events if e.node.startswith("r1")]
        r2_events = [e for e in events if e.node.startswith("r2")]

        # r1 should have 2 CONFIRMED (per-rack limit) and 1 NONE (excess)
        r1_confirmed = sum(1 for e in r1_events if e.exclusion_type == ExclusionType.CONFIRMED)
        r1_none = sum(1 for e in r1_events if e.exclusion_type == ExclusionType.NONE)
        self.assertEqual(r1_confirmed, 2, "Rack r1 should keep 2 nodes as CONFIRMED (per-rack limit)")
        self.assertEqual(r1_none, 1, "Rack r1 should change 1 excess node to NONE")

        # r2 should remain CONFIRMED (within limit)
        for event in r2_events:
            self.assertEqual(event.exclusion_type, ExclusionType.CONFIRMED)

    def test_fallback_to_per_cluster_when_no_rack_structure(self):
        """Test that per-rack mode falls back to per-cluster limits when no rack structure detected."""
        # Enable rack-aware isolation
        self.settings.rack_aware_isolation_enabled = True
        self.settings.max_isolated_nodes_per_rack = 2
        self.settings.max_isolated_nodes = 5  # Per-cluster limit

        # Use nodes without rack structure (will all be grouped as "unknown")
        self.job_status.allocated_node_list = [
            "pool0-00123",
            "pool0-00456",
            "pool1-00789",
            "pool1-00890",
            "pool2-00111",
            "pool2-00222",
            "pool3-00333",
        ]

        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # Create 7 confirmed nodes (exceeds per-cluster limit of 5)
        node_events = []
        for node in self.job_status.allocated_node_list:
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Should fallback to per-cluster logic:
        # 7 confirmed nodes > 5 (max_isolated_nodes), so all should be changed to NONE
        events = result.node_attributions[job_execution_error]
        for event in events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.NONE,
                f"Node {event.node} should be NONE (fallback to per-cluster limits)",
            )

    def test_max_racks_to_isolate_limit(self):
        """Test that max_racks_to_isolate limit is enforced correctly."""
        # Set max_racks_to_isolate to 2 (only allow 2 racks with isolations)
        self.settings.max_racks_to_isolate = 2

        # Update job status with nodes from 4 different racks
        self.job_status.allocated_node_list = [
            # Rack 010: 3 confirmed nodes (exceeds per-rack limit)
            "nvl72010-T01",
            "nvl72010-T02",
            "nvl72010-T03",
            # Rack 020: 2 confirmed nodes (at per-rack limit)
            "nvl72020-T01",
            "nvl72020-T02",
            # Rack 030: 1 confirmed node (below per-rack limit)
            "nvl72030-T01",
            # Rack 040: 1 confirmed node (below per-rack limit)
            "nvl72040-T01",
        ]

        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        # Create node events for all 4 racks
        node_events = []
        for node in ["nvl72010-T01", "nvl72010-T02", "nvl72010-T03"]:  # Rack 010: 3 nodes
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        for node in ["nvl72020-T01", "nvl72020-T02"]:  # Rack 020: 2 nodes
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        for node in ["nvl72030-T01"]:  # Rack 030: 1 node
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        for node in ["nvl72040-T01"]:  # Rack 040: 1 node
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        # Sanitize
        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        # Check results
        events = result.node_attributions[job_execution_error]
        rack010_events = [e for e in events if e.node.startswith("nvl72010")]
        rack020_events = [e for e in events if e.node.startswith("nvl72020")]
        rack030_events = [e for e in events if e.node.startswith("nvl72030")]
        rack040_events = [e for e in events if e.node.startswith("nvl72040")]

        # Rack 010 has 3 confirmed nodes (most), so it's in top 2 racks
        # Should be kept fully isolated (no per-rack limits applied)
        for event in rack010_events:
            self.assertEqual(
                event.exclusion_type, ExclusionType.CONFIRMED, "Rack 010 should remain CONFIRMED (top priority rack)"
            )

        # Rack 020 has 2 confirmed nodes (second most), so it's in top 2 racks
        # Should be kept fully isolated (no per-rack limits applied)
        for event in rack020_events:
            self.assertEqual(
                event.exclusion_type, ExclusionType.CONFIRMED, "Rack 020 should remain CONFIRMED (top priority rack)"
            )

        # Rack 030 is excess (beyond top 2), so per-rack limit applies
        # Has 1 node which is <= 2 (per-rack limit), so it remains CONFIRMED
        for event in rack030_events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.CONFIRMED,
                "Rack 030 should remain CONFIRMED (within per-rack limit)",
            )

        # Rack 040 is excess (beyond top 2), so per-rack limit applies
        # Has 1 node which is <= 2 (per-rack limit), so it remains CONFIRMED
        for event in rack040_events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.CONFIRMED,
                "Rack 040 should remain CONFIRMED (within per-rack limit)",
            )

    def test_max_racks_with_excess_nodes_in_kept_racks(self):
        """Test max_racks_to_isolate with racks that exceed per-rack limit in top priority racks."""
        # Set max_racks_to_isolate to 2, per-rack limit to 2
        self.settings.max_racks_to_isolate = 2
        self.settings.max_isolated_nodes_per_rack = 2

        # Scenario from user:
        # Rack 1: 3 bad nodes -> Top priority, keep ALL (no per-rack limit)
        # Rack 2: 3 bad nodes -> Second priority, keep ALL (no per-rack limit)
        # Rack 3: 3 bad nodes -> Excess, apply per-rack limit (2 kept, 1 NONE)
        # Rack 4: 1 bad node -> Excess, apply per-rack limit (1 kept)

        self.job_status.allocated_node_list = [
            "nvl72001-T01",
            "nvl72001-T02",
            "nvl72001-T03",  # Rack 1: 3 nodes
            "nvl72002-T01",
            "nvl72002-T02",
            "nvl72002-T03",  # Rack 2: 3 nodes
            "nvl72003-T01",
            "nvl72003-T02",
            "nvl72003-T03",  # Rack 3: 3 nodes
            "nvl72004-T01",  # Rack 4: 1 node
        ]

        job_execution_error = JobExecutionError(code=0, message="Test error", description="Test", signature="test-sig")

        node_events = []
        for node in self.job_status.allocated_node_list:
            node_events.append(
                NodeEvent(
                    node=node,
                    timestamp=datetime.now(UTC),
                    job_id="test-job-123",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.ADI,
                    error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.CONFIRMED,
                    fact_status=FactStatus.OK,
                )
            )

        attribution_result = AnomalyAttributionResult(
            job_id="test-job-123",
            job_attempt_index=0,
            anomaly_type=EventType.HANG,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={job_execution_error: node_events},
            global_attributions={},
        )

        result = self.anomaly_processor._sanitize_attribution_result(attribution_result, self.job_status)

        events = result.node_attributions[job_execution_error]
        rack001_events = [e for e in events if e.node.startswith("nvl72001")]
        rack002_events = [e for e in events if e.node.startswith("nvl72002")]
        rack003_events = [e for e in events if e.node.startswith("nvl72003")]
        rack004_events = [e for e in events if e.node.startswith("nvl72004")]

        # Count how many racks with 3 nodes are fully isolated (should be exactly 2)
        racks_with_3_kept = []
        for rack_name, rack_events in [("001", rack001_events), ("002", rack002_events), ("003", rack003_events)]:
            if all(e.exclusion_type == ExclusionType.CONFIRMED for e in rack_events):
                racks_with_3_kept.append(rack_name)

        # Exactly 2 of the 3 racks with 3 nodes should be kept fully
        self.assertEqual(len(racks_with_3_kept), 2, "Exactly 2 racks with 3 nodes should be kept fully (top priority)")

        # The remaining rack with 3 nodes should have 2 CONFIRMED (per-rack limit) and 1 NONE
        racks_with_partial = []
        for rack_name, rack_events in [("001", rack001_events), ("002", rack002_events), ("003", rack003_events)]:
            confirmed_count = sum(1 for e in rack_events if e.exclusion_type == ExclusionType.CONFIRMED)
            none_count = sum(1 for e in rack_events if e.exclusion_type == ExclusionType.NONE)
            if confirmed_count == 2 and none_count == 1:
                racks_with_partial.append(rack_name)

        self.assertEqual(
            len(racks_with_partial),
            1,
            "Exactly 1 rack with 3 nodes should have 2 CONFIRMED and 1 NONE (excess rack, per-rack limit applied)",
        )

        # Rack 4: Excess rack, per-rack limit applies (limit=2)
        # 1 node <= 2, so it should remain CONFIRMED
        for event in rack004_events:
            self.assertEqual(
                event.exclusion_type,
                ExclusionType.CONFIRMED,
                "Rack 4 (excess, 1 confirmed <= limit of 2) should remain CONFIRMED",
            )
        self.assertEqual(len(rack004_events), 1)
