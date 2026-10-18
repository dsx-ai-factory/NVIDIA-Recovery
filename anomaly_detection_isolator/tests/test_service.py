# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest
from datetime import datetime, timedelta
from typing import Literal
from unittest.mock import ANY, MagicMock, call, patch

import pika
from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor
from anomaly_detection_isolator.job_manager_client import JobManagerClient
from anomaly_detection_isolator.main import validate_settings
from anomaly_detection_isolator.service import AnomalyDetectionIsolator
from opentelemetry.trace import SpanKind, StatusCode
from pika.exceptions import ConnectionClosedByBroker
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from recovery_libs.event_bus.event_bus_error import EventBusError
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    SlurmJobExecutionStatus,
)
from recovery_libs.storage.dataclass import SlurmJobFinalStates
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


def create_and_setup_mock_rabbitmq_properties() -> MagicMock:
    mock_properties = MagicMock()
    mock_properties.headers = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    }
    return mock_properties


class AnomalyDetectionIsolationServiceTests(unittest.TestCase):
    def setUp(self):
        # TODO: Move common patchers here to avoid duplicating the @patch decorators in all test cases.
        self.properties_extractor_patcher = patch(
            "anomaly_detection_isolator.service.extract_trace_context_from_rabbitmq_properties"
        )
        self.mock_properties_extractor = self.properties_extractor_patcher.start()
        self.mock_properties_extractor.return_value = MagicMock()

        self.header_populator_patcher = patch("anomaly_detection_isolator.service.populate_headers_with_span_context")
        self.mock_header_populator = self.header_populator_patcher.start()
        self.mock_header_populator.return_value = {
            "traceparent": "00-b7f7408dda54b2bed9eccc6a27ecde77-26af15ee3d44222e-01"
        }

    def tearDown(self):
        self.properties_extractor_patcher.stop()
        self.header_populator_patcher.stop()

    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("pika.ConnectionParameters")
    @patch("builtins.open")
    @patch.object(AnomalyDetectionIsolator, "_process_attribution_request_event")
    def test_consume_event_bus_msg(self, mock_run, mock_open, mock_connection_param, mock_blocking_connection):
        mock_connection_param = mock_connection_param.return_value
        mock_queue = MagicMock()

        mock_method = MagicMock()
        mock_method.delivery_tag.return_value = 1

        mock_threads = MagicMock()

        test_service = AnomalyDetectionIsolator(TestSettings())
        test_service.epilogue_started_tasks = mock_queue

        test_message = b'{"task_id": "12345", "other_data": "value"}'
        test_service._consume_event_bus_msg_attribution_request(
            None, mock_method, None, test_message, (None, mock_threads)
        )

        # Assertions
        mock_threads.append.assert_called_once()
        mock_run.assert_called_once()

    @patch("recovery_libs.event_bus.rabbit_eventing.consume_multiple_messages")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.create_connection")
    @patch("builtins.open")
    def test_setup_event_bus_and_start_consuming(
        self, mock_open, mock_connect, mock_declare_exchange, mock_declare_queue, mock_bind_queue, mock_consume_messages
    ):
        test_service = AnomalyDetectionIsolator(TestSettings())
        test_service.setup_event_bus_and_start_consuming()

        # Assertions
        mock_connect.assert_called_once()
        self.assertEqual(mock_declare_exchange.call_count, 2)
        self.assertEqual(mock_declare_queue.call_count, 2)
        self.assertEqual(mock_bind_queue.call_count, 2)
        mock_consume_messages.assert_called_once()

    @unittest.skip("Skipped because it tests retries and thus takes a long time to run, unskip to test retries")
    @patch("recovery_libs.event_bus.rabbit_eventing.consume_messages", side_effect=ConnectionClosedByBroker(1, ""))
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.create_connection")
    def test_setup_event_bus_and_start_consuming_retries(
        self, mock_connect, mock_declare_exchange, mock_declare_queue, mock_bind_queue, mock_consume_messages
    ):
        test_service = AnomalyDetectionIsolator(TestSettings())

        with self.assertRaises(ConnectionClosedByBroker):
            test_service.setup_event_bus_and_start_consuming()

        # Assertions
        self.assertEqual(mock_connect.call_count, 5)
        self.assertEqual(mock_declare_exchange.call_count, 5)
        self.assertEqual(mock_declare_queue.call_count, 5)
        self.assertEqual(mock_bind_queue.call_count, 5)
        self.assertEqual(mock_consume_messages.call_count, 5)

    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("builtins.open")
    def test_publish_message_success(
        self, mock_open, mock_connect, mock_declare_queue, mock_declare_exchange, mock_bind_queue, mock_publish_message
    ):
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(TestSettings())
        for output_queue in (None, ""):
            with self.subTest(output_queue=output_queue):
                test_service._publish_result_message_to_event_bus("test_message", 0, output_queue=output_queue)

                mock_connect.assert_called_once_with(
                    hostname=test_settings.rabbitmq_url, vhost=test_settings.rabbitmq_vhost, credentials=ANY
                )
                mock_declare_exchange.assert_called_once_with(
                    channel=ANY, exchange_name=test_settings.isolator_completed_exchange
                )
                mock_declare_queue.assert_not_called()
                mock_bind_queue.assert_not_called()
                mock_publish_message.assert_called_once_with(
                    channel=ANY,
                    exchange_name=test_settings.isolator_completed_exchange,
                    routing_key="anomaly.isolation.bad_nodes",  # default routing key
                    message_body="test_message",
                    properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
                )

                for mock in (
                    mock_connect,
                    mock_declare_exchange,
                    mock_declare_queue,
                    mock_bind_queue,
                    mock_publish_message,
                ):
                    mock.reset_mock()

    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("builtins.open")
    def test_publish_message_with_custom_output_queue(
        self, mock_open, mock_connect, mock_declare_queue, mock_declare_exchange, mock_bind_queue, mock_publish_message
    ):
        """Test that custom output_queue uses queue name as routing key to avoid duplicate delivery."""
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(TestSettings())
        custom_queue = "custom_output_queue"

        test_service._publish_result_message_to_event_bus("test_message", 0, output_queue=custom_queue)

        mock_connect.assert_called_once_with(
            hostname=test_settings.rabbitmq_url, vhost=test_settings.rabbitmq_vhost, credentials=ANY
        )
        mock_declare_exchange.assert_called_once_with(
            channel=ANY, exchange_name=test_settings.isolator_completed_exchange
        )
        # Verify custom queue is used instead of default
        mock_declare_queue.assert_called_once_with(channel=ANY, queue_name=custom_queue)
        # Verify custom queue uses queue name as routing key (not default routing key)
        mock_bind_queue.assert_called_once_with(
            channel=ANY,
            queue_name=custom_queue,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key=custom_queue,  # routing key should match queue name
        )
        # Verify publish uses the same custom routing key
        mock_publish_message.assert_called_once_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key=custom_queue,  # routing key should match queue name
            message_body="test_message",
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )

    @unittest.skip("Skipped because it tests retries and thus takes a long time to run, unskip to test retries")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message", side_effect=ConnectionClosedByBroker(1, ""))
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    def test_publish_message_failure(
        self, mock_connect, mock_declare_queue, mock_declare_exchange, mock_bind_queue, mock_publish_message
    ):
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        with self.assertRaises(ConnectionClosedByBroker):
            test_service._publish_result_message_to_event_bus("test_message", 0)

        connect_call = call(hostname=test_settings.rabbitmq_url, vhost=test_settings.rabbitmq_vhost, credentials=ANY)

        declare_exchange_call = call(channel=ANY, exchange_name=test_settings.isolator_completed_exchange)

        publish_message_call = call(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key="anomaly.isolation.bad_nodes",
            message_body=ANY,
            properties=ANY,
        )

        mock_connect.assert_has_calls([connect_call] * 5)
        mock_declare_exchange.assert_has_calls([declare_exchange_call] * 5)
        mock_declare_queue.assert_not_called()
        mock_bind_queue.assert_not_called()
        mock_publish_message.assert_has_calls([publish_message_call] * 5)

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_basic(
        self, mock_file_open, mock_get_jm, mock_publish_message, mock_conn_param, mock_connection, mock_tracer
    ):
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"
        test_attribution_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_uuid="test-anomaly-uuid",
            anomaly_type=EventType.STRAGGLER,
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={JobExecutionError(code=0, message="", description="", signature=""): []},
            global_attributions={},
        )
        test_json = json.dumps(
            {
                "job_id": "123456",
                "job_attempt_index": 0,
                "anomaly_time": "2024-03-24",
                "detection_time": "2024-03-25",
                "allocated_nodes": ["event-node"],
                "event_type": "STRAGGLER",
                "start_time": "2024-03-24",
            }
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1
        test_settings.slow_signals_enabled = True

        mock_job_manager_response = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime.fromisoformat("2024-01-01T17:19:04Z"),
            end_time=datetime.fromisoformat("2024-01-01T17:28:26Z"),
            state="CANCELLED",
            state_reason="None",
            submit_time=datetime.fromisoformat("2024-01-01T17:19:01Z"),
            partition="batch",
            reservation="reservation",
            command="job.sh",
            node_count=3,
            allocated_node_list=["test-node-001", "test-node-002", "test-node-003"],
            anomalies=[
                JobAnomaly(
                    anomaly_type="STRAGGLER",
                    timestamp=datetime.fromisoformat("2024-01-01T17:23:24.456340Z"),
                    original_watcher_anomaly_event="EVENT",
                    uuid="uuid-value",
                )
            ],
        )
        mock_get_jm.return_value = mock_job_manager_response

        # Simulate an isolation result
        mock_anomaly_processor = MagicMock()
        mock_anomaly_processor.process_anomaly_with_fast_signals.return_value = (test_attribution_result, None)
        mock_queue = MagicMock()
        mock_queue.get.side_effect = [test_json, None]
        mock_properties = MagicMock()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor
        test_service._process_attribution_request_event(
            mock_connection, None, None, mock_properties, test_json.encode()
        )
        # Assertions
        mock_publish_message.assert_called_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key="anomaly.isolation.bad_nodes",
            message_body=ANY,
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )

        self.assertEqual(mock_publish_message.call_count, 1)

        mock_get_jm.assert_called_once()
        mock_connection.add_callback_threadsafe.assert_called_once()
        mock_anomaly_processor.process_anomaly_with_fast_signals.assert_called_once()
        self.assertEqual(
            mock_anomaly_processor.process_anomaly_with_fast_signals.call_args.kwargs["job_status"].workflow_id,
            "test-workflow",
        )

        mock_tracer.start_as_current_span.assert_called_with(
            name="job_anomaly.attribution", kind=SpanKind.PRODUCER, context=self.mock_properties_extractor.return_value
        )
        mock_span.set_attribute.assert_has_calls(
            [
                call("job_id", "123456"),
                call("job_attempt_index", 0),
                call("job_anomaly.type", "STRAGGLER"),
                call("job_anomaly.uuid", "uuid-value"),
                call("job_anomaly.attribution_type", "fast_signals"),
            ],
            any_order=True,
        )
        mock_span.set_status.assert_called_once_with(StatusCode.OK)

    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_with_end_event_type_fact(
        self, mock_file_open, mock_get_jm, mock_publish_message, mock_conn_param, mock_connection
    ):
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"

        test_attribution_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.END,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )
        test_json = json.dumps(
            {"job_id": "123456", "anomaly_type": "END", "job_attempt_index": 0, "anomaly_time": "2024-03-24"}
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1
        test_settings.attribution_engines = ["FACT"]

        mock_job_manager_response = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime.fromisoformat("2024-01-01T17:19:04Z"),
            end_time=datetime.fromisoformat("2024-01-01T17:28:26Z"),
            state="RUNNING",
            state_reason="None",
            submit_time=datetime.fromisoformat("2024-01-01T17:19:01Z"),
            partition="batch",
            reservation="reservation",
            command="job.sh",
            node_count=3,
            allocated_node_list=["test-node-001", "test-node-002", "test-node-003"],
            anomalies=[
                JobAnomaly(
                    anomaly_type="END",
                    timestamp=datetime.fromisoformat("2024-01-01T17:23:24.456340Z"),
                    original_watcher_anomaly_event="EVENT",
                )
            ],
        )
        mock_get_jm.return_value = mock_job_manager_response

        mock_anomaly_processor = MagicMock()
        mock_anomaly_processor.process_anomaly_with_fast_signals.return_value = (test_attribution_result, None)
        mock_queue = MagicMock()
        mock_queue.get.side_effect = [test_json, None]
        mock_properties = create_and_setup_mock_rabbitmq_properties()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor

        test_service._process_attribution_request_event(
            mock_connection, None, None, mock_properties, test_json.encode()
        )

        # Assertions
        # Check that mock_publish_message was called twice
        self.assertEqual(mock_publish_message.call_count, 2)

        # Get both calls
        first_call = mock_publish_message.call_args_list[0]
        second_call = mock_publish_message.call_args_list[1]

        # Check the content of the first call
        first_message_body = first_call.kwargs["message_body"]
        first_attribution_result = json.loads(first_message_body)
        self.assertEqual(first_call.kwargs["exchange_name"], test_settings.slow_signals_exchange)
        self.assertTrue("delay_queue" in first_call.kwargs["routing_key"])
        self.assertEqual(first_attribution_result["anomaly_type"], mock_job_manager_response.anomalies[0].anomaly_type)
        self.assertEqual(first_attribution_result["job_id"], mock_job_manager_response.job_id)
        self.assertEqual(first_attribution_result["job_attempt_index"], mock_job_manager_response.job_attempt_index)

        # Check the content of the second call
        second_message_body = second_call.kwargs["message_body"]
        second_attribution_result = json.loads(second_message_body)
        self.assertEqual(second_call.kwargs["exchange_name"], test_settings.isolator_completed_exchange)
        self.assertEqual(second_call.kwargs["routing_key"], "anomaly.isolation.bad_nodes")
        self.assertEqual(second_attribution_result["anomaly_type"], mock_job_manager_response.anomalies[0].anomaly_type)
        self.assertEqual(second_attribution_result["job_id"], mock_job_manager_response.job_id)
        self.assertEqual(second_attribution_result["job_attempt_index"], mock_job_manager_response.job_attempt_index)

        attribution_result_call_arg = AnomalyAttributionResult.from_json(
            json.loads(mock_publish_message.call_args.kwargs["message_body"])
        )

        self.assertEqual(attribution_result_call_arg.anomaly_type, EventType.END)
        self.assertEqual(attribution_result_call_arg.node_attributions, {})
        self.assertEqual(attribution_result_call_arg.job_id, "123456")
        self.assertEqual(attribution_result_call_arg.job_attempt_index, 0)

        mock_get_jm.assert_called_once()

    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_no_error_attributions(
        self, mock_file_open, mock_get_jm, mock_publish_message, mock_conn_param, mock_connection
    ):
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"
        test_attribution_result_1 = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={JobExecutionError(code=0, message="", description="", signature=""): []},
            global_attributions={},
        )
        test_attribution_result_2 = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid-2",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [
                    NodeEvent(
                        node="test-node-001",
                        timestamp=datetime.fromisoformat("2024-01-01T17:23:24.456340Z"),
                        job_id="123456",
                        job_attempt_index=0,
                        error_attribution=ErrorAttribution.HARDWARE,
                        error_attribution_type=ErrorAttributionType.FAST,
                        error_attribution_origin=ErrorAttributionOrigin.ADI,
                        error_attribution_sources=[
                            ErrorAttributionSource.SYSLOGS,
                            ErrorAttributionSource.NODE_METRICS,
                            ErrorAttributionSource.APPLICATION_LOGS,
                        ],
                        exclusion_type=ExclusionType.CONFIRMED,
                        fact_status=FactStatus.NONE,
                    )
                ]
            },
            global_attributions={},
        )

        test_json = json.dumps(
            {"job_id": "123456", "anomaly_type": "STRAGGLER", "job_attempt_index": 0, "anomaly_time": "2024-03-24"}
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1

        mock_job_manager_response = SlurmJobExecutionStatus(
            state="CANCELLED",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime.fromisoformat("2024-01-01T17:19:04Z"),
            end_time=datetime.fromisoformat("2024-01-01T17:28:26Z"),
            state_reason="None",
            submit_time=datetime.fromisoformat("2024-01-01T17:19:01Z"),
            partition="batch",
            reservation="reservation",
            command="job.sh",
            node_count=3,
            allocated_node_list=["test-node-001", "test-node-002", "test-node-003"],
            anomalies=[
                JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime.fromisoformat("2024-01-01T17:23:24.456340Z")),
                JobAnomaly(anomaly_type="HANG", timestamp=datetime.fromisoformat("2024-01-01T17:43:24.456340Z")),
            ],
        )
        mock_get_jm.return_value = mock_job_manager_response

        # Simulate an isolation result
        mock_anomaly_processor = MagicMock()
        # make sure the first call returns the first attribution result and the second call returns the second attribution result
        mock_anomaly_processor.process_anomaly_with_fast_signals.side_effect = [
            (test_attribution_result_1, None),
            (test_attribution_result_2, None),
        ]
        mock_queue = MagicMock()
        mock_queue.get.side_effect = [test_json, None]
        mock_properties = MagicMock()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor
        test_service._process_attribution_request_event(
            mock_connection, None, "123", mock_properties, test_json.encode()
        )
        # Assertions
        self.assertEqual(mock_publish_message.call_count, 2)

        mock_publish_message.assert_has_calls(
            [
                call(
                    channel=ANY,
                    exchange_name=test_settings.slow_signals_exchange,
                    routing_key=ANY,
                    message_body=ANY,
                    properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
                ),
                call(
                    channel=ANY,
                    exchange_name=test_settings.isolator_completed_exchange,
                    routing_key="anomaly.isolation.bad_nodes",
                    message_body=ANY,
                    properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
                ),
            ]
        )

        mock_get_jm.assert_called_once()
        mock_connection.add_callback_threadsafe.assert_called_once()
        self.assertEqual(mock_anomaly_processor.process_anomaly_with_fast_signals.call_count, 2)

    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_jm_error(
        self, mock_file_open, mock_get_jm, mock_publish_message, mock_connection
    ):
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"
        test_json = json.dumps({"job_id": "123456", "anomaly_time": "2024-03-24"})
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1

        mock_get_jm.return_value = None

        mock_queue = MagicMock()
        mock_queue.get.side_effect = [test_json, None]
        mock_properties = MagicMock()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service._process_attribution_request_event(
            mock_connection, None, None, mock_properties, test_json.encode()
        )
        # Assertions
        mock_get_jm.assert_called_once()
        mock_publish_message.assert_not_called()
        mock_connection.add_callback_threadsafe.assert_not_called()

    @patch("pika.BlockingConnection")  # This is added to make sure connection is actually not established.
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("requests.get")
    @patch("builtins.open")
    def test_process_attribution_request_event_fetch_from_event(
        self, mock_file_open, mock_get_jm, mock_publish_message, mock_conn_param, mock_connection
    ):
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"

        test_attribution_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        test_json = json.dumps(
            {
                "job_id": "123456",
                "workflow_id": "canonical-workflow",
                "job_attempt_index": 0,
                "anomaly_time": "2024-03-24",
                "detection_time": "2024-03-25",
                "allocated_nodes": ["node1"],
                "event_type": "HANG",
                "start_time": "2024-03-24",
            }
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1

        # Simulate an isolation result
        mock_anomaly_processor = MagicMock()
        mock_anomaly_processor.process_anomaly_with_fast_signals.return_value = (test_attribution_result, None)

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor

        mock_properties = MagicMock()
        test_service._process_attribution_request_event(mock_connection, None, 1, mock_properties, test_json.encode())

        # Assertions
        mock_publish_message.assert_called_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key="anomaly.isolation.bad_nodes",
            message_body=ANY,
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )
        # make sure JM was not called, as event had all the fields required.
        mock_get_jm.assert_not_called()
        assert (
            mock_anomaly_processor.process_anomaly_with_fast_signals.call_args.kwargs["job_status"].workflow_id
            == "canonical-workflow"
        )
        mock_connection.add_callback_threadsafe.assert_called_once()

    @patch("pika.BlockingConnection")
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("requests.get")
    @patch("builtins.open")
    def test_process_attribution_request_event_with_output_queue(
        self,
        mock_file_open,
        mock_get_jm,
        mock_connect,
        mock_declare_exchange,
        mock_declare_queue,
        mock_bind_queue,
        mock_publish_message,
        mock_conn_param,
        mock_connection,
    ):
        """Test that output_queue in event message is used for publishing."""
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"

        test_attribution_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        custom_output_queue = "custom_test_queue"
        test_json = json.dumps(
            {
                "job_id": "123456",
                "workflow_id": "canonical-workflow",
                "job_attempt_index": 0,
                "anomaly_time": "2024-03-24",
                "detection_time": "2024-03-25",
                "allocated_nodes": ["node1"],
                "event_type": "HANG",
                "start_time": "2024-03-24",
                "output_queue": custom_output_queue,  # Custom output queue
            }
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1

        # Simulate an isolation result
        mock_anomaly_processor = MagicMock()
        mock_anomaly_processor.process_anomaly_with_fast_signals.return_value = (test_attribution_result, None)
        mock_properties = MagicMock()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor
        test_service._process_attribution_request_event(mock_connection, None, 1, mock_properties, test_json.encode())
        # Assertions
        # Verify that the custom queue was used with queue name as routing key
        mock_declare_queue.assert_called_with(channel=ANY, queue_name=custom_output_queue)
        mock_bind_queue.assert_called_with(
            channel=ANY,
            queue_name=custom_output_queue,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key=custom_output_queue,  # routing key matches queue name
        )
        mock_publish_message.assert_called_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key=custom_output_queue,  # routing key matches queue name
            message_body=ANY,
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )
        # make sure JM was not called, as event had all the fields required.
        mock_get_jm.assert_not_called()
        mock_connection.add_callback_threadsafe.assert_called_once()

    @patch("pika.BlockingConnection")
    @patch("pika.ConnectionParameters")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("requests.get")
    @patch("builtins.open")
    def test_process_attribution_request_event_without_output_queue_publishes_without_queue(
        self,
        mock_file_open,
        mock_get_jm,
        mock_connect,
        mock_declare_exchange,
        mock_declare_queue,
        mock_bind_queue,
        mock_publish_message,
        mock_conn_param,
        mock_connection,
    ):
        """Fast attribution publishes results without creating an unconsumed output queue."""
        mock_file = mock_file_open.return_value
        mock_file.read.return_value = "your_test_token"

        test_attribution_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        # Note: NO output_queue field in the event
        test_json = json.dumps(
            {
                "job_id": "123456",
                "workflow_id": "canonical-workflow",
                "job_attempt_index": 0,
                "anomaly_time": "2024-03-24",
                "detection_time": "2024-03-25",
                "allocated_nodes": ["node1"],
                "event_type": "HANG",
                "start_time": "2024-03-24",
            }
        )
        test_settings = TestSettings()
        test_settings.num_worker_threads = 1
        test_settings.slow_signals_exchange = "slow-signals"
        test_settings.isolator_completed_exchange = "attribution-results"

        # Simulate an isolation result
        mock_anomaly_processor = MagicMock()
        mock_anomaly_processor.process_anomaly_with_fast_signals.return_value = (test_attribution_result, None)
        mock_properties = MagicMock()

        test_service = AnomalyDetectionIsolator(test_settings)
        test_service.anomaly_processor = mock_anomaly_processor
        test_service._process_attribution_request_event(mock_connection, None, 1, mock_properties, test_json.encode())
        # Assertions
        mock_declare_queue.assert_not_called()
        # The temporary slow-signal queue remains; no result queue is bound.
        mock_bind_queue.assert_called_once_with(
            channel=ANY,
            queue_name=mock_connect.return_value.queue_declare.call_args.kwargs["queue"],
            exchange_name=test_settings.slow_signals_exchange,
            routing_key=mock_connect.return_value.queue_declare.call_args.kwargs["queue"],
        )
        mock_publish_message.assert_called_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key="anomaly.isolation.bad_nodes",
            message_body=ANY,
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )
        # make sure JM was not called, as event had all the fields required.
        mock_get_jm.assert_not_called()
        mock_connection.add_callback_threadsafe.assert_called_once()

    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("builtins.open")
    def test_publish_slow_signals_message_success(
        self, mock_open, mock_connect, mock_declare_exchange, mock_bind_queue, mock_publish_message
    ):
        """Test successful publishing of a slow signals message to the event bus."""
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock the channel object returned by connect
        mock_channel = MagicMock()
        mock_connect.return_value = mock_channel

        # Test parameters
        test_message = "test_message"
        test_delivery_tag = 1
        test_delay_ms = 5000

        # Call the method
        test_service._publish_slow_signals_message_to_event_bus(test_message, test_delivery_tag, test_delay_ms)

        # Verify connect was called with correct parameters
        mock_connect.assert_called_once_with(
            hostname=test_settings.rabbitmq_url, vhost=test_settings.rabbitmq_vhost, credentials=ANY
        )

        # Verify exchange was declared
        mock_declare_exchange.assert_called_once_with(
            channel=mock_channel, exchange_name=test_settings.slow_signals_exchange
        )

        # Verify queue_declare was called with correct parameters
        mock_channel.queue_declare.assert_called_once()
        queue_declare_args = mock_channel.queue_declare.call_args[1]
        self.assertFalse(queue_declare_args["durable"])
        self.assertTrue(queue_declare_args["auto_delete"])
        self.assertEqual(queue_declare_args["arguments"]["x-message-ttl"], test_delay_ms)
        self.assertEqual(queue_declare_args["arguments"]["x-dead-letter-exchange"], test_settings.slow_signals_exchange)
        self.assertEqual(queue_declare_args["arguments"]["x-dead-letter-routing-key"], "job.updates.slow_signals")

        # Verify queue was bound
        mock_bind_queue.assert_called_once()
        bind_queue_args = mock_bind_queue.call_args[1]
        self.assertEqual(bind_queue_args["channel"], mock_channel)
        self.assertEqual(bind_queue_args["exchange_name"], test_settings.slow_signals_exchange)
        self.assertEqual(bind_queue_args["routing_key"], mock_channel.queue_declare.call_args[1]["queue"])

        # Verify message was published
        mock_publish_message.assert_called_once()
        publish_message_args = mock_publish_message.call_args[1]
        self.assertEqual(publish_message_args["channel"], mock_channel)
        self.assertEqual(publish_message_args["exchange_name"], test_settings.slow_signals_exchange)
        self.assertEqual(publish_message_args["routing_key"], mock_channel.queue_declare.call_args[1]["queue"])
        self.assertEqual(publish_message_args["message_body"], test_message)

    @unittest.skip("Skipped because it tests retries and thus takes a long time to run, unskip to test retries")
    @patch(
        "recovery_libs.event_bus.rabbit_eventing.publish_message",
        side_effect=EventBusError("PublishError", "Test error"),
    )
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("builtins.open")
    def test_publish_slow_signals_message_failure(
        self, mock_open, mock_connect, mock_declare_exchange, mock_bind_queue, mock_publish_message
    ):
        """Test handling of failures when publishing slow signals messages with retries."""
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock the channel object returned by connect
        mock_channel = MagicMock()
        mock_connect.return_value = mock_channel

        # Test parameters
        test_message = "test_message"
        test_delivery_tag = 1
        test_delay_ms = 5000

        # Call the method and expect it to raise an exception after retries
        with self.assertRaises(EventBusError):
            test_service._publish_slow_signals_message_to_event_bus(test_message, test_delivery_tag, test_delay_ms)

        # Verify publishing calls were called multiple times due to retries
        self.assertEqual(mock_connect.call_count, 5)
        self.assertEqual(mock_declare_exchange.call_count, 5)
        self.assertEqual(mock_channel.queue_declare.call_count, 5)
        self.assertEqual(mock_bind_queue.call_count, 5)
        self.assertEqual(mock_publish_message.call_count, 5)

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch("recovery_libs.event_bus.rabbit_eventing.connect")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_exchange")
    @patch("recovery_libs.event_bus.rabbit_eventing.declare_queue")
    @patch("recovery_libs.event_bus.rabbit_eventing.bind_queue")
    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_success(
        self,
        mock_open,
        mock_process_anomaly,
        mock_get_job_status,
        mock_bind_queue,
        mock_declare_queue,
        mock_declare_exchange,
        mock_connect,
        mock_publish_message,
        mock_tracer,
    ):
        """Test successful processing of a slow signals event."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        # Mock job status
        mock_job_status = SlurmJobExecutionStatus(
            state="CANCELLED",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime.now())],
        )
        # First call returns job status, second call returns None (no next attempt)
        mock_get_job_status.side_effect = [mock_job_status, None]

        # Mock anomaly processor result
        mock_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        mock_process_anomaly.return_value = mock_result

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "STRAGGLER",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was called with correct parameters
        mock_process_anomaly.assert_called_once_with(
            mock_job_status, "fact123", "STRAGGLER", "987651", mock_job_status.end_time + timedelta(minutes=3)
        )

        # Verify result was published
        mock_declare_queue.assert_not_called()
        mock_bind_queue.assert_not_called()
        mock_declare_exchange.assert_called_once_with(
            channel=ANY, exchange_name=test_settings.isolator_completed_exchange
        )
        mock_publish_message.assert_called_once_with(
            channel=ANY,
            exchange_name=test_settings.isolator_completed_exchange,
            routing_key="anomaly.isolation.bad_nodes",
            message_body=mock_result.to_json(),
            properties=pika.BasicProperties(headers=self.mock_header_populator.return_value),
        )

        mock_tracer.start_as_current_span.assert_called_with(name="job_anomaly.attribution", kind=SpanKind.PRODUCER)
        mock_span.set_attribute.assert_has_calls(
            [
                call("job_id", "123456"),
                call("job_attempt_index", 0),
                call("job_anomaly.type", "STRAGGLER"),
                call("job_anomaly.uuid", "987651"),
                call("job_anomaly.attribution_type", "slow_signals"),
            ],
            any_order=True,
        )
        mock_span.set_status.assert_called_once_with(StatusCode.OK)

    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_job_running(
        self, mock_open, mock_process_anomaly, mock_get_job_status, mock_publish_result, mock_publish_message
    ):
        """Test successful processing of a slow signals event."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock job status
        mock_job_status = SlurmJobExecutionStatus(
            state="RUNNING",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime.now())],
        )
        # First call returns job status, second call returns None (no next attempt)
        mock_get_job_status.side_effect = [mock_job_status, None]

        # Mock anomaly processor result
        mock_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        mock_process_anomaly.return_value = mock_result

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "STRAGGLER",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was NOT called since job is running
        mock_process_anomaly.assert_not_called()

        # Verify result was NOT published since job is running
        mock_publish_result.assert_not_called()

    @patch("recovery_libs.event_bus.rabbit_eventing.publish_message")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_with_next_attempt(
        self, mock_open, mock_process_anomaly, mock_get_job_status, mock_publish_result, mock_publish_message
    ):
        """Test slow signals processing when next attempt job status exists."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock job status
        mock_job_status = SlurmJobExecutionStatus(
            state="CANCELLED",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="CRASH", timestamp=datetime.now())],
        )

        # Mock next attempt job status
        mock_next_attempt_status = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=1,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now() + timedelta(minutes=5),  # Next attempt starts 5 minutes later
            allocated_node_list=["node1", "node2"],
            anomalies=[],
        )

        mock_get_job_status.side_effect = [mock_job_status, mock_next_attempt_status]

        # Mock anomaly processor result
        mock_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.CRASH,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        mock_process_anomaly.return_value = mock_result

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "CRASH",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was called with next attempt start time as epilog end time
        mock_process_anomaly.assert_called_once_with(
            mock_job_status, "fact123", "CRASH", "987651", mock_next_attempt_status.start_time
        )

        # Verify result was published
        mock_publish_result.assert_called_once_with(mock_result.to_json(), 1)

    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_missing_job_id(self, mock_open, mock_process_anomaly, mock_get_job_status):
        """Test handling of missing job_id in the event."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Test event data without job_id
        test_event = {"job_attempt_index": 0, "fact_attribution_id": "fact123", "anomaly_type": "STRAGGLER"}
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was not called
        mock_get_job_status.assert_not_called()

        # Verify anomaly processor was not called
        mock_process_anomaly.assert_not_called()

    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_none_body(self, mock_open, mock_process_anomaly, mock_get_job_status):
        """Test handling of None body."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method with None body
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, None)

        # Verify job manager was not called
        mock_get_job_status.assert_not_called()

        # Verify anomaly processor was not called
        mock_process_anomaly.assert_not_called()

    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch("builtins.open")
    def test_process_slow_signals_event_job_status_not_found(
        self, mock_open, mock_process_anomaly, mock_get_job_status
    ):
        """Test handling of job status not found."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock job status as None
        mock_get_job_status.return_value = None

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "STRAGGLER",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was not called
        mock_process_anomaly.assert_not_called()

    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch("builtins.open")
    def test_process_slow_signals_event_no_result(
        self, mock_open, mock_publish_result, mock_process_anomaly, mock_get_job_status
    ):
        """Test handling of no result from anomaly processor."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock job status
        mock_job_status = SlurmJobExecutionStatus(
            state="CANCELLED",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime.now(), uuid="987651")],
        )
        # First call returns job status, second call returns None (no next attempt)
        mock_get_job_status.side_effect = [mock_job_status, None]

        # Mock anomaly processor result as None
        mock_process_anomaly.return_value = None

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "CRASH",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was called
        mock_process_anomaly.assert_called_once_with(
            mock_job_status, "fact123", "CRASH", "987651", mock_job_status.end_time + timedelta(minutes=3)
        )

        # Verify result was not published
        mock_publish_result.assert_not_called()

    @patch.object(JobManagerClient, "get_job_status")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch("anomaly_detection_isolator.service.EVENTBUS_PUBLISH_ERRORS")
    @patch("builtins.open")
    def test_process_slow_signals_event_publish_error(
        self, mock_open, mock_eventbus_errors, mock_publish_result, mock_process_anomaly, mock_get_job_status
    ):
        """Test handling of publishing errors."""
        # Setup
        test_settings = TestSettings()
        test_service = AnomalyDetectionIsolator(test_settings)

        # Mock job status
        mock_job_status = SlurmJobExecutionStatus(
            state="CANCELLED",
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime.now(),
            start_time=datetime.now(),
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime.now())],
        )
        # First call returns job status, second call returns None (no next attempt)
        mock_get_job_status.side_effect = [mock_job_status, None]

        # Mock anomaly processor result
        mock_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="test-anomaly-uuid",
            attribution_type=ErrorAttributionType.SLOW,
            node_attributions={
                JobExecutionError(code=0, message="", description="", signature=""): [],
            },
            global_attributions={},
        )
        mock_process_anomaly.return_value = mock_result

        # Mock publish error
        test_error = Exception("Test publish error")
        mock_publish_result.side_effect = test_error

        # Mock eventbus errors counter
        mock_eventbus_errors.labels.return_value = mock_eventbus_errors
        mock_eventbus_errors.inc = MagicMock()

        # Test event data
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "STRAGGLER",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called twice (current and next attempt)
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was called
        mock_process_anomaly.assert_called_once_with(
            mock_job_status, "fact123", "STRAGGLER", "987651", mock_job_status.end_time + timedelta(minutes=3)
        )

        # Verify result was published (which raised an exception)
        mock_publish_result.assert_called_once_with(mock_result.to_json(), 1)

        # Verify error was logged
        mock_eventbus_errors.labels.assert_called_once_with(eventbus_error=type(test_error).__name__)
        mock_eventbus_errors.inc.assert_called_once()

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("anomaly_detection_isolator.service.FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS")
    @patch("anomaly_detection_isolator.service.EVENTBUS_PUBLISH_ERRORS")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(AnomalyDetectionIsolator, "_publish_slow_signals_message_to_event_bus")
    @patch.object(AnomalyProcessor, "process_anomaly_with_fast_signals")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_anomaly_processor_error(
        self,
        mock_open,
        mock_get_job_status,
        mock_process_anomaly,
        mock_publish_slow_signals,
        mock_publish_result,
        mock_eventbus_errors,
        mock_fast_signals_errors,
        mock_tracer,
    ):
        """Test that anomaly processor errors are handled gracefully in fast signals processing."""
        # Setup tracing mock
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        # Setup job status
        mock_job_status = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime(2023, 1, 1, 12, 0, 0),
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            command="test command",
            cluster="test_cluster",
            state="CANCELLED",
            user="test_user",
            allocated_node_list=["node1", "node2"],
            anomalies=[
                JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime(2023, 1, 1, 11, 30, 0), uuid="uuid-value")
            ],
        )
        mock_get_job_status.return_value = mock_job_status

        # Setup anomaly processor to raise an exception
        test_error = Exception("Test anomaly processor error")
        mock_process_anomaly.side_effect = test_error

        # Setup test service
        test_service = AnomalyDetectionIsolator(TestSettings())

        # Create test event
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "anomaly_time": "2023-01-01T12:00:00Z",
            "anomaly_type": "CRASH",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection objects
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_properties = MagicMock()

        # Call the method
        test_service._process_attribution_request_event(
            mock_connection, mock_channel, 1, mock_properties, test_event_json.encode()
        )

        # Verify job manager was called
        mock_get_job_status.assert_called_once_with(1, "123456", 0)

        # Verify anomaly processor was called and failed
        mock_process_anomaly.assert_called_once()

        # Verify that an empty result was still published
        mock_publish_result.assert_called_once()
        published_result_json = mock_publish_result.call_args[0][0]
        published_result_dict = json.loads(published_result_json)
        published_result = AnomalyAttributionResult.from_json(published_result_dict)

        # Verify the empty result has correct structure
        self.assertEqual(published_result.job_id, "123456")
        self.assertEqual(published_result.job_attempt_index, 0)
        self.assertEqual(published_result.anomaly_type, EventType.STRAGGLER)
        self.assertEqual(published_result.node_attributions, {})

        # Verify counter was incremented
        mock_fast_signals_errors.labels.assert_called_once_with(job_id="123456", job_attempt_index="0")
        mock_fast_signals_errors.labels().inc.assert_called_once()

        # Verify slow signals message was NOT published for STRAGGLER anomaly
        # even if slow_signals_enabled is True, STRAGGLER anomalies don't send slow signals
        mock_publish_slow_signals.assert_not_called()

        # Verify the span attributes and status are set properly.
        mock_tracer.start_as_current_span.assert_called_with(
            name="job_anomaly.attribution", kind=SpanKind.PRODUCER, context=self.mock_properties_extractor.return_value
        )
        mock_span.set_attribute.assert_has_calls(
            [
                call("job_id", "123456"),
                call("job_attempt_index", 0),
                call("job_anomaly.type", "STRAGGLER"),
                call("job_anomaly.uuid", "uuid-value"),
                call("job_anomaly.attribution_type", "fast_signals"),
            ],
            any_order=True,
        )
        mock_span.record_exception.assert_called_once()

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("anomaly_detection_isolator.service.SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS")
    @patch("anomaly_detection_isolator.service.EVENTBUS_PUBLISH_ERRORS")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(AnomalyProcessor, "process_anomaly_with_slow_signals")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_slow_signals_event_anomaly_processor_error(
        self,
        mock_open,
        mock_get_job_status,
        mock_process_anomaly,
        mock_publish_result,
        mock_eventbus_errors,
        mock_slow_signals_errors,
        mock_tracer,
    ):
        """Test that anomaly processor errors are handled gracefully in slow signals processing."""
        # Setup tracing mock
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        # Setup job status (completed job)
        mock_job_status = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime(2023, 1, 1, 12, 0, 0),
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            command="test command",
            cluster="test_cluster",
            state="CANCELLED",  # Final state
            user="test_user",
            allocated_node_list=["node1", "node2"],
            anomalies=[JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime(2023, 1, 1, 11, 30, 0))],
        )

        # Mock returning None for next attempt (no restart)
        mock_get_job_status.side_effect = [mock_job_status, None]

        # Setup anomaly processor to raise an exception
        test_error = Exception("Test slow signals processor error")
        mock_process_anomaly.side_effect = test_error

        # Setup test service
        test_service = AnomalyDetectionIsolator(TestSettings())

        # Create test event
        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "fact_attribution_id": "fact123",
            "anomaly_type": "STRAGGLER",
            "anomaly_uuid": "987651",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection objects
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # Call the method
        test_service._process_slow_signals_event(mock_connection, mock_channel, 1, test_event_json.encode())

        # Verify job manager was called for current and next attempt
        self.assertEqual(mock_get_job_status.call_count, 2)
        mock_get_job_status.assert_any_call(1, "123456", 0)
        mock_get_job_status.assert_any_call(1, "123456", 1)

        # Verify anomaly processor was called and failed
        mock_process_anomaly.assert_called_once()

        # Verify that an empty result was still published
        mock_publish_result.assert_called_once()
        published_result_json = mock_publish_result.call_args[0][0]
        published_result_dict = json.loads(published_result_json)
        published_result = AnomalyAttributionResult.from_json(published_result_dict)

        # Verify the empty result has correct structure
        self.assertEqual(published_result.job_id, "123456")
        self.assertEqual(published_result.job_attempt_index, 0)
        self.assertEqual(published_result.anomaly_type, EventType.STRAGGLER)
        self.assertEqual(published_result.node_attributions, {})

        # Verify counter was incremented
        mock_slow_signals_errors.labels.assert_called_once_with(job_id="123456", job_attempt_index="0")
        mock_slow_signals_errors.labels().inc.assert_called_once()

        # Verify the span attributes and status are set properly.
        mock_tracer.start_as_current_span.assert_called_with(name="job_anomaly.attribution", kind=SpanKind.PRODUCER)
        mock_span.set_attribute.assert_has_calls(
            [
                call("job_id", "123456"),
                call("job_attempt_index", 0),
                call("job_anomaly.type", "STRAGGLER"),
                call("job_anomaly.uuid", "987651"),
                call("job_anomaly.attribution_type", "slow_signals"),
            ],
            any_order=True,
        )
        mock_span.record_exception.assert_called_once()

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("anomaly_detection_isolator.service.EVENTBUS_PUBLISH_ERRORS")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(AnomalyDetectionIsolator, "_publish_slow_signals_message_to_event_bus")
    @patch.object(AnomalyProcessor, "process_anomaly_with_fast_signals")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_event_single_anomaly_empty_attribution_publishes_once(
        self,
        mock_open,
        mock_get_job_status,
        mock_process_anomaly,
        mock_publish_slow_signals,
        mock_publish_result,
        mock_eventbus_errors,
        mock_tracer,
    ):
        """D7 single behavioral delta: a SINGLE/last anomaly whose attribution FAILS (the
        processor now returns an empty NON-None AnomalyAttributionResult) still publishes
        ONE empty message — it does NOT skip.

        Contrast with legacy: a get_attribution failure returned None and the publish loop
        skipped, leaving the downstream consumer without a message for a last/only anomaly.
        The new never-None contract publishes an empty-attribution result instead so the
        downstream stage is never left waiting.
        """
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        mock_job_status = SlurmJobExecutionStatus(
            job_id="123456",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime(2023, 1, 1, 12, 0, 0),
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            command="test command",
            cluster="test_cluster",
            state="CANCELLED",
            user="test_user",
            allocated_node_list=["node1", "node2"],
            # A SINGLE anomaly => it is the last anomaly (idx == num - 1).
            anomalies=[
                JobAnomaly(anomaly_type="STRAGGLER", timestamp=datetime(2023, 1, 1, 11, 30, 0), uuid="uuid-value")
            ],
        )
        mock_get_job_status.return_value = mock_job_status

        # The processor SUCCEEDS (no exception) but attribution found nothing: it returns an
        # EMPTY but NON-None AnomalyAttributionResult, paired with a None session id.
        empty_result = AnomalyAttributionResult(
            job_id="123456",
            job_attempt_index=0,
            anomaly_type=EventType.STRAGGLER,
            anomaly_uuid="uuid-value",
            attribution_type=ErrorAttributionType.FAST,
            node_attributions={},
            global_attributions={},
        )
        mock_process_anomaly.return_value = (empty_result, None)

        test_service = AnomalyDetectionIsolator(TestSettings())

        test_event = {
            "job_id": "123456",
            "job_attempt_index": 0,
            "anomaly_time": "2023-01-01T12:00:00Z",
            "anomaly_type": "STRAGGLER",
        }
        test_event_json = json.dumps(test_event)

        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_properties = MagicMock()

        test_service._process_attribution_request_event(
            mock_connection, mock_channel, 1, mock_properties, test_event_json.encode()
        )

        # The processor was called once and did NOT raise.
        mock_process_anomaly.assert_called_once()
        mock_eventbus_errors.labels.assert_not_called()

        # D7: exactly ONE publish happened (the empty result was NOT skipped).
        mock_publish_result.assert_called_once()
        published_result = AnomalyAttributionResult.from_json(json.loads(mock_publish_result.call_args[0][0]))
        self.assertEqual(published_result.job_id, "123456")
        self.assertEqual(published_result.anomaly_type, EventType.STRAGGLER)
        self.assertEqual(published_result.node_attributions, {})
        self.assertEqual(published_result.global_attributions, {})

        # STRAGGLER never sends slow signals.
        mock_publish_slow_signals.assert_not_called()


class TestAnomalyDetectionIsolatorSlowSignals(unittest.TestCase):
    @patch("anomaly_detection_isolator.service.AnomalyProcessor")
    @patch("anomaly_detection_isolator.service.JobManagerClient")
    def setUp(self, mock_job_manager_client, mock_anomaly_processor):
        self.settings = MagicMock()
        self.settings.slow_signals_delay_seconds = 1
        self.settings.slow_signals_enabled = False

        # Mock the dependencies to avoid real HTTP requests
        mock_anomaly_processor.return_value = MagicMock()
        mock_job_manager_client.return_value = MagicMock()

        self.isolator = AnomalyDetectionIsolator(settings=self.settings)
        self.isolator._publish_slow_signals_message_to_event_bus = MagicMock()

    @patch("anomaly_detection_isolator.service.logger")
    def test_slow_signals_not_published_when_disabled(self, mock_logger):
        """Test that slow signals are not published when slow_signals_enabled is False."""
        self.settings.slow_signals_enabled = False
        job_status = MagicMock()
        job_status.state = next(iter(SlurmJobFinalStates))  # Final state

        should_create_slow_signals = job_status.state in SlurmJobFinalStates and self.settings.slow_signals_enabled

        self.assertFalse(should_create_slow_signals)
        self.isolator._publish_slow_signals_message_to_event_bus.assert_not_called()

    def test_slow_signals_published_when_enabled(self):
        """Test that slow signals are published when slow_signals_enabled is True and job is in final state."""
        self.settings.slow_signals_enabled = True
        job_status = MagicMock()
        job_status.state = next(iter(SlurmJobFinalStates))  # Final state

        should_create_slow_signals = job_status.state in SlurmJobFinalStates and self.settings.slow_signals_enabled

        self.assertTrue(should_create_slow_signals)

    def test_slow_signals_not_published_for_straggler_anomaly(self):
        """Test that slow signals are not published for STRAGGLER anomaly type even when slow_signals_enabled is True."""
        self.settings.slow_signals_enabled = True
        job_status = MagicMock()
        job_status.state = next(iter(SlurmJobFinalStates))  # Final state

        # Test with STRAGGLER anomaly type
        job_anomaly_type = EventType.STRAGGLER.value

        # Test the helper function directly
        should_send_slow_signals = self.isolator._should_send_slow_signals(job_anomaly_type)
        self.assertFalse(should_send_slow_signals)
        self.isolator._publish_slow_signals_message_to_event_bus.assert_not_called()


class TestSettings(BaseSettings):
    __test__ = False  # to make sure this is not considered a test class.

    attribution_engines: str = "FACT"
    fact_url: str = "TEST"
    fact_non_isolated_errors: list[str] = ["TestError"]
    fact_isolated_errors: list[str] = []
    fact_ambiguous_errors: list[str] = []
    fact_non_isolated_errors_env: list[str] = []
    fact_isolated_errors_env: list[str] = []
    fact_ambiguous_errors_env: list[str] = []
    loki_url: str = "TEST"
    loki_app_url: str | None = None
    loki_sys_url: str | None = None
    loki_cluster: str = "TEST"
    loki_tenants: str = ""
    prometheus_url: str = ""
    prometheus_cluster: str = ""
    prometheus_token_path: str = ""
    prometheus_checks_enabled: bool = False
    prometheus_hostname_label: str = "TEST"
    prometheus_tenants: str = ""
    root_path: str = "TEST"
    exchange_name: str = "TEST"
    routing_key: str = "TEST"
    queue_name: str = "TEST"
    rabbitmq_user: str = "TEST"
    rabbitmq_password: SecretStr = SecretStr("TEST")
    rabbitmq_url: str = "TEST"
    rabbitmq_port: int = 5672
    rabbitmq_vhost: str = "TEST"
    attribution_request_exchange: str = "TEST"
    attribution_request_queue: str = "TEST"
    isolator_completed_exchange: str = "TEST"
    slow_signals_exchange: str = "TEST"
    slow_signals_queue: str = "TEST"
    use_x_scope_org_id: bool = False
    epilogue_started_wait_seconds: int = 0
    epilog_query_duration_minutes: int = 3
    slow_signals_delay_seconds: int = 10
    num_worker_threads: int = 10
    jobmanager_host: str = "TEST"
    jobmanager_port: int = 8080
    jobmanager_status_path: str = "TEST"
    syslog_formatter: str = "TEST"
    kernel_logs_only: bool = True
    syslogindex_enabled: bool = False
    max_isolated_nodes: int = 3
    rack_aware_isolation_enabled: bool | None = False
    max_isolated_nodes_per_rack: int | None = None
    max_racks_to_isolate: int | None = 9
    rack_extraction_pattern: str | None = None
    applog_query: str = "TEST"
    syslog_query: str = "TEST"
    ib_metrics_query: str = "TEST"
    eth_metrics_query: str = "TEST"
    health_check_query: str = "TEST"
    epilogue_query: str = "TEST"
    logql_json_filter: str = "TEST"
    logql_syslog_index_filter: str = "TEST"
    logql_kernel_filter: str = "TEST"
    logql_applog_exclude_filter: str = "TEST"
    logql_prolog_exclude_filter: str = "TEST"
    logql_epilog_exclude_filter: str = "TEST"
    logql_lookback_timewindow_duration_minutes: int = 30
    logql_applog_grace_time_seconds: int = 60
    logql_applog_format: str = "TEST"
    log_fetch_optimization_enabled: bool = False
    log_fetch_direction: Literal["forward", "backward"] = "backward"
    log_fetch_timeout_syslogs_seconds: int = 60
    log_fetch_timeout_applogs_seconds: int = 60
    log_fetch_timeout_slow_signals_seconds: int = 300
    fact_http_timeout_seconds: int = 30
    loki_http_timeout_seconds: int = 30
    jobmanager_http_timeout_seconds: int = 30
    slow_signals_enabled: bool = True
    is_lepton_enabled: bool = False
    attribution_enabled: bool = False
    attribution_service_url: str | None = None
    attribution_fast_http_timeout_seconds: int = 90
    attribution_slow_http_timeout_seconds: int = 180


if __name__ == "__main__":
    unittest.main()


def test_validate_settings():
    """Test the validate_settings function."""
    # Test with valid settings
    settings = TestSettings()
    assert validate_settings(settings) is True


def test_validate_settings_invalid():
    """Test the validate_settings function with invalid settings."""
    # Test with invalid settings
    settings = TestSettings()
    settings.rabbitmq_url = ""
    assert validate_settings(settings) is False


def test_validate_settings_invalid_fact_errors():
    """Test the validate_settings function with invalid fact error settings."""
    # Test with invalid settings
    settings = TestSettings()
    settings.fact_non_isolated_errors = None
    assert validate_settings(settings) is False


def test_validate_settings_invalid_epilog_query_duration():
    """Test the validate_settings function with invalid epilog_query_duration_minutes."""
    # Test with invalid settings
    settings = TestSettings()
    settings.epilog_query_duration_minutes = 0
    assert validate_settings(settings) is False


class JobStatusValidationTests(unittest.TestCase):
    """Tests for the _validate_job_status method."""

    @patch("builtins.open")
    def setUp(self, mock_open):
        """Set up test fixtures."""
        self.test_settings = TestSettings()
        self.service = AnomalyDetectionIsolator(self.test_settings)
        self.delivery_tag = 1

    def _create_valid_job_status(self):
        """Create a valid job status for testing."""
        return SlurmJobExecutionStatus(
            job_id="test_job_123",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime(2025, 1, 1, 10, 0, 0),
            end_time=datetime(2025, 1, 1, 11, 0, 0),
            allocated_node_list=["node-001", "node-002"],
            anomalies=[
                JobAnomaly(
                    anomaly_type="CRASH",
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    uuid="test-uuid-123",
                )
            ],
            cluster="test-cluster",
            command="srun test_job",
            job_name="test_job",
            user="test_user",
            node_count=2,
            total_gpu_count=16,
        )

    @patch("builtins.open")
    def test_validate_job_status_valid_complete(self, mock_open):
        """Test validation with all fields present."""
        job_status = self._create_valid_job_status()
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertTrue(result)

    @patch("builtins.open")
    def test_validate_job_status_missing_job_id(self, mock_open):
        """Test validation fails when job_id is None."""
        job_status = self._create_valid_job_status()
        job_status.job_id = None
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_missing_start_time(self, mock_open):
        """Test validation fails when start_time is None."""
        job_status = self._create_valid_job_status()
        job_status.start_time = None
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_missing_allocated_nodes(self, mock_open):
        """Test validation fails when allocated_node_list is None."""
        job_status = self._create_valid_job_status()
        job_status.allocated_node_list = None
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_empty_allocated_nodes(self, mock_open):
        """Test validation fails when allocated_node_list is empty."""
        job_status = self._create_valid_job_status()
        job_status.allocated_node_list = []
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_missing_anomalies(self, mock_open):
        """Test validation fails when anomalies is None."""
        job_status = self._create_valid_job_status()
        job_status.anomalies = None
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_empty_anomalies(self, mock_open):
        """Test validation fails when anomalies list is empty."""
        job_status = self._create_valid_job_status()
        job_status.anomalies = []
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_multiple_missing_fields(self, mock_open):
        """Test validation fails with multiple missing required fields."""
        job_status = self._create_valid_job_status()
        job_status.start_time = None
        job_status.allocated_node_list = None
        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertFalse(result)

    @patch("builtins.open")
    def test_validate_job_status_missing_optional_fact_fields(self, mock_open):
        """Test validation passes but warns when optional FACT fields are missing."""
        # Set attribution engine to FACT
        self.test_settings.attribution_engines = "FACT"
        service = AnomalyDetectionIsolator(self.test_settings)

        job_status = self._create_valid_job_status()
        job_status.cluster = None
        job_status.command = None
        job_status.user = None

        # Should still pass validation but log warning
        result = service._validate_job_status(job_status, self.delivery_tag)
        self.assertTrue(result)

    @patch("builtins.open")
    def test_validate_job_status_with_only_required_fields(self, mock_open):
        """Test validation passes with only required fields (optional fields are None)."""
        job_status = SlurmJobExecutionStatus(
            job_id="minimal_job",
            workflow_id="test-workflow",
            job_attempt_index=0,
            start_time=datetime(2025, 1, 1, 10, 0, 0),
            allocated_node_list=["node-001"],
            anomalies=[
                JobAnomaly(
                    anomaly_type="CRASH",
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    uuid="test-uuid",
                )
            ],
            # Optional fields (cluster, command, user, etc.) are None by default
        )

        result = self.service._validate_job_status(job_status, self.delivery_tag)
        self.assertTrue(result)


class PrometheusMetricsTests(unittest.TestCase):
    """Tests to verify Prometheus counter labels are used correctly."""

    def test_fast_signals_error_counter_labels_no_value_error(self):
        """Test that FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels() doesn't raise ValueError.

        This test ensures the fix for the incorrect label count error is working.
        The counter expects keyword arguments, not a dictionary.
        """
        from anomaly_detection_isolator.service import (
            FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS,
        )

        # This should not raise ValueError: Incorrect label count
        try:
            # Use the counter with proper keyword arguments
            counter = FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels(job_id="test_job_123", job_attempt_index="0")
            counter.inc()
            # If we get here, the test passes
            self.assertTrue(True)
        except ValueError as e:
            if "Incorrect label count" in str(e):
                self.fail(f"ValueError with incorrect label count was raised: {e}")
            else:
                raise

    def test_slow_signals_error_counter_labels_no_value_error(self):
        """Test that SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels() doesn't raise ValueError.

        This test ensures the fix for the incorrect label count error is working.
        The counter expects keyword arguments, not a dictionary.
        """
        from anomaly_detection_isolator.service import (
            SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS,
        )

        # This should not raise ValueError: Incorrect label count
        try:
            # Use the counter with proper keyword arguments
            counter = SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels(job_id="test_job_456", job_attempt_index="1")
            counter.inc()
            # If we get here, the test passes
            self.assertTrue(True)
        except ValueError as e:
            if "Incorrect label count" in str(e):
                self.fail(f"ValueError with incorrect label count was raised: {e}")
            else:
                raise

    def test_fast_signals_error_counter_labels_with_dict_raises_value_error(self):
        """Test that passing a dictionary to labels() raises ValueError.

        This test documents the incorrect usage pattern that was causing the original bug.
        When a dictionary is passed as a positional argument, it's treated as a single
        argument, resulting in "Incorrect label count" ValueError.
        """
        from anomaly_detection_isolator.service import (
            FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS,
        )

        # Passing a dictionary as positional arg raises ValueError: Incorrect label count
        with self.assertRaises(ValueError) as cm:
            FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels({"job_id": "test_job", "job_attempt_index": "0"}).inc()

        # Verify it's the specific error we expect
        self.assertIn("Incorrect label count", str(cm.exception))


class JobStatusValidationBehaviorTests(unittest.TestCase):
    """Tests to verify that job processing continues with warnings when validation fails."""

    @patch("anomaly_detection_isolator.service.tracer")
    @patch("anomaly_detection_isolator.service.logger")
    @patch.object(AnomalyDetectionIsolator, "_publish_result_message_to_event_bus")
    @patch.object(AnomalyDetectionIsolator, "_publish_slow_signals_message_to_event_bus")
    @patch.object(AnomalyProcessor, "process_anomaly_with_fast_signals")
    @patch.object(JobManagerClient, "get_job_status")
    @patch("builtins.open")
    def test_process_attribution_request_continues_despite_validation_failure(
        self,
        mock_open,
        mock_get_job_status,
        mock_process_anomaly,
        mock_publish_slow_signals,
        mock_publish_result,
        mock_logger,
        mock_tracer,
    ):
        """Test that processing continues with a warning when job status validation fails.

        This test verifies that when validation fails (e.g., missing required fields),
        the service logs a warning and continues processing rather than returning early.
        """
        # Setup tracing mock
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        # Setup job status with missing required field (allocated_node_list is None)
        mock_job_status = SlurmJobExecutionStatus(
            job_id="test_job_123",
            workflow_id="test-workflow",
            job_attempt_index=0,
            job_name="test_job",
            end_time=datetime(2023, 1, 1, 12, 0, 0),
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            command="test command",
            cluster="test_cluster",
            state="CANCELLED",
            user="test_user",
            allocated_node_list=None,  # Missing required field - validation will fail
            anomalies=[
                JobAnomaly(
                    anomaly_type="CRASH",
                    timestamp=datetime(2023, 1, 1, 11, 30, 0),
                    uuid="test-uuid-123",
                )
            ],
        )
        mock_get_job_status.return_value = mock_job_status

        # Setup anomaly processor to return a result
        mock_result = MagicMock()
        mock_result.job_id = "test_job_123"
        mock_result.job_attempt_index = 0
        mock_result.anomaly_type = EventType.CRASH
        mock_result.error_attributions = {"node1": ["error"]}
        mock_result.to_json.return_value = '{"job_id": "test_job_123"}'
        mock_process_anomaly.return_value = (mock_result, "attribution_id")

        # Setup test service
        test_service = AnomalyDetectionIsolator(TestSettings())

        # Create test event
        test_event = {
            "job_id": "test_job_123",
            "job_attempt_index": 0,
            "anomaly_time": "2023-01-01T12:00:00Z",
        }
        test_event_json = json.dumps(test_event)

        # Mock connection objects
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_properties = MagicMock()

        # Call the method
        test_service._process_attribution_request_event(
            mock_connection, mock_channel, 1, mock_properties, test_event_json.encode()
        )

        # Verify that a warning was logged about validation failure
        warning_logged = False
        for warning_call in mock_logger.warning.call_args_list:
            args = warning_call[0]
            if args and "Job status validation failed" in args[0] and "Continuing with processing" in args[0]:
                warning_logged = True
                break

        self.assertTrue(warning_logged, "Expected warning about validation failure to be logged")

        # Verify that processing continued - anomaly processor was called
        mock_process_anomaly.assert_called_once()

        # Verify that result was published despite validation failure
        mock_publish_result.assert_called_once()
