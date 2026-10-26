# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HttpOTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from opentelemetry.trace import Span, SpanContext, TraceFlags, TraceState
from pika.spec import BasicProperties
from recovery_libs.tracing.traces import (
    extract_trace_context,
    extract_trace_context_from_rabbitmq_properties,
    extract_trace_id_from_rabbitmq_properties,
    extract_trace_id_from_span,
    inject_trace_context,
    populate_headers_with_span_context,
    restore_trace_context,
    serialize_trace_context,
    set_up_trace_provider,
)


class TestTraces(unittest.TestCase):
    def test_set_up_trace_provider(self):
        # Mock dependencies
        with (
            patch("recovery_libs.tracing.traces.Resource") as mock_resource,
            patch("recovery_libs.tracing.traces.TracerProvider") as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace") as mock_trace,
        ):
            # Setup mock returns
            mock_resource.create.return_value = MagicMock(name="mock_resource")
            mock_provider = MagicMock(name="mock_provider")
            mock_provider_class.return_value = mock_provider

            # Test with default parameters (no OTLP endpoint)
            service_name = "test_service"
            set_up_trace_provider(service_name=service_name)

            # Verify resource creation
            mock_resource.create.assert_called_once_with({"service.name": service_name})

            # Verify TracerProvider creation and configuration
            mock_provider_class.assert_called_once_with(resource=mock_resource.create.return_value, sampler=ALWAYS_OFF)

            # No span processor should be added when OTLP endpoint is not configured
            mock_provider.add_span_processor.assert_not_called()

            # Verify tracer provider was set
            mock_trace.set_tracer_provider.assert_called_once_with(mock_provider)

    def test_set_up_trace_provider_with_custom_exporter_and_processor(self):
        # Mock dependencies
        with (
            patch("recovery_libs.tracing.traces.Resource") as mock_resource,
            patch("recovery_libs.tracing.traces.TracerProvider") as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace") as mock_trace,
        ):
            # Setup mock returns
            mock_resource.create.return_value = MagicMock(name="mock_resource")
            mock_provider = MagicMock(name="mock_provider")
            mock_provider_class.return_value = mock_provider

            # Create custom mocks for exporter and processor
            custom_exporter = MagicMock(name="custom_exporter")
            custom_processor = MagicMock(name="custom_processor")

            # Test with custom exporter and processor
            service_name = "test_service"
            set_up_trace_provider(
                service_name=service_name,
                span_exporter=custom_exporter,
                span_processor=custom_processor,
            )

            # Verify resource creation
            mock_resource.create.assert_called_once_with({"service.name": service_name})

            # Verify TracerProvider creation
            mock_provider_class.assert_called_once_with(resource=mock_resource.create.return_value)

            # Verify custom processor was added
            mock_provider.add_span_processor.assert_called_once_with(custom_processor)

            # Verify tracer provider was set
            mock_trace.set_tracer_provider.assert_called_once_with(mock_provider)

    def test_set_up_trace_provider_with_gprc_otlp_exporter_with_auth(self):
        """
        Tests creation of TracerProvider with GrpcOTLPSpanExporter when both OTEL_EXPORTER_OTLP_ENDPOINT
        and OTEL_EXPORTER_OTLP_TOKEN environment variables are set and the protocol is "grpc".
        """
        with (
            patch("recovery_libs.tracing.traces.TracerProvider", return_value=MagicMock()) as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace"),
            patch("recovery_libs.tracing.traces.GrpcOTLPSpanExporter") as mock_grpc_exporter,
            patch.dict(
                "os.environ",
                {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317", "OTEL_EXPORTER_OTLP_TOKEN": "test_token"},
            ),
        ):
            # Setup mock returns
            mock_grpc_exporter.return_value = MagicMock(name="mock_grpc_exporter_instance")

            # Test with default parameters
            set_up_trace_provider(service_name="test_service", protocol="grpc")

            # Verify TracerProvider creation and configuration
            mock_grpc_exporter.assert_called_once_with(
                endpoint="http://localhost:4317", headers={"authorization": "Bearer test_token"}
            )

            # Verify span processor was added
            self.assertEqual(mock_provider_class.return_value.add_span_processor.call_count, 1)
            added_processor = mock_provider_class.return_value.add_span_processor.call_args[0][0]
            self.assertIsInstance(added_processor, BatchSpanProcessor)

    def test_set_up_trace_provider_with_gprc_otlp_exporter_without_auth(self):
        """
        Tests creation of TracerProvider with GrpcOTLPSpanExporter when both OTEL_EXPORTER_OTLP_ENDPOINT
        environment variables is set and the protocol is "grpc".
        """
        with (
            patch("recovery_libs.tracing.traces.TracerProvider", return_value=MagicMock()) as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace"),
            patch("recovery_libs.tracing.traces.GrpcOTLPSpanExporter") as mock_grpc_exporter,
            patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317"}),
        ):
            # Setup mock returns
            mock_grpc_exporter.return_value = MagicMock(name="mock_grpc_exporter_instance")

            # Test with default parameters
            set_up_trace_provider(service_name="test_service", protocol="grpc")

            # Verify TracerProvider creation and configuration
            mock_grpc_exporter.assert_called_once_with(endpoint="http://localhost:4317", insecure=True)

            # Verify span processor was added
            self.assertEqual(mock_provider_class.return_value.add_span_processor.call_count, 1)
            added_processor = mock_provider_class.return_value.add_span_processor.call_args[0][0]
            self.assertIsInstance(added_processor, BatchSpanProcessor)

    def test_set_up_trace_provider_with_http_otlp_exporter_with_auth(self):
        """
        Tests creation of TracerProvider with HttpOTLPSpanExporter when both OTEL_EXPORTER_OTLP_ENDPOINT
        and OTEL_EXPORTER_OTLP_TOKEN environment variable are set and the protocol is "http".
        """
        with (
            patch("recovery_libs.tracing.traces.TracerProvider", return_value=MagicMock()) as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace"),
            patch("recovery_libs.tracing.traces.HttpOTLPSpanExporter") as mock_http_exporter,
            patch.dict(
                "os.environ",
                {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318", "OTEL_EXPORTER_OTLP_TOKEN": "test_token"},
            ),
        ):
            # Setup mock returns
            mock_http_exporter.return_value = MagicMock(name="mock_http_exporter_instance")

            # Test with default parameters
            set_up_trace_provider(service_name="test_service", protocol="http")

            # Verify TracerProvider creation and configuration
            mock_http_exporter.assert_called_once_with(
                endpoint="http://localhost:4318/v1/traces", headers={"authorization": "Bearer test_token"}
            )

            # Verify span processor was added
            self.assertEqual(mock_provider_class.return_value.add_span_processor.call_count, 1)
            added_processor = mock_provider_class.return_value.add_span_processor.call_args[0][0]
            self.assertIsInstance(added_processor, BatchSpanProcessor)

    @pytest.mark.regression
    def test_set_up_trace_provider_with_http_otlp_exporter_without_auth(self):
        """
        Tests creation of TracerProvider with HttpOTLPSpanExporter when OTEL_EXPORTER_OTLP_ENDPOINT
        environment variable is set and the protocol is "http". Setup used to pass the gRPC-only
        `insecure` argument, which the HTTP exporter rejects with TypeError.
        """
        with (
            patch("recovery_libs.tracing.traces.TracerProvider", return_value=MagicMock()) as mock_provider_class,
            patch("recovery_libs.tracing.traces.trace"),
            patch.dict(
                "os.environ",
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http",
                    "OTEL_EXPORTER_OTLP_TOKEN": "",
                },
            ),
        ):
            set_up_trace_provider(service_name="test_service", protocol="http")

            self.assertEqual(mock_provider_class.return_value.add_span_processor.call_count, 1)
            added_processor = mock_provider_class.return_value.add_span_processor.call_args[0][0]
            self.assertIsInstance(added_processor, BatchSpanProcessor)
            self.assertIsInstance(added_processor.span_exporter, HttpOTLPSpanExporter)
            added_processor.shutdown()

    def test_set_up_trace_provider_with_invalid_protocol(self):
        """
        Tests that ValueError is raised when an invalid protocol is provided.
        """
        with self.assertRaises(ValueError):
            set_up_trace_provider(service_name="test_service", protocol="invalid_protocol")

        with (
            patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_PROTOCOL": "invalid_protocol"}),
            self.assertRaises(ValueError),
        ):
            set_up_trace_provider(service_name="test_service")

    def test_populating_headers_with_span_context(self):
        # Create a mock span with a specific span context
        trace_id = 0x1234567890ABCDEF1234567890ABCDEF
        span_id = 0x1234567890ABCDEF
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        # Call the function to populate headers
        headers = populate_headers_with_span_context(mock_span)

        # Verify that the headers contain the correct trace context
        self.assertIn("traceparent", headers)
        expected_traceparent = f"00-{trace_id:032x}-{span_id:016x}-01"
        self.assertEqual(headers["traceparent"], expected_traceparent)

    def test_inject_trace_context_uses_context_and_preserves_carrier(self):
        span_context = SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context
        context = trace.set_span_in_context(mock_span)
        carrier = {"content-type": "application/json"}

        result = inject_trace_context(carrier, context=context)

        self.assertIs(result, carrier)
        self.assertEqual(result["content-type"], "application/json")
        self.assertEqual(
            result["traceparent"],
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
        )

    def test_inject_trace_context_uses_current_span(self):
        span_context = SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        with trace.use_span(mock_span, end_on_exit=False):
            carrier = inject_trace_context()

        self.assertEqual(
            carrier["traceparent"],
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
        )

    def test_serialize_and_restore_trace_context_with_tracestate(self):
        span_context = SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState((("vendor", "value"), ("other", "one"))),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        serialized = serialize_trace_context(span=mock_span)
        restored = restore_trace_context(serialized["traceparent"], serialized.get("tracestate"))

        self.assertEqual(
            serialized,
            {
                "traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
                "tracestate": "vendor=value,other=one",
            },
        )
        self.assertIsNotNone(restored)
        self.assertTrue(trace.get_current_span(restored).get_span_context().is_remote)
        self.assertEqual(serialize_trace_context(context=restored), serialized)

    def test_serialize_trace_context_degrades_without_valid_span(self):
        self.assertEqual(serialize_trace_context(context=Context()), {})

    def test_extract_trace_context_accepts_case_insensitive_w3c_fields(self):
        context = extract_trace_context(
            {
                "TraceParent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
                "TraceState": "vendor=value",
            }
        )

        self.assertIsNotNone(context)
        self.assertEqual(serialize_trace_context(context=context)["tracestate"], "vendor=value")

    def test_extract_trace_context_degrades_for_absent_or_invalid_carrier(self):
        self.assertIsNone(extract_trace_context(None))
        self.assertIsNone(extract_trace_context({}))
        self.assertIsNone(extract_trace_context({"traceparent": "not-a-traceparent"}))

    def test_restore_trace_context_degrades_for_absent_or_invalid_fields(self):
        self.assertIsNone(restore_trace_context(None))
        self.assertIsNone(restore_trace_context(""))
        self.assertIsNone(restore_trace_context("not-a-traceparent"))
        self.assertIsNone(
            restore_trace_context(
                "00-00000000000000000000000000000000-1234567890abcdef-01",
                "vendor=value",
            )
        )

    def test_extract_trace_context_from_rabbitmq_properties_with_headers(self):
        properties = BasicProperties(headers={"traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"})

        # Call the function to extract trace context
        context = extract_trace_context_from_rabbitmq_properties(event_body=b"test_event_body", properties=properties)

        # Verify that the context is not None
        self.assertIsNotNone(context)

    def test_extract_trace_context_from_rabbitmq_properties_without_headers(self):
        properties = BasicProperties()

        # Call the function to extract trace context
        context = extract_trace_context_from_rabbitmq_properties(event_body=b"test_event_body", properties=properties)

        # Verify that the context is None
        self.assertIsNone(context)

    @patch("recovery_libs.tracing.traces.TraceContextTextMapPropagator")
    def test_extract_trace_context_from_rabbitmq_properties_with_exception(self, mock_propagator):
        mock_propagator.return_value.extract.side_effect = Exception("Test exception")
        properties = BasicProperties(headers={"traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"})

        # Call the function to extract trace context
        context = extract_trace_context_from_rabbitmq_properties(event_body=b"test_event_body", properties=properties)

        # Verify that the context is None
        self.assertIsNone(context)

    def test_extract_trace_id_from_span_with_valid_context(self):
        trace_id = 0x1234567890ABCDEF1234567890ABCDEF
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        result = extract_trace_id_from_span(mock_span)
        self.assertEqual(result, "1234567890abcdef1234567890abcdef")

    def test_extract_trace_id_from_span_with_zero_trace_id(self):
        span_context = SpanContext(
            trace_id=0,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        result = extract_trace_id_from_span(mock_span)
        self.assertIsNone(result)

    def test_extract_trace_id_from_span_omits_sampled_out_trace(self):
        # Nothing is exported for an unsampled span, so a stored ID would link to no trace.
        span_context = SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.DEFAULT),
            trace_state=TraceState(),
        )
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.return_value = span_context

        self.assertIsNone(extract_trace_id_from_span(mock_span))

    def test_extract_trace_id_from_span_with_exception(self):
        mock_span = MagicMock(spec=Span)
        mock_span.get_span_context.side_effect = Exception("Test exception")

        result = extract_trace_id_from_span(mock_span)
        self.assertIsNone(result)

    def test_extract_trace_id_from_rabbitmq_properties_with_traceparent(self):
        properties = BasicProperties(headers={"traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"})

        result = extract_trace_id_from_rabbitmq_properties(properties)
        self.assertEqual(result, "1234567890abcdef1234567890abcdef")

    def test_extract_trace_id_from_rabbitmq_properties_omits_sampled_out_trace(self):
        properties = BasicProperties(headers={"traceparent": "00-1234567890abcdef1234567890abcdef-1234567890abcdef-00"})

        self.assertIsNone(extract_trace_id_from_rabbitmq_properties(properties))

    def test_extract_trace_id_from_rabbitmq_properties_without_headers(self):
        properties = BasicProperties()

        result = extract_trace_id_from_rabbitmq_properties(properties)
        self.assertIsNone(result)

    def test_extract_trace_id_from_rabbitmq_properties_without_traceparent(self):
        properties = BasicProperties(headers={"other_header": "value"})

        result = extract_trace_id_from_rabbitmq_properties(properties)
        self.assertIsNone(result)

    def test_extract_trace_id_from_rabbitmq_properties_none_properties(self):
        result = extract_trace_id_from_rabbitmq_properties(None)
        self.assertIsNone(result)
