# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from collections.abc import Mapping
from enum import StrEnum

from opentelemetry import trace
from opentelemetry.context.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter as GrpcOTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter as HttpOTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanProcessor,
)
from opentelemetry.trace import Span, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pika.spec import BasicProperties

logger = logging.getLogger(__name__)

allowed_protocols = {"grpc", "http"}
_W3C_TRACE_HEADERS = ("traceparent", "tracestate")


def set_up_trace_provider(
    service_name: str,
    span_exporter: SpanExporter | None = None,
    span_processor: SpanProcessor | None = None,
    protocol: str = "grpc",
) -> None:
    """
    Set up trace provider for a service with the given service name, span exporter, and span processor.

    Args:
        service_name (str): The name of the service that is being instrumented.
        span_exporter (Optional[SpanExporter]): The span exporter to use. When not provided, if the environment variable
            OTEL_EXPORTER_OTLP_ENDPOINT is set, it uses OTLPSpanExporter with the value of it as the endpoint,
            otherwise no exporter is configured (spans are not exported but context propagation remains active).
        span_processor (Optional[SpanProcessor]): The span processor to use. When not provided, it defaults to the
            BatchSpanProcessor.
        protolcol: The protocol to use for OTLP exporter, either "grpc" or "http". Defaults to "grpc".

    """
    if env_protocol := os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL"):
        protocol = env_protocol.lower()

    if protocol not in allowed_protocols:
        raise ValueError(f"Protocol provided '{protocol}' is invalid. Allowed protocols: {allowed_protocols}.")

    logger.info(f"Setting up trace provider for service: {service_name}")
    if span_exporter is None:
        if otlp_endpoint := os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
            logger.info(f"Initializing OTLPSpanExporter with endpoint: {otlp_endpoint} and protocol: {protocol}")

            if otlp_bearer_token_header := os.getenv("OTEL_EXPORTER_OTLP_TOKEN"):
                logger.info("Authentication method: bearer token.")
                headers = {"authorization": f"Bearer {otlp_bearer_token_header}"}
                span_exporter = (
                    GrpcOTLPSpanExporter(endpoint=otlp_endpoint, headers=headers)
                    if protocol == "grpc"
                    else HttpOTLPSpanExporter(endpoint=otlp_endpoint, headers=headers)
                )
            else:
                logger.info("Authentication method: no authentication.")
                span_exporter = (
                    GrpcOTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
                    if protocol == "grpc"
                    else HttpOTLPSpanExporter(endpoint=otlp_endpoint)
                )
        else:
            logger.warning(
                "OTEL_EXPORTER_OTLP_ENDPOINT not set. "
                "Trace context propagation is active but spans will not be exported."
            )
    else:
        logger.info("Using the custom span exporter provided.")

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if span_exporter is not None:
        if span_processor is None:
            span_processor = BatchSpanProcessor(span_exporter)
        provider.add_span_processor(span_processor)

    trace.set_tracer_provider(provider)


def inject_trace_context(
    carrier: dict[str, str] | None = None,
    *,
    context: Context | None = None,
    span: Span | None = None,
) -> dict[str, str]:
    """Inject W3C trace context into a carrier-neutral dictionary.

    When ``span`` is provided it is installed in ``context`` before injection.
    With neither argument, OpenTelemetry's current context is used. Existing
    carrier entries are preserved.
    """
    if carrier is None:
        carrier = {}

    if span is not None:
        context = trace.set_span_in_context(span, context)

    TraceContextTextMapPropagator().inject(carrier=carrier, context=context)
    return carrier


def extract_trace_context(carrier: Mapping[str, str] | None) -> Context | None:
    """Extract and validate W3C trace context from a generic dictionary.

    Missing and malformed carriers return ``None`` so tracing data can degrade
    without affecting the caller's product behavior.
    """
    if not carrier:
        return None

    try:
        normalized_carrier = {
            key.lower(): value
            for key, value in carrier.items()
            if isinstance(key, str) and key.lower() in _W3C_TRACE_HEADERS
        }
        if not isinstance(normalized_carrier.get("traceparent"), str):
            return None

        context = TraceContextTextMapPropagator().extract(normalized_carrier)
        if not trace.get_current_span(context).get_span_context().is_valid:
            return None
        return context
    except Exception as e:
        logger.warning("Failed to extract W3C trace context", exc_info=True, extra={"error": str(e)})
        return None


def serialize_trace_context(*, context: Context | None = None, span: Span | None = None) -> dict[str, str]:
    """Serialize a context or span as canonical W3C propagation fields.

    The current OpenTelemetry context is used when neither argument is given.
    An invalid or absent span context produces an empty dictionary.
    """
    carrier = inject_trace_context(context=context, span=span)
    return {header: carrier[header] for header in _W3C_TRACE_HEADERS if header in carrier}


def restore_trace_context(traceparent: str | None, tracestate: str | None = None) -> Context | None:
    """Validate persisted W3C fields and restore them as a remote context."""
    if not isinstance(traceparent, str) or not traceparent:
        return None
    if tracestate is not None and not isinstance(tracestate, str):
        return None

    carrier = {"traceparent": traceparent}
    if tracestate:
        carrier["tracestate"] = tracestate
    return extract_trace_context(carrier)


def populate_headers_with_span_context(span: Span, headers: dict[str, str] | None = None) -> dict[str, str]:
    """
    Populate the given headers dictionary with the trace context from the provided span.

    Args:
        span (Span): The span whose trace context is to be injected into the headers.
        headers (Optional[dict[str, str]]): The headers dictionary to populate with trace context. If not provided,
            a new dictionary will be created.
    """
    return inject_trace_context(headers, span=span)


def extract_trace_context_from_rabbitmq_properties(event_body: bytes, properties: BasicProperties) -> Context | None:
    """
    Extract the trace context from RabbitMQ message properties.

    Args:
        event_body (bytes): The body of the event/message, used for logging in case of missing headers.
        properties (BasicProperties): The RabbitMQ message properties containing the headers.

    Returns:
        Optional[Context]: The extracted trace context, or None if no headers are present.
    """
    try:
        if properties and properties.headers:
            return extract_trace_context(properties.headers)
        else:
            logger.warning(
                f"No trace context provided in the event properties headers. Event body: {event_body.decode()}"
            )
            return None
    except Exception as e:
        logger.exception(
            f"Error extracting trace context from RabbitMQ properties. Event body: {event_body!r}",
            extra={"error": str(e)},
        )
        return None


def extract_trace_id_from_span(span: Span) -> str | None:
    """
    Extract the trace ID as a hex string from a span's context.

    Only a sampled span's trace is exported, so an unsampled span yields None
    rather than an ID that would link to a trace that never exists.

    Args:
        span: The span whose trace ID to read.

    Returns:
        The trace ID as a 32-character lowercase hex string, or None if the span
        context is invalid, has a zero trace ID, or is not sampled.
    """
    try:
        span_context = span.get_span_context()
        if span_context and span_context.trace_id != 0 and span_context.trace_flags.sampled:
            return format(span_context.trace_id, "032x")
    except Exception as e:
        logger.warning("Failed to extract trace_id from span", exc_info=True, extra={"error": str(e)})
    return None


def extract_trace_id_from_rabbitmq_properties(properties: BasicProperties) -> str | None:
    """
    Extract the trace ID from the ``traceparent`` header in RabbitMQ message properties.

    The W3C traceparent format is: ``{version}-{trace_id}-{span_id}-{trace_flags}``.
    The producer's sampled flag decides whether that trace is exported, so an
    unsampled traceparent yields None rather than an ID with no trace behind it.

    Args:
        properties: The message properties that may carry a ``traceparent`` header.

    Returns:
        The trace ID as a 32-character lowercase hex string, or None if the
        header is missing, malformed, or not sampled.
    """
    try:
        if properties and properties.headers:
            traceparent = properties.headers.get("traceparent")
            if traceparent and isinstance(traceparent, str):
                parts = traceparent.split("-")
                if len(parts) >= 4 and int(parts[3][:2], 16) & int(TraceFlags.SAMPLED):
                    return parts[1]
    except Exception as e:
        logger.warning("Failed to extract trace_id from RabbitMQ properties", exc_info=True, extra={"error": str(e)})
    return None


class JobAnomalySpanName(StrEnum):
    """
    A class that defines the names of different spans relevant to job anomaly handling

    ATTRIBUTION: The span name for the attribution phase of job anomaly handling.
    ATTRIBUTION_ACTION: The span name for the action phase after attribution result is available.
    ATTRIBUTION_PERSISTENCE: The span name for the persistence phase of attribution results.
    DETECTION: The span name for the detection phase of job anomalies.
    REMEDIATION_ACTION: The span name for the remediation action phase of job anomaly handling when the anomaly is first
        seen, it happens before the attribution.
    PERSISTENCE: The span name for the persistence phase of job anomalies.
    """

    ATTRIBUTION = "attribution"
    ATTRIBUTION_ACTION = "attribution_action"
    ATTRIBUTION_PERSISTENCE = "attribution_persistence"
    DETECTION = "detection"
    REMEDIATION_ACTION = "remediation_action"
    PERSISTENCE = "persistence"
