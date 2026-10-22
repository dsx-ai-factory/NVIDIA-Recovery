# NVIDIA Recovery Shared Tracing Library

This directory contains a shared tracing library that can be easily integrated in any python services to emit
OpenTelemetry traces. It supports both OTLP span exporter with both GRPC and HTTP protocols as well as the basic
ConsoleSpanExporter.

## Basic Usage

```python
from recovery_libs.tracing.traces import set_up_trace_provider

set_up_trace_provider(service_name="<your-service-name>")
```

Make sure the `set_up_trace_provider` is called at the entry point of your service so that all tracers created in
different modules have the same configuration.

Then in business logic, you can add module level tracer and use it for instrumentation.

When used as a function decorator:
```python
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

tracer = trace.get_tracer(__name__)


@tracer.start_as_current_span(name="<your-span-name>")
def target_function():
    ...
    span = trace.get_current_span()
    span.set_attribute("<your-attribute-name>", "<your-attribute-value>")
    span.set_status(StatusCode.OK)
    ...
```

When used as a context manager:

```python
some_value = 1

with tracer.start_as_current_span(
    name="<your-span-name>",
    attributes={
        "attribute_1": "value_1",
        "attribute_2": some_value,
    },
) as my_span:
    # Normaly business logic
    span.set_status(StatusCode.OK)
```

For full list of parameters, please refer to OpenTelemetry's [official documentations](https://opentelemetry.io/docs/concepts/signals/traces/).

## Configuration

The behaviors of the current version of the tracing library can be controlled either thorough function parameters or
environment variables.

The span exporter, processor and the portocol can all be passed in explicitly as function parameters. If you need custom
implementation of them, they can be initialized and configured like this.

Alternatively, they can also be configured through a few environment variables, namely:
* `OTEL_EXPORTER_OTLP_PROTOCOL`: The protocol used when exporting the spans: `grpc`, `http/protobuf`, or its alias
`http` (case insensitive).
* `OTEL_EXPORTER_OTLP_ENDPOINT`: The collector base URL shared by traces and logs. gRPC uses it as-is; HTTP sends spans
to its `/v1/traces` path and logs to its `/v1/logs` path, keeping any base path.
* `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`: Optional complete trace URL, used verbatim instead of the shared endpoint.
* `OTEL_EXPORTER_OTLP_TOKEN`: The bearer token used to authenticate to the endpoint.

When no endpoint is configured, no exporter is installed and spans are never sampled. Trace context still propagates,
but it is marked unsampled so services do not record trace IDs for traces that are never exported. The span processor
defaults to `BatchSpanProcessor`.

## Deployment Setup

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an operator-managed OpenTelemetry
collector. When the endpoint requires authentication, supply the bearer token
at runtime through `OTEL_EXPORTER_OTLP_TOKEN`; the repository does not provide
a default backend or credential.
