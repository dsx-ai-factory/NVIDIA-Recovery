# Attribution Adapter HTTP API

The Adapter provides `POST /v1/attribution` and `GET /healthz` on port 8000.
The [public models](../../recovery_libs/recovery_libs/schema/attribution_interface/request.py)
are owned by NVIDIA Recovery. The checked-in OpenAPI input is exported from
`attribution_adapter.main:app.openapi()` without starting a server, constructing
Settings, importing the private FACT client, or calling a provider.

## Optional provider boundary

The public installation disables attribution in ADI and omits the Adapter and
FACT workloads. Disabled attribution returns an empty normal result to ADI;
it does not claim a diagnosis. Importable models and a successful `/healthz`
response establish only the public boundary. Starting the Adapter workload explicitly initializes its provider. Missing
dependencies or configuration fail startup visibly; they do not become a healthy
provider-off deployment.

The [optional deployment guide](optional-attribution.md) requires separately
authorized private provider source. The private FACT protocol and generated
client are implementation details, not another public API.

## Request and phases

Send JSON with `schema_version: "0.1.0"`, a `workload_id`, `scope`,
`failure_context`, `timeout_ms`, `workload_context`, and `phase`. Use UTC timestamps
with offsets. `workload_context.workflow_id` is a nonempty canonical string of
at most 256 characters, and `attempt_index` identifies the attempt.

```json
{
  "schema_version": "0.1.0",
  "workload_id": "12345",
  "scope": {"nodes": ["worker-01"], "ranks": []},
  "failure_context": {
    "detected_at": "2026-09-09T12:00:00Z",
    "detection_signal": "application_error"
  },
  "timeout_ms": 10000,
  "workload_context": {"workflow_id": "training-example", "attempt_index": 0},
  "phase": "fast"
}
```

Each request runs one phase synchronously. `fast` starts the provider's fast
signal pass. For `slow`, return the opaque `attribution_session_id` as
`continue_session` and supply the epilog window as `scope.start_time` and
`scope.end_time`. A missing slow window yields an empty result with
`no_attribution_available`; it does not fabricate findings. The public
walkthrough leaves slow analysis off because epilog health-check logs are not
configured. `timeout_ms` is a contract field, not a guarantee that the Adapter
cancels all backend work by that deadline; set a caller timeout and qualify
provider timeout behavior separately.

## Results and errors

Responses carry `schema_version`, service identity/version, `request_id`, an
opaque session ID, `attributions`, `unattributed_targets`, and optional `error`.
A finding targets either a `Node` or `AllJobResources` and includes an
`error_category`, `observed_at`, and available evidence. Do not infer node
isolation from an empty list, a missing confidence value, or an informational
error. Recovery policy belongs to the caller and downstream policy processing.

The public error enum contains `timeout`, `insufficient_telemetry`,
`service_error`, and `no_attribution_available`. These are response data, not
HTTP status codes. Provider processing failures can return HTTP 200 with
`error.type: "service_error"`; callers must inspect the body. Invalid request
shapes produce HTTP 422. Provider construction failures can surface as HTTP
500 before processing starts. `/healthz` returns `{"status":"ok"}` without
qualifying provider availability. Do not advertise it as a provider readiness
check.

## Caller and network requirements

The Adapter implements no inbound authentication or TLS. Limit access to trusted
service callers with network policy and an authenticated TLS or mTLS boundary
where needed. Do not expose its port or metrics endpoint publicly. Requests and
responses can carry user IDs, node names and log excerpts; restrict access
accordingly. W3C `traceparent`/`tracestate` headers carry tracing context only.
They are not credentials.

Schema version negotiation is not implemented. Deploy matching ADI/Adapter
contract versions. Changes to required fields, enums, resource semantics or
phase/session behavior require public contract review and regenerated OpenAPI.
A schema export validates the public shape, not licensed-provider results.
