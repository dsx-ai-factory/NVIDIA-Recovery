# API Gateway HTTP and gRPC

The Gateway exposes the scheduler-neutral `heimdall.v1.HeimdallService` and
audit ingestion and reads through `heimdall.v1.AuditService`. The authoritative
RPC and message definitions are
[`heimdall.proto`](../../api-gateway/protos/heimdall/v1/heimdall.proto) and
[`audit.proto`](../../api-gateway/protos/heimdall/v1/audit.proto).
The HTTP reference is generated from those definitions with the repository's
pinned Buf plugins, and Fern reads that checked-in Swagger file directly.
No separate handwritten HTTP schema is maintained.

## Transports and callers

| Listener default | Intended caller | Authentication |
| --- | --- | --- |
| HTTP `:8080`, gRPC `:9090` | External clients and dashboard | Configured OIDC or JWT; disabled only with explicit insecure mode |
| HTTP `:8081`, gRPC `:9091` | Controller and trusted in-cluster callers | Optional Kubernetes service-account TokenReview authentication; otherwise private network isolation is required |

These are process ports. The Helm internal Service can map a different service
port onto the internal listener. Use the rendered Service address and ports,
not an assumed process port. `HEIMDALL_ENABLE_EXTERNAL_ENDPOINTS=false` disables
the external listeners. `HEIMDALL_URL_PREFIX` and
`HEIMDALL_INTERNAL_URL_PREFIX` configure the respective HTTP mount prefixes.

The process listeners use plaintext transport. Operate them behind a reviewed
TLS or mTLS termination boundary; expose neither an unauthenticated listener nor
an internal Service directly to the Internet. `HEIMDALL_INSECURE=true` is only
for an isolated evaluation. The walkthrough uses private networking and local
port forwarding.

## Authentication

Send `Authorization: Bearer <access-token>` for HTTP and `authorization` metadata
with the same bearer value for gRPC. Browser OIDC login uses the configured
session flow. At least one enabled authenticator must accept the request.
JWT validation requires the configured issuer, audience, signature and `exp`;
restart after JWKS key rotation because keys are loaded at startup.

Configure the issuer, audience and claim mappings using the
[Gateway configuration reference](../../api-gateway/README.md#authentication).
The internal Kubernetes authentication mode requires the configured audience
and service-account identity. API-key management endpoints remain disabled
compatibility stubs: they return internal errors and do not issue usable keys.
They are present in the generated HTTP reference because they remain on the wire.
Do not treat them as an authentication option.

## Methods

All RPCs are unary. HTTP uses protobuf JSON field names (for example,
`pageToken` and `onlyActive`); protobuf source uses `page_token` and `only_active`.

| RPC | HTTP mapping | Behavior |
| --- | --- | --- |
| `GetJobs` | `GET /v1/jobs` | Paginated workload listing |
| `GetJob` | `GET /v1/jobs/{heimdall_job_id}` | Lookup by NVIDIA Recovery UUID |
| `GetJobsBySchedulerId` | `GET /v1/jobs/by-scheduler-id/{scheduler_job_id}` | Paginated scheduler-ID lookup; IDs may have multiple attempts |
| `CreateOrUpdateJob` | `POST /v1/jobs` | Publish workload state and scheduler metadata |
| `GetWorkflows` | `GET /v1/workflows` | List workflows |
| `GetWorkflow` | `GET /v1/workflows:lookup` or `GET /v1/workflows/{workflow_id}` | Workflow lookup; prefer the query route for IDs containing reserved characters |
| `ListAdvice` | `GET /v1/advice` | Desired state and exclusions, optionally only active advice |
| `UpdateAdviceReconciliationStatus` | `PUT /v1/advice/{heimdall_job_id}/reconciliation-status` | Idempotent observation for one advice generation; HTTP body is `status` |
| `AuditService.GetAuditEvents` | `GET /v1/audit/events` | Query persisted events by job, attempt, event filters, and opaque cursors |
| `AuditService.CreateAuditEvents` | `POST /v1/audit/events` | Submit a batch of producer audit events; the body contains `items` |

Job updates reject unsupported `state` names or numeric values with HTTP 400 /
gRPC `InvalidArgument` before contacting Job Manager. HTTP job-update bodies use
strict protobuf JSON decoding: unknown fields and unknown enum names are rejected.
Omitting `state` (or using `null` or `JOB_STATUS_UNSPECIFIED`) leaves the state
unspecified for a partial update. A nullable backend state is returned as
`JOB_STATUS_UNSPECIFIED` without interrupting the gateway.

Use response `nextPageToken` values for pagination without changing filters
mid-traversal. Job-list `pageToken` and `pageNumber` are mutually exclusive;
`limit` defaults to 100 and is capped at 250. Active-advice tokens are opaque and
bind the query filters. Set `onlyActive=true` for controller reconciliation;
historical listings remain available when that filter is omitted. The server's
active filter does not replace controller ownership, exact-attempt, expiration,
or scheduler-state checks. Status applies only to its matching generation.

## Audit ingestion

Invoke `heimdall.v1.AuditService/CreateAuditEvents` using the same gRPC connection
and credentials as the workload API, or post protobuf JSON to `/v1/audit/events`
under the configured HTTP prefix. The Gateway forwards the batch to Audit Trail
through `HEIMDALL_AUDIT_ENDPOINT`; it does not store events in Job Manager.

Each item includes `heimdallJobId`, `eventProducer`, `eventType`, `eventLevel`,
structured `payload`, `eventProducedAt` and `idempotencyKey`. In the HTTP contract,
`eventProducedAt` is Unix microseconds encoded as a decimal string. Keep each
producer-assigned idempotency key stable when retrying an overloaded request.

Successful Gateway ingestion returns HTTP 200, including partial acceptance.
Inspect `partialSuccess.rejectedEvents` and `partialSuccess.errorMessage`;
do not retry a partially accepted batch. Overload returns HTTP 429
(gRPC `ResourceExhausted`). Acceptance does not promise durable persistence.
The downstream service always exposes audit ingestion. Use the generated Gateway
reference for producer clients: the downstream service's HTTP 202 acceptance
status is not the public Gateway status.

## Audit reads

`AuditService.GetAuditEvents` and its generated `GET /v1/audit/events` route
use the existing listeners, authentication, and `HEIMDALL_AUDIT_ENDPOINT`.
Select `heimdallJobId` or `jobId` with an optional `attemptIndex`; omitting
`attemptIndex` includes all attempts of that scheduler job. Explicit attempt
zero selects attempt zero. An attempt without a job, or an internal ID combined
with scheduler selectors, returns `InvalidArgument` / HTTP 400.

Optional `eventType`, `eventLevel`, and `eventProducer` match exactly.
`minEventProducedAt` and `maxEventProducedAt` are inclusive Unix-microsecond
bounds. `sortBy` accepts `event_produced_at:asc` or `event_produced_at:desc`
(default). `limit` defaults to 100 and must be between 1 and 100. Omit job
selectors to query across jobs. The generated HTTP parser also accepts the
corresponding protobuf snake_case query names.

Responses contain `items`, each with an event and human-readable `summary`,
and `cursor.prevCursor` / `cursor.nextCursor`. Reuse a returned cursor with the
same filters and order. Public events omit the storage row ID and ingestion
idempotency key. As with ingestion, response field names are camelCase and
`eventProducedAt` is a decimal-string int64. Downstream Notifications uses its
own snake_case JSON schema; it is not the public Gateway wire contract.

Reads allow a downstream page up to 4 MiB; a larger response fails with
`ResourceExhausted` / HTTP 429 instead of returning a truncated page. Reduce
`limit` and retry. Ingestion replies retain their separate 4 KiB bound. Client
cancellation and deadlines propagate to the downstream request, whose configured
HTTP timeout is 10 seconds.

```bash
curl --fail-with-body --get "$RECOVERY_HTTP_URL/v1/audit/events" \
  --header "Authorization: Bearer $RECOVERY_ACCESS_TOKEN" \
  --data-urlencode 'jobId=12345' \
  --data-urlencode 'eventType=recovery_decided' \
  --data-urlencode 'limit=25'
```

## Read-only examples

Replace the endpoint with your TLS-terminated Gateway and obtain a token through
your configured identity provider. Include the configured URL prefix, if any.

```bash
export RECOVERY_HTTP_URL=https://recovery.example.com
curl --fail-with-body --get "$RECOVERY_HTTP_URL/v1/jobs" \
  --header "Authorization: Bearer $RECOVERY_ACCESS_TOKEN" \
  --data-urlencode 'limit=10'

curl --fail-with-body --get "$RECOVERY_HTTP_URL/v1/advice" \
  --header "Authorization: Bearer $RECOVERY_ACCESS_TOKEN" \
  --data-urlencode 'onlyActive=true'
```

Generate a gRPC client from the checked-in protobuf definitions using the
pinned Buf configuration. Invoke `heimdall.v1.HeimdallService/GetJobs` with
`{"limit":10}` and bearer metadata, or `ListAdvice` with `{"onlyActive":true}`.
Set a client deadline. Reflection is not required; use the generated descriptors.
Do not assume the HTTP URL prefix applies to gRPC method names.

Invalid inputs and upstream failures use gRPC status codes, mapped to HTTP by
the Gateway; the HTTP schema includes the `rpcStatus` error envelope. An empty
successful advice list means no matching advice, not an upstream error.

## Compatibility

The `/v1` path and `heimdall.v1` package identify the current contract family;
they do not promise stability across experimental releases. Retain protobuf
field numbers and enum meanings when adding compatible fields. Review removed
fields, changed semantics, authentication changes, and new required inputs as
public API changes. Regenerate specifications and consumer bindings together;
qualify the controller before deploying a changed recovery contract. A docs
render does not qualify transport authentication or scheduler behavior.
