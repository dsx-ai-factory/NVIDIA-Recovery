# ADR 0031: Expose Audit Trail Through the Shared API Gateway

Status: accepted

Date: 2026-09-08

> **Implementation note (2026-10-13):** SAC and watchdog support verified TLS
> for gateway calls, and the public JWT listener supports exact subject
> restrictions; see the
> [cross-cluster setup](../../deploy/helm/slurm-advisor-controller/README.md#cross-cluster-authentication-with-public-jwt).

The feature-flag requirements below are superseded by
[ADR 0036](0036-always-produce-and-expose-audit-events.md). The remaining decision
continues to apply.

The restriction of generated audit routes to ingestion is partially superseded
by [ADR 0050](0050-expose-authenticated-whole-job-audit-reads.md), which adds
authenticated reads while preserving the shared listener and storage boundaries.

## Context

Audit Trail, currently deployed as Notifications, owns audit ingestion and has
no direct ingress. SAC already uses API Gateway as its Job Manager boundary.
The gateway exposes Job Manager through a typed gRPC service, generated HTTP
routes, and a generated downstream HTTP client.

[ADR 0030](0030-route-audit-ingestion-through-internal-gateway-grpc.md)
restricted audit ingestion to the internal gRPC listener and excluded it from
HTTP and OpenAPI generation. The public ingress targets the public listener,
so that audit-specific restriction prevents the existing ingress from serving
external producers.

## Decision

- Audit ingestion follows Job Manager's facade. Register `AuditService`
  alongside `HeimdallService` on the same gRPC listeners and use the same
  listener authentication. Retire the audit-only internal registration.
- Annotate `AuditService.CreateAuditEvents` as `POST /v1/audit/events` and
  register its generated HTTP handler alongside Job Manager's handler. Include
  audit ingestion in the generated gateway OpenAPI contract.
- SAC uses an `AuditServiceClient` on its existing Job Manager connection and
  credentials. Audit events still go to Audit Trail, not Job Manager.
- API Gateway translates protobuf batches through the generated downstream
  HTTP client. Audit Trail retains ownership of validation, acceptance,
  idempotency, storage, and its default-off `AUDIT_LOG_ENABLED` flag.
- Gateway configuration uses `audit` / `HEIMDALL_AUDIT_ENDPOINT`. Existing
  Notifications Service and generated HTTP client names remain until the
  service rename is deployed.
- The gateway's existing public ingress handles audit traffic. Audit Trail
  receives no direct ingress, and no new listener or authentication mechanism
  is introduced for it.

This decision supersedes ADR 0030's audit-specific listener restriction.

## Consequences

- Job and audit producers share one service-discovery and authentication
  boundary; the gateway does not own audit persistence.
- Public HTTP ingestion uses protobuf JSON, including camelCase field names
  and decimal-string int64 microsecond timestamps, just like other generated
  gateway routes. Producer clients use the gateway's generated contract.
- Downstream HTTP 202 becomes gRPC success / gateway HTTP 200. Partial rejection
  information is preserved; producers do not retry partial success. Overload
  becomes `ResourceExhausted` / HTTP 429 and can be retried with the same
  idempotency keys. A successful submission does not promise durable storage.
- Feature-disabled ingestion remains `NotFound` / HTTP 404.
- Existing non-ingestion HTTP routes retain their service-owned contracts;
  audit POST specifically uses the generated facade.
- The current SAC client supports plaintext gRPC with projected ServiceAccount
  tokens. A deployment using TLS ingress needs matching client transport
  support, independently of service registration.

## Alternatives Rejected

### Keep audit ingestion internal-only

This leaves the existing public ingress unable to serve audit producers,
contrary to the Job Manager exposure pattern.

### Add direct ingress to Audit Trail

This adds a separate service-discovery and authentication boundary to producer
clients instead of reusing API Gateway.

### Reverse-proxy audit POST instead of generating the gateway handler

This gives audit ingestion a separate HTTP contract and routing path from the
Job Manager producer facade. The typed adapter already owns the necessary
translation, so both gateway protocols use it.
