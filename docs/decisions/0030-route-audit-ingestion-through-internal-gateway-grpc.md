# ADR 0030: Route Audit Ingestion Through Internal Gateway gRPC

Status: superseded

Date: 2026-09-01

Superseded by [ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md),
which exposes Audit Trail ingestion through the same shared HTTP/gRPC facade
as Job Manager and retires the audit-specific internal-only restriction.

## Context

The Slurm Advisor Controller can be installed independently from the NVIDIA Recovery
services it calls. It already uses API Gateway as its stable, authenticated
NVIDIA Recovery boundary for job publication and advice retrieval. Calling the
downstream audit service directly would give the controller a second
service-discovery, authentication, and wire-contract dependency.

API Gateway exposes scheduler-neutral protobuf contracts to scheduler adapters
and translates those requests to service-owned HTTP APIs. Job publication uses
this pattern for Job Manager. The downstream audit service already owns the
audit JSON and HTTP acceptance contract, including partial acceptance and
overload responses.

## Decision

- API Gateway owns a scheduler-neutral `AuditService` protobuf contract for
  producer batch ingestion.
- API Gateway registers `AuditService` only on its internal gRPC listener. It
  is not registered on the external gRPC or HTTP listeners.
- Producers such as the Slurm Advisor Controller use the existing internal
  API Gateway connection and authentication mechanism to submit audit events.
- API Gateway translates protobuf batches to the audit service's existing
  HTTP JSON ingestion contract and translates its acceptance and error results
  back to gRPC.
- The audit service continues to own ingestion semantics, persistence,
  idempotency enforcement, retention, and query APIs.

## Consequences

- Standalone scheduler adapters need only API Gateway service discovery and
  credentials.
- Producer code depends on a typed scheduler-neutral contract rather than the
  deployment topology or FastAPI schema of the downstream audit service.
- API Gateway carries a narrow translation adapter and must keep it compatible
  with the audit service's ingestion contract.
- Other in-cluster services may continue using the audit service's OpenAPI
  client when API Gateway is not their architectural boundary.
- The internal audit service is unavailable through the external API Gateway
  listeners by construction.

## Alternatives Rejected

### Transparently reverse-proxy the downstream HTTP route

A transparent internal HTTP proxy avoids schema translation, but leaves the
producer coupled to the audit service's JSON and HTTP semantics and requires
separate HTTP credential handling in the controller.

### Let the controller call the audit service directly

Direct access makes standalone installation depend on downstream audit service
discovery and reachability and bypasses the controller's existing authenticated
NVIDIA Recovery boundary.

### Move audit ingestion ownership into API Gateway

API Gateway is a facade, not the audit persistence owner. Moving ingestion
semantics or storage there would duplicate downstream audit service
responsibilities.
