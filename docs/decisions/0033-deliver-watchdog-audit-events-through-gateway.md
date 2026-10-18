# ADR 0033: Deliver Watchdog Audit Events Through API Gateway

Status: accepted

Date: 2026-09-08

> **Implementation note (2026-10-13):** SAC and watchdog support verified TLS
> for gateway calls, and the public JWT listener supports exact subject
> restrictions; see the
> [cross-cluster setup](../../deploy/helm/slurm-advisor-controller/README.md#cross-cluster-authentication-with-public-jwt).

The feature-flag requirements below are superseded by
[ADR 0036](0036-always-produce-and-expose-audit-events.md). The remaining decision
continues to apply.

## Context

[ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md)
puts audit ingestion behind the shared API Gateway contract. SAC can reuse its
Job Manager connection, but the standalone watchdog has no such connection.
Its earlier HTTP audit destination bypassed the gateway. A blanket dependency
check also excluded gRPC and gateway bindings from the entire watchdog binary.

The instructing human approved moving watchdog audit delivery to gateway gRPC,
retaining logging as an option, and revising that dependency rule.

## Decision

Both Go producers select `logging` or `gateway`; logging remains the default,
and an empty destination retains that fallback. Remove their HTTP destination.
Keep delivery implementations in `internal/audit/destinations`.

SAC shares its existing Job Manager connection and credentials. The watchdog
opens an audit-only connection only when auditing is enabled and gateway delivery
is selected. It uses the generated `AuditServiceClient` and the same projected
ServiceAccount credential implementation. Helm may inherit the controller's
gateway address and Kubernetes authentication settings; standalone configuration
supplies the watchdog gateway address and optional token path explicitly.

Audit delivery remains asynchronous and best effort. A gateway outage can drop
audit events after bounded retries; it does not gate scheduler decisions. Drain
the audit buffer before closing the connection on orderly shutdown.

Narrow the dependency guard to preserve the actual scheduling boundary: the
watchdog scheduler core must not depend on gateway, Advisor, or Job Manager
clients. Audit transport at process startup may depend on generated gateway
bindings, gRPC, and shared credentials. This qualifies
[ADR 0015](0015-scope-slurm-watchdog-by-aps-environment-pattern.md)'s statement that
the watchdog is independent of NVIDIA Recovery: its release eligibility and actions
remain based on current Slurm state, with no job or Advice RPCs.

## Consequences

The optional audit transport adds no scheduler policy, workload action, or
synchronous remote prerequisite to a watchdog tick. Logging mode requires no
audit connection. Gateway mode uses the existing gateway listener authentication;
plaintext gRPC retains the existing deployment transport limitations.

Deployment tests must verify endpoint and token projection for watchdog gateway
mode. [PR #87](https://github.com/NVIDIA/NVIDIA-Recovery/pull/87)'s E2E values must select gateway for both Go producers explicitly,
and persisted-event assertions must cover SAC and watchdog producers so logging
cannot silently satisfy the delivery checks.

Human review of credential wiring and the revised dependency check is required
before merge. Scheduler-action policy remains governed by the existing ADRs.
