# ADR 0017: Persist Best-Effort Audit Events in Notification Service

Status: proposed

Date: 2026-08-14

> **Implementation note (2026-09-23):** Core audit delivery and persistence exist, but configurable retention remains unresolved: no expiration worker or setting was found in [Notifications](../../notifications/notifications) or its [chart](../../deploy/helm/notifications) at `47a8fdc9`.

Related:
[ADR 0008](0008-baseline-oss-database-schemas.md) assigns ownership of the
existing Notifications tables and their schema migrations to Notification
Service.
[ADR 0009](0009-derive-watchdog-release-from-slurm-state.md) defines the
standalone watchdog release path that must be represented in the audit trail.
[ADR 0038](0038-catalog-audit-event-payloads-and-identifiers.md) supplements
this proposal by adopting the persistence and delivery boundary and defining
the release-coupled event, payload, and identifier contract.

[ADR 0048](0048-start-recovery-traces-in-backend-and-share-observations.md)
defines shared operation observations with recovery tracing while preserving
the independent audit persistence and delivery boundary.

## Context

Customers and NVIDIA Recovery developers need a workload-centric record of what
NVIDIA Recovery observed, decided, attempted, or skipped without reconstructing that
history from several service logs.

Recovery activity spans Watcher, Job Manager, ADI, the Advisor Controller, and
the watchdog. The resulting record describes NVIDIA Recovery's knowledge and behavior;
it is not a complete scheduler history, a request log, or an RBAC or security
audit log.

Audit logging must not block or alter recovery. The first implementation
therefore prioritizes isolation from the recovery path over complete or durable
event delivery.

The existing Notifications table represents user-visible messages rather than
structured audit events. Creating a separate service would add operational
complexity and complicate a future design in which audit records drive those
messages.

## Decision

- Notification Service owns ingestion, persistence, retention, and
  workload-centric queries for the audit trail.
- Audit events are stored in a dedicated table, while the existing
  Notifications table retains its current meaning.
- Audit events are append-only until they expire under a configurable retention
  policy.
- Audit events are emitted only for tracked workloads that already have a
  canonical `heimdall_job_id`. That exact job-attempt ID is the required
  cross-component correlation and workload query key; scheduler-specific job
  and attempt identifiers remain event-specific payload data.
- Producers include `payload.job_id` and `payload.attempt_index` whenever
  those identifiers are available, using the same keys across event types.
  Attempt index `0` is a known first attempt, not a placeholder for an
  unknown attempt.
- Event payloads may carry narrower identifiers such as an Advice ID,
  attribution request UUID, anomaly UUID, or `RecoverySequence` sequence ID
  when their own lifecycle needs correlation. A separate correlation UUID is
  not required on every recovery event.
- Producers compute an idempotency key for each event and reuse it when
  retrying delivery. Notification Service stores the supplied key under a
  uniqueness constraint but does not compute it.
- Event payloads are not independently versioned; their schemas are implicitly
  versioned with the NVIDIA Recovery release.
- Producers deliver events asynchronously through in-memory buffers. Delivery
  is best effort and never blocks recovery or existing service operations.
- A process crash or buffer overflow can permanently lose events, so the audit
  trail provides no completeness guarantee.

## Consequences

- Users can query what NVIDIA Recovery knew and did for a workload without searching
  individual service logs.
- Notification Service gains a distinct workload-history responsibility while
  retaining the existing notification data model.
- Recovery availability and latency do not depend on audit ingestion.
- Audit history can contain permanent gaps and cannot serve as complete
  scheduler, security, or compliance evidence.
- Producer-computed idempotency keys prevent retries of a received event from
  creating duplicate rows.
- Notifications may later be derived from audit records, but that change is
  outside this decision.

## Alternatives Rejected

### Reuse the existing Notifications table

Rendered recipient messages do not preserve producer identity, structured
operation payloads, recovery correlation, skips, retries, or separate
production and persistence timestamps. Adding those semantics to the existing
table would couple notification delivery state to audit retention and queries.

### Create a dedicated audit service

A new service is conceptually clearer, but it adds operational setup and
complicates a future design in which audit events drive user-visible
notifications. Notification Service is therefore the audit consumer for the
first implementation.

### Derive audit events from exported spans

Span-derived audit would make a user-visible product depend on optional
instrumentation, sampling, exporter configuration, Collector availability,
attribute limits, and trace retention. It would also require a custom
span-to-audit projector and a stable public contract for telemetry attributes.
Audit events and spans may share source data without making exported spans the
audit source of truth.

### Use RecoveryContext or Advice as the audit history

RecoveryContext carries recovery and trace correlation, while Advice
represents desired state. Neither records the append-only sequence of
evaluations, attempts, skips, failures, or retries. Storing audit history in
either would couple controller activity to Advice lifetime and mutability.

### Deliver audit events synchronously

Waiting for Notification Service would make an auxiliary history product part
of the recovery availability and latency path. A Notification Service outage
could then delay scheduler actions or cause an operation to be repeated for
reporting rather than recovery reasons.

### Add a durable producer outbox or scheduler-native stash initially

A write-ahead log, transactional outbox, broker, or scheduler-native stash can
improve delivery completeness but adds persistent producer state, recovery
logic, cleanup, and operational failure modes. The initial requirement accepts
event loss and does not justify that complexity.

### Compute idempotency keys in Notification Service

The producer owns the event-specific idempotency material. Computing keys in
Notification Service would require the consumer to understand every producer's
payload schema and evolve with those schemas. Producers therefore compute and
submit the key, while Notification Service only enforces uniqueness.
