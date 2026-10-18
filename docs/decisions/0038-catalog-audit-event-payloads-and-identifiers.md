# ADR 0038: Catalog Audit Event Payloads and Identifiers

Status: accepted

Implementation note: [ADR 0048](0048-start-recovery-traces-in-backend-and-share-observations.md) adds optional `recovery_sequence_id`, `trace_id`, and `span_id` to the Advice and attribution payloads below; historical payloads remain readable.

Date: 2026-09-09

Related:
[ADR 0017](0017-persist-best-effort-workload-audit-events-in-notifications.md)
records the original proposal for ownership, persistence, and best-effort
delivery of workload audit events; this accepted decision adopts that boundary.
[ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md)
defines the shared API Gateway ingestion path.
[ADR 0033](0033-deliver-watchdog-audit-events-through-gateway.md)
defines watchdog delivery through that path.

## Context

ADR 0017 proposed the audit-trail architecture but intentionally left payload
schemas release-coupled. This decision adopts the persistence and delivery
boundary described below and defines the release's event vocabulary. Watcher,
Anomaly Detection Isolator (ADI), Job Manager, Slurm Advisor Controller (SAC),
and the standalone Slurm watchdog now emit typed events. Without a catalog,
readers have to inspect several Python and Go packages to discover that
vocabulary.

Job lifecycle events carry an explicit, privacy-reviewed projection of the
public Job representation. They do not carry complete storage records. Advice
lifecycle events carry complete public Advice snapshots, including the Advice
scheduler metadata, Issues, attribution, and recovery context. Advice events
also record creation and change reasons, including DAG causes that do not
originate from anomaly detection.

Two UUID-shaped values serve different purposes. Audit Trail creates an
internal UUIDv7 for each accepted row. Producers supply a UUIDv4 idempotency
key and reuse it across delivery retries. Treating either value as the other
would make retry behavior, ordering, and API ownership ambiguous.

## Decision

Notification Service owns ingestion, persistence, retention, and
workload-centric queries for audit events in a dedicated append-only table.
Producers deliver events asynchronously through in-memory buffers; delivery is
best effort and must not block recovery or existing service operations. A
process crash or buffer overflow can permanently lose events, so the audit
trail is not complete scheduler, security, or compliance evidence. Producers
reuse one idempotency key across retries of a logical event, and Notification
Service enforces uniqueness for accepted keys.

Payloads remain implicitly versioned with the NVIDIA Recovery release. Event type
names and payload field names are machine-readable contract values. Adding,
removing, or changing a field requires coordinated producer, consumer,
documentation, and E2E updates.

The release-owned source schemas are authoritative for event payload fields,
nested model expansion, and enum values:

- Job and Advice HTTP models are defined in
  [`recovery_libs.schema.job_manager.models`](../../recovery_libs/recovery_libs/schema/job_manager/models.py).
- Job Manager event payloads and the Job audit projection are defined by
  [`job_manager.audit.events`](../../job_manager/job_manager/audit/events.py) and
  [`job_manager.audit.job_changes`](../../job_manager/job_manager/audit/job_changes.py).
- Watcher anomaly payloads are defined in
  [`watcher.models.audit`](../../watcher/watcher/models/audit.py).
- ADI attribution audit payloads are defined in
  [`anomaly_detection_isolator.models.audit`](../../anomaly_detection_isolator/anomaly_detection_isolator/models/audit.py).
- SAC and watchdog payloads are defined in
  [`slurm-advisor-controller/internal/audit`](../../slurm-advisor-controller/internal/audit/event.go).
- `ApsSlurmJobSpec` is defined in
  [`recovery_libs.schema.aps_spec.spec`](../../recovery_libs/recovery_libs/schema/aps_spec/spec.py).
- Attribution request and response models are defined under
  [`recovery_libs.schema.attribution_interface`](../../recovery_libs/recovery_libs/schema/attribution_interface/).
- API Gateway recovery payloads are defined in
  [`heimdall.proto`](../../api-gateway/protos/heimdall/v1/heimdall.proto).

The [E2E audit assertions](../../tests/e2e_harness/audit.py) query the
Notifications API for scenario-significant events and check Job and Advice
snapshot shapes. They allow additional events and do not prove that every
cataloged event type was emitted; the producer schemas above remain the
source of truth for the complete catalog.

## Envelope

Every producer submits a `ProducerEvent` with these fields:

- `heimdall_job_id` (`string`): query key for the exact Job attempt.
- `event_producer` (`string`): component that emitted the event.
- `event_type` (`string`): stable identifier selecting the payload schema.
- `event_level` (`recovery | operational`): event classification.
- `payload` (`object`): event-specific typed payload.
- `event_produced_at` (`int64`): producer emission time as Unix microseconds.
- `idempotency_key` (`UUIDv4 string`): producer-owned retry identity.

The Notifications `/v1/audit/events` query response uses snake_case field
names and numeric `int64` values. It omits `idempotency_key` and the internal
row ID. Pagination exposes the row ID only indirectly through opaque cursors.

For every event, envelope `heimdall_job_id` is the required correlation key.
Payload identifiers such as `advice_id`, `request_uuid`, `anomaly_uuid`, and
optional `correlation_uuid` values correlate narrower lifecycles.

## Snapshot contracts

### `JobAuditSnapshot`

Job Manager builds this snapshot from the outgoing public Job representation
and an explicit allowlist. The fields are:

- Identity: `job_id`, `heimdall_job_id`, and `job_attempt_index`.
- State: `state` and `state_reason`.
- Lifecycle time: `submit_time`, `start_time`, and `end_time`.
- Placement: `cluster`, `partition`, `reservation`, `namespace`, and
  `worker_cluster_name`.
- Resources: `node_count`, `total_gpu_count`, `total_cpu_count`, and
  `total_memory_mb`.
- Nodes: `allocated_node_list`, `inclusion_node_list`,
  `exclusion_node_list`, `failed_node_list`, and
  `sticky_exclusion_node_list`.
- Recovery configuration: `aps_spec` and
  `config_validation_failure_types`.

The snapshot intentionally excludes user-entered labels, `workflow_id`,
`job_group_id`, job names, raw `anomalies`, Job `scheduler_metadata`, scheduler
user/command/dependency fields, log paths, checkpoints, storage timestamps,
on-demand bookkeeping, attribution bookkeeping, retry references, and all
other internal storage fields. Adding a field to storage or the public Job
response does not add it to audit without an explicit allowlist change.

`job_attempt_updated` compares only this approved projection. A write that
changes only excluded fields emits no generic Job update event. A later
scheduler observation can still emit an update when an approved field such as
`state_reason`, `submit_time`, or `end_time` changes.

### `AdviceAuditSnapshot`

Advice creation and update events use the complete public `Advice`
representation, excluding only the storage-managed `updated_at` field:

- `id`, `visible`, `job_id`, and `restart_count`.
- `scheduler_metadata`.
- `advice`, containing `state` and `node_constraints`.
- `generation`, `desired_state_changed_at`, and `reconciliation_status`.
- `issues`, `fact_attribution`, and `recovery_context`.
- `created_at`.

The presence of `scheduler_metadata` in an Advice snapshot does not reintroduce
it into Job audit snapshots. Advice recovery consumers still require the
public scheduler context, while the Job operational stream uses the narrower
allowlist above.

## Event type catalog

Each event type has one payload schema. Field lists below describe the
top-level payload; linked source schemas remain authoritative for nested
objects.

### Operational events

| Event type | Producer | Payload fields |
| --- | --- | --- |
| `anomaly_detected` | `watcher` | `job_id`, `attempt_index`, `anomaly_type`, `anomaly_uuid`, `workflow_step`, `event_occurred_at` |
| `attribution_request_forwarded` | `anomaly-detection-isolator` | `advice_id`, `job_id`, `attempt_index`, `request_payload`, `request_uuid` |
| `attribution_response_received` | `anomaly-detection-isolator` | `advice_id`, `job_id`, `attempt_index`, `request_uuid`, `response_payload` |
| `job_attempt_observed` | `job-manager` | `job_id`, `attempt_index`, `job` (`JobAuditSnapshot`) |
| `job_attempt_updated` | `job-manager` | `job_id`, `attempt_index`, `old_job`, `new_job` (`JobAuditSnapshot`) |

`job_attempt_observed` is emitted for the first persisted observation of an
attempt. `job_attempt_updated` is emitted only when the approved Job projection
changes. Consumers must not infer the absence of an underlying storage write
from the absence of an update event.

Watcher reports `event_occurred_at` as Unix microseconds from the source event.
This can precede `event_produced_at` when log ingestion or detection is delayed.
The audit stream continues to order and paginate by producer time; consumers
that present a workload timeline may use source occurrence time and fall back
to producer time for historical events.

### Job Manager recovery events

| Event type | Payload fields |
| --- | --- |
| `advice_created` | `advice_id`, `job_id`, `attempt_index`, `advice`, `creation_reasons`, `correlation_uuid` |
| `advice_updated` | `advice_id`, `job_id`, `attempt_index`, `old_advice`, `new_advice`, `change_reasons`, `correlation_uuid` |
| `advice_unpublished` | `advice_id`, `job_id`, `attempt_index`, `reason`, `correlation_uuid` |

These events use producer `job-manager` and level `recovery`. Creation reasons
include persisted Issue types and caller-known causes. DAG processing uses
`DAG_DEPENDENCIES_SATISFIED` for release and `DAG_PARENT_TERMINATED` for
descendant termination. When neither source supplies a creation reason, Job
Manager falls back to the Advice state, or `unspecified` if it has no state.
`change_reasons` can be empty when a complete Advice snapshot changes without
a new Issue or caller-supplied reason. Checkpoint-only Issue changes do not
emit `advice_updated`; an update with other changes still carries complete old
and new snapshots, including any checkpoint Issues.

### Slurm Advisor Controller recovery events

| Event type | Payload fields |
| --- | --- |
| `advice_scheduler_action_completed` | `advice_id`, `job_id`, `attempt_index`, optional `correlation_uuid`, `action`, `success`, optional `error` |
| `advice_application_applied` | `advice_id`, `job_id`, `attempt_index`, optional `correlation_uuid` |
| `advice_application_evaluated` | `advice_id`, `job_id`, `attempt_index`, optional `correlation_uuid`, `outcome`, `planned_actions` |
| `advice_application_failed` | `advice_id`, `job_id`, `attempt_index`, optional `correlation_uuid`, optional `error` |
| `advice_reconciliation_converged` | `advice_id`, `job_id`, `attempt_index`, `generation`, `reason` |
| `advice_reconciliation_abandoned` | `advice_id`, `job_id`, `attempt_index`, `generation`, `reason` |
| `job_auto_requeue_hold_completed` | `job_id`, `attempt_index`, optional `correlation_uuid`, `success`, optional `error` |

These events use producer `slurm-advisor-controller` and level `recovery`.
Scheduler action names include `slurm.requeue_hold`,
`slurm.update_constraints`, `slurm.release`, and `slurm.cancel`.

### Watchdog recovery event

`watchdog_release_completed` uses producer `slurm-watchdog`, level `recovery`,
and payload fields `job_id`, `attempt_index`, `success`, and optional `error`.

## Internal event UUID and producer idempotency key

| Property | Internal event UUID | Idempotency key |
| --- | --- | --- |
| Stored field | `AuditEvents.id` | `AuditEvents.idempotency_key` |
| Owner | Audit Trail | Event producer |
| Required form | UUIDv7 | UUIDv4 |
| Generated | When Audit Trail constructs an accepted row | Before the first delivery attempt |
| Retry behavior | New accepted logical event receives a new ID | Retries of one logical event reuse the same key |
| Purpose | Row identity, pagination, deterministic tie-breaking | Suppress duplicate persistence after delivery retries |
| Ordering | Roughly time-sortable | Random; conveys no ordering |
| Query visibility | Not returned directly | Not returned |

Audit queries order by `event_produced_at` and use the internal UUIDv7 as a
tie-breaker. `event_produced_at` records producer time and `recorded_at` records
acceptance time. Consumers use timestamps and opaque cursors rather than UUID
ordering.

Every producer generates an idempotency key before its first delivery attempt.
Audit Trail treats the value as opaque, places a unique constraint on it, and
preserves the first accepted event for a duplicate key. A producer generates a
new UUIDv4 for each distinct event and retains it for retries of that event.

## Consequences

- Users and developers have one release-specific inventory of emitted audit
  event types and payloads.
- Approved Job and complete Advice snapshots make lifecycle transitions
  inspectable without reconstructing mutable database state.
- Job audit payload growth and private-surface exposure are controlled by an
  explicit allowlist.
- Advice creation and change reasons expose DAG as well as anomaly causality.
- Internal row identity remains owned by Audit Trail, while producers own retry
  identity.
- Payload evolution remains release-coupled. Producer schemas define the full
  catalog; scenario E2E assertions cover the events and causal facts their
  workflows exercise.

## Alternatives Rejected

### Persist complete Job storage records

Complete records contain raw anomalies, scheduler metadata, user-entered
labels, log paths, and internal bookkeeping that are not required to understand
the public Job lifecycle. Persisting them would expand the private-surface and
payload-size risk and would silently grow as storage evolves.

### Infer changed fields at query time

Query-time reconstruction would depend on mutable current state, retention,
and neighboring events being complete. Persisting old and new approved Job
projections and complete Advice snapshots keeps each emitted transition
independently inspectable despite best-effort delivery.

### Treat the idempotency key as the event ID

The producer must know and reuse an idempotency key before Audit Trail accepts
the event, while the row ID belongs to Audit Trail. Combining them would leak
producer retry policy into storage identity and would not safely represent
distinct accepted events.

### Define a generic untyped payload for every event

An untyped JSON object would move schema discovery into dashboards and E2E
tests. Producer-specific payload types preserve ownership and make incompatible
changes visible.

### Give payloads independent schema-version fields

ADR 0017 versions payloads with the release. Independent versions would add
migration and compatibility machinery that the current best-effort product
does not require.
