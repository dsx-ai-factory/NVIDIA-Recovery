# ADR 0059: Publish Kubernetes Workloads Through Workload-Map

Status: proposed

Date: 2026-11-17

> **Implementation note (2026-11-20):** The [initial publisher](../../kubernetes-job-controller/main.go)
> uses `job_id` as the standalone workflow ID and records observations before
> native retry tracking and execution boundary extraction land. The
> [controller](../../kubernetes-job-controller/discovery.go) preserves node
> evidence during teardown, tracks terminal and unenrolled attempts through
> UID-verified closure, and schedules an exact-attempt read-back after ambiguous
> open writes, while Job Manager fixes each workload UID's lookup keys.

## Context

Direct Kubernetes recovery needs workload identity, state, attempts, and node
membership in the backend. The inherited adapter watches PyTorchJobs and stages
their metadata in HeimdallJob resources. Extending that pattern would duplicate discovery
and couple publication to another Kubernetes resource lifecycle.

Workload-Map supplies common workload properties and Pod-selection building
blocks. Its normalized API has no attempt history or timestamped execution
boundaries; those require native status and Pod lifecycle evidence. The
[job API](../../api-gateway/protos/heimdall/v1/heimdall.proto) carries scheduler
metadata for scheduler-specific execution timestamps.

## Decision

Introduce a Kubernetes job-controller that discovers enrolled workloads through
Workload-Map and publishes directly through the existing API Gateway job API,
with Job Manager owning persistence. Replace the PyTorchJob-specific discovery
and HeimdallJob publication dependency for this path. Start with JobSet and
PyTorchJob; qualification must establish supported versions and behavior.

The controller reads workload and Pod state without adopting, patching, or
finalizing workloads. Admission instrumentation and recovery actions remain
separate components. Observe enrolled zero-restart workloads too. Operate on
one cluster per Recovery instance.

Admission generates a fresh UUID as `job_id` on ordinary workload creation,
persists it on the workload, and copies it into worker Pod templates. Preserve
it across updates and execution attempts; ordinary same-name recreation gets a
new UUID even when the submitted manifest carries the old value. Discovery
consumes this stamped identity and never generates or patches it. Admission
labeling and telemetry propagation are separate implementation dependencies.
Retain the Kubernetes-assigned workload UID as concrete object metadata.
This ADR fixes identity, node membership, and lifecycle evidence; advancing the
attempt index on restart is a separate dependency.

Publish common identity, attempt index, state, and allocated nodes through the
job contract. Maintain node membership from owned Pod events, publish it in
deterministic order, and treat it as membership, not global rank mapping.
Keep scheduler metadata at workload and attempt granularity, including
Kubernetes identity and observed workload termination reasons; Pod snapshots
stay out of job rows. Extend Job Manager's
compatibility boundary to validate and project Kubernetes execution timestamps
into persisted attempt fields; the gateway's metadata transport stays opaque,
following [ADR 0028](0028-project-slurm-job-fields-from-scheduler-metadata.md).

**Restart inventory.** Job Manager's open attempts are the durable inventory of
what the controller has published; there is no HeimdallJob, finalizer, recurring
backend scan, or direct database access. Once at startup, before starting its
watches, the controller reads the open Kubernetes attempts for its namespace
through the paginated job API, retrying with backoff, and seeds the
controller-runtime queue with them, including each attempt's recorded `job_id`
and workload UID. Workloads whose enrollment label is gone from the filtered
cache therefore remain reconcilable after a restart, and a same-name replacement
can be matched to the old attempt by UID. Workload-Map status and incremental
UID-based Pod placement drive reconciliation from there. Controller startup
depends on Job Manager being readable, as in
[ADR 0022](0022-bootstrap-sac-job-discovery-once.md). This also depends on Job
Manager's stale-job cleanup continuing to exclude these records.

**Lifecycle evidence.** Only exact UID evidence may declare a same-name
replacement or scheduler-side deletion. A cache miss is verified with a live,
name-filtered list. Unenrollment, temporary API errors, and an unavailable kind
leave attempts open. Live node removals update an open attempt. Terminal
cleanup keeps the last known node evidence.

**Terminal acknowledgment.** After verifying loss of a workload UID, upsert
`DELETED` for each tracked attempt or replay its observed terminal outcome.
An attempt can get a terminal row even if its first open write never committed.
Leave an unobserved end time unset. Job Manager's row lock and terminal-state
guard protect the outcome from late open writes.

Clear tracking only when the upsert response or a targeted read after an
ambiguous reply confirms a terminal record for the same attempt and UID.
An open or missing row still needs the terminal upsert. Controller-runtime
retries transient failures. Report permanent rejections or UID conflicts
without acknowledging closure or automatically retrying them.

**Deferred.** Native retry inference and closure of superseded attempts on a
surviving workload require more execution evidence. Until then those attempts
can remain open and increase the startup inventory.

Reconcile current state after startup and watch interruption, retry publication
idempotently, and retain confirmed attempt history in the backend. Publish
separately tracked attempts and boundaries from qualified native status and Pod
lifecycle evidence, preserve unknown intervals explicitly, and avoid inventing
missed attempts or treating disappearance as successful completion. Keep the
canonical attempt UUID defined by
[`generate_heimdall_job_id`](../../recovery_libs/recovery_libs/schema/job_manager/job_identity.py).

**Advice boundary.** The controller makes no Advice decisions. Until global
rank mapping is qualified, Job Manager withholds rank-sensitive FACT attribution
for directly published Kubernetes rows. It does so where the observation is
emitted, before the at-most-once claim, so the anomaly is preserved and later
enablement can still act; general Advice behavior is unchanged. Operators must
run one active publisher per namespace; the controller does not enforce it.

Acceptance still requires resolving the required `workflow_id` for standalone
workloads. Restart-count tracking, which advances the attempt to fence
telemetry and Advice, is separately scoped; Workload-Map alone cannot supply it.

## Consequences

Discovery gains a common workload boundary and uses the existing production
ingress, consistent with
[ADR 0055](0055-retire-job-status-event-consumer.md). Kubernetes history survives
controller restarts without HeimdallJob resources, at the cost of lifecycle
projection, attempt reconciliation, and a startup read of Job Manager. Complete
history cannot be promised when workload or Pod evidence disappears before
publication, including during outages; attempts whose end is unprovable stay
open rather than receive a guessed outcome. Pending closure is in memory.
If the controller crashes before any backend row exists, startup has no attempt
to recover. Two open observations can still commit out of order. An observed
reason alone cannot establish root cause or prove training progress. Operators
provide RBAC and run one publisher per namespace.

## Alternatives Rejected

- **Port HeimdallJob and its per-kind watchers:** offers an existing publication
  buffer but retains duplicate lifecycle state and CRD-specific discovery.
- **Recurring backend scan:** adds a second reconciliation loop. The startup
  API read seeds the existing queue.
- **Close only rows the backend already holds:** a grace-period read cannot tell
  a write that never committed from a slow one, and drops the history of a
  workload that existed.
- **Derive everything from Pods:** avoids workload definitions but cannot
  reliably distinguish whole-job attempts, enrollment, or terminal state.
- **Write backend storage directly:** bypasses the supported API and couples
  the controller to database schema and credentials.
- **Name-derived identity or Kubernetes UID:** names merge unrelated
  recreations; Kubernetes UID is unavailable for template labeling during
  workload CREATE admission.
