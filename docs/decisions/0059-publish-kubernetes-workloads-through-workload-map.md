# ADR 0059: Publish Kubernetes Workloads Through Workload-Map

Status: proposed

Date: 2026-11-17

## Context

Direct Kubernetes recovery needs workload identity, state, attempts, and Pod/node
associations in the backend. The inherited adapter discovers PyTorchJobs through
an intermediate HeimdallJob CRD. Extending that pattern would duplicate discovery
and couple publication to another Kubernetes resource lifecycle.

Workload-Map supplies common workload properties and Pod-selection building
blocks. Its normalized API has no attempt history or timestamped execution
boundaries; those require native status and Pod lifecycle evidence. The
[job API](../../api-gateway/protos/heimdall/v1/heimdall.proto) carries scheduler
metadata, but its write request lacks execution timestamps. Job Manager's
[lifecycle projection](../../job_manager/job_manager/job_lifecycle_times.py)
currently handles only Slurm metadata.

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

Publish common identity, attempt index, state, and allocated nodes through the
job contract. Preserve workload group/version/kind, namespace/name, UID,
Pod identities and placements, observed termination reasons, and lifecycle
evidence in Kubernetes scheduler metadata. Publish selected metadata rather
than copying complete specifications, labels, annotations, or container
environments. Extend Job Manager's compatibility boundary to validate and
project Kubernetes execution timestamps into persisted attempt fields; keep
the gateway's metadata transport opaque, following
[ADR 0028](0028-project-slurm-job-fields-from-scheduler-metadata.md).

Reconcile current state after startup and watch interruption, retry publication
idempotently, and retain confirmed attempt history in the backend. Publish
separately tracked attempts and boundaries based on qualified native status
and Pod lifecycle evidence. Recover persisted boundaries
after controller restart, preserve unknown intervals explicitly, and avoid
inventing missed attempts or treating disappearance as successful completion.
Keep the canonical attempt UUID defined by
[`generate_heimdall_job_id`](../../recovery_libs/recovery_libs/schema/job_manager/job_identity.py).

Acceptance still requires resolving the required `workflow_id` for standalone
workloads. Admission, logs, and discovery must agree on `job_id`. Restart-count
tracking is separately scoped: a worker rollout or workload restart advances
the attempt to fence telemetry and Advice; Workload-Map alone cannot supply it.

## Consequences

Discovery gains a common workload boundary and uses the existing production
ingress, consistent with
[ADR 0055](0055-retire-job-status-event-consumer.md). Kubernetes history survives
controller restarts without HeimdallJob resources, at the cost of lifecycle
projection and attempt-reconciliation work. Complete history cannot be promised
when workload or Pod evidence disappears before publication, including during
outages. Reporting observed reasons does not establish root-cause attribution
or prove training progress.

## Alternatives Rejected

- **Port HeimdallJob and its per-kind watchers:** offers an existing publication
  buffer but retains duplicate lifecycle state and CRD-specific discovery.
- **Derive everything from Pods:** avoids workload definitions but cannot
  reliably distinguish whole-job attempts, enrollment, or terminal state.
- **Write backend storage directly:** bypasses the supported API and couples
  the controller to database schema and credentials.
- **Name-derived identity or Kubernetes UID:** names merge unrelated
  recreations; Kubernetes UID is unavailable for template labeling during
  workload CREATE admission.
