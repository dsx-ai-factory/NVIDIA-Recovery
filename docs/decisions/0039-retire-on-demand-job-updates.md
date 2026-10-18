# ADR 0039: Retire On-Demand Job Updates

Status: accepted

Date: 2026-09-29

## Context

Job Manager inherited a stale-job refresh request protocol for the retired
workflow controller. Its durable RabbitMQ queue has no receiver in the Advisor
architecture, so requests accumulate without improving job freshness.

The Slurm Advisor Controller already reconstructs discovery from the backend
API at startup, then uses fresh Slurm state, Advice, and its local seen-attempt
set, as defined by [ADR 0022](0022-bootstrap-sac-job-discovery-once.md) and
[ADR 0037](0037-filter-sac-bootstrap-by-aps-environment.md).

## Decision

Remove the on-demand job update protocol entirely: request publication,
response bookkeeping, request model, configuration, queue bindings, and stored
tracking fields while retaining stale-job detection.

Job Manager continues leader-owned stale scanning, warnings, its stale-job
metric, and the existing optional stale cleanup policy. A stale observation
never requests a scheduler refresh or checks broker backlog. Scheduler state
publication remains the Advisor Controller's responsibility.

## Consequences

No supported configuration can reactivate refresh requests. Existing raw
service configuration may retain unused legacy keys, but operators must remove
them and the obsolete queue binding from custom configuration; Helm values no
longer support the removed properties.

A forward migration removes the two nullable request-tracking columns without
changing job state or timestamps. Historical migrations remain intact. See the
[rollout notes](../architecture/slurm-advisor-controller.md#retired-on-demand-job-updates)
for migration ordering and existing broker queues.

This retirement does not expand SAC discovery guarantees or close the
post-bootstrap discovery gaps recorded in ADR 0022.

## Alternatives Rejected

- Disable the feature by default: leaves an unsupported protocol that old
  configuration can reactivate.
- Retain or add a RabbitMQ consumer: duplicates the Advisor Controller's
  reconciliation responsibility solely to support obsolete requests.
