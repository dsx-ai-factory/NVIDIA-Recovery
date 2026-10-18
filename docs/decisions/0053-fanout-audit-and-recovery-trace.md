# ADR 0053: Fan out Recovery Trace and Audit

Status: superseded

Date: 2026-08-20

Discussion: [PR #39](https://github.com/NVIDIA/NVIDIA-Recovery/pull/39)

Originally proposed as ADR 0018; renumbered because that number is retired.
Superseded by [ADR 0048](0048-start-recovery-traces-in-backend-and-share-observations.md).
Maintainer confirmed this disposition on 2026-10-16: retain this record as
historical rationale; ADR 0048 carries the fanout principle forward through
shared operation observations and independent trace, audit, and milestone delivery.

Related:

- Extends [ADR 0013](0013-correlate-recovery-with-durable-trace-context.md)
  which adds a recovery sequence model for e2e tracing.
- Extends [ADR 0017](0017-persist-best-effort-workload-audit-events-in-notifications.md)
  which introduces the audit event architecture.

## Context

In ADR 0013, a new first-class recovery sequence model is created to enable
end-to-end tracing. This allows for end-to-end measurements of recovery
latencies, with each scheduler action recorded as a recovery milestone.

In ADR 0017, a new audit event architecture is introduced. This introduces a
mechanism to propagate and send audit events in a separate table in the
Notifications Service.

A core architectural question is if the recovery sequence model and transport
can be used for driving audit events, or if they are separate.

## Decision: Fan out Where Events Overlap

The recovery sequence model captures the information needed for end-to-end
recovery latency tracing. It currently does not take into account cases where
the Slurm Advisor Controller (SAC) skips applying a piece of advice.

Furthermore, the audit event service intends to also keep track of operational
event records for things like Watcher detecting an anomaly, and requests to ADI
and attribution service. These audit events serve a different purpose than
the recovery events.

We fan out to both spans and audit events where a recovery operation is also
auditable.

## Consequences

### Implement Helper Function to Fan Out

There are still a lot of overlap in handling recovery sequences. We introduce
a helper function so that where we record an event, the function either creates
a span (for recovery tracing), or an audit event, or both. This allows
developers to not have to be concerned with having to be concerned with having
to record both, where that is necessary.

Any failure in the fanout should not block the scheduler work.

### Emitting milestone spans and audit events leads to two outgoing calls

There will be two outgoing calls to both the Job Manager, and the Notifications
Service, when there are overlaps. This is a cost that we are accepting, and it
is further mitigated by components maintaining an asynchronous FIFO with a
batching background worker to smooth out bursts of events, in the case of audit
events.

## Alternatives Rejected

### Use `RecoverySequence` to Derive Audit Events

Rejected because the audit trail feature covers more than the recovery
sequence. Forcing the `RecoverySequence` data model to adopt more audit event
violates its scope and design goals.
