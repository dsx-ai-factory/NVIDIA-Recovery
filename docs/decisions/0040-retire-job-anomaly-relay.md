# ADR 0040: Retire the Job Anomaly Relay

Status: accepted

Date: 2026-09-29

## Context

Job Manager inherited an anomaly relay to the retired ARM service. Watcher
anomalies, immediate state-based END/CRASH events, and periodic failed-job
CRASH events could still publish to that unconsumed RabbitMQ queue. Job Manager
already sends attribution requests directly to ADI after persisting anomalies.

## Decision

Retire every JM-to-ARM anomaly publication and the dedicated publisher,
exchange/queue configuration, relay delay, enablement flag, and relay counter.
ARM is no longer part of the architecture.

Preserve anomaly generation and persistence, trace identity, Advice issue
creation, and direct ADI emission with its existing eligibility, feature gates,
and atomic claim. Running, completing, and final attempts remain eligible for
ADI when they carry a processable anomaly. Retain Watcher ingestion and its
missing-job redelivery path.

Persist terminal cancellation/requeue-limit Advice independently of relay
settings. This preserves Job Manager's lifecycle ownership under
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md). Do not mark a
Watcher anomaly processed merely to record relay delivery. The direct ADI
emitter's existing storage claim remains the delivery deduplication mechanism.

## Consequences

State-based END/CRASH anomalies remain available without a legacy broker
publication. Metrics and dashboards describe generated anomalies instead of
published ARM events; the relay metric is removed. Existing custom overrides
must remove the retired publisher settings, and operators must retire existing
broker queues after stopping old producers. See the
[operations notes](../architecture/slurm-advisor-controller.md#retired-job-anomaly-relay).

This change does not alter ADI's existing at-most-once emission semantics or
add retries for failed attribution publication. No additional database
migration is needed for anomaly relay retirement.

## Alternatives Rejected

- Keep a disabled relay: leaves obsolete configuration capable of reactivating
  an unconsumed stream, while generated events still need independent cleanup.
- Route raw relayed events to ADI: ADI already consumes the dedicated
  `AttributionRequest` contract after anomaly persistence.
