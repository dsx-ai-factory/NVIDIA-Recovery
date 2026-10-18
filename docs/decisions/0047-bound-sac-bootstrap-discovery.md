# ADR 0047: Bound SAC Bootstrap Discovery

Status: accepted

Date: 2026-10-06

## Context

[ADR 0022](0022-bootstrap-sac-job-discovery-once.md) bootstraps SAC once from
`GetJobs(reconciliation_eligible_jobs=true)`. That query excludes ordinary
completed history but includes all failure-like terminal attempts, regardless
of age or Advice completion. Startup transfer and targeted accounting therefore
grow with retained failure history. Bounded accounting concurrency limits
simultaneous requests, not total work.

Job Manager already supports `completed_jobs=false` and an end-time lower
bound, applied in SQL before count and pagination. SAC already discovers owned
active Advice targets independently under
[ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md) and its
successors. Eager requeue, retained for compatibility under
[ADR 0045](0045-require-advice-before-slurm-requeue.md), also needs recent
failure candidates without Advice after restart.

## Decision

Expose Job Manager's optional `completed_jobs` and inclusive `end_time_from`
filters through the gateway. Omission preserves existing list behavior;
explicit false must reach Job Manager. Completion, explicit state, and enabled
reconciliation-eligibility filters are mutually exclusive.

SAC bootstraps the union of:

- non-final attempts from `GetJobs(completed_jobs=false)`, without an age limit;
- owned unresolved Advice targets from `ListAdvice(only_active=true)`, without
  a bootstrap age limit;
- when eager requeue is enabled, attempts from
  `GetJobs(reconciliation_eligible_jobs=true, end_time_from=cutoff)`.

The eager failure cutoff is the snapshot observation time minus a configurable
positive lookback, defaulting to 30 minutes. Keep it fixed across all pages of
that read. Separate queries keep the end-time predicate off active attempts;
both paginated job reads must succeed before bootstrap completes. Any
non-final attempts with end times that appear in both reads are deduplicated
by the existing snapshot attempt keys. Missing end times do not match the
bounded failure read; do not substitute last-update or submission time.

This supersedes ADR 0022's unbounded failure-inclusive bootstrap selection.
Preserve its startup-only lifecycle, observation/publication retries, and
fresh scheduler authority, plus
[ADR 0037](0037-filter-sac-bootstrap-by-aps-environment.md)'s admission rules.
Eager handling of fresh Slurm and seen attempts remains unchanged. Finished
failures older than the cutoff are no longer rediscovered solely from job
history; unresolved Advice remains an independent discovery source.

## Consequences

Startup payload and accounting candidates depend on backend non-final attempts,
unresolved Advice, and recent failures rather than lifetime terminal history.
Backend attempts whose final state was missed during downtime remain eligible
because their persisted state is non-final. This does not bound unresolved
accounting retries or close the between-polls discovery gap.

An eager-recovery attempt can age out of bootstrap during controller downtime.
Operators can increase the lookback to retain a larger window at the cost of
more startup accounting. The cutoff is not a retry deadline for work already
seen, and does not change Advice expiration or scheduler-action safety checks.

Update all gateway replicas before SAC; roll back SAC before the gateway.
Older gateways may ignore the new protobuf fields and return unbounded history.
Existing callers and the legacy eligibility filter retain their behavior.
No database migration is needed; both predicates already execute in storage
before pagination. This change does not claim a measured SQL latency bound.
See the [architecture guide](../architecture/slurm-advisor-controller.md) for
configuration and rollout details.

## Alternatives Rejected

- **Only active attempts and unresolved Advice in eager mode.** Removes
  restart-time eager recovery of recent persisted failures without Advice.
- **One age filter across all work.** Can hide long-running active attempts or
  durable unresolved Advice. Only the additional eager failure read is bounded.
- **Filter historical jobs inside SAC.** Still transfers and decodes lifetime
  failure history before discarding it.
- **Use Advice alone.** Misses backend-active attempts that disappeared from
  Slurm while SAC was down and still require final publication.
