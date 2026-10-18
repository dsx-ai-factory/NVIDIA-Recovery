# ADR 0026: Classify Advice Expiration in the Scheduler Controller

Status: accepted

Date: 2026-09-04

Related:

- [ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md)
  defines `only_active`, Advice generations, and terminal reconciliation
  status. This record supersedes ADR 0025 only where it assigns generation-age
  filtering to Job Manager.
- [ADR 0027](0027-route-each-advice-generation-to-one-reconciler.md)
  supersedes this record only where expiration status is scoped to a logical
  controller. Status is now global to one routed Advice generation.

## Context

ADR 0025 originally assigned ordinary-Advice expiration to Job Manager's
`only_active` query. The implementation added an
`active_advice_max_age_seconds` setting and an internal `active_after` storage
boundary. SAC already had its own `SLURM_ADVISOR_ADVICE_MAX_AGE` setting and
the scheduler-aware logic needed to decide whether an Advice generation had
expired.

Those parallel settings could disagree. More importantly, filtering an old
unresolved generation in Job Manager prevented SAC from observing it and
publishing the terminal `ABANDONED: ADVICE_EXPIRED` status promised by the
lifecycle contract. Such records disappeared from active reads without an
auditable reason, and remained unresolved in storage.

Job Manager still needs `desired_state_changed_at`. It owns Advice mutation and
therefore knows when action-affecting desired state advances the generation.
SAC needs that timestamp to give each newly asserted generation a fresh
applicability window without relying on the Advice record's original creation
time.

## Decision

SAC exclusively classifies generation expiration. For non-durable Advice, it
compares its configured maximum age with `desired_state_changed_at`, falling
back to `created_at` during a mixed-version rollout. `RELEASED` and
`TERMINATED` remain exempt because they carry durable exact-attempt intent.

When SAC observes an expired generation, it writes terminal reconciliation
status for its stable controller scope:

```text
disposition = ABANDONED
reason = ADVICE_EXPIRED
observed_generation = advice.generation
```

Subsequent `only_active` queries exclude that generation through the existing
current-generation terminal-status predicate.

Job Manager's `only_active` query filters only state that Job Manager can own
without scheduler policy: visibility, the presence of a supported
reconciliation path, and terminal reconciliation status for the requested
controller scope and current generation. It does not apply a wall-clock age
predicate. The Job Manager age setting, internal `active_after` argument,
page-token age boundary, and age-oriented index are removed.

An explicit `created_after` query remains an independent diagnostic and
compatibility filter. When supplied, its value remains bound into pagination
tokens. SAC does not use it for `only_active` reconciliation.

After an outage, an unresolved Advice generation that expired while SAC was
down may be transferred once. SAC then records `ADVICE_EXPIRED`, and later
polls omit it. Pagination bounds each request, while persisted terminal status
makes steady-state work proportional to unresolved Advice. This is the same
convergence model used for scheduler-derived stale-attempt classification.

## Consequences

Expiration has one policy owner and one configuration value. Every generation
that stops solely because of age receives an auditable terminal reason instead
of being silently hidden by a read predicate. A newly asserted generation uses
its own `desired_state_changed_at`, so old records can reopen with a full
freshness window.

The first reconciliation after controller downtime can fetch more work than a
Job Manager age cutoff would return. The increase is bounded to unresolved
work accumulated before or during the outage, not retained terminal history,
and SAC converges it into terminal status for later polls.

Job Manager remains scheduler-neutral and does not need to match SAC's
configuration. SAC remains responsible for policy that depends on scheduler
semantics, while Job Manager remains responsible for Advice generation and
status-backed selection.

## Alternatives Rejected

### Keep matching expiration settings in Job Manager and SAC

Matching defaults do not prevent configuration drift. Even when the values
match, server-side filtering prevents SAC from writing `ADVICE_EXPIRED`, so
the status model would not explain why reconciliation stopped.

### Have Job Manager write abandonment during reads

Reads must not mutate lifecycle state, and Job Manager does not own the
controller scope or scheduler policy required to assert a reconciliation
outcome.

### Add a Job Manager expiration worker

A background worker would duplicate SAC's policy and configuration while
introducing another reconciler. SAC already has the periodic loop, controller
identity, and status-write path needed to converge expired generations.

### Remove `desired_state_changed_at`

Using only `created_at` would cause a new generation on an old Advice record to
expire immediately. Using general-purpose `updated_at` would let unrelated
metadata or issue updates extend actionable lifetime. The dedicated timestamp
preserves generation-scoped freshness without moving expiration into Job
Manager.
