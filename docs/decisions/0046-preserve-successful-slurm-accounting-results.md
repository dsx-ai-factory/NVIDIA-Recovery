# ADR 0046: Preserve Successful Slurm Accounting Results

Status: accepted

Date: 2026-10-06

Partially supersedes
[ADR 0020](0020-bound-slurm-controller-publication-and-accounting-concurrency.md):
its all-or-nothing accounting publication rule. Bounded concurrency, request
boundaries, deterministic ordering, and serialized ticks remain in force.

## Context

Targeted accounting already runs one `sacct` request per unique job ID through
a bounded worker pool. However, one failed command or response discards all
successful results and prevents publication of fresh active observations too.
A persistent failure therefore blocks unrelated job-state updates and causes
already successful completed-job lookups to repeat on subsequent ticks.

The requested behavior is to retain successful query work and tolerate
individual failures. Scheduler decisions need stronger evidence than job-state
publication: a missing newer attempt can make an older attempt appear current.

## Decision

Return records from successful accounting requests alongside joined errors
from failed requests. A response that fails decoding or conversion contributes
no records, so a partially parsed attempt history is never trusted. Preserve
exact-attempt filtering and stable record and error ordering.

The controller admits and publishes fresh successful accounting and active
observations even when accounting is incomplete. Normal publication rules
apply: completed attempts leave the seen set after successful publication,
unresolved attempts remain retryable, and, when eager recovery is enabled,
failed attempts pending recovery stay retained until their recovery decision
completes. With eager recovery disabled, Advice remains the discovery source
for subsequent recovery under [ADR 0045](0045-require-advice-before-slurm-requeue.md). Publication or cleanup
failures must not hide the original accounting error.

The accounting source, polling stage, and overall tick report failure when
accounting fails. A phase-local accounting deadline allows already completed
results to be published; cancellation of the overall tick stops further work.
Queued and in-flight accounting work remains bounded and joined before return.

Failed-job recovery and Advice reconciliation remain blocked for that tick
when any required accounting lookup fails. This change does not authorize
scheduler mutations or terminal Advice decisions from an incomplete snapshot.

## Consequences

An isolated accounting failure no longer stalls all monitoring updates.
Completed attempts published successfully do not need to be queried again
solely because another request failed. Errors remain observable and failed
lookups retry through existing discovery and seen-state mechanisms.

A persistent accounting failure still delays recovery and Advice actions.
Relaxing that constraint would require explicit per-job completeness tracking
and a separate review of scheduler-action safety.

## Alternatives Rejected

- Discard all results: wastes successful work and stalls unrelated publication.
- Suppress errors: hides incomplete observations and dependency failures.
- Run all actions on partial results: missing attempts can invalidate recovery
  or Advice decisions.
- Increase concurrency or combine IDs in one command: concurrency already
  exists, and neither change fixes the all-or-nothing result handling.
