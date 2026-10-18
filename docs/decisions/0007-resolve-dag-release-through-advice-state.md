# ADR 0007: Resolve DAG Release Through AdviceState

Status: accepted

Date: 2026-08-05

Related:
[ADR 0001](0001-use-repository-local-architecture-decision-records.md) defines
the repository's decision-record process, and
[ADR 0005](0005-require-causal-evidence-in-slurm-recovery-e2e-tests.md) requires
exact-attempt and action-source evidence in Slurm recovery tests.
[ADR 0009](0009-derive-watchdog-release-from-slurm-state.md) supersedes this
record's watchdog eligibility and predecessor-evidence decision. The
`ADVICE_STATE_RELEASED` decision in this record remains accepted.
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md) defines the
orphan policy that propagates a parent's final `TERMINATED` Advice to its DAG
descendants.
[ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md)
supersedes this record's post-convergence rule: fresh convergence makes an
exact release generation terminal for reconciliation and removes the separate
producer-withdrawal lifecycle. The exact-attempt and freshness-exemption
decisions here remain accepted until convergence.

## Context

DAG scheduling currently uses a legacy `JobRelease` message from Job Manager
to a workflow controller. That request-response path is separate from the
Advisor API used for restart and termination advice, even though both paths
ultimately request a Slurm scheduler action.

The supported architecture needs one level-triggered reconciliation boundary.
Job Manager owns dependency evaluation and resolves DAG, recovery, and
termination intent. The Slurm Advisor Controller owns Slurm-specific mutations.
A DAG child whose dependencies are not yet satisfied is submitted held because
a polling controller cannot safely recover the dependency boundary after an
unheld job becomes scheduler-eligible.

Two Advisor representations were considered. One adds an independent
open-or-closed scheduling gate alongside lifecycle advice. The other adds a
resolved release action to `AdviceState`. The separate gate represents the two
concerns independently, but also creates compound states such as a closed gate
with restart advice. Those states are unnecessary when Job Manager is the
single producer responsible for resolving conflicting intent before advice is
published.

The standalone watchdog creates a second release path. A dependency-blocked
DAG child must not be released by that path, so its initial attempt is submitted
with watchdog monitoring disabled. That persisted setting must not leave later
recovery attempts permanently outside watchdog protection.

## Decision

### Represent DAG release as a resolved AdviceState

The Advisor API will add `ADVICE_STATE_RELEASED` without adding a separate DAG
scheduling-gate field:

```proto
enum AdviceState {
  ADVICE_STATE_UNSPECIFIED = 0;
  ADVICE_STATE_RESTARTED = 1;
  ADVICE_STATE_TERMINATED = 2;
  ADVICE_STATE_RELEASED = 3;
}
```

`RELEASED(N)` means the exact scheduler attempt identified by the advice's
`(job_id, attempt_index=N)` should be scheduler-eligible. It does not assert
that the attempt is already running.

The Slurm Advisor Controller reconciles `RELEASED(N)` as follows:

- If the observed attempt is exactly `N`, is pending, and is held, issue
  `scontrol release` through the existing scheduler-action boundary.
- If exact attempt `N` is already unheld, running, or final, treat the advice as
  satisfied and perform no scheduler mutation.
- If the observed attempt is older than `N`, treat the advice as future and
  wait for a fresh observation. If it is newer than `N`, treat the advice as
  stale. Never copy the release action to a later attempt.
- `RELEASED` does not run `requeuehold` and does not carry or apply node
  constraints. Recovery sequencing and recovery constraints remain owned by
  `RESTARTED`.
- Observe mode reports the planned action without mutating Slurm. Action mode
  retains the existing explicit scheduler-action acknowledgement.
- A failed release is retried from fresh Slurm and Advisor state. Repeated
  reconciliation is idempotent because an unheld or running exact attempt
  already satisfies the recommendation.

Job Manager is the single intent arbiter. While dependencies are unmet, a held
DAG child has no release-bearing advice. Job Manager must not emit `RELEASED`
or `RESTARTED` for that child. Once every dependency is satisfied, Job Manager
may publish `RELEASED` for only the child's current exact attempt. A failed or
cancelled parent leaves the child held unless a separate `TERMINATED`
recommendation applies an orphan policy.

Job Manager withdraws or replaces `RELEASED` after fresh state confirms the
attempt is unheld or running when later operator holds must be preserved. Until
then, an operator re-hold remains subject to level-triggered reconciliation and
may be released again.

### Keep RELEASED level-triggered

`RESTARTED` and `TERMINATED` are event-like lifecycle decisions whose evidence
can become stale. `RELEASED` represents a continuing DAG decision: dependencies
are satisfied and exact attempt `N` should remain scheduler-eligible.

The generic lifecycle advice age limit must not silently expire `RELEASED`.
`RELEASED` remains actionable until Job Manager withdraws or replaces it, its
exact attempt satisfies it, or the observed attempt makes it stale. The initial
implementation will exempt `RELEASED` from the generic `AdviceMaxAge`. A future
issuance or renewal mechanism may replace that exemption only if it preserves
the same explicit, producer-owned active/withdrawn semantics.

Metrics and logs must distinguish expired lifecycle advice, active release
advice, satisfied release advice, stale or future attempts, and advice withdrawn
by Job Manager. If the Advisor API is unavailable, the controller performs no
advice-driven mutation and the DAG child remains safely held.

### Derive watchdog eligibility for DAG retries

Slurm attempt indexes are zero-based. The dependency-blocked initial DAG
attempt, attempt 0, remains watchdog-disabled. For a DAG job, every observed
attempt with `attempt_index >= 1` is watchdog-eligible even when its persisted
APS/HEIMDALL specification still contains `watchdog_eligible=false`.

This attempt rule controls eligibility only; it is not sufficient authorization
to release. A watchdog release candidate must also be managed, pending, held,
inside the configured partition and action policy, past the release timeout,
and have evidence that a predecessor attempt ran. That evidence may come from
the watchdog's observation or from durable Slurm accounting. A requeue by
itself increments the attempt index but does not prove that DAG dependencies
were satisfied.

DAG identity is determined from the validated job specification's non-empty
dependency definition. Attempt index alone must not override an explicit
watchdog opt-out for a non-DAG job. The effective eligibility rule is:

```text
if job is a DAG child:
    eligible = attempt_index >= 1
else:
    eligible = the existing watchdog_eligible policy
```

The watchdog must retain or reconstruct predecessor-running evidence across
process restarts. Its current in-memory arming state is not, by itself, enough
to guarantee monitoring when the watchdog restarts after a predecessor ran but
before the retry is observed held. An implementation may recover that evidence
from Slurm accounting or another production read interface; it must not infer
dependency satisfaction from `attempt_index >= 1` alone.

The controller does not rewrite `watchdog_eligible` before applying
`RELEASED`, and a comment-update failure does not block DAG release. The legacy
`JobRelease` spec-update behavior is removed with the legacy message path.

The following producer and submission invariants are required:

- every dependency-blocked DAG child is submitted held with its initial attempt
  watchdog-disabled
- Job Manager emits `RELEASED` only after every dependency is satisfied
- Job Manager never emits `RESTARTED` while dependencies are unmet
- all advice is scoped to an exact job attempt and is never carried forward
- non-DAG jobs continue to honor their explicit `watchdog_eligible` setting
- only the scheduler adapter mutates Slurm, under its existing action-mode
  safety controls

After production-interface tests prove parity, the DAG RabbitMQ publisher,
response consumer, timeout republisher, and release-time spec update will be
removed.

## Consequences

The Advisor schema and controller state machine remain smaller than the
independent-gate design. Every reachable DAG action is represented by one
resolved state, and the Slurm Advisor Controller does not need to define
precedence across lifecycle advice and a second gate field.

Safety depends more strongly on Job Manager's producer contract. Invalid states
such as a dependency-blocked child with `RESTARTED` advice are prevented by the
producer rather than blocked by an independent closed gate. Tests must cover
those negative producer invariants as well as the controller's exact-attempt
checks.

`ADVICE_STATE_RELEASED` is a public schema addition that must be propagated
through generated clients, Job Manager persistence and serialization, API
Gateway translation, and scheduler-adapter reconciliation. Implementations and
tests must not confuse a release recommendation with an observed running state.

The level-triggered freshness exception can cause an active recommendation to
undo a later operator hold. Job Manager retirement and observable advice status
are therefore part of the behavior, not optional cleanup.

DAG attempt 0 stays protected from watchdog fail-open while dependencies are
unmet. After the job has run and advances to attempt 1 or later, watchdog
protection no longer depends on mutating a persisted comment. Durable
predecessor evidence adds implementation work but preserves this behavior
across watchdog restarts without treating an arbitrary requeue as proof that
dependencies were satisfied.

Production-interface tests must prove the Advice record, exact attempt,
controller action or skip reason, watchdog decision, predecessor evidence, and
resulting Slurm state. Action-mode release, restart, and cancellation tests
retain the required human scheduler-action review.

## Alternatives Rejected

### Add a separate scheduling-gate field

An independent `CLOSED` or `OPEN` gate models dependency permission separately
from lifecycle recovery and allows the controller to detect conflicting
combinations. It also expands the public API and creates a compound
reconciliation state. Job Manager already owns both inputs and can publish one
resolved action, so the additional state dimension is unnecessary.

### Keep the legacy JobRelease message path

The request-response message, response consumer, and timeout republisher create
a second scheduler-action protocol alongside the Advisor API. Retaining them
would preserve split ownership and event-like retry behavior instead of using
the supported level-triggered controller boundary.

### Rewrite watchdog_eligible before every DAG release

Updating the persisted job comment before release couples dependency progress
to a second scheduler mutation and makes comment-update failure block an
otherwise valid release. Attempt-aware watchdog eligibility protects DAG
retries without changing the initial dependency guard.

### Treat attempt_index >= 1 as sufficient release evidence

A manual or automated requeue can increment the attempt index without proving
that the child ran or that its dependencies were satisfied. The attempt rule is
an eligibility override only; predecessor-running evidence and the normal
watchdog release conditions remain required.

### Enable watchdog monitoring for every DAG attempt

Enabling the watchdog for attempt 0 could release a held child before its
dependencies are satisfied. Eligibility begins at attempt 1 and still requires
evidence of a running predecessor.

### Apply the DAG retry override to every job

An explicit watchdog opt-out on a non-DAG job is an operator policy choice.
Attempt count alone is not sufficient to override that choice; the override is
limited to jobs with a validated dependency definition.

### Expire RELEASED with lifecycle advice

A short generic age limit can strand a correctly held DAG child after API lag
or a controller outage even though its dependencies remain satisfied. Release
advice remains active until explicit producer withdrawal or exact-attempt
satisfaction makes it unnecessary.
