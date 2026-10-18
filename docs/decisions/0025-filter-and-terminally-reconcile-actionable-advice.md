# ADR 0025: Filter and Terminally Reconcile Actionable Advice

Status: accepted

Date: 2026-08-31

Amended: 2026-09-04 to remove the unimplemented producer-withdrawal field and
generic observed-target context; generation freshness, reconciliation status,
and stable reasons define active work and its terminal outcome.

Related:
[ADR 0007](0007-resolve-dag-release-through-advice-state.md) defines exact-attempt
`RELEASED` Advice. This record supersedes ADR 0007's post-convergence rule that
kept satisfied release Advice active after fresh convergence.
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md) makes
`TERMINATED` Advice durable until the exact attempt reaches a terminal state.
[ADR 0022](0022-bootstrap-sac-job-discovery-once.md) identifies attempt-aware
selection and durable Advice completion as phase-two correctness requirements.
[ADR 0026](0026-classify-advice-expiration-in-scheduler-controller.md)
supersedes this record only where it assigns generation-age filtering to Job
Manager. SAC now classifies expiration and records `ABANDONED: ADVICE_EXPIRED`;
Job Manager excludes the generation on subsequent `only_active` reads through
that terminal status.
[ADR 0027](0027-route-each-advice-generation-to-one-reconciler.md) supersedes
this record where it partitions active selection and reconciliation status by
logical controller scope. Routing now selects one authoritative reconciler per
Advice generation, whose status is global to that generation.

## Context

The Slurm Advisor Controller (SAC) reconciles scheduler-neutral Advice against
fresh Slurm state. Advice is scoped to an exact `(job_id, attempt_index)`, but
the current list contract can repeatedly return records that SAC can prove no
longer require work. SAC applies freshness, attempt, ownership, routing,
partition, mode, scheduler-state, and recommendation checks only after fetching
and decoding the records.

Creation-time filtering bounds ordinary Advice but deliberately retains old
`RELEASED` and `TERMINATED` records because controller downtime must not discard
durable scheduler intent. As retained history grows, those durable states can
again make list cost proportional to the lifetime Advice population.

Two independent facts can make Advice terminal for reconciliation:

1. **Convergence:** fresh scheduler state satisfies the desired state.
2. **Abandonment:** the desired state can no longer be reached for the target
   and generation, so retrying cannot change the outcome.

Examples of abandonment include an expired recovery recommendation, a stale
attempt beyond the recommendation's applicability window, or a successor
that is already running in a state where required constraints can no longer be
applied safely. Missing accounting data, a future target attempt, an
observe-only controller, and transient scheduler failures are not terminal.

A single `reconciled` boolean cannot represent these distinctions. It would
also lose the reason that operators need to distinguish successful recovery
from intentionally abandoned work. Command acceptance alone is insufficient:
SAC can crash after a scheduler command, and only a later fresh observation can
prove convergence.

`RELEASED` also needs an unambiguous completion boundary. Once fresh scheduler
state proves that the exact attempt is unheld, running, requeued, or final, the
release action has converged. A later operator re-hold is a new event and must
not implicitly reopen historical Advice that already converged.

## Decision

### Expose one active Advice filter

The normal Advice-consumer contract will expose one optional `only_active`
filter. When it is true, the server returns Advice that is not terminal under
Job Manager-owned lifecycle state or a persisted reconciliation status for the
requested logical controller scope at the fixed query observation time. Job
Manager owns the query contract without duplicating scheduler-state
reconciliation performed by SAC.

`only_active` applies all of these predicates before count, ordering, and
pagination:

- the record is visible and contains a valid reconciliation path;
- the current ordinary Advice generation is fresh, while freshness-exempt
  durable states are retained;
- the current generation has not converged or been abandoned for the requested
  controller scope.

`only_active` requires the stable logical controller scope because convergence
and abandonment are scoped observations. All pages must repeat the same scope,
observation time, and filters. Omitting `only_active`, or setting it to false,
preserves historical list behavior for UI, audit, and diagnosis.

The API may expose narrower diagnostic filters using concepts already present
in the system, including `created_after`, Advice state, and
`reconciliation_disposition`. Those filters are not required to reconstruct
`only_active`, and their omission must not change its semantics.

An explicit `created_after` remains a diagnostic filter on when the Advice
record was first created. It does not define the freshness of a later desired-
state generation. `only_active` evaluates its age policy against when the
current desired state last changed.

SAC will continue applying its local safety checks even when the server narrows
the response. Server filtering is an optimization and durable work-selection
contract, not authorization for a scheduler mutation.

### Require visible Advice to have a reconciliation path

Publicly visible Advice must contain a scheduler desired state or non-empty node
constraints. An issue-only record used while Advice is under construction stays
hidden until a recommendation is attached. New writes that make an empty
recommendation visible must be rejected.

`only_active` defensively excludes malformed or legacy visible records without
a reconciliation path, but the API will not expose a separate
`recommendation_only` filter. Informational issue evidence remains attached to
Advice history; if it needs an independent public lifecycle in the future, it
must use a diagnostic resource rather than an action-oriented Advice record.

### Classify attempts in the scheduler controller

SAC owns state-aware attempt applicability because it already reconciles Advice
against fresh scheduler state. Job Manager will not reproduce these rules from
persisted attempts during `only_active` queries. SAC receives unresolved
Advice, applies the same attempt checks that guard scheduler actions, and
reports `ABANDONED: STALE_ATTEMPT` when a newer observed attempt proves that the
current generation can never apply again. Later `only_active` queries exclude
that generation through its persisted terminal status.

Applicability is state-aware:

| Recommendation | Applicable scheduler attempts |
| --- | --- |
| `RESTARTED(N)` | Exact attempt `N` and successor attempt `N+1` |
| `TERMINATED(N)` | Exact attempt `N` only |
| `RELEASED(N)` | Exact attempt `N` only |
| Constraint-only Advice for `N` | Exact attempt `N` only |

The effective stale-attempt predicate is therefore:

```text
RESTARTED: abandon when latest_attempt > advice_attempt + 1
otherwise: abandon when latest_attempt > advice_attempt
```

For example, `RELEASED(4)`, `TERMINATED(4)`, and constraint-only Advice for
attempt 4 become stale when fresh scheduler state shows attempt 5: their exact
target no longer exists, and applying their intent to attempt 5 would change a
different execution. `RESTARTED(4)` remains applicable at attempt 5 because
that successor is the expected result of restarting attempt 4 and SAC may
still need to apply constraints or release it. `RESTARTED(4)` becomes stale
only when fresh state reaches attempt 6. This can happen when another actor or
retry advances the job before SAC finishes the older Advice, or when SAC
returns after an outage and observes that the scheduler has already advanced
beyond the Advice window.

An Advice target newer than the latest observed attempt is future work and is
not abandoned. Job persistence does not scan or update Advice status, Job
Manager does not run a parallel reconciliation task, and Advice list reads do
not write status. SAC's existing periodic reconciliation loop is the only
component that materializes scheduler-derived attempt outcomes.

Persisted reconciliation status keeps restart behavior bounded across SAC
process restarts. With a stable logical controller scope, a cold start receives
the unresolved working set that existed before shutdown plus new generations
created while SAC was unavailable, not the lifetime Advice population. Durable
Advice that became stale during the outage may be fetched once and classified;
subsequent polls exclude it through the status SAC writes.

This rule relies on attempt indexes being monotonic for one scheduler job
identity. If a controller can observe multiple schedulers or clusters with
overlapping job IDs, its identity and lookup must also include the stable
scheduler scope before SAC classifies attempt applicability there.

### Record terminal reconciliation dispositions

Advice reconciliation status will expose one reconciliation disposition for a
logical controller scope and Advice generation:

```text
PENDING
CONVERGED
ABANDONED
```

`CONVERGED` means fresh scheduler state satisfies the current Advice
generation. `ABANDONED` means retrying that generation cannot reach its desired
state. Both are terminal for reconciliation work, including `RELEASED`.
`PENDING` includes
work not yet attempted, in progress, waiting for a future observation, or
retrying a transient failure.

Status will include at least:

- the stable logical controller scope, not an ephemeral pod identity;
- the observed Advice generation;
- the reconciliation disposition;
- a stable machine-readable reason;
- the observation and transition timestamps;
- optional bounded diagnostic text.

Reconciliation status does not duplicate observed scheduler state. Advice,
job history, and controller telemetry provide detailed diagnostic context. If
a future workflow requires durable decision evidence, it will add narrowly
typed fields or a dedicated audit record for that demonstrated requirement.

Stable terminal reasons will include at least:

- `DESIRED_STATE_OBSERVED`;
- `ALREADY_SATISFIED`;
- `ADVICE_EXPIRED`;
- `STALE_ATTEMPT`;
- `TARGET_STATE_FINAL`;
- `SUCCESSOR_ALREADY_RUNNING`;
- `INVALID_RECOMMENDATION`; and
- `UNSUPPORTED_RECOMMENDATION`.

Implementations may expose intermediate conditions such as `APPLIED` or a last
action result, but those conditions are not terminal. A successful scheduler
command remains pending until fresh scheduler state proves convergence or a
permanent condition proves abandonment.

The following classifications are required:

| Observation | Classification |
| --- | --- |
| Restart reaches attempt `N+1`, unheld, with required constraints | `CONVERGED` |
| Exact termination target is final | `CONVERGED` |
| Exact release target is unheld, running, requeued, or final | `CONVERGED` |
| Ordinary Advice exceeds its configured maximum age | `ABANDONED: ADVICE_EXPIRED` |
| Fresh Slurm state exceeds the applicability window | `ABANDONED: STALE_ATTEMPT` |
| A live successor cannot safely receive required constraints | `ABANDONED: SUCCESSOR_ALREADY_RUNNING` |
| Accounting has not produced the target record yet | `PENDING` |
| Advice targets a future attempt | `PENDING` |
| Scheduler action fails transiently | `PENDING` |
| Controller is limited to observe mode | non-terminal observed/planned status |

Only monotonic evidence or an explicitly resettable policy decision may produce
`ABANDONED`. Ownership mismatch, environment mismatch, partition filtering,
and ignore or observe mode are controller-selection or operator-policy results;
they must not globally abandon Advice that another scope or later configuration
can reconcile.

### Separate desired-state generation and status

Advice will carry a generation that advances only when action-affecting desired
state changes for the same exact target: the recommendation, constraints, or
routing scope. Adding issue evidence or rewriting unrelated metadata must not
reopen scheduler work.

The Advice target `(job_id, attempt_index)` is immutable. Desired state for a
different attempt is a different Advice record whose generation begins at 1.
The fact that `RESTARTED(N)` remains applicable while SAC completes recovery on
successor attempt `N+1` does not retarget that Advice from attempt `N`.

Advice will also carry `desired_state_changed_at`, assigned by Job Manager when
actionable desired state is first asserted and reset whenever generation
advances. Job Manager leaves it unchanged for issue evidence and other updates
that do not affect scheduler intent. For ordinary Advice, freshness is measured
from `desired_state_changed_at`, not from the Advice record's original
`created_at` or its general-purpose `updated_at`. Freshness-exempt durable
states remain exempt regardless of generation age.

For example, generation 1 may assert `RELEASED` and later converge. If the
producer changes the same exact-attempt Advice to `RESTARTED`, Job Manager
advances it to generation 2 and resets `desired_state_changed_at`. The
generation-1 convergence remains historical,
while `only_active` treats generation 2 as newly asserted work. Adding issue
evidence without changing the desired state does not advance generation or
extend its actionable lifetime. If the producer later asserts desired state
for the successor attempt, that assertion uses a separate Advice record at
generation 1.

Status applies only when `observed_generation` equals the current generation.
A newer generation becomes pending automatically without deleting the earlier
terminal history. Status updates must use the status boundary rather than a
whole-Advice replacement so that SAC cannot overwrite producer fields.

This decision does not introduce a separate Advice-withdrawal field or
operation. Current work selection is defined by generation freshness and
per-scope terminal status. If a future producer needs to rescind unchanged
Advice before reconciliation terminates, that requires a concrete producer-
cancellation API and lifecycle decision; a passive timestamp without a writer
is not part of this contract.

The normal DAG release sequence is:

1. Once the dependencies for child attempt `N` are satisfied, Job Manager
   asserts `RELEASED(N)`.
2. SAC observes the attempt held and issues the release action.
3. A later fresh snapshot observes attempt `N` unheld or running, so SAC
   reports `CONVERGED` for that generation.
4. `only_active` excludes that generation immediately.
5. If an operator later holds the attempt for an unrelated reason, the
   historical converged release generation remains terminal and does not undo
   that hold.

If the producer intends to override a later hold, that is new release intent
and must be represented by a new generation rather than by implicitly reopening
terminal status. A re-hold observed before step 3 is still reconciled because
the release generation has not yet converged.

### Use terminal status to derive active Advice

For ordinary Advice, `only_active=true` will use this effective predicate:

```text
the current generation is fresh or freshness-exempt
and (
    no status exists for this controller scope
    or status.observed_generation < advice.generation
    or reconciliation disposition is non-terminal
)
```

Current-generation `CONVERGED` and `ABANDONED` Advice is excluded, including
`RELEASED` Advice.

The status write is at-least-once and idempotent. If a scheduler action succeeds
but status publication fails, the Advice remains work; the next tick observes
fresh Slurm state and safely reports convergence. If terminal status succeeds,
reconciliation stops for that controller scope and generation.

### Preserve history and observability

Filtering does not delete Advice. Historical APIs will expose the desired
state, current generation, per-scope reconciliation disposition, reason, and
timestamps.

Metrics will distinguish fetched work, expired Advice, SAC-classified stale-
attempt Advice, converged Advice, abandoned Advice by reason, status
publication failures, and generation reopenings. Per ADR 0026, expiration is
now SAC-classified rather than server-filtered. Reason labels must use a bounded
enumeration.

## Consequences

Regular Advice transfer and reconciliation become proportional to the unresolved
working set rather than retained Advice history. Generation-scoped freshness
bounds ordinary Advice, while persisted terminal status removes work that SAC
has observed to succeed or become impossible. Advice that becomes stale while
SAC is unavailable may be transferred once after restart before its terminal
status contracts later polls.

Operators can distinguish successful convergence from deliberate abandonment.
An expired final job no longer consumes reconciliation cycles indefinitely, but
its Advice and terminal reason remain available for audit.

The design adds a public Advice generation, desired-state change timestamp, and
status contract, persistence, status-write authorization, indexes, API Gateway
translation, and controller reporting. Because these cross component and trust
boundaries, implementation requires schema migration, generated artifact
updates, mixed-version rollout coverage, and human review of the status-write
authorization path.

Attempt filtering must preserve the `RESTARTED(N)` successor window. A blanket
`latest_attempt > advice_attempt` predicate would strand a restart between
requeue-hold and release on attempt `N+1`.

Terminal status does not replace fresh Slurm observation, local attempt checks,
mode policy, routing, partition policy, or scheduler-action acknowledgement.
It records and narrows completed work after those controls run.

## Alternatives Rejected

### Use a reconciled boolean

A boolean cannot distinguish successful convergence from permanent
abandonment, cannot explain why work stopped, and cannot safely represent
intermediate command application.

### Filter every Advice whose target is older than the latest attempt

`RESTARTED(N)` remains actionable on held successor attempt `N+1` while SAC
finishes node constraints and release. A blanket comparison would discard
required recovery work.

### Duplicate attempt applicability in Job Manager

Job Manager could compare persisted attempt indexes with each recommendation's
applicability window before returning Advice. That would reduce the first poll
after an outage, but it would reproduce scheduler reconciliation rules in both
Python and Go. Stable, persisted SAC status already prevents replay of terminal
history, so the additional policy implementation is not justified.

### Treat command success as convergence

A process crash, delayed scheduler update, or partial restart sequence can occur
after command acceptance. Fresh scheduler observation, not command return, is
the convergence boundary.

### Abandon missing, future, observe-only, or transiently failing work

Those conditions can change without a new Advice generation. Treating them as
terminal would turn temporary unavailability or operator policy into silent
work loss.

### Add a producer-withdrawal timestamp without an operation

A passive timestamp would add public state and another active-selection
predicate without a supported writer or a concrete cancellation use case. If a
producer later needs to rescind unchanged Advice, its API must define authority,
idempotence, and whether a subsequent assertion advances the generation.

### Delete terminal Advice

Deletion would conflate work selection with retention and remove evidence needed
for audit, diagnosis, and recovery history. Terminal records remain queryable
when `only_active` is omitted.

### Expose every active predicate as a required client filter

Requiring ordinary consumers to combine freshness, attempt applicability,
recommendation, and reconciliation-disposition concepts would duplicate
lifecycle policy in every client and make omissions unsafe.
`only_active` expresses the consumer intent; narrower filters remain optional
diagnostic tools.
