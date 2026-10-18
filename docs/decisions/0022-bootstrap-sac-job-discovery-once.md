# ADR 0022: Bootstrap SAC Job Discovery Once

Status: accepted

Date: 2026-08-28

> **Implementation note (2026-10-06):** [ADR 0043](0043-bound-failed-job-requeue-retries.md)
> supersedes unlimited failed-job action retries with bounded local retries and
> process-local give-up; active COMPLETING attempts still receive final accounting.
> [ADR 0047](0047-bound-sac-bootstrap-discovery.md) replaces the
> bootstrap query with non-final attempts, active Advice, and a cutoff for eager-recovery failures.

The requirement below to retain every returned bootstrap key is superseded by
[ADR 0037](0037-filter-sac-bootstrap-by-aps-environment.md), which applies stored
APS environment matching before retaining backend candidates.

[ADR 0045](0045-require-advice-before-slurm-requeue.md) supersedes the direct
failed-job recovery and associated recovery-decision retention rules below;
bootstrap, observation/publication retries, final accounting, and Advice
discovery remain in force.

Related:
[ADR 0010](0010-resolve-squeue-final-state-during-completing.md) defines how
SAC selects a final state while a Slurm job is still completing.
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md) preserves the
Job Manager ownership of lifecycle intent and the SAC ownership of Slurm
mutation.
[ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md) defines
active Advice filtering and durable completion. Those concerns are relevant to
phase two of this decision but are not changed here. ADR 0020,
"Bound SAC Publication and Slurm Accounting Concurrency," separately proposes
bounded concurrency for SAC publication and Slurm accounting.

## Context

The Slurm Advisor Controller (SAC) reconciles three different kinds of work:

- it observes active Slurm jobs through one cluster-wide `squeue` snapshot;
- it observes attempts that disappeared from `squeue` through targeted `sacct`
  queries and publishes their final state to Job Manager (JM);
- it reads JM Advice and translates scheduler-neutral intent into Slurm
  operations such as `requeuehold`, `release`, and `scancel`.

SAC previously called JM `GetJobs` during every reconciliation tick. Every job
returned by JM that was absent from `squeue` became a candidate for targeted
`sacct`. An unfiltered query therefore made API pagination, payload size, and
Slurm accounting work grow with all retained job history rather than with the
current cluster workload.

The JM `GetJobs` API accepts `reconciliation_eligible_jobs=true` to perform this
selection at the database. The filtered contract returns non-final jobs and
failure-like terminal jobs that may still require SAC's direct recovery path.
It excludes historical states that do not require broad scheduler discovery,
including `COMPLETED`, `CANCELLED`, `STOPPED`, `SUSPENDED`, and `REQUEUED`.
It also excludes `NODE_FAIL` and `PREEMPTED`: after JM has persisted those
states, Slurm performs their automatic requeue without an NVIDIA Recovery scheduler
mutation.

Filtering reduces each response, but a filtered full read on every tick still
repeats backend pagination and accounting discovery. SAC already has enough
state to follow normal transitions after startup:

1. the current `squeue` snapshot contains active attempts;
2. the existing process-local seen-job store contains attempts observed in an
   earlier tick;
3. `ListAdvice` independently contains exact attempts requiring scheduler
   action.

When an attempt seen in the prior tick disappears from `squeue`, its key can
seed targeted `sacct` without reading the same job from JM again. JM job data
is not used as a cache of Slurm state; only the exact `(job_id, attempt_index)`
identity is needed for discovery.

The startup case is different. A new SAC process has no seen-attempt state. It
needs a bounded JM read to recover relevant attempts that were already known
before SAC started, especially failure-like terminal attempts that may still
need direct recovery.

## Decision

### Use filtered GetJobs only as a startup bootstrap

Each SAC controller process starts with JM job discovery incomplete. Until it
completes, the base snapshot reads these sources:

- current jobs from `squeue`;
- `GetJobs(reconciliation_eligible_jobs=true)` from JM;
- current Advice from JM;
- the process-local seen-job store.

SAC records every valid `(job_id, attempt_index)` returned by `GetJobs` in the
seen-job store. Bootstrap becomes complete only after the full paginated JM
read and every base-snapshot source succeeds and all returned keys have been
recorded. Marking a key seen is idempotent, so a partial store write is safe to
repeat. If a source or store operation fails, SAC fails that tick and retries
the complete bootstrap on the next tick.

After bootstrap completes, SAC does not call `GetJobs` again during the life of
that controller process. Every regular tick continues to read `squeue`,
`ListAdvice`, and the seen-job store. A SAC restart creates a new controller,
loses the process-local seen set, and performs the filtered bootstrap again.

The implementation assumes `SyncOnce` calls for one controller are serialized,
as they are in the production polling loop. The bootstrap-complete marker and
seen-job store are not a multi-replica coordination mechanism.

### Keep fresh Slurm state authoritative

The startup response contributes job-attempt keys, not cached scheduler state.
SAC still obtains current active state from `squeue` and final or historical
state from targeted `sacct`.

During regular ticks:

1. every active, admitted attempt is marked seen and published to JM;
2. a seen or Advice attempt absent from `squeue` is queried through targeted
   `sacct`;
3. if `sacct` has no record yet, the seen key is retained for a later tick;
4. after SAC publishes an ordinary final record, it removes the seen key;
5. current Advice is reconciled independently and therefore remains available
   for states excluded from the JM job bootstrap.

SAC does not use prior JM job state to override a fresh Slurm observation.

### Retain failure work until its recovery decision completes

Failure-like terminal attempts are different from ordinary final records
because SAC can recover them directly without Advice. Removing a failed key
immediately after publishing its state would lose recovery work if
`requeuehold` then failed and no per-tick `GetJobs` call existed to rediscover
the attempt.

SAC therefore retains a failed accounting attempt in the seen-job store until
one of these outcomes is reached:

- policy rejects recovery, such as a non-requeue mode or exhausted limit;
- observe mode records the planned action without mutating Slurm;
- action mode successfully applies `requeuehold`.

A transient scheduler error leaves the attempt seen so the next tick retries
the decision using a fresh `sacct` record. After successful mutation SAC marks
the exact attempt processed before removing it from the seen store. If cleanup
fails, that in-process marker prevents the scheduler mutation from being
repeated while a later tick retries cleanup.

An active failure in the Slurm `COMPLETING` window remains seen even after its
recovery decision so its eventual disappearance from `squeue` still causes the
final accounting read defined by ADR 0010.

### Preserve Advice-driven handling for excluded states

`ListAdvice` remains a per-tick JM read. It is not replaced by the startup
bootstrap.

`COMPLETED` remains excluded from general discovery. JM owns the policy inputs
for completion recovery, including `requeue_on_completion`, recovery mode,
requeue limits, checkpoint stopping rules, and attempt history. When recovery
is approved, exact-attempt `RESTARTED` Advice causes SAC to query `sacct` and
run the existing requeue-and-release path. Without that Advice, a historical
completed attempt is not recovered.

`REQUEUED` also remains excluded from general discovery. Exact-attempt
`RESTARTED` Advice can still rediscover it. If `sacct` reports that the attempt
was already requeued, SAC skips a second `requeuehold` and performs the
remaining release step.

Current jobs are not hidden by these exclusions. A job already observed by SAC
is in the seen set until its transition is resolved, and a live Slurm job is in
the next `squeue` snapshot. SAC currently normalizes raw Slurm `SUSPENDED` to
internal `RUNNING`; a live `STOPPED` allocation also remains visible in
`squeue` even though its historical JM state is not part of the bootstrap.

### Defer discovery and durability hardening to phase two

Phase one deliberately uses the existing process-local seen-job store and
production single-controller polling model. It removes the repeated full read
without introducing a second source of scheduler truth.

Phase two must address the following gaps before the design is treated as a
general multi-controller or lossless event-processing solution.

#### P0: correctness and recovery

1. **Close the post-bootstrap discovery gap.** A job registered after
   bootstrap that starts and finishes entirely between two `squeue` snapshots,
   and has no Advice, may never enter the seen set. Add a bounded incremental
   JM discovery contract, such as a stable creation/update cursor or change
   stream. It must return only changes since the acknowledged watermark and
   must not restore a full `GetJobs` scan to every tick.

2. **Make bootstrap attempt-aware.** The filtered query must not expose an old
   failed attempt as recoverable when a newer attempt exists in an excluded
   state such as `COMPLETED` or `REQUEUED`. JM should either select eligibility
   after resolving the latest attempt for each Slurm job ID or return enough
   successor information for SAC to reject stale recovery candidates.

3. **Define durable Advice completion.** Creation-time expiry alone must not
   discard an unprocessed `RESTARTED` or `TERMINATED` action after SAC downtime,
   or strand a restart between `requeuehold` and `release`. Define a durable
   pending/claimed/completed or cursor-based Advice lifecycle with idempotent
   stage recovery. ADR 0025 defines this lifecycle.

4. **Bound unresolved accounting retries without silently losing work.** Add
   backoff, age and attempt metrics, and an explicit terminal or operator-visible
   outcome for keys that `sacct` never resolves. A simple time-based deletion is
   insufficient because it recreates the missed-final-state problem.

The Advice interruption scenarios in item 3 already exist when SAC applies an
age limit locally; moving that limit into a backend query changes efficiency,
not the failure semantics. They are included here because a future lossless
reconciliation claim must cover every remaining JM input, not because the
startup-only `GetJobs` change creates them.

#### P1: resilience, scale, and broader deployment models

1. **Persist a recovery checkpoint if restart cost becomes material.** A
   durable watermark and unresolved-key checkpoint can reduce or eliminate the
   startup scan. It must be validated against fresh Slurm state and must not
   cache mutable job state as authority.

2. **Scope identity if one service can observe multiple schedulers.** The
   current deployment assumes one JM/SAC service boundary per Slurm cluster and
   applies environment admission inside SAC. If that topology changes, job
   identity, bootstrap, Advice, and incremental cursors must include stable
   scheduler/cluster and environment scope because Slurm job IDs can overlap.

3. **Coordinate multiple active SAC replicas.** Before supporting concurrent
   reconcilers for one cluster, add leadership, leases, or durable idempotency
   for scheduler mutations and checkpoint ownership. A process-local processed
   set is not sufficient across replicas.

4. **Reduce the remaining per-tick cost.** Measure and, where justified, batch
   or bound concurrency for targeted `sacct`, JM publication, and Advice work.
   ADR 0020 covers one proposal for this optimization; it is independent of
   whether `GetJobs` runs during regular ticks.

5. **Add operational observability.** Export bootstrap calls, pages, jobs, and
   duration; bootstrap retry causes; seen and unresolved set size and maximum
   age; targeted accounting volume; recovery retries; and incremental cursor
   lag. Alert on a bootstrap that never completes and unresolved work whose age
   exceeds the operational objective.

Phase-two validation must include jobs that complete between polls, SAC
downtime longer than the Advice age window, a crash between `requeuehold` and
`release`, an old failed attempt followed by a newer excluded attempt,
unresolvable `sacct` records, controller restart with a large bootstrap set,
and any supported multi-replica or multi-cluster topology.

## Consequences

Regular reconciliation no longer paginates JM job history. Its job-status work
is proportional to current `squeue` state, unresolved seen attempts, and
Advice targets rather than to all jobs retained in JM. This removes the
largest observed source of backend payload and repeated targeted accounting as
the database grows.

Startup still pays for one filtered, paginated JM read. SAC does not mark
bootstrap complete after a partial or failed read, so a prolonged JM outage can
prevent the controller from advancing beyond base snapshot construction. This
is fail-closed for discovery and avoids running from an incomplete startup set.

The process-local store contains exact attempt keys only. Memory usage grows
with active and unresolved attempts, not with completed history, but an
attempt that never resolves through Slurm remains retained and queried until
phase two defines an explicit outcome.

The design relies on the current deployment and timing invariants: one serial
controller per cluster, normally observable jobs appearing in `squeue`, held
DAG children remaining in `squeue`, and exact scheduler work continuing to
arrive through `ListAdvice`. It also assumes a failure-like attempt returned by
the filtered bootstrap is not made stale by a newer, excluded attempt unknown
to the snapshot. Phase one does not claim lossless discovery or general
stale-attempt protection when those invariants are violated; phase-two P0
items 1 and 2 close those gaps.

The `reconciliation_eligible_jobs` API remains useful even though it is no
longer called every tick: it bounds startup payload and preserves direct
failed-job recovery after a controller restart. Generated API and client
artifacts remain part of that contract.

## Alternatives Rejected

### Continue filtered GetJobs on every tick

State filtering reduces the candidate set but retains a repeated backend scan
whose cost can grow with retained failure history. SAC already has the active
and transition identities needed for normal regular-tick reconciliation.

### Use ListAdvice as the only JM input

Advice represents scheduler intent, not a complete job-status feed. A failure
may not have been analyzed yet, a non-recoverable final transition may never
receive Advice, and SAC still must publish ordinary Slurm state changes to JM.
Using only Advice would lose startup recovery and status synchronization.

### Bootstrap from all historical jobs

An unfiltered startup read would eventually recreate the same large payload
and targeted `sacct` fanout during every controller restart. The
`reconciliation_eligible_jobs` filter bounds the initial candidate set while
preserving the current direct-recovery path.

### Add a full local job cache

Caching JM or Slurm job records would introduce another mutable source of
truth and require invalidation, persistence, and cross-replica coordination.
Phase one needs only exact attempt identities; fresh Slurm reads remain
authoritative.

### Run periodic full scans as a safety net

A periodic scan reduces frequency but does not bound work as retained history
grows, and its recovery latency depends on the scan interval. Phase two should
close discovery gaps with a stable incremental cursor or event stream instead.

### Remove failure-like terminal states from bootstrap

That would reduce startup work further but break SAC's existing direct failed
recovery after restart when no Advice exists. Failure candidates remain until
JM owns the complete recovery decision or a replacement durable work contract
is adopted.
