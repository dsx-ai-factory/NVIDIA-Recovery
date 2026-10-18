# ADR 0011: Cancel DAG Dependents After Parent Abort

Status: accepted

Date: 2026-08-11

Related:
[ADR 0007](0007-resolve-dag-release-through-advice-state.md) assigns DAG
dependency evaluation to Job Manager and scheduler mutation to the Slurm
Advisor Controller. This record defines the orphan policy that ADR 0007 leaves
open when Job Manager gives up retrying an unsuccessful parent.
[ADR 0009](0009-derive-watchdog-release-from-slurm-state.md) derives watchdog
eligibility from current Slurm state and attempt index.
[ADR 0010](0010-resolve-squeue-final-state-during-completing.md) records why
`REQUEUED` can appear as a final attempt state and is not, by itself, a DAG
workflow abort.
[ADR 0016](0016-expose-dag-dependency-evaluation-reasons.md) supersedes this
record's placement of parent-termination causality in a synthetic child
`Issue`; the termination policy and JM/SAC ownership defined here remain
accepted.

## Context

A dependency-blocked DAG child is submitted held. Job Manager releases its
exact initial attempt only after every configured parent satisfies the
`COMPLETED` trigger. If Job Manager gives up on a parent because it was
cancelled or stopped, or because it failed after exhausting its requeue limit,
that condition can no longer be satisfied. Job Manager represents that final
decision with `ADVICE_STATE_TERMINATED`. The current behavior does not apply
that parent decision to the DAG, so the child remains held indefinitely.

Leaving the child held avoids executing invalid work, but it does not resolve
the workflow. The cancellation policy must also reach grandchildren and deeper
descendants so the entire blocked branch reaches a terminal state.

The existing component boundary is intentional. Job Manager owns DAG,
recovery, and lifecycle intent. The Slurm Advisor Controller owns
Slurm-specific mutations such as `scontrol release`, `requeuehold`, and
`scancel`. `ADVICE_STATE_TERMINATED` already represents scheduler-neutral
termination intent, so this policy does not require another public Advice
state.

This decision relies on the existing DAG scheduling invariant: a dependent
attempt remains held until every parent has completed and Job Manager releases
it. A dependency-blocked child therefore cannot run, fail, or enter recovery
before its parents complete. This record does not add behavior for a child that
somehow violates that invariant because of scheduler misconfiguration or an
unrelated manual release.

Name-based DAG references require additional care because termination produces
a scheduler kill. A global job-name match is not sufficient evidence that an
observed job is the intended parent.

## Decision

### Treat parent TERMINATED Advice as an absorbing workflow abort

When Job Manager produces `ADVICE_STATE_TERMINATED` for a resolved DAG parent,
it terminates every reachable descendant. One terminated parent is sufficient
even when a child has other completed or incomplete parents.

Job Manager must not emit `RELEASED` or `RESTARTED` for that dependent. Requeue
limits do not override the decision: the held child has not failed and does not
enter failed-job recovery; it receives termination intent and is cancelled.
Repeated parent and child observations are idempotent.

The parent Advice, rather than its raw Slurm state, is authoritative. A failed
parent with retries remaining receives `RESTARTED`, so its children remain held
while the parent recovers. A failed parent that has exhausted its effective
requeue limit receives `TERMINATED`, so its children are cancelled. Explicit
parent cancellation or stop also resolves to `TERMINATED` and cancels the
children. Raw `FAILED`, `TIMEOUT`, `PREEMPTED`, `REQUEUED`, and other scheduler
states do not independently trigger DAG cancellation.

### Keep intent in Job Manager and scheduler mutation in the adapter

The cancellation evaluation is tied to Job Manager's resolved lifecycle
decision, not to a particular job-ingestion path. Whenever Job Manager durably
persists `ADVICE_STATE_TERMINATED` for a job that is a DAG parent, it evaluates
that parent's dependents and propagates the terminal decision through the
blocked DAG branch. This includes terminal Advice produced for explicit
cancellation or stop and for failure after the effective requeue limit is
exhausted.

The production Advisor `POST /jobs` path remains the scheduler-state ingress
used by SAC. This decision does not require adding cancellation behavior to the
legacy workflow-controller event consumer. Retirement of that retained
compatibility code is separate cleanup.

For each dependent of a terminated parent, Job Manager upserts exact-attempt
`ADVICE_STATE_TERMINATED` Advice with de-duplicated causal evidence containing
the parent job ID, parent attempt, terminal Advice reason, observed scheduler
state, and workflow ID. It also marks the dependency action terminal so
completion processing cannot later release the child. These writes are
idempotent for repeated observations.

Job Manager never calls Slurm. The Slurm Advisor Controller remains the only
component authorized to translate termination Advice into `scancel`, under its
existing mode policy and explicit scheduler-action acknowledgement.

### Propagate termination through the blocked DAG branch

Consider a three-job chain `A -> B -> C`, where each arrow means "the job on
the right depends on the job on the left." Before `A` completes, both `B` and
`C` remain held. If Job Manager produces `TERMINATED` Advice for `A`—whether
because `A` was cancelled or stopped or because its final failed attempt
exhausted the requeue limit—Job Manager produces `TERMINATED` Advice for `B`.
Producing that terminal decision also evaluates `B` as the parent of `C`, so
Job Manager produces `TERMINATED` Advice for `C` without waiting for SAC to
cancel `B` and report a later `B=CANCELLED` state. The same rule continues
through every level of the valid DAG.

An ID dependency matches only the exact parent job ID. A name dependency is
resolved only within the dependent child's canonical `workflow_id`, and the
resolved job ID must equal the parent observation being processed. Ambiguous or
unresolved name references fail closed without producing a scheduler kill.

Completion and termination reconciliation use the same resolver so the release
and termination paths cannot disagree about parent identity. A child registered
after its parent already has `COMPLETED` or `TERMINATED` intent is reconciled
immediately from persisted parent state and Advice rather than waiting for
another parent transition.

### Use the existing SAC termination path

Advice reconciliation retains the existing exact-attempt termination behavior:

- a non-final exact attempt is killed in action mode
- observe mode reports the planned kill without mutating Slurm
- a final exact attempt already satisfies the recommendation
- stale or future attempt Advice does not mutate the observed attempt

The normal descendant in this decision is pending and held, so action mode
issues `scancel`. Slurm then reports it as `CANCELLED`. It never enters the
failed-job requeue path, regardless of the configured requeue limit. No new SAC
requeue-precedence rule or same-tick Advice refresh is part of this decision.

## Consequences

Final parent termination becomes an explicit workflow outcome instead of
leaving held descendants behind. The policy covers explicit cancellation or
stop and failure after the effective requeue limit is exhausted. It reaches
direct children and all deeper descendants.

SAC reads Advice when it builds the snapshot at the start of a reconciliation
tick, before it posts that tick's Slurm job observations to Job Manager. If a
job upsert causes Job Manager to persist parent and descendant `TERMINATED`
Advice, that new Advice is not part of SAC's existing snapshot. SAC reads and
acts on it in the next normal tick. The added cancellation latency is therefore
up to one configured SAC polling interval, plus processing time; the default
polling interval is 15 seconds. Descendants remain pending and held throughout
that interval, so the delay does not make them scheduler-eligible or place them
in failed-job recovery. This decision does not add a same-tick Advice refresh.

`ADVICE_STATE_TERMINATED` already exists, and the dependency action status is
already string-valued. No new public Advice enum, scheduler RPC, or database
column is required. If dependency status exposes a new terminal value through
the listing API, its description and generated artifacts must include it.

Observe mode continues to prohibit scheduler mutation. It proves the decision
and planned action but cannot make Slurm report descendants as cancelled.
Action-mode validation requires the existing explicit scheduler-action
acknowledgement and human review because the behavior can kill workloads.

Production-interface validation must cover a held child and grandchild,
multiple parents, workflow-scoped name resolution, duplicate observations,
parent-before-child registration, and the absence of release, restart, or a
later child attempt after parent termination. It must include positive cases
for explicit cancellation or stop and exhausted requeue limits, plus a negative
case proving that a failed parent with retries remaining keeps its children
held rather than cancelling them.

## Alternatives Rejected

### Leave descendants held

Keeping the current fail-closed hold prevents premature execution but never
resolves the workflow or gives the held descendants a terminal outcome.

### Let Job Manager call scancel directly

Direct scheduler access from Job Manager would duplicate Slurm credentials,
mode policy, action acknowledgement, retries, and observability. It violates
the scheduler-neutral producer and scheduler-adapter boundary established by
ADR 0007.

### Terminate direct children only

Direct-only propagation would wait for SAC to cancel each level and for Job
Manager to observe that state before evaluating the next level. Propagating the
terminal decision through Job Manager's DAG data cancels the entire blocked
branch without that scheduler round trip between levels.

### Derive cancellation directly from raw Slurm states

Failures, timeouts, preemption, and requeue artifacts can be inputs to normal
recovery. Treating raw states as absorbing would cancel children while a parent
is still eligible for recovery. Job Manager's resolved `RESTARTED` or
`TERMINATED` Advice already captures whether recovery remains possible, so it
is the correct DAG cancellation input.

### Add a failed-child requeue race policy

A correctly held DAG child cannot run or fail while its dependency is unmet,
so it cannot enter failed-job requeue reconciliation before the parent abort is
processed. Handling a prematurely released or failed child would design around
a violation of the established DAG scheduling invariant and is outside this
decision.

### Resolve name dependencies globally

Job names are not globally unique enough to authorize a kill. Workflow-scoped
resolution with exact parent-ID confirmation provides a defensible identity
boundary; ambiguous references remain safely unresolved.
