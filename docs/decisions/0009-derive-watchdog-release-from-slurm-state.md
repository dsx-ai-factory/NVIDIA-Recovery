# ADR 0009: Derive Watchdog Release From Slurm State

Status: accepted

Date: 2026-08-08

Related:
[ADR 0007](0007-resolve-dag-release-through-advice-state.md) defines exact-attempt
DAG release through `AdviceState`. This record supersedes only ADR 0007's
watchdog eligibility and predecessor-evidence decision.

## Context

ADR 0007 protects a dependency-blocked DAG child by leaving attempt 0 held and
requiring exact `RELEASED(0)` advice before the Slurm Advisor Controller makes
that attempt scheduler-eligible. It separately makes DAG retries watchdog
eligible, but requires durable proof that a predecessor ran and retains an
explicit `watchdog_eligible` policy for non-DAG jobs.

That split creates two sources of watchdog policy: current Slurm state and a
persisted flag in the APS/HEIMDALL job comment. It also requires an accounting
lookup or another durable history source after a watchdog restart. The desired
product behavior is simpler: normal managed jobs are always protected, the
initial held DAG child is protected by its attempt-0 gate, and every later
attempt is protected because DAG requirements are monotonic once Job Manager
has opened that gate.

Slurm `squeue --json` supplies the active job state, the authoritative hold
bit, and `restart_cnt`, which the adapter exposes as the zero-based
`attempt_index`. A validated APS/HEIMDALL comment supplies managed-job and DAG
identity. These fields are sufficient to reconstruct release eligibility after
a standalone watchdog restart, although the in-memory held-duration timer
restarts with the process.

## Decision

### Preserve exact initial DAG release

ADR 0007's `ADVICE_STATE_RELEASED` contract remains unchanged. A DAG child is
submitted as attempt 0 in `PENDING + Held` while its dependencies are unmet.
The watchdog never releases that state. Job Manager publishes `RELEASED(0)`
only after every dependency is satisfied, and the Slurm Advisor Controller
reconciles that exact attempt through `scontrol release`.

Job Manager must not issue `RESTARTED` and no supported actor may requeue a
dependency-blocked child before its initial release. Once the gate is opened,
dependency satisfaction is monotonic for that job. A later attempt therefore
does not need separate predecessor-running evidence.

### Compute watchdog eligibility from each squeue snapshot

The standalone watchdog derives policy on every poll instead of reading or
persisting `watchdog_eligible`:

```text
if the job is unmanaged, final, or outside configured action policy:
    do not release
else if the job is a DAG child and attempt_index == 0:
    do not release through the watchdog
else if state is PENDING and hold is true:
    start or continue the held timer
    release after the configured timeout
else:
    clear the held timer and perform no release
```

The watchdog identifies a DAG child from a non-empty `dag_dependencies` value
in the validated APS/HEIMDALL specification. It does not infer DAG identity
from job name, Slurm reason text, or attempt index.

For non-DAG managed jobs, watchdog protection is always enabled, including
attempt 0. For DAG jobs, attempt 0 is excluded from watchdog release and every
attempt with `attempt_index >= 1` is enabled. A held retry is a release
candidate even when this watchdog process did not observe the preceding
attempt. The ordinary partition allowlist, observe-versus-act mode, explicit
scheduler-action acknowledgement, hold timeout, and exact Slurm job identity
checks remain mandatory.

`PENDING + Unheld` means a job is scheduler-eligible, so the watchdog performs
no mutation even if the job remains pending for resources or priority.
`RUNNING` likewise requires no watchdog action. A successful `scontrol release`
must be confirmed by a later fresh snapshot showing the job unheld or running;
if it remains held, reconciliation retries according to the timeout policy.

### Remove the per-job watchdog flag

Remove `watchdog_eligible` from the APS job specification, Advisor recovery
configuration, Slurm comment conversion, generated clients, examples, and
tests. Reserve its protobuf field number and name so they cannot be reused.
Existing serialized comments that contain the field remain valid as job
comments, but the watchdog ignores the field.

The removal affects only per-job watchdog policy. Global action mode,
scheduler-action acknowledgement, and partition scoping remain operator-owned
safety controls.

### Reconcile DAG actions through persisted advice

Implement ADR 0007 together with this decision. Job Manager persists
`RELEASED(0)` for the dependency-gated initial child attempt instead of
publishing the legacy `JobRelease` request. It refuses to move that dependency
decision to a retry attempt. Dependency action status records that release
advice was persisted; scheduler completion is observed independently through
fresh job state. Remove the legacy DAG response consumer and RabbitMQ timeout
republisher after production-interface tests cover the replacement path.

## Scenarios

| Scenario | Fresh observation | Outcome |
| --- | --- | --- |
| DAG child waiting on parents | attempt 0, `PENDING + Held` | Watchdog does nothing; only exact `RELEASED(0)` may open the gate. |
| DAG gate opened | attempt 0, `PENDING + Unheld` | No watchdog mutation; Slurm may schedule the job. |
| Active attempt | any attempt, `RUNNING` | No watchdog mutation. |
| DAG recovery retry | attempt 1 or later, `PENDING + Held` | Start the timeout and release when it expires. |
| Normal managed job held | any attempt, `PENDING + Held` | Start the timeout and release when it expires. |
| Job awaits placement | any attempt, `PENDING + Unheld` | Do not repeat release; expose the Slurm pending reason. |
| Watchdog restarts on held retry | attempt 1 or later, `PENDING + Held` | Reconstruct eligibility and restart the held timer. |
| Release command fails | eligible held attempt | Preserve the candidate and retry from fresh state. |
| Job is final, unmanaged, or filtered | any | Do not mutate Slurm and clean up tracking state. |

## Consequences

The watchdog no longer needs a mutable job-comment flag, a predecessor-running
latch, or a Slurm accounting lookup to decide whether a retry is protected.
The same current-state rules work after a process restart and for attempts that
move through `RUNNING` faster than the watchdog polling interval.

The design intentionally treats a supported DAG retry as proof that the
initial dependency gate was already opened. A manual or automated requeue of a
dependency-blocked attempt 0 violates the producer and scheduler-action
contract; if it creates attempt 1, the watchdog will eventually release it.
Tests and telemetry must make that violation visible.

Removing the per-job flag also removes the ability to opt a normal managed job
out of watchdog release. An operator hold on a non-DAG job or a DAG retry will
eventually be overridden. Operators must use observe mode, remove NVIDIA Recovery
management, or change the global action policy before applying a hold that must
persist.

The held timer remains process-local. Restarting the watchdog delays release by
up to one new timeout interval, but cannot permanently forget that a retry is
eligible. Persisting hold duration is outside this decision.

This behavior can release workloads and therefore requires focused controller
and production-interface tests plus explicit human scheduler-action review
before merge.

## Alternatives Rejected

### Keep watchdog_eligible as a durable latch

Updating a second scheduler field before release couples DAG progress to a
comment mutation and preserves a per-job opt-out that is no longer part of the
product policy.

### Require evidence that a predecessor ran

Running is stronger evidence than the policy needs. DAG requirements become
satisfied when Job Manager opens the gate, not when Slurm allocates resources.
Requiring a running observation creates polling races and requires historical
accounting after restart.

### Keep only an in-memory eligibility latch

Arming after `PENDING + Unheld` or `RUNNING` works during uninterrupted
observation but forgets eligibility if the watchdog restarts after a retry is
already held.

### Release every held attempt 0

This would allow the watchdog to bypass the initial DAG dependency gate. DAG
identity remains necessary even though per-job watchdog policy does not.
