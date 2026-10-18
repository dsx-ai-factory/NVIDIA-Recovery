# ADR 0043: Bound Failed-Job Requeue Retries

Status: accepted

Date: 2026-09-30

> **Implementation note (2026-10-01):** [ADR 0044](0044-budget-failed-job-recovery-phase.md)
> supersedes the same-tick requirement with a phase budget and carries the
> remaining three-command allowance across ticks.

This decision partially supersedes [ADR 0022](0022-bootstrap-sac-job-discovery-once.md):
failed-job command errors no longer retain recovery work for unlimited ticks.
The instructing maintainer requested bounded retry and explicitly selected the
three-attempt policy during implementation.

## Context

A failed job can remain in accounting after Slurm has purged it from the
controller. Its `requeuehold` then fails permanently, but SAC previously left
the attempt in `SeenJobs`, repeating the command and tick error indefinitely.
Transient failures also benefit from prompt retries before Slurm ages out the
job. `MaxRequeueTimes` limits successful historical requeues, not command errors.

## Decision

For direct failed-job recovery, classify known permanent Slurm command
rejections in the Slurm adapter and stop after the first rejection. Transient
and unknown errors receive three total command attempts within the current
tick, with 250 ms and 500 ms waits. Exhaustion also ends recovery for that
exact attempt in this controller process. Caller cancellation interrupts the
command or wait without committing a give-up decision.

Reuse the processed-attempt marker before removing terminal accounting work
from `SeenJobs`. Preserve active `COMPLETING` attempts until their final
accounting read, while suppressing further recovery commands for them. A newer
Slurm attempt is separate recovery work. Audit every returned command through
the existing scheduler-action observation and return final failures once at
the tick boundary.

Defer durable give-up status in Job Manager and recovery-aborted audit semantics
until recovery converges on Advice, as requested by the maintainer. Leave a
code TODO explaining that restart/bootstrap can retry the same attempt.
Advice-driven retry behavior is outside this decision.

## Consequences

Permanent rejection and retry exhaustion stop repetitive scheduler calls and
tick errors for the same attempt within one SAC process. A transient outage
that outlasts the local budget can leave a failed job unrecovered. Command
timeouts still bound each individual call; caller cancellation leaves work
eligible for a later tick. Process restart resets the local give-up decision.

## Alternatives Rejected

- Unlimited retries on later ticks preserve the original failure loop and
  delay time-sensitive recovery.
- Treating every unknown error as permanent can discard a newly introduced
  transient failure without giving recovery another chance.
- Persisting a separate job-state recovery-aborted protocol would duplicate
  the planned Advice-driven recovery contract.
