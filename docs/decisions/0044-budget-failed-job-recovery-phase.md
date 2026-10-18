# ADR 0044: Budget The Failed-Job Recovery Phase

Status: accepted

Date: 2026-10-01

Discussion: [PR 207](https://github.com/NVIDIA/NVIDIA-Recovery/pull/207)

## Context

[ADR 0043](0043-bound-failed-job-requeue-retries.md) bounds the number of
failed-job requeue calls, but serial execution can still postpone Advice and
fresh snapshots for minutes when several commands stall. Three 30-second
command timeouts consume approximately 91 seconds for one failed job.
The instructing maintainer selected a 30-second phase budget and a command
timeout counter in response to this review concern.

## Decision

Partially supersede ADR 0043's requirement to spend all retries in the same
tick. Bound direct failed-job recovery with
`SLURM_ADVISOR_FAILED_JOB_RECOVERY_TIMEOUT`, initially 30 seconds. Use a child
context so phase expiry interrupts a command or backoff without canceling
subsequent Advice reconciliation.

Retain the three-command allowance across ticks, keyed by job ID and Slurm
attempt index. An executed command interrupted by the phase deadline consumes
one attempt; an interrupted backoff does not consume another. Unfinished
recovery stays in `SeenJobs`. A third failure or permanent rejection still
finishes the local decision, even if the phase deadline has just expired.
Caller shutdown cancellation preserves the remaining allowance. Resume from
fresh scheduler observations, rotating past the last attempted job so later
jobs have access to the next tick's budget.

Record actual command deadline terminations at the shared execution boundary
with `slurm_advisor_controller_slurm_command_timeouts_total{command}`. Count
both command and enclosing-phase deadlines; exclude normal cancellation,
commands that never started, ordinary failures, and success. Command labels
are bounded and contain no arguments or job identifiers. Expose the counter
for operator alerts on repeated timeouts, with deployment-specific routing
and thresholds.

## Consequences

Failed-job scheduler work yields after the configured budget, plus at most
100 ms of configured output-pipe cleanup under normal process termination.
Other reconciliation phases have their own latency bounds. Pending recovery
and retry counts remain process-local, so restart can rediscover failed work;
durable give-up and recovery-aborted audit semantics remain deferred to Advice.
Slow recovery may span ticks, while quick transient failures retain local retries.

Timeout alerts indicate interrupted scheduler commands, including interruption
by a busy phase's remaining budget; they do not alone prove a Slurm outage.
Operators must correlate repeated events with command logs, phase durations,
and configured deadlines before intervening.

## Alternatives Rejected

- Retry counts alone do not bound the phase's elapsed time.
- One call per job per tick still takes ten minutes for twenty 30-second calls.
- Resetting retry counts at each phase deadline recreates endless retries.
- Parallel scheduler mutations introduce additional ordering and ownership
  requirements that are unnecessary for bounding this serial phase.
