# ADR 0010: Resolve The Squeue Final State During COMPLETING

Status: accepted

Date: 2026-08-11

Related:
[ADR 0001](0001-use-repository-local-architecture-decision-records.md) defines
the repository's decision-record process, and
[ADR 0009](0009-derive-watchdog-release-from-slurm-state.md) derives watchdog
release eligibility from the current Slurm state and attempt index. This
decision supersedes the prior squeue-selection policy, which selected the
first squeue token and reserved final-state selection for sacct.
It supports the HEIMDALL-3081 work that detects job failure during the Slurm
COMPLETING epilog window.

## Context

NVIDIA Recovery recovers Slurm jobs faster by acting during the COMPLETING (epilog)
window instead of waiting for the terminal state transition, so a requeue can
overlap the epilog and stay inside Slurm's `MinJobAge` requeue window.

Two Slurm forms report a terminal outcome while the epilog is still running:

- Slurm `>=23.11` reports the base state and flags together in the `job_state`
  array, for example `["TIMEOUT", "COMPLETING"]` or `["FAILED", "COMPLETING"]`.
- Older Slurm reports a bare `COMPLETING` state and carries the outcome in the
  state reason (`NonZeroExitCode`/`RaisedSignal` imply FAILED; `TimeLimit`
  implies TIMEOUT).

The prior policy parsed squeue with first-token selection
(`selectSlurmJobState(states, chooseFinal=false)` in Go,
`parse_job_state(state, choose_final_state=False)` in Python) and reserved
final-state selection for sacct. Under that policy `["TIMEOUT", "COMPLETING"]`
resolved to `RUNNING` (the base `TIMEOUT` was dropped because `COMPLETING` was
not first, or the base was masked), which meant the controller could not observe
the failure until the terminal transition. The COMPLETING-window feature
therefore requires squeue to prefer the final state in the array.

The single shared selector already used by sacct picks "an exact final token in
input order, otherwise the first token." Reusing it for squeue is the smallest
change and keeps one selection contract. A consequence is that any final token
in a squeue array now wins, including `REQUEUED`, which both the Go
`isFinalSlurmState` predicate and the Python `SlurmJobFinalStates` enum classify
as final (a documented pragmatic choice, because sacct can surface `REQUEUED` as
a terminal artifact). So `["PENDING", "REQUEUED"]` now resolves to `REQUEUED`
where it previously resolved to `PENDING`.

## Decision

### Parse squeue with final-state selection

Squeue conversion selects the final state in the `job_state` array, matching
sacct:

- Go: `slurm-advisor-controller/internal/slurm/json_conversion.go` calls
  `selectSlurmJobState(jobData.JobState, true)`.
- Python: `recovery_libs/recovery_libs/storage/dataclass.py`
  `SlurmJobExecutionRecord.from_squeue_json` calls
  `parse_job_state(json["job_state"], choose_final_state=True)`.

Scalar states from older Slurm are unaffected: a bare `"RUNNING"` or
`"COMPLETING"` still maps to its existing coarse state. The older-Slurm
bare-`COMPLETING` failure case is resolved separately from the state reason,
not from the array, by `resolve_effective_final_state` (Job Manager) and
`jobIsCompletingFailure` (`internal/reconcile/failures.go`).

### Accept `["PENDING", "REQUEUED"]` resolving to `REQUEUED`

`REQUEUED` is a final token, so a squeue sighting of `["PENDING", "REQUEUED"]`
now maps to `cluster.JobStateRequeued` instead of `cluster.JobStatePending`.
This is accepted rather than special-cased, because a `REQUEUED` sighting means
the attempt has already been through a requeue, and treating it as "requeue
already happened" is the intended reconciliation input. The controller
consumers behave as follows for that sighting:

- Advice reconciliation (`internal/reconcile/advice.go`, `RESTARTED`,
  `attemptDelta == 0`): `requeueCompleted` becomes true, so the controller
  issues `JobRelease` (release the hold) instead of a second `JobRequeueHold`.
  This avoids a redundant requeue of an already-requeued attempt.
- Failed-job reconciliation (`internal/reconcile/failures.go`): unaffected.
  `REQUEUED` is neither `JobStateFailed` nor a COMPLETING failure, so it never
  triggers a requeue there.
- Job Manager Advisor path: unaffected. `resolve_effective_final_state` returns
  `REQUEUED`, which is not `FAILED`, so no state-based anomaly, terminal Advice,
  or attribution is produced.
- NVIDIA Recovery status projection (`internal/heimdall/upsert.go`): the pre-existing
  `Requeued -> JOB_STATUS_FAILED` mapping now applies to this sighting, which
  previously projected `JOB_STATUS_PENDING`.
- Standalone watchdog (`internal/watchdog/watchdog.go`): a `REQUEUED` sighting
  falls to the state machine's default `disarm()` branch rather than the
  held-pending release-candidate branch.

## Consequences

The COMPLETING-window failure detection works for Slurm `>=23.11` because
`["TIMEOUT", "COMPLETING"]` and `["FAILED", "COMPLETING"]` resolve to a failed
state at squeue conversion time, which the controller and Job Manager act on
during the epilog. Healthy `["COMPLETED", "COMPLETING"]` and
`["CANCELLED", "COMPLETING"]` still resolve to non-failure states and produce no
recovery evidence.

The `REQUEUED` reconciliation change is beneficial in the advice path (it
replaces a redundant re-requeue with a release) but has two effects that must be
validated against real Slurm, because they only occur if Slurm actually emits
the two-element `["PENDING", "REQUEUED"]` array for a held, requeued attempt
rather than a settled `["PENDING"]`:

- The standalone watchdog no longer treats such a sighting as a held
  release-candidate; it disarms instead. The watchdog is a fail-open backstop
  for a held attempt that the primary advice-driven release missed, so this
  narrows that backstop for the specific `["PENDING", "REQUEUED"]` sighting. The
  state-derived watchdog (ADR 0009) re-evaluates from the current observation
  each tick and holds no arming history, so once the attempt settles to a held
  `["PENDING"]` sighting it becomes a release candidate again; the narrowing is
  therefore limited to ticks where the array form is actually observed.
- The NVIDIA Recovery/dashboard status for that sighting projects `FAILED` instead of
  `PENDING`.

Because these depend on Slurm's exact JSON emission, the harness and recovery
e2e (real Slurm) are the authority. The requeue-limit guardrail and any
watchdog release scenarios must confirm the held attempt is still released and
the reported status is acceptable. If a held-requeued attempt is observed as
`["PENDING", "REQUEUED"]` and the backstop or status regression is material, the
follow-up is to restrict squeue final-state selection to failure states only
(see Alternatives Rejected) rather than reverting the COMPLETING-window
behavior.

## Alternatives Rejected

### Keep squeue first-token selection

Retaining `chooseFinal=false` for squeue preserves the previous
`["PENDING", "REQUEUED"] -> PENDING` behavior and the watchdog and status
projection for that sighting, but it also drops the base failure token in
`["TIMEOUT", "COMPLETING"]`, which defeats the COMPLETING-window detection this
change exists to enable. The feature requires final-state selection for squeue.

### Select only failure final states in squeue arrays

A narrower selector could prefer only failure-class final states
(`FAILED`, `TIMEOUT`, `NODE_FAIL`, `OUT_OF_MEMORY`, `BOOT_FAIL`, `DEADLINE`,
`PREEMPTED`) and continue to prefer the base token over `REQUEUED`,
`SUSPENDED`, `STOPPED`, `COMPLETED`, and `CANCELLED`. This would keep
`["PENDING", "REQUEUED"] -> PENDING` while still detecting failures during
COMPLETING. It was not adopted now because it diverges the squeue and sacct
selectors, splits the single shared selection contract, and treats `REQUEUED`
inconsistently with its documented final-state classification. It remains the
preferred follow-up if the real-Slurm harness shows the `REQUEUED` sighting is
common enough for its watchdog or status effects to matter.
