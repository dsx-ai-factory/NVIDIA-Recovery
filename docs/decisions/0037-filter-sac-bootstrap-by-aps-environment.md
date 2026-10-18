# ADR 0037: Filter SAC Bootstrap by APS Environment

Status: accepted

Date: 2026-09-22

> **Implementation note (2026-09-29):** Username selection now applies only
> to action policy, not bootstrap or Advice discovery; environment filtering
> remains unchanged under [ADR 0042](0042-align-sac-and-watchdog-job-action-policy.md)
> ([PR 193](https://github.com/NVIDIA/NVIDIA-Recovery/pull/193)).

This decision supersedes only the requirement in
[ADR 0022](0022-bootstrap-sac-job-discovery-once.md) to retain every valid key
returned by startup discovery. Its bootstrap lifecycle and fresh-state rules
remain in force. Matching follows
[ADR 0014](0014-route-slurm-jobs-by-aps-environment.md).

## Context

Changing SAC from `prod` to a named environment can bootstrap thousands of
historical production attempts from the same backend. Previously every returned
attempt entered the seen store, and attempts absent from `squeue` required
`sacct` before environment rejection. An accounting timeout discarded partial
results and left those candidates cached for another expensive attempt.

The backend response already carries the stored APS environment in
`recovery_config.environment`. It can exclude historical discovery candidates
without querying their scheduler state.

## Decision

Apply the existing literal environment matching rules before a backend job
enters the snapshot's job candidates or the seen store. Use the job's typed APS
environment, never the publishing backend's `scheduler_metadata.environment`.

Missing recovery configuration, absent/null environment, and empty environment
all mean undeclared for this discovery filter. `prod` and `unspecified` admit
such candidates; other named environments reject them. No fallback accounting
lookup is made solely to determine a missing backend environment, including for
legacy records. A backend candidate still needs fresh Slurm onboarding,
environment, mode, and state checks before publication or scheduler actions.

Do not persist backend rejection decisions or insert them into the fresh Slurm
rejection index. Fresh matching `squeue` records must remain discoverable,
including when the stored environment is stale or a newer attempt appears.
Fresh scheduler rejections also exclude backend candidates they block.

Advice remains independently discoverable through the existing ownership and
recommendation checks. Apply those checks before Advice contributes a job key,
while retaining all Advice for normal skip reporting. Advice for another
environment must not recreate an accounting candidate merely by appearing in
the snapshot. Independently admitted jobs and seen attempts retain their keys.

## Consequences

Startup still reads the paginated backend candidate set, but only admitted
backend candidates consume seen-store entries or targeted accounting work.
Accounting timeouts retain admitted unresolved work without retaining rejected
backend candidates. Existing timeout and snapshot metric behavior is unchanged.

Named controllers no longer rediscover historical jobs solely through a backend
record that omitted their environment during an older conversion. Current Slurm
jobs and owned Advice remain independent discovery paths. Advice without an
environment retains its existing ownership semantics and may still require
accounting; this change does not treat missing Advice metadata as rejection.

## Alternatives Rejected

- Query `sacct` to establish every historical environment: preserves the startup
  cost and timeout backlog this change addresses.
- Use backend ownership metadata as the job environment: confuses controller
  identity with the job's declared routing value.
- Cache negative backend decisions: stale backend data could suppress later
  scheduler observations or independently owned Advice.
