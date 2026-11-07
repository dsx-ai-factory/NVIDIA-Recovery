# Job Array Support Implementation Plan

This is the delivery plan for
[ADR 0054](../decisions/0054-recover-slurm-array-tasks-independently.md), not a
claim that array support is implemented. The ADR owns identity, policy, and
scope; this document keeps implementation sequencing outside the decision.

## Starting Point

Repository inspection at `8cb8739` found reusable partial support:

- [Squeue conversion](../../slurm-advisor-controller/internal/slurm/json_conversion.go)
  already builds composite task IDs and skips unset task IDs, but its numeric
  wrapper does not accept the plain integer forms handled by the reference.
- [Sacct conversion](../../slurm-advisor-controller/internal/slurm/sacct_conversion.go)
  already groups rows by composite ID, but assumes a plain array parent and a
  wrapped task ID. Attempt indices are reconstructed from accounting groups;
  consistency with active restart counts needs coverage for interleaved tasks
  and incomplete history.
- [Job publication](../../slurm-advisor-controller/internal/heimdall/upsert.go)
  already carries the four Slurm identity keys in scheduler metadata.
- [Retry policy](../../recovery_libs/recovery_libs/advice/requeue.py) evaluates
  the attempt index and checkpoint history selected by `job_id`. Correct task
  identity can reuse this policy without a new array budget.
- [Job Manager event matching](../../job_manager/job_manager/service.py)
  starts from the supplied event job ID and writes checkpoint saves using that
  label. Both matching and persistence must use the resolved task identity.
- [Watcher identity](../../watcher/watcher/lib/utils/job.py) and
  [audit creation](../../watcher/watcher/lib/utils/audit.py) also need the resolved
  identity before deduplication and audit construction.
- [Job audit snapshots](../../job_manager/job_manager/audit/job_changes.py)
  intentionally exclude checkpoints, so association needs an explicit audit
  event rather than expanding the Job snapshot.

The Warthog reference inspected for this proposal is
`warthog_libs/warthog_libs/storage/dataclass.py`: `parse_slurm_int`,
`parse_slurm_array_task_id`, and the `squeue`/`sacct` identity parsers. Reuse the
plain/wrapped numeric parsing, task-zero handling, and synthetic-row distinction.
Do not import its job-group persistence, in-job cycles, or orchestrator lifecycle.
Add sanitized local fixtures; implementation and tests must not require a
checkout of that reference repository.

## Proposed PR Stack

Each PR builds on its predecessor and remains independently reviewable. Push
and PR creation follow explicit maintainer instruction. Task identity is split
into PR 2a and PR 2b so parsing and backend preservation can be reviewed separately.

| PR | Scope | Evidence required |
| --- | --- | --- |
| 1: Decision | Review ADR 0054 and this plan; record acceptance when selected. | Identity, retry, checkpoint, audit, and scope choices reviewed together. |
| 2a: Task identity parsing | Normalize SAC `squeue`/`sacct` parsing and publish canonical task identity through existing scheduler metadata. | Both numeric representations produce the same task ID; task zero survives; synthetic ranges and invalid identities produce no actionable job; standalone IDs remain unchanged; publication and gateway mappings retain identity. |
| 2b: Backend identity preservation | Preserve identity through Job/Advice updates and validate conflicting identity at the Job Manager boundary using a scheduler-neutral storage policy. | Partial updates retain established identity; contradictory updates are rejected; existing standalone-job behavior is preserved. |
| 3: Task recovery | Complete task-scoped accounting/attempt reconciliation, bootstrap, Advice, recovery bookkeeping, scheduler selectors, log enrichment, and watchdog handling. Reuse backend retry policy. | Two siblings recover independently; one exhausts its allowance without affecting the other; restart/bootstrap preserves budgets; commands and convergence remain task-specific. |
| 4: Checkpoint association | Normalize event identity at Slurm ingestion and backend compatibility boundaries before Watcher lookups/deduplication and JM matching/writes. Add explicit checkpoint-association audits. | Save/load events and their audits resolve to the same task attempt; delayed registration retries; ambiguous events do not alter progress; sibling checkpoints cannot postpone another task's stop decision. |
| 5: End-to-end qualification | Close audit propagation/query gaps across producers, update the release audit catalog, add isolated array scenarios, and update current support/operations documentation. | Real task failures, checkpoint ingestion, persisted audit reads, and unaffected siblings are demonstrated through production interfaces. |

Tests that prove a change belong in its owning PR; the final PR adds integration
evidence rather than deferring correctness checks. If a seam already satisfies
the contract, retain it and add only justified behavioral coverage.

### Task Identity Parsing Implementation (PR 2a)

The parsing slice normalizes plain and wrapped Slurm numeric fields, retains
task zero, skips synthetic pending rows, and rejects malformed task identity.
Existing publication and gateway mappings preserve the canonical ID and
scheduler metadata without new contract fields.

### Backend Identity Preservation Implementation (PR 2b)

Job Manager validates Slurm identity at its compatibility boundary. Partial Job
and Advice updates retain established identity keys; conflicting updates are
rejected. Storage applies the caller-provided metadata policy under its existing
row lock before updating a record or comparing Advice desired state, keeping
the persistence mechanism scheduler-neutral. Recovery and checkpoint behavior
remain follow-up slices; this slice does not qualify array recovery for rollout.

### Task Recovery Implementation

Job Manager serializes identity admission across Job and Advice with a generic,
database-scoped exact-attempt MySQL advisory lock. Storage pins one connection
for the scope, permits same-attempt reentrancy, and rejects nested scopes for
different attempts. A ten-second acquisition timeout or a failed identity read
fails admission before recovery side effects. All Job Manager replicas must
run this implementation before task actions are enabled; older writers do not
participate in the lock.

SAC reconciles task accounting using explicit restart counts when available or
matching submission/start timestamps from observed task attempts. It omits
unresolved array history with diagnostics instead of assigning an ordinal that
could reset the retry allowance. Bootstrap observations supply identity anchors,
but fresh scheduler state remains necessary for actions. Advice and bootstrap
identity checks reject inconsistent task selectors and attempt UUIDs. Task-level
selectors, retry budgets, and independent watchdog timers have focused coverage.

Checkpoint association and production end-to-end qualification remain required
before claiming supported array recovery. The Docker Slurm harness currently
cannot build in this environment because GitHub's certificate issuer is not
trusted by its base image; TLS verification remains enabled.

## Behavioral Acceptance

- With an effective `max_requeue_times` of two, task A stops after its second
  requeue while task B can still use its own remaining allowance. Include a
  controller restart and backend rediscovery. Verify the configured system cap
  still applies. Failed command retries are not successful workload requeues.
- Exercise the task whose raw job ID equals the array submission ID. Every
  mutating command still selects the composite task ID. Unrelated siblings
  continue unchanged, including pending tasks represented by a synthetic row.
- Interleave accounting rows from multiple tasks and attempts, including
  duplicate rows and incomplete responses. Never merge siblings, turn a step
  into an attempt, or reset an established attempt index from partial history.
  Verify `squeue` and `sacct` resolve the same attempt UUID.
- Cover array IDs and task IDs in supported plain/wrapped forms, task zero,
  unset wrappers, negative or sentinel values, invalid types, and a concrete
  task appearing after a synthetic pending-range observation.
- Ingest checkpoints through the existing production log/event path using a
  canonical task ID, explicit array/task metadata, and an unambiguous raw-ID
  mapping with explicit raw-ID provenance. Test conflicting identity, array-only
  labels (including when a task's raw ID equals that array ID), shared paths,
  explicit attempts awaiting registration, delayed events from an older attempt,
  and duplicate delivery. Checkpoint loads must not count as saved progress.
- Query persisted audits through the authenticated gateway by task ID and by
  exact attempt UUID. Verify detection, checkpoint association, decision, action,
  and terminal outcomes where the scenario exercises them. Task A queries must
  exclude B; whole-job reads for A must include A's retries. Verify audit payload
  identity agrees with its envelope and sibling event deduplication is separate.
- Keep ordinary jobs, observe mode, environment/partition admission, action
  acknowledgement, delayed Advice, and the watchdog's existing release policy
  covered. Audit delivery failures must not trigger a scheduler action retry.

Use focused parser, policy, producer, and gateway contract tests with sanitized
fixtures. Run owning `make verify-<area>` targets and the Slurm controller
harness for scheduler changes. Final E2E scenarios follow
[ADR 0052](../decisions/0052-run-e2e-tests-as-isolated-staged-scenarios.md), using
production submission, ingestion, and HTTP observation interfaces. Assertions
must establish task isolation and persisted outcomes, not merely generated
metadata text. Scheduler-action changes require human review.

## Rollout and Compatibility

Deploy event/metadata readers and checkpoint audit payload support before
enabling their producers. Qualify every deployed Slurm JSON version using
captured, sanitized output. Roll out arrays with eager requeue disabled so
backend retry and checkpoint stop policy precedes creation of a successor.
Retained eager compatibility behavior is not evidence for that guarantee.

No new array-specific database columns or tables are planned. Before enabling
array recovery, inspect existing array-related records for raw-ID/composite-ID
duplicates and missing identity metadata. Preserve historical IDs and audit
records; if reconciliation is needed, design and review it separately instead
of guessing a migration. Resolve how incomplete retained accounting can confirm
an attempt before enabling mutation; if identity cannot be established, defer
the action and report the gap.

Disable scheduler actions for affected array workloads before rolling back to
a controller whose task parsing or action targeting is unqualified. Retain
metadata and audit history, and do not turn composite IDs back into array IDs.
Update the current architecture's simple-job limitation only after the final
qualification evidence passes.
