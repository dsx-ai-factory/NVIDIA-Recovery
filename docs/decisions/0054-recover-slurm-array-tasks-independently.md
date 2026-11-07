# ADR 0054: Recover Slurm Array Tasks Independently

Status: accepted

Date: 2026-11-03

> **Implementation note (2026-11-07):** The user approved the remaining implementation
> slices. Task identity, recovery, and checkpoint association are implemented,
> including replay-stable checkpoint audits described in ADR 0038. Production
> end-to-end qualification remains required before rollout; see the
> [implementation plan](../architecture/job-array-support-plan.md).

## Context

A Slurm job array contains independently scheduled tasks. NVIDIA Recovery must
recognize each task, recover its failures with its own retry allowance, associate
its checkpoint events with its attempts, and retain matching audit history.
Treating the array submission as one recovery job would mix sibling retry counts,
checkpoint progress, Advice, and scheduler actions.

SAC already constructs composite task IDs in parts of its `squeue` and `sacct`
parsers and publishes array identity in scheduler metadata. This is partial
support: the parsers accept different numeric JSON representations, and the
[current architecture](../architecture/slurm-advisor-controller.md#current-limitations)
does not claim supported array recovery. Existing event matching starts from the
event's supplied job ID; an array-level label does not identify a task.

The existing scheduler metadata boundary in
[ADR 0028](0028-project-slurm-job-fields-from-scheduler-metadata.md) lets the
Slurm adapter convey scheduler details without adding Slurm fields to the
Advisor contract. Task support should use that boundary and the existing generic
job/attempt model. It does not require an in-job orchestrator, in-job cycles, or
an array-level backend lifecycle.

## Decision

### Represent each concrete task as a recovery job

Use the canonical string `<array_job_id>_<array_task_id>` as `job_id` for an
array task, for example `4200_0`. Keep a standalone job's existing decimal ID.
The backend treats either form as an opaque job identifier. An array submission
does not get a synthetic recovery job representing all its children.

Parse array identity from `array_job_id` and `array_task_id` in `squeue --json`,
and from `array.job_id` and `array.task_id` in `sacct --json`. Support the plain
integer and typed numeric object representations in supported Slurm versions.
A valid array ID is positive; a concrete task ID is nonnegative, including zero.
For a typed task value, `set` must explicitly be true. Unset array IDs do not
establish array membership; malformed, infinite, or sentinel values must not
become task identities.

Skip synthetic pending-range rows whose task ID is unset. Do not expand them
into fabricated attempts or mistake their numeric placeholder zero for task
zero. Once Slurm exposes a concrete task, ordinary discovery can admit it.
If a row establishes array membership but has missing or invalid task identity,
do not fall back to the bare array ID or raw numeric job ID for publication or
actions. Retain an actionable diagnostic for malformed identity.

Task indices are not inferred from raw numeric job IDs, their ordering, or
filename suffixes. Slurm permits out-of-order task creation and exposes a
distinct raw job ID alongside array identity. Commands that receive a bare
array ID can affect all tasks; task selectors must remain explicit.
See [Slurm's job array documentation](https://slurm.schedmd.com/job_array.html).

An attempt is `(canonical job_id, job_attempt_index)`. Keep the invariant
`heimdall_job_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{job_id}-{job_attempt_index}")`.
The Slurm restart count identifies the task's current attempt; accounting
history reconstruction must group and reconcile attempts within that same task.
Sibling rows, accounting duplicates, and job steps must not advance its attempt
index. Incomplete accounting must not reset an established attempt or retry
allowance.

### Carry Slurm identity through scheduler metadata

Retain and consistently populate these existing metadata keys:

| Key | Value |
| --- | --- |
| `scheduler` | `"slurm"` |
| `slurm_job_id` | Canonical job ID, such as `"4200_0"`. |
| `slurm_raw_job_id` | Slurm's raw numeric job ID as a decimal string. |
| `slurm_array_job_id` | Array submission ID as a decimal string; absent for standalone jobs. |
| `slurm_array_task_id` | Task index as a decimal string, including `"0"`; absent for standalone jobs. |

Job upserts, reads, Advice, and bootstrap must preserve this identity. The
generic `job_id` and `slurm_job_id` must agree, and the array pair must identify
that same task. Reject conflicting identity instead of merging histories or
retargeting actions. An observation missing optional metadata must not erase
known array identity.

API Gateway transports scheduler metadata opaquely. Slurm-specific parsing and
validation belong in scheduler adapters and the existing backend compatibility
boundary. Do not add top-level array ID, task ID, job-group, or in-job-cycle
fields or tables to the backend data model. This extends the metadata pattern
without expanding ADR 0028's legacy typed-field projections.

### Recover and budget tasks independently

Apply the existing `max_requeue_times` setting separately to each task. Preserve
the existing job/system limit resolution and completion policy; no shared array
budget or new task-specific configuration is introduced. With an effective
limit of two, each task can have initial attempt zero and requeued attempts one
and two. Exhaustion by `4200_0` does not consume `4200_1`'s allowance. Controller
restarts and rediscovery do not reset either allowance.

Job Manager evaluates policy and produces task-attempt Advice under
[ADR 0045](0045-require-advice-before-slurm-requeue.md). SAC applies restart,
constraint updates, release, termination, and any step signal only to the
selected task. Seen-state, accounting lookups, recovery-sequence identity, Advice
generation status, and watchdog timers must use the same task-attempt identity.
Fresh state for one sibling cannot satisfy another's Advice. Existing admission,
observe mode, action acknowledgement, and fresh-state safeguards still apply.

Distinguish the workload's requeue allowance from retries of a failed scheduler
command under [ADR 0043](0043-bound-failed-job-requeue-retries.md) and
[ADR 0044](0044-budget-failed-job-recovery-phase.md). Repeated delivery or command
retries do not themselves create a new task attempt. The retained eager
compatibility path must keep its existing job-limit checks and command-retry
bookkeeping scoped to each task. This does not add backend policy to SAC. Its documented
race with effective system/job limits and checkpoint stopping remains. The full
policy guarantee in this ADR requires Advice-only recovery; array rollout and
acceptance testing use that mode.

### Associate checkpoint events with the corresponding task attempt

Normalize Slurm event identity before job lookup, deduplication, checkpoint
persistence, and audit creation. Accept an already canonical task `job_id`, or
resolve it from an explicit array/task pair carried in the event's optional
`attributes.scheduler_metadata` map, using the same keys and types above.
Log ingestion may translate scheduler-specific labels into this metadata at its
adapter boundary. An explicitly identified `slurm_raw_job_id` can resolve to a
task only through unambiguous stored scheduler metadata for that task; do not
infer it from numeric ordering. A bare array-level `job_id` must not be treated
as a raw-task alias merely because a task's raw ID equals the array ID.
Conflicting supplied identities remain unmatched. The event envelope remains
generic; it does not gain fixed Slurm array fields.

Use the resolved task ID throughout Watcher and Job Manager. Within that task,
preserve existing attempt matching: an explicit `slurm_job_attempt` is
authoritative; otherwise use trustworthy log-path and occurrence-time evidence.
Shared log paths alone cannot distinguish siblings or attempts. An explicit
attempt awaiting registration must not fall through to a sibling or newer
attempt. Use the existing bounded redelivery behavior for registration races;
ambiguity or exhausted matching must be diagnosed, not assigned to a guessed
task. Persist using the resolved record's identity rather than the original
array-level or raw event label.

Checkpoint-save events update only the matching task attempt's checkpoint
history. Existing checkpoint-progress stopping examines only that task's
attempts; a sibling's checkpoint does not reset its progress history or retry
allowance. Checkpoint-load events retain their existing non-progress semantics
and must not be counted as a new saved checkpoint.

Array-ID-only events cannot establish which task emitted a checkpoint. In this
iteration they remain unassociated; do not fan them out to every task, select
the latest sibling, or create an array-level checkpoint record. A future
iteration may support array-level checkpoint event association, with an
explicit scope and separately defined effects on task recovery policy.

### Store audit events under the same task identity

Task events must receive the same audit coverage as standalone jobs across
Watcher detections, Job lifecycle changes, attribution, Advice decisions,
scheduler actions and outcomes, and watchdog releases. Envelope
`heimdall_job_id` and payload job/attempt identity must name the same resolved
task attempt. Distinct sibling events must not collide in producer
deduplication or idempotency handling.

Add explicit typed checkpoint-association audit events for matched save and
load events, naming the task, attempt, source event UUID, checkpoint kind, and
source occurrence time. Job Manager owns these after successful association
and, for saves, checkpoint persistence. An unresolved event must not claim a
successful association. Do not rely on generic Job-change audits to cover
checkpoints: their approved snapshot intentionally excludes checkpoint lists.
This adds event coverage to [ADR 0038](0038-catalog-audit-event-payloads-and-identifiers.md)
without broadening the Job snapshot allowlist.

Notifications remains the audit storage owner, with existing asynchronous,
best-effort delivery and producer idempotency. Array support does not add a
durable outbox or make recovery wait for audit ingestion. Under healthy delivery,
persisted entries must be retrievable through existing exact-attempt and
whole-job reads from [ADR 0050](0050-expose-authenticated-whole-job-audit-reads.md):
`job_id=4200_0` selects that task's attempts, not every task in array `4200`.
Trace sampling must not suppress these audit events, consistent with
[ADR 0048](0048-start-recovery-traces-in-backend-and-share-observations.md).

## Consequences

- The backend's existing job/attempt, Advice, checkpoint, and audit mechanisms
  can serve arrays without a Slurm-shaped hierarchy. Metadata keys and task
  normalization become a cross-component compatibility contract.
- Recovery and checkpoint progress remain isolated even when tasks share an
  array submission, retry configuration, log file, or scheduler node.
- Synthetic pending ranges are not individually represented until concrete
  scheduler observations exist. Array-wide views, array-wide recovery policy,
  and array-level checkpoint association are outside this iteration.
- Array-aware log producers must supply enough task identity. Bare array labels
  and ambiguous historical data cannot be safely repaired by guessing.
- Existing records already using canonical task IDs retain their deterministic
  identities. Do not silently rename legacy raw-ID records or rewrite audit
  history; any required data reconciliation needs a separately reviewed plan.
- Scheduler mutations, checkpoint policy, and identity propagation require
  cross-component and scheduler-harness review before claiming support.

The [implementation plan](../architecture/job-array-support-plan.md) defines
the proposed PR stack, rollout constraints, and behavioral acceptance evidence.

## Alternatives Rejected

- **One recovery job and retry budget per array.** Conflates independently
  failing tasks and lets one task's retries or checkpoints affect its siblings.
- **Use raw numeric job IDs as the public task identity.** Loses the explicit
  array/task identity used by event producers and scheduler selectors, and
  conflicts with SAC's existing composite identity.
- **Add Slurm array fields and an array lifecycle to the backend schema.**
  Couples generic recovery to a scheduler hierarchy that this iteration does
  not need; scheduler metadata already carries the required information.
- **Port in-job cycles and orchestrator integration along with array parsing.**
  Adds another execution and recovery lifecycle without a current requirement.
- **Associate an array checkpoint with all tasks or the latest sibling.**
  Manufactures progress evidence and can change retry stopping incorrectly.
- **Require synchronous durable audit delivery before recovery.** Changes the
  accepted audit availability contract independently of task support.
