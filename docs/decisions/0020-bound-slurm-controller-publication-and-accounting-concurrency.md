# ADR 0020: Bound Slurm Controller Publication and Accounting Concurrency

Status: accepted

Date: 2026-08-27

Related:
[ADR 0003](0003-discover-slurm-job-log-paths.md) requires Slurm metadata
enrichment to remain bounded and best effort.
[ADR 0010](0010-resolve-squeue-final-state-during-completing.md) defines how
the controller selects fresh Slurm state during the completing window.
[ADR 0014](0014-route-slurm-jobs-by-aps-environment.md) requires environment
admission before publication or scheduler work.

> **Implementation note (2026-10-06):** [ADR 0046](0046-preserve-successful-slurm-accounting-results.md) supersedes the all-or-nothing accounting publication rule; successful reads may be published after partial failure, while scheduler actions and Advice reconciliation still require a complete accounting phase.

## Context

The Slurm Advisor Controller is a synchronous, level-triggered reconciler. It
builds one fresh snapshot and then publishes job state and evaluates recovery
actions. Independent source reads and active log-metadata enrichment already
use bounded concurrency, but two I/O-heavy paths remain serial:

- every admitted active or finalized attempt is upserted to NVIDIA Recovery one at a
  time;
- every unique job ID requiring targeted accounting can cause a separate
  `sacct` invocation, also one at a time.

During a scale incident with more than 150 active jobs, one reconciliation
reached 849 seconds. The job-reconciliation stage averaged 370 seconds and the
job-status-polling stage averaged 479 seconds, while failed-job reconciliation
was negligible. Filtering historical jobs and Advice reduces unnecessary
accounting candidates, but the remaining necessary I/O still scales as the sum
of individual request latencies.

Unbounded concurrency would move the bottleneck into NVIDIA Recovery, Job Manager,
SlurmDBD, or the operating system. Overlapping complete reconciliation ticks
would also break the controller's single-snapshot model and race process-local
recovery and metadata state.

## Decision

### Keep reconciliation ticks serialized

The process continues to run at most one `SyncOnce` call at a time. Snapshot
construction, mode resolution, failed-job actions, and Advice actions retain
their existing phase ordering. This decision does not parallelize scheduler
mutations.

### Use a bounded pool for NVIDIA Recovery job upserts

The controller may publish different exact job attempts concurrently through a
dedicated, process-local worker pool. The pool has a validated positive
worker-pool size and a conservative default. Its limit is independent of
Slurm accounting concurrency so the two upstream systems have separate
backpressure domains.

Per-attempt ordering remains unchanged:

- an active attempt is marked seen before its upsert is submitted;
- a finalized attempt is forgotten only after its upsert succeeds;
- ignored and environment-rejected attempts are forgotten without publication.

Workers return indexed results to the coordinating goroutine. The coordinator
assembles errors and observations deterministically and is the only writer to
the reconciliation observation. An upsert failure remains isolated to that
attempt and does not cause a finalized attempt to be forgotten.

Cancellation stops admission of new work. Already-started workers receive the
same cancellation context and are joined before the phase returns. Independent
upsert failures remain joined so their causal identities are preserved.

### Bound necessary targeted accounting

Candidate filtering happens before concurrency: only exact attempts that still
require fresh accounting evidence enter this path. The remaining attempts are
grouped by unique job ID. The controller issues exactly one
`sacct -Xj <job-id> --duplicates --json` request for each unique job ID through
a separate bounded pool; it does not combine multiple job IDs in one `sacct`
invocation. One job ID is processed by at most one worker in a tick.
Workers may complete in any order. The coordinator retains each result under
its requested job ID. After all workers complete, it processes records in
lexicographic job-ID and ascending attempt-index order and joins request errors
in lexicographic job-ID order. Worker completion order is not observable.
A phase-level deadline bounds the pool in addition to the existing per-command
timeout. Supplemental log-metadata enrichment retains its separate deadline,
command budget, and retry behavior.

Targeted accounting preserves the current snapshot failure contract: if any
required lookup or parse fails, the controller returns a joined error and does
not continue with a partially trusted accounting snapshot. Read-only commands
that completed before cancellation need no rollback.

### Prove boundedness and ordering

Tests must exercise production interfaces and cover:

- a hard maximum on simultaneous NVIDIA Recovery and Slurm operations;
- overlap between independent jobs;
- one `sacct` request per unique job ID and no multi-ID invocation;
- preserved active and finalized seen-store ordering;
- stable result and joined-error assembly;
- cancellation while work is queued and in flight;
- exact-attempt attribution for parallel accounting results; and
- race-enabled execution of the affected Go module.

The operation and workload measurements required to tune these pools are
defined separately by ADR 0021.

## Consequences

Job publication and targeted accounting latency become proportional to bounded
waves of independent I/O rather than the sum of every individual latency. A
slow request no longer prevents all unrelated requests from starting.

NVIDIA Recovery, Job Manager, and Slurm accounting will observe more simultaneous
requests. Separate limits allow operators to protect each dependency, but
defaults must remain conservative and deployments must tune them from measured
latency and saturation rather than CPU count alone.

Completion order becomes nondeterministic even though snapshot contents,
telemetry aggregation, and returned error ordering remain deterministic. Tests
and fakes that record calls must therefore be concurrency-safe and must not
assert incidental global call order.

The one-job-ID request boundary preserves the inherited accounting query and
duplicate-record behavior. The controller reduces latency through bounded
concurrency without expanding one command's attribution or failure domain to
unrelated jobs.

This decision improves throughput but does not remove unnecessary work.
Backend filtering, Advice filtering, and suppression of already-satisfied
durable recovery candidates remain complementary optimizations.

## Alternatives Rejected

### Keep both paths serial

Serial execution has simple ordering, but observed reconciliation duration is
not sustainable above the current active-job scale. Independent I/O does not
need global serialization to preserve per-attempt state transitions.

### Launch one goroutine per job

Unbounded fan-out makes latency attractive in small tests but can exhaust file
descriptors, subprocess capacity, database connections, SlurmDBD capacity, or
Job Manager workers. It also gives operators no stable load ceiling.

### Batch multiple job IDs in one `sacct` invocation

The inherited WFC accounting parser treats one `sacct --duplicates` response as
the history of one job ID and applies per-job attempt attribution. Combining
job IDs would require a new parsing and failure contract across recycled IDs,
requeues, arrays, duplicate rows, and supported Slurm versions. Bounded
concurrency provides independent progress without changing that established
request boundary.

### Overlap complete reconciliation ticks

Overlapping ticks would mix snapshots observed at different times and race
process-local caches, recovery suppression, telemetry, and scheduler actions.
The controller retains one serialized level-triggered loop.

### Use one shared pool for NVIDIA Recovery and Slurm

A shared pool couples unrelated dependencies. Slow `sacct` commands could
starve job publication, while slow NVIDIA Recovery calls could prevent required
accounting reads. Each dependency requires its own concurrency and pressure
limit.

### Parallelize scheduler mutations with the read and publication work

Scheduler actions have stronger ordering, idempotency, and human-review
requirements than read-only accounting or idempotent publication. They remain
serialized outside this decision.
