# ADR 0035: Enrich Active Slurm Log Metadata Asynchronously

Status: accepted

Date: 2026-09-16

> **Implementation note (2026-10-06):** The stable-parent rule below replaces [ADR 0003](0003-discover-slurm-job-log-paths.md)'s proposed periodic parent refresh as well as moving active supplemental discovery into the background; the introductory statement about unchanged contracts must be read with that exception. The [implementation](../../slurm-advisor-controller/internal/slurm/log_metadata.go) follows this rule and now selects [direct legacy text discovery or JSON-only discovery for newer and unknown clients](../operators/slurm.md#log-path-discovery) once at startup, retaining the same background cache and limits.

Related:
[ADR 0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md) defines how
Slurm log paths are represented and projected at the Job Manager boundary.
[ADR 0003](0003-discover-slurm-job-log-paths.md) defines which parent and
numeric-step paths are discovered, exact-attempt attribution, validation,
last-known-good behavior, and retry policy. This decision changes where active
supplemental discovery runs; it does not change those contracts.
[ADR 0020](0020-bound-slurm-controller-publication-and-accounting-concurrency.md)
keeps complete reconciliation ticks serialized while permitting bounded
independent I/O.
[ADR 0021](0021-measure-slurm-controller-dependency-latency.md) defines the
metrics needed to distinguish source latency from individual dependency calls.

## Context

The Slurm Advisor Controller (SAC) publishes Slurm stdout paths as
`scheduler_metadata.slurm_log_file_paths`. These paths help NVIDIA Recovery correlate
logs with one exact job attempt, but they do not determine job state, recovery
eligibility, Advice handling, or scheduler actions.

SAC currently enriches every admitted active-job snapshot before it continues
job reconciliation. The active `squeue` response supplies a parent path on
Slurm versions that expose `stdout_expanded`. For missing parent paths and
numeric-step paths, SAC performs supplemental per-job `sacct` queries and may
fall back to `scontrol show job -o <job-id>`. The tick waits for this complete
bounded enrichment pass even though the result is optional metadata.

This creates the following critical path:

```text
squeue active-job snapshot
        |
        v
supplemental sacct and scontrol log-path queries
        |
        v
publish job state and reconcile recovery
```

Production measurements demonstrated the scaling problem. With approximately
383 active jobs, an eight-worker enrichment pass repeatedly exhausted its
30-second deployment deadline. Active Slurm polling averaged about 2.6 seconds,
log-metadata enrichment averaged about 36.2 seconds including cancellation and
logging overhead, the job-status-polling stage averaged about 39.3 seconds, and
the full tick averaged about 55.3 seconds despite a 15-second polling interval.
Approximately 95 percent of measured enrichment passes took between 30 and
40 seconds.

A single read-only `sacct` or `scontrol` request can still complete quickly.
The bottleneck is the number of per-job commands, fallback work, retries, and
cancellation diagnostics accumulated in one serialized tick. Lowering the
enrichment deadline limits the damage but still makes optional metadata part of
the state-reconciliation latency budget.

The existing process-local log-metadata result cache,
`logMetadataEnricher.attempts`, already preserves parent and step paths per
exact `(JobID, AttemptIndex)`, separates source success from failure, retains
last-known-good paths, applies retry backoff, prunes attempts no longer
observed, and is rebuilt after restart. It stores normalized paths and
refresh/retry state, not raw `sacct` or `scontrol` output and not complete
active-job records. It is currently accessed as part of the synchronous tick
and is not a concurrency boundary for a long-lived worker.

This decision reuses that cache rather than introducing a second one. The
implementation makes the same logical cache safe to share between the tick and
background workers and adds only the coordination state needed for queued,
in-flight, and generation-checked work.

## Decision

### Keep ownership in the Slurm adapter

The Slurm adapter owns the background service because it owns Slurm commands,
response parsing, path normalization, exact-attempt attribution, and the
existing metadata cache. Reconciliation supplies fresh admitted active-job
records and receives copies decorated from cache; it does not parse command
output, manage workers, or gain Slurm-specific retry rules.

Process wiring starts one service before the reconciliation loop and closes it
during normal controller shutdown. There is not one service per tick and no
background service is created lazily per job. Tests may inject the established
command executor and clock boundaries, but the production scheduling and cache
interfaces remain the interfaces under test.

### Remove supplemental active log discovery from the serialized tick

The active-job polling path will perform only these log-metadata operations
synchronously:

1. Read the normal structured `squeue` snapshot.
2. Accept and validate an expanded parent stdout path already present in that
   snapshot. This requires no additional Slurm command.
3. Add paths already held in the exact-attempt cache to the fresh active-job
   record.
4. Non-blockingly schedule missing or stale supplemental work.

The tick does not wait for supplemental `sacct` or `scontrol` work. It proceeds
with job publication, recovery evaluation, and Advice reconciliation using the
fresh scheduler state. Missing log paths never change a job state, suppress a
recovery action, fail a tick, or delay a scheduler action.

Supplemental active-job discovery runs in a process-local background service
whose lifetime spans reconciliation ticks:

```text
serialized SAC tick                         background enrichment

squeue snapshot
      |
      +-- use squeue path and cached paths
      |
      +-- offer exact-attempt work --------> deduplicate and queue
      |
      v                                      bounded workers
continue reconciliation                          |
                                                 +-- sacct
                                                 +-- optional scontrol fallback
                                                 |
                                                 v
                                            update cache
                                                 |
                                                 v
                                      next fresh job publication uses paths
```

Workers may start before the submitting tick ends and may continue after it
ends. A subsequent tick may observe a completed cache update. Complete
reconciliation ticks remain serialized as required by ADR 0020; only this
read-only, best-effort collaborator overlaps them.

### Use one exact-attempt, source-aware work identity

Work and cache entries are keyed by `JobAttemptRef`, which contains the
canonical Slurm job identity and attempt index. A recycled job ID or requeued
attempt must not reuse another attempt's paths.

Within one attempt, parent and numeric-step discovery retain separate state.
The coordinator records, at minimum:

- normalized last-known-good paths;
- whether a source has produced a successful result;
- last observation, attempt, and success times;
- next eligible retry time and failure count;
- whether work is queued or in flight; and
- a cache generation used to reject a result for an evicted or replaced entry.

At most one work item for an exact attempt is queued or in flight. Repeated
ticks update observation state but do not create duplicate commands. A worker
performs the established `sacct` discovery first and uses `scontrol` only when
the parent path remains unresolved and the work budget permits it.

`sacct --duplicates` returns records that can be attributed to an exact
attempt. The `scontrol show job` fallback is different: it describes the
job's current attempt even when the work item was admitted for an older one.
This matters when a job is requeued while background work is in flight. The
resolved path often remains unchanged, but it can differ when the filename
uses an attempt-sensitive replacement such as Slurm's restart-count token.

Before accepting an `scontrol` result, the worker must verify that the
response's restart or attempt counter matches the `AttemptIndex` captured by
the work item. If the response does not provide a verifiable attempt identity,
or if the identity no longer matches, the result is discarded rather than
written to either attempt's cache. Discarding the result does not mark the
older attempt as successfully enriched. That attempt remains unresolved and
is handed to finalized `sacct --duplicates` reconciliation, which can
attribute historical paths and publish them only to the matching Job Manager
attempt row. The newer attempt is admitted separately after its next fresh
scheduler observation and publishes only to its own Job Manager attempt row.
This preserves enrichment for every observable attempt without assigning one
attempt's paths to another. Paths remain best-effort when Slurm does not expose
them through any supported source.

Cache-generation validation still applies, but it does not replace this
source-level identity check because a requeue can occur before SAC observes
the new attempt in its next tick.

### Treat cache completeness differently for parent and step paths

For one exact attempt, a valid nonempty parent path is stable. Once the parent
path has been obtained from `squeue`, `sacct`, or `scontrol`, active enrichment
does not periodically query that source again unless the cache entry is lost.

Numeric-step paths can appear after the allocation starts. A successful step
lookup therefore does not make the active attempt permanently complete. Step
metadata becomes eligible for refresh after the configured refresh interval,
initially five minutes, while the attempt remains active. A successful refresh
replaces the step-source cache, including a successful empty result, according
to ADR 0003. The deterministic published value remains the normalized union of
parent and step source caches.

A missing or failed lookup is also cached as retry state. It does not create a
tight retry loop. Source-specific bounded exponential backoff starts from the
configured retry base, initially the poll interval, and is capped by the normal
refresh interval. Success resets the backoff. Failure retains last-known-good
paths.

Job state is used only to manage the metadata lifecycle:

- an active attempt may need later step refreshes;
- a state transition may make unresolved parent or step metadata eligible for
  refresh;
- a terminal attempt may need the existing final accounting enrichment; and
- an attempt no longer observed becomes eligible for eviction.

State is not an input to path validity and metadata availability is not an
input to recovery behavior.

### Keep the existing log-metadata result cache bounded and process-local

The cache avoids repeating the same supplemental Slurm commands every tick and
holds asynchronous results until a later fresh job record can publish them. It
remains an optimization and availability mechanism, not scheduler authority.
The initial retention rules remain:

- update `lastObservedAt` only from a fresh scheduler observation, not from a
  background completion;
- remove an attempt after it has not been observed for twice the refresh
  interval, initially ten minutes;
- cap the total number of cached attempt entries, initially at 65,536, and
  remove the least-recently-observed entries in their entirety, regardless of
  enrichment state, when that limit is exceeded;
- enforce the existing aggregate cached-path memory ceiling, initially
  64 MiB, by evicting path values from least-recently-observed entries; and
- discard the complete cache and queue on SAC restart.

The 64 MiB ceiling measures the sum of normalized cached path-string bytes. It
does not include raw Slurm command output or active-job records. It remains an
internal implementation default rather than requiring a new deployment setting
in the first implementation. The separate attempt-entry limit bounds retry,
timestamp, and coordination metadata even when entries contain no paths. Both
limits are internal defaults. Cached path bytes, cached attempt entries, and
limit-triggered evictions must be observable so the defaults can be revisited
using production measurements. The cache remains bounded in both dimensions
even if the initial values are adjusted later.

An in-flight result carries the cache generation captured at admission. If its
entry has been evicted or replaced before completion, the result is discarded
instead of recreating stale state. Background work cannot extend an entry's
retention merely by finishing late.

Pruning and worker completion enforce the same limits while holding the cache
lock. A completion first validates its generation, applies its candidate
update, and enforces the attempt-entry and path-byte limits before making the
result visible. The lock is not released while either limit is exceeded. This
means a result admitted before path eviction cannot restore paths above the
ceiling, while removal of an entire attempt entry still makes that worker's
generation stale.

The cache and queued/in-flight flags are concurrency-safe. External commands
must never execute while holding the cache lock. Admission captures immutable
work, releases the lock, and sends it to a worker. Completion validates the
generation and updates the cache under a short lock. The tick reads a copied
snapshot of cached paths and never shares mutable slices or maps with workers.

### Bound background work independently of tick duration

The service uses a validated fixed worker limit and a bounded queue. It does
not launch one goroutine per job. Command execution retains explicit deadlines,
bounded immediate attempts, and a global command-admission limit so moving work
off the tick cannot move the bottleneck into SlurmDBD, the Slurm controller,
the SSH login node, or the local process.

Submitting work from a tick is non-blocking. If the queue is full, the cache
entry remains due and not queued; a later observation may offer it again after
retry eligibility. Queue saturation cannot block job polling, and it must be
visible through bounded metrics.

Tick frequency alone does not multiply background work. An exact attempt can
have at most one queued or in-flight item, so later ticks coalesce observations
instead of enqueueing duplicates. The real arrival rate comes from newly
observed attempts and metadata that becomes eligible for refresh. Startup
bursts, high job turnover, or slow Slurm commands can still make that arrival
rate exceed worker throughput.

During sustained overload, job-state and recovery reconciliation continue and
log metadata is allowed to become stale. Queue admission prioritizes the
oldest-due attempts and must not let repeatedly observed jobs starve other due
attempts. The first implementation uses validated fixed queue and worker
defaults. Additional operator settings should be added only when production
metrics show that a deployment needs them.

The existing active-pass timeout no longer defines how long job polling waits.
Its implementation replacement must bound background item and command lifetime
rather than reintroduce a tick-level wait. Configuration migration must be
explicit: deployments with a large historical
`SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT` value must not silently turn it
into an equally large critical-path deadline.

On process shutdown, the service stops admitting work, cancels queued and
in-flight commands through the process context, and joins workers within the
normal shutdown boundary. No scheduler-mutating command is part of this
service.

### Publish only from fresh job records

A background completion updates only the metadata cache. It does not directly
upsert the full job record captured when work was scheduled. The next regular
active-job publication combines the cached paths with a newly observed job
record and publishes through ADR 0002. This intentionally permits one or more
ticks of eventual-consistency delay and prevents late metadata from overwriting
newer state, Advice-related fields, or other scheduler metadata.

The first implementation moves active supplemental enrichment only. Finalized
job accounting and its final log-metadata enrichment retain their current
bounded reconciliation path. That path can consume the same cache and provides
a final opportunity to publish paths when an attempt leaves `squeue`. Moving
finalized enrichment into the background would require either a narrow,
server-side metadata merge contract or another fresh terminal observation; it
must not be implemented as a stale full-record upsert without a follow-up
decision.

### Make degradation and saturation observable

The source-level `slurm_log_metadata` measurement must no longer add
supplemental command duration to `job_status_polling`. Cache decoration and
non-blocking admission may be measured separately, but an asynchronous command
must be attributed to the background service rather than to the tick that
offered it.

Following ADR 0021's bounded-label rules, telemetry must distinguish:

- cache hits, misses, and stale entries by the closed parent/steps source;
- current cached path bytes, cached attempts, active internal limits, and
  evictions by the closed reasons `ttl`, `attempt_limit`, and `path_bytes`;
- work offered, deduplicated, admitted, rejected because the queue is full,
  started, completed, failed, timed out, canceled, and discarded as stale;
- queue depth, worker limit, workers in flight, and oldest queued-work age;
- `sacct` and `scontrol` command duration and outcome; and
- paths discovered and attempts carrying published cached metadata.

Metrics must not contain job IDs, attempt indexes, paths, usernames, command
output, or raw errors. Detailed identifiers remain in bounded logs. A wave of
deadline cancellations is summarized or rate-limited; it must not emit one
large stack trace per job and extend worker completion through logging
backpressure.

### Preserve failure isolation

The required behavior for important failure cases is:

| Failure | Required behavior |
| --- | --- |
| `sacct` fails | Retain cached paths, apply source backoff, and allow the optional `scontrol` fallback only when its established conditions and budget allow. |
| `scontrol` fails | Retain cached parent path, record backoff, and finish the work item without affecting reconciliation. |
| Output is malformed | Treat it as a source failure; never replace last-known-good paths with partially parsed data. |
| Queue is full | Leave the entry due for a later non-blocking offer and increment a bounded saturation metric. |
| Job changes state while work runs | Accept the result only for the same exact attempt and live cache generation; an `scontrol` result whose attempt identity is missing or no longer matches is discarded. State reconciliation proceeds independently. |
| Job disappears or cache entry is evicted | Discard a late result and rely on finalized accounting when applicable. |
| Job Manager is unavailable | Normal publication reports its own failure; metadata workers continue to update only the local cache and do not create a second publication retry system. |
| SAC restarts | Lose process-local work and cache, then rebuild lazily from subsequent fresh observations. |

### Keep adjacent behavior out of scope

This decision does not read, tail, copy, upload, or retain log-file contents.
It discovers and publishes scheduler-reported path strings only. It does not
authorize a scheduler action, overlap complete reconciliation ticks, introduce
a durable work queue, change the scheduler-metadata key or Job Manager
projection contract, or move finalized enrichment into the background in the
first implementation. Log collection, filesystem mounts, optional link
materialization, and log-query backends retain their existing owners.

### Verify behavior and boundedness

Tests must cover:

- job publication and recovery proceeding while metadata commands are blocked;
- no supplemental active metadata command extending
  `job_status_polling` duration;
- work continuing across tick boundaries without overlapping complete ticks;
- exact-attempt deduplication across repeated observations and requeues;
- rejection of an `scontrol` result when a requeue changes the current attempt
  after work was admitted, including an attempt-sensitive output path, followed
  by finalized publication for the older attempt and separate active
  publication for the newly observed attempt without cross-attempt paths;
- rejection of an `scontrol` result with no verifiable attempt identity without
  writing it to either attempt cache or marking the older attempt complete;
- one in-flight item per attempt under race-enabled execution;
- immediate use of structured `squeue` parent paths without queueing fallback
  work;
- parent completion, periodic active-step refresh, successful empty step
  replacement, partial success, and last-known-good preservation;
- retry backoff, queue saturation, command deadlines, cancellation, and clean
  shutdown;
- work coalescing across fast ticks, oldest-due admission, and sustained
  producer load exceeding worker throughput without delaying reconciliation;
- eviction during in-flight work and stale-generation result rejection;
- cache TTL, attempt-entry-limit, and path-byte-limit pruning based on scheduler
  observation time, including rapid job turnover where every entry is still
  incomplete or pathless;
- a completion racing with path-value eviction while the path-byte limit
  remains satisfied whenever the cache lock is released;
- fresh-record publication on a later tick and no worker-originated stale
  full-record upsert;
- the retained finalized-enrichment handoff when an active job disappears;
- metrics, closed label vocabularies, and absence of job-derived labels; and
- a scale test where hundreds of active jobs cannot make metadata enrichment
  delay state reconciliation.

## Consequences

Job state, cancellation, recovery, and Advice processing no longer wait for
best-effort active log discovery. A slow or overloaded Slurm metadata source
reduces log-path freshness instead of reducing SAC's state-reconciliation
frequency.

Log paths become more explicitly eventually consistent. A path discovered
after a tick normally appears in Job Manager on the next fresh publication.
Consumers must continue to tolerate a missing `slurm_log_file_paths` key, as
already required by ADR 0002.

SAC gains a long-lived concurrent subsystem with queue, cache, cancellation,
and shutdown ownership. The implementation and tests are more complex than a
synchronous function call. Bounded workers, non-blocking admission, exact
attempt identity, generation checks, and short cache critical sections are
required to keep that complexity isolated.

The process-local cache can lose work and paths on restart. This is acceptable
because Slurm remains the source of truth and metadata is rebuilt lazily. A
durable queue or cache would add database ownership, cleanup, migration, and
cross-replica coordination without improving scheduler correctness.

The background service can still overload Slurm if its command rate is tuned
poorly. Removing work from tick latency is not permission for unbounded fan-out;
operators need the queue and operation measurements before increasing limits.

Finalized log enrichment remains on its existing bounded path until a safe
late metadata publication contract is selected. This ADR removes the observed
active-job bottleneck without allowing a background worker to overwrite a
terminal record with stale state.

## Alternatives Rejected

### Only reduce the enrichment timeout

A shorter timeout protects the tick and should be used as immediate
containment, but optional work remains on the critical path and cancellation
overhead still scales with the number of in-flight commands.

### Fetch metadata only once per attempt

The parent path is stable once resolved, but numeric-step paths may appear
throughout a running allocation. One lookup can permanently miss later steps.
Periodic step refresh plus one finalized opportunity preserves the required
correlation behavior without repeatedly querying a resolved parent path.

### Run one asynchronous goroutine per observed job

Unbounded goroutines and subprocesses can exhaust file descriptors, overload
Slurm services or the SSH login node, and make shutdown unpredictable. A
bounded, deduplicating service provides explicit backpressure.

### Let workers asynchronously upsert captured job records

The record captured when work begins can be older than the job state and
metadata in a later tick. Upserting it after enrichment risks overwriting newer
information. Cache-only completion followed by fresh-record publication keeps
job-state authority in the serialized reconciler.

### Persist the work queue and cache

Log paths are reconstructible best-effort metadata. Durable storage would need
schema, migration, cleanup, ownership, and failover semantics disproportionate
to the consequence of temporarily missing a path.

### Remove supplemental discovery

Structured `squeue` does not provide every parent path on every supported Slurm
version and does not replace numeric-step accounting metadata. Removing
supplemental discovery would break the correlation behavior established by
ADRs 0002 and 0003.
