# ADR 0003: Discover Slurm Job Log Paths

Status: proposed

Date: 2026-07-27

> **Implementation note (2026-10-13):** Accepted [ADR 0035](0035-enrich-active-slurm-log-metadata-asynchronously.md) governs active background enrichment; [PR 216](https://github.com/NVIDIA/NVIDIA-Recovery/pull/216) adds [startup-selected text or JSON discovery](../operators/slurm.md#log-path-discovery), and [PR 295](https://github.com/NVIDIA/NVIDIA-Recovery/pull/295) allows text discovery to include application output flags, differing from the universal JSON-only and native-only rules below. Acceptance of this proposal's remaining terms is unresolved.

Builds on: [ADR 0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md),
which defines publication through Slurm scheduler metadata and translation at
the Job Manager compatibility boundary.

Related: [ADR 0004](0004-materialize-slurm-log-links.md) proposes the optional
filesystem materializer that would consume the paths discovered here.

## Context

NVIDIA Recovery correlates log observations with a specific job attempt. The retired
Slurm Workflow Controller discovered the parent job's `StdOut` path and custom
step output paths, then published them to Job Manager. The standalone Slurm
Advisor Controller currently publishes scheduler state without those paths, so
Watcher can fall back to weaker correlation signals or fail to associate a log
filename with the intended attempt.

The inherited implementation is useful behavioral evidence but is not an
architecture to copy literally:

- It reads the parent path from text returned by `scontrol show job`.
- It reads custom-step submit commands from `sacct` and parses `--output`, `-o`,
  and configured application-specific options itself.
- It expands only `%x`, `%j`, and `%A`, while Slurm supports a larger filename
  pattern vocabulary.
- It accepts only an inherited APS filename prefix, creating the strict naming
  onboarding requirement that the OSS product intends to relax.
- It combines discovery with optional filesystem symlink behavior that requires
  permissions and deployment mounts unrelated to metadata correlation.

The Advisor Controller already polls structured `squeue` and `sacct` data
through a local Slurm command boundary. It must retain reliable state
publication when supplemental log metadata is unavailable, and it must not add
unbounded per-job command work to the normal polling loop.

## Decision

### Keep Slurm discovery in the Slurm adapter

The Slurm adapter owns Slurm commands, response shapes, array identity, output
patterns, and path normalization. Reconciliation may orchestrate best-effort
metadata enrichment, but it must not parse Slurm command output or make Slurm
filename rules part of the scheduler-neutral job model.

Discovery uses the controller's existing local command executor and timeout
boundary. Legacy Workflow Controller SSH and Paramiko execution paths are not
ported.

### Prefer scheduler-expanded structured output

For active parent jobs, use the structured, expanded stdout value supplied by
the pinned Slurm JSON representation after proving its exact shape with
redacted fixtures. For finalized jobs and custom steps, use a supplemental,
targeted, all-row `sacct --duplicates --json` query with Slurm pattern
expansion. The existing status query uses `-X`, which excludes steps, and
therefore cannot discover custom-step paths.

Use `scontrol show job` as a compatibility fallback for the parent path when
the supported Slurm representation does not expose an expanded value. Do not
maintain an independent controller-side table that attempts to reproduce
Slurm's full filename-pattern language.

Native `srun --output` and `-o` paths come from Slurm accounting metadata rather
than reparsing the submitted shell command. Parsing `SubmitLine` is permitted
only for a separately approved, explicitly configured application-log option
that Slurm does not represent as stdout metadata.

### Preserve the useful Workflow Controller semantics

The first implementation discovers stdout paths only:

- Include the parent job stdout and every valid numeric custom-step stdout.
- Exclude Slurm's synthetic `.batch` and `.extern` steps from custom-step
  enumeration; the parent path already represents batch output.
- Support standalone jobs and array tasks using the controller's existing
  canonical, raw allocation, array-job, and array-task identities.
- Associate every published path with one exact `(JobID, AttemptIndex)` rather
  than with a Slurm job ID alone.
- Return partial results when one metadata source fails.
- Deduplicate paths and publish them in deterministic order.

Treat discovered paths as opaque scheduler metadata. Validate UTF-8, reject NUL
characters, and enforce bounded path count and length, but do not require a
particular basename, extension, directory, absolute-path form, filesystem
existence, or readability. Valid Slurm defaults such as `slurm-%j.out` must not
be rejected by an APS-specific naming rule.

### Associate accounting rows with exactly one attempt

A published path list belongs to exactly one controller `JobAttemptRef`. For an
active job, key it by the canonical `squeue` identity and `restart_cnt`, matching
the existing active-job conversion. Never seed a new attempt's cache with paths
from the previous attempt.

Accounting enrichment reuses the controller's existing `sacct` attempt
algorithm rather than defining a second attempt index:

- Filter synthetic array-parent rows and group allocation rows by canonical job
  identity: the job ID for a standalone job, or the array job and task IDs for
  a concrete array task.
- Preserve `sacct --duplicates` response order.
- Collapse allocation records that share the same `(submission, start)` pair
  unless both timestamps are zero. Treat each `(0, 0)` record as distinct.
- Assign the zero-based `AttemptIndex` after that collapse.

Accept numeric step stdout only from step records structurally nested beneath
allocation records in that deduplicated attempt group. When duplicate allocation
rows collapse into one attempt, union their numeric-step paths. A raw job ID or
numeric step ID is not an attempt identity because either may recur after a
requeue.

If the pinned Slurm representation cannot prove which allocation attempt owns
a step, omit that step path from the attempt and emit bounded diagnostics rather
than assigning it by proximity. The parent path may still be published when its
attempt ownership is known.

### Make enrichment bounded and failure-tolerant

Job-state publication must continue when log discovery fails. Supplemental
queries use bounded retries and a per-job-attempt metadata cache rather than
running additional commands for every job on every controller poll.

Maintain source-scoped cache state for parent and custom-step discovery. A
successful source query replaces that source's cached paths, including clearing
them when the successful result is empty. A failed source query retains that
source's last known good paths. Publish the deterministic union of the current
source caches, so partial success cannot erase valid metadata from the source
that failed.

Refresh supplemental metadata when an attempt is first observed, relevant job
state changes, or a configurable expiration elapses. Use approximately five
minutes as the initial expiration default, matching the proven inherited
behavior.

Each source refresh has a bounded immediate retry budget. After that budget is
exhausted, set a source-specific next-retry time using bounded exponential
backoff capped by the normal refresh interval. A persistent Slurm metadata
failure must not schedule another supplemental command on every controller
poll. A successful query resets that source's backoff.

The cache is an optimization and availability mechanism, not authority for
scheduler actions. It may be in-memory and rebuilt after controller restart.

### Publish through the ADR 0002 boundary

Publish known paths as `scheduler_metadata.slurm_log_file_paths` according to
ADR 0002. API Gateway forwards the metadata without interpreting Slurm-owned
keys. Job Manager validates and projects nonempty paths into its legacy
top-level `log_file_paths` field. Real paths take precedence over the
NVIDIA Recovery ID compatibility placeholder, and a degraded refresh must not replace
known real paths with that placeholder.

### Keep adjacent behavior out of the initial system

This decision does not add:

- stderr discovery
- arbitrary application-log command parsing by default
- the inherited APS filename-prefix validator
- `LOG_FILE_PATH_NOT_FOUND` as a consumer-visible validation failure
- symlink creation, file reads, or adjacent-directory scanning as part of log
  path discovery; ADR 0004 would separately govern an opt-in compatibility
  materializer and its bounded adjacent scanner
- Loki, Alloy, or OpenTelemetry collection configuration
- Workflow Controller heartbeat or cycle-log metadata

These are separate product or deployment capabilities. Adding a consumer-visible
missing-path status requires an explicit contract decision because the current
Advisor API has no typed representation for that validation result.

### Prove the production correlation path

Verification must cover:

- redacted fixtures for the pinned `squeue` and `sacct` JSON shapes, including
  an all-row accounting response with expanded stdout
- default and explicit parent stdout, custom numeric steps, arrays, missing
  fields, malformed values, duplicate paths, and partial command failures
- source-scoped partial refreshes, successful empty replacement, and
  last-known-good preservation for the failed source
- bounded retries and persistent-failure backoff with command-rate assertions
- requeued attempts with duplicate allocation rows, reused numeric step IDs,
  `.batch` and `.extern` entries, and distinct output paths, proving duplicate
  rows merge without creating an attempt and each attempt publishes only its
  own parent and numeric-step paths
- API Gateway opaque pass-through and Job Manager validation and projection of
  Slurm metadata
- a real-Slurm controller harness scenario for parent and custom-step output
- an end-to-end Watcher correlation scenario that omits
  `slurm_job_attempt`, so filename matching is the behavior under test

Tests must use production command and API boundaries rather than a hidden
test-only metadata ingress.

## Consequences

The system preserves the user-visible parent and custom-step correlation
behavior while removing inherited filename restrictions and avoiding a
Slurm-specific Advisor API field. Slurm remains the source of truth for pattern
expansion, including patterns the inherited controller did not understand.

Structured output and caching reduce brittle text parsing and command pressure,
but supported Slurm versions now need representative fixture coverage. A
fallback parser remains necessary where the pinned Slurm representation lacks
the expanded parent path.

Discovery remains eventually consistent. A newly created step path may not
appear until the next eligible refresh, and a controller restart rebuilds the
cache. Job state and recovery reconciliation continue even while log metadata
is degraded.

This decision makes filenames available for correlation but does not make the
underlying files readable or collected. Operators still need a separately
configured log pipeline that can observe the paths their workloads use.

## Alternatives Rejected

### Copy the Workflow Controller implementation unchanged

That would preserve incomplete pattern expansion, fragile submit-line parsing,
strict APS naming, legacy SSH plumbing, and filesystem side effects that do not
belong in the standalone Advisor Controller.

### Discover only the parent batch path

Training jobs may direct `srun` custom steps to separate files. Parent-only
discovery would not preserve the inherited correlation behavior.

### Parse every native step's submit command

Slurm accounting already owns the resulting stdout field and pattern
expansion. Reinterpreting shell-like submit text adds quoting and option-parsing
failure modes without improving the native-output contract.

### Query every job on every poll

The controller's polling interval is much shorter than log metadata normally
changes. Uncached per-job queries would add unnecessary scheduler load and make
state publication more sensitive to supplemental metadata failures.

### Require the inherited APS filename prefix

That convention is not a Slurm requirement and conflicts with the OSS goal of
reducing onboarding configuration. The scheduler-reported path is authoritative
for correlation.

### Couple log symlinks to discovery

Symlink creation requires workload filesystem access, writable mounts, and
additional permissions. Those deployment and collection concerns are
independent of discovering and publishing scheduler metadata. Proposed ADR
0004 would therefore define materialization as a disabled-by-default
collaborator with a separate deployment and ownership contract.
