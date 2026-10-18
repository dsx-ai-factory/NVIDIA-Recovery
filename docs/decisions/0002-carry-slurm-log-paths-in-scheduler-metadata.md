# ADR 0002: Carry Slurm Log Paths in Scheduler Metadata

Status: accepted

Date: 2026-07-28

> **Implementation note (2026-10-05):** Unique per-attempt log paths remain the safe default because Slurm can briefly report stale paths during requeue. A workload that intentionally reuses paths must set `log_file_paths_shared_across_attempts: true` in its APS comment; Job Manager then retains the path and uses the latest attempt when an event has no trusted `slurm_job_attempt`. Explicit legacy-path preservation and non-Slurm-only ID placeholders still qualify the projection contract; see [projection](../../job_manager/job_manager/job_log_file_paths.py) and [retry filtering](../../job_manager/job_manager/storage_utils.py).

Related: [ADR 0003](0003-discover-slurm-job-log-paths.md) defines the Slurm
discovery system that produces this metadata.

## Context

The retired Slurm Workflow Controller discovered batch and custom-step stdout
paths and published them to Job Manager. The standalone Slurm Advisor Controller
does not currently discover or publish those paths.

The Advisor API has a scheduler-neutral job contract and an existing
`scheduler_metadata` structure. Adding `log_file_paths` directly to that public
contract would make a Slurm filesystem concept part of every scheduler adapter.
Kubernetes-native controllers generally identify logs by workload, pod,
container, and stream rather than by a scheduler-owned filesystem path, so there
is no demonstrated cross-scheduler contract yet.

Job Manager and Watcher still have a legacy top-level `log_file_paths` field.
Watcher uses it to correlate an observed log filename with a job attempt. API
Gateway currently places the NVIDIA Recovery job ID in that field as a compatibility
key, which does not represent the real Slurm output path.

## Decision

The Slurm adapter will publish known job log paths in
`scheduler_metadata.slurm_log_file_paths` as a deterministic, deduplicated list
of strings. The metadata key is Slurm-owned and is omitted when no path is
known. Discovery status, retry behavior, and last-known-good caching remain
controller concerns rather than values inferred from an empty path list.

API Gateway will forward the NVIDIA Recovery job ID and scheduler metadata without
interpreting scheduler-specific keys or populating `log_file_paths`. It remains
a scheduler-neutral transport boundary.

Job Manager is the compatibility boundary for its legacy top-level
`log_file_paths` field. For jobs whose scheduler metadata identifies Slurm, Job
Manager will validate `slurm_log_file_paths` and project nonempty values into
that field. Missing or empty Slurm log path metadata leaves the incoming
top-level field unset so an existing last-known-good value is preserved.
Job Manager also owns the NVIDIA Recovery ID compatibility placeholder needed for
callers that do not publish Slurm log metadata.

Real Slurm paths take precedence over the NVIDIA Recovery ID compatibility placeholder.
A transient Slurm discovery failure must not replace previously known real paths
with that placeholder. Compatibility behavior required by non-Slurm,
operator-originated workloads remains separate and must not be removed by
implication.

The Advisor API will not gain a scheduler-neutral log-file-path field until
another scheduler demonstrates a compatible concept. If a future cross-scheduler
consumer needs log correlation, it should introduce a contract based on that
consumer's actual identifiers rather than generalizing Slurm filesystem paths
prematurely.

This decision covers metadata publication and translation only; ADR 0003 owns
the discovery behavior. [ADR 0004](0004-materialize-slurm-log-links.md)
separately proposes optional, deployment-scoped link materialization from that
metadata. Log-file access, shared-volume configuration, and shipping logs to
Loki or another backend remain collector and deployment concerns.

## Consequences

The Advisor public contract stays scheduler-neutral, and other scheduler
adapters do not need to model Slurm filesystem semantics. The Slurm-specific
field can evolve with Slurm discovery behavior without regenerating public API
bindings.

The metadata key is an untyped `google.protobuf.Struct` value, so the Slurm
adapter and Job Manager must share and test its name, element type,
normalization, and size limits across their language boundary. Job Manager must
deliberately translate the value because Watcher consumes its legacy
`log_file_paths` field rather than scheduler metadata directly.

The legacy Job Manager representation temporarily duplicates the paths: once in
scheduler metadata and once in its top-level compatibility field. That
duplication is isolated at the Job Manager boundary and can be removed when the
legacy consumer contract is replaced.

## Alternatives Rejected

### Add `log_file_paths` to the Advisor job contract

This is strongly typed but promotes a Slurm-specific filesystem representation
without a demonstrated scheduler-neutral use case.

### Make API Gateway interpret Slurm scheduler metadata

That would make the scheduler-neutral facade understand a Slurm-owned key and
its validation, replacement, and last-known-good semantics. Job Manager already
owns the Slurm-shaped legacy `log_file_paths` persistence and correlation
contract, so it is the narrower compatibility boundary for this translation.

### Continue using the NVIDIA Recovery job ID as the Slurm log path

The identifier supports an existing compatibility flow but is not a real Slurm
filename. It cannot provide exact filename correlation for ordinary Slurm
stdout.

### Couple symlink creation to metadata publication

Metadata discovery does not require filesystem mutation. Reintroducing symlink
and collection behavior as an unconditional part of publication would expand
controller permissions and deployment scope independently of the correlation
requirement. Proposed ADR 0004 instead specifies a disabled-by-default
materializer with its own security and lifecycle contract.
