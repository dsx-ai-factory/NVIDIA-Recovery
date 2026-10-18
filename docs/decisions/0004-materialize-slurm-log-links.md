# ADR 0004: Materialize Slurm Log Links in the Advisor Controller

Status: proposed

Date: 2026-07-28

> **Implementation note (2026-09-23):** Core behavior matches the inspected [materializer](../../slurm-advisor-controller/internal/loglinks/materializer.go) and [handoff](../../slurm-advisor-controller/internal/reconcile/async_loglinks.go) at `47a8fdc9`, but acceptance remains unresolved. Deployment qualification was not established by this audit.

Revised: 2026-07-28, to make the non-blocking orchestration, source-root trust
boundary, and collector sandbox contracts explicit.

Builds on:
[ADR 0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md), which
defines how Slurm log paths cross the scheduler metadata boundary, and
[ADR 0003](0003-discover-slurm-job-log-paths.md), which defines how the Slurm
adapter discovers paths for an exact job attempt.

## Context

Some Slurm deployments expose workload logs to a collector through a shared,
flat directory of symbolic links. The retired Slurm Workflow Controller
supported that convention:

- its event poller requested links for the parent stdout path and numeric custom
  step stdout paths
- its filesystem action helpers created the links and optionally scanned the
  immediate parent directories for configured application-log patterns
- its maintenance handler periodically removed old or broken links
- the OpenTelemetry collector tailed the resulting link directory and shipped
  logs to Loki; it did not create the links itself

That behavior enabled deployments where workload output is distributed across
job directories but the collector watches one stable directory. Public product
scope retains this symlink-plus-Loki architecture as a reference integration.
The supported architecture requires the standalone Advisor Controller and does not retain the
Workflow Controller deployment.

The inherited implementation is evidence for compatibility, not an
implementation to copy unchanged. It used a flat basename namespace, had
different overwrite behavior for local and SSH execution, scanned on every
poll, and removed links based on target existence or target modification time.
Cleanup did not have a durable ownership boundary, so it could remove links it
did not create or age out a quiet but active log.

ADR 0002 and ADR 0003 deliberately separated path discovery and publication
from filesystem mutation. The resulting exact-attempt log path metadata now
provides a bounded source from which an independently enabled compatibility
feature can materialize links.

## Decision

### Put optional materialization behind the Slurm Advisor Controller

The Slurm Advisor Controller process will own orchestration of an optional log
link materializer. Filesystem operations will live in an isolated collaborator
with a narrow interface rather than in scheduler command parsing,
scheduler-neutral Advisor API types, or job-state conversion.

The materializer will consume the exact-attempt paths already discovered under
ADR 0003. It will not query Slurm independently and will not add a public
Advisor API field. API Gateway, Job Manager, Watcher, Alloy, OpenTelemetry, and
Loki will not create or remove these links.

Materialization is disabled by default and configured independently from
`ignore`, `observe`, and `act` scheduler-action policy. Enabling it explicitly
authorizes the controller's filesystem writes even when scheduler actions are
in observe mode. Only active jobs eligible for NVIDIA Recovery management are
materialized; a job selected for explicit ignore is excluded.

The first implementation will be in-process. It will not restore the Workflow
Controller or introduce another public service or daemon. The filesystem
collaborator must remain separable so a future deployment can move it behind a
more restrictive process boundary without changing discovery or Advisor API
contracts.

Reconciliation will hand filesystem work to one in-process worker instead of
waiting for it inline. The handoff has one pending snapshot; when the worker is
busy, a newer snapshot replaces the pending snapshot so the latest state wins.
Submission does not wait for filesystem I/O, and the single worker preserves the
one-writer manifest and link ordering. Process cancellation signals cancellation
to in-flight work and drops pending work without a shutdown drain. A subsequent
process recovers the ownership manifest and submits a fresh active-job snapshot.
This bounded, eventually consistent handoff prevents the compatibility feature
from backpressuring job publication, advice, or scheduler recovery.

### Use an explicit, bounded filesystem configuration

Enabling the feature requires:

- one dedicated destination directory
- one or more allowed source roots
- mounts and permissions that make the source paths visible to the Advisor
  Controller and make only the destination writable

Source targets must be absolute, normalized paths beneath an allowed source
root. The materializer rejects NUL characters, empty basenames, `.` and `..`,
paths outside the configured roots, and values over configured count and length
limits. It must not follow a symlink while validating configured source or
destination directory boundaries.

The destination keeps the inherited flat compatibility layout:
`<destination>/<basename(target)>`. This intentionally preserves the convention
expected by existing collectors. The materializer creates a link even when the
validated target does not exist yet, because Slurm may report the output path
before the file is created.

The allowed source roots are controller input allowlists, not a filesystem
sandbox for the collector. An ordinary symbolic link is resolved again when the
collector opens it. A process that can write beneath a source root can therefore
replace a previously validated target, or an ancestor below that root, with
another symbolic link and influence what the collector reads. This is especially
important for an absent target: validation cannot constrain the file type or
resolution of a path that a writer creates later. A pre-creation check or
periodic revalidation may detect some substitutions but cannot close the
time-of-check/time-of-use race, so the missing-target behavior remains a
deliberate compatibility choice rather than a confinement guarantee.

Enabling materialization consequently asserts one of these deployment
preconditions:

- every identity that can mutate a configured source tree is trusted not to
  create or substitute symbolic links for exposed targets or their ancestors
- the collector itself enforces resolution beneath the allowed roots at file
  open time

If neither precondition is true, the deployment must not enable this
compatibility feature.

Link creation is idempotent and must not overwrite an existing filesystem
entry:

- an existing symlink to the same normalized target is an existing success
- an existing symlink to a different target is a collision
- a regular file, directory, or other entry at the destination is a collision

Collisions are reported and left unchanged. A basename collision therefore
cannot redirect a collector to another workload or replace an operator-managed
entry. An existing same-target link is refreshed as owned only when a matching
manifest record already proves ownership; otherwise it remains an unmanaged
entry that the materializer will never remove.

### Support bounded adjacent application-log discovery

Primary links cover the parent stdout path and valid numeric custom-step stdout
paths supplied by ADR 0003. Operators may additionally configure a bounded set
of filename patterns for legacy application logs that Slurm does not expose as
stdout metadata.

Patterns are validated and compiled at controller startup. They may substitute
the concrete job ID and, for an array task, its array-parent job ID. The scanner
examines only the immediate parent directories of validated primary targets. It
does not recurse, traverse directory symlinks, or inspect unrelated roots.
Scans run on a configurable cadence independent of the controller poll, with
limits on patterns, directories, entries, matches, and work per cycle.

The inherited application-specific submit-command options are not part of this
decision. Parsing such options changes log-path discovery and requires a
separate extension to ADR 0003 if an adopter demonstrates that need.

### Track ownership and clean up only owned links

The materializer will maintain an atomically replaced ownership manifest in the
destination directory. Each record identifies the destination name, normalized
target, exact job attempt, whether it came from scheduler metadata or adjacent
application-log scanning, and the time it was last observed. A primary record's
time is refreshed only when that exact normalized target remains in the current
active snapshot. An application record is refreshed while its exact attempt
remains active so scan cadence and work caps do not spuriously age it out.
Origin-less records from an older manifest are handled conservatively as
primary records. A new ownership record is written only after this writer
successfully creates the link. A crash between link creation and the manifest
update may conservatively leave an unmanaged link, but must never turn a
pre-existing entry into a managed one.

Before removing a link, cleanup must verify with `lstat` that:

- the link is present in the ownership manifest
- the current destination entry is still a symlink
- the current link target is the target recorded in the manifest

Cleanup never removes an unmanaged entry or a path that replaced a previously
managed link. Age is based on the manifest's last-observed time, not on target
existence or modification time. The initial defaults are a 24-hour retention
period and an hourly cleanup cadence, both configurable.

Manifest recovery after restart is conservative. Invalid manifest entries and
filesystem entries whose identity cannot be proven are reported but not
deleted. Manifest updates and link operations must tolerate process restarts
without broad directory cleanup.

### Isolate failures from recovery behavior

Invalid enabled configuration, invalid patterns, or an unusable destination
will fail controller startup with a clear configuration error. After startup,
an individual target, scan, collision, permission failure, or manifest update
failure is best-effort and must not block job upserts, advice evaluation,
scheduler actions, or publication of the ADR 0002 metadata.

The component will expose bounded metrics for links created, already present,
rejected, collided, failed, and removed; currently owned links; scan work and
duration; and cleanup failures. Filesystem paths and job identifiers must not
be metric labels. Logs may include paths only at a deliberate diagnostic level
and must avoid logging path-derived customer data by default.

### Make collector access an explicit deployment contract

The controller chart will expose typed log-link configuration and volume mounts
rather than relying on arbitrary environment injection. Source mounts should be
read-only, and the dedicated destination should be writable only by the
controller identity. Source and destination backing stores must not alias: a
writable destination mount that names the same tree as a source would bypass
the source's read-only mount. A collector following absolute links must mount
both the destination and each source root at the same absolute paths seen by
the controller.

The collector is part of the security boundary. Its filesystem namespace must
expose only the destination and required source roots and must not expose
service-account tokens, credentials, secret mounts, the host root filesystem, or
unrelated volumes that a substituted source link could reach. Source and
destination mounts are read-only in the collector, but read-only mounts alone do
not prevent a source writer from redirecting resolution. The collector should
run as a dedicated unprivileged identity with its own hardened mount namespace.

The native systemd packaging will provide matching configuration examples and
an opt-in hardening drop-in that grants write access only to the destination.
The default service remains unable to write log links.

This decision does not define the packaging or operation of Loki, Alloy, or an
OpenTelemetry collector. The product requirement for a bundled reference
log-collection deployment remains separate work. Its deployment documentation
will demonstrate this shared-filesystem contract, while the materializer will
not own collector lifecycle. Each destination directory supports one active
materializer writer.

### Verify through production boundaries

Verification will cover:

- disabled-by-default and startup configuration behavior
- primary parent and numeric-step links for standalone, array, and requeued
  attempts
- path-root validation, traversal attempts, basename collisions, existing
  entries, absent targets, and permission failures
- the trusted-source-root precondition and a collector namespace that exposes no
  unrelated mounts or secrets
- bounded adjacent scanning and pattern validation
- manifest restart recovery, exact ownership checks, last-observed retention,
  and refusal to remove unmanaged or replaced entries
- non-blocking latest-snapshot handoff, coalescing while busy, cancellation
  without a shutdown drain, and failure isolation from job publication, advice,
  and scheduler actions
- Helm rendering and systemd hardening configuration
- an end-to-end link-to-collector-to-Loki-to-Watcher scenario that proves
  filename correlation without a `slurm_job_attempt` fallback

Tests must use the production controller, filesystem, collector, and API
boundaries rather than hidden test-only ingress.

## Consequences

Deployments using the inherited flat-link convention can migrate without
restoring the Workflow Controller. Discovery remains scheduler-owned, while
filesystem mutation is an explicit, independently enabled deployment
capability.

The Advisor Controller needs additional mounts and destination write permission
only when the feature is enabled. This increases its optional filesystem attack
surface, so path validation, collision refusal, manifest ownership, and
least-privilege deployment configuration are part of the feature rather than
follow-up hardening.

The collector also gains an optional filesystem attack surface. Workload or
application identities that can mutate a source tree can influence a collector
that follows ordinary symbolic links from that tree. Read-only controller and
collector mounts do not remove that influence; trusted source writers or
collector-enforced beneath-root resolution are mandatory.

Flat basenames preserve collector compatibility but cannot represent two active
targets with the same basename. The system reports and preserves the first
existing entry instead of choosing or overwriting a target. Operators needing a
collision-free namespace will require a future collector convention and a new
decision.

Adjacent application-log discovery remains eventually consistent and may miss a
short-lived file between scans. Bounds and failure isolation take precedence
over exhaustive directory monitoring.

Primary materialization is also eventually consistent with controller
reconciliation. A successful controller poll means the latest eligible snapshot
was submitted, not that filesystem work has completed. Superseded pending
snapshots and pending work at shutdown may be skipped; a later snapshot or
restart converges from the manifest and current active-job state.

## Alternatives Rejected

### Restore the Workflow Controller

This would reintroduce a retired deployment and scheduler integration boundary
that the standalone Advisor Controller replaces.

### Put link creation in API Gateway, Job Manager, or Watcher

Those components do not own Slurm path interpretation or the workload
filesystem. Granting them access would cross scheduler-neutral boundaries and
spread filesystem privilege into unrelated services.

### Make the collector discover workload directories itself

Some operators can use collector-native discovery, but it does not preserve the
stable flat-directory convention required by existing deployments. It remains
a valid deployment alternative when compatibility links are unnecessary.

### Introduce a separate mandatory link daemon

A daemon could isolate filesystem permissions, but it adds a deployment,
identity, transport, and lifecycle contract before the behavior requires one.
Keeping the collaborator isolated preserves that future option without making
it mandatory now.

### Copy inherited overwrite and cleanup behavior

Replacing existing links can redirect unrelated logs, and deleting by target
existence or modification time can remove unmanaged or quiet active links.
Explicit collision handling and manifest-scoped cleanup provide a safer
ownership boundary.

### Add log links to the public Advisor API

The links are local deployment artifacts derived from Slurm metadata, not a
scheduler-neutral job property. Public API expansion would couple all adapters
and clients to one collector filesystem convention.
