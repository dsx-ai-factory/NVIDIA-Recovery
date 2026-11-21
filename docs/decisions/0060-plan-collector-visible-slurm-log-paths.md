# ADR 0060: Plan Collector-Visible Slurm Log Paths

Status: proposed

Date: 2026-11-17

## Context

The optional Slurm log materializer uses source basenames in a flat directory.
Different sources can have the same basename, and arbitrary names may not carry
the job ID or extension expected by a collector. Changing a link name without
changing the published path breaks the filename relationship used by Watcher
and Job Manager. [ADR 0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md)
keeps that relationship behind scheduler metadata and the Job Manager projection
boundary. [ADR 0004](0004-materialize-slurm-log-links.md) describes the existing
ownership and asynchronous filesystem behavior; its acceptance remains unresolved.

## Decision

Propose an independently opt-in naming extension to the existing materializer.
`SLURM_ADVISOR_LOG_LINKS_RENAME_ENABLED` (Helm:
`controller.logLinks.renameEnabled`) defaults to false and requires materialization
to be enabled. Existing APS-prefixed primary paths retain their basenames.
Other eligible primary paths get a deterministic `.log` name containing a bounded
sanitized stem, the first 64 bits of the source path's SHA-256 digest, the attempt
index, canonical Slurm job ID, and UTC submission time. The digest avoids ordinary
basename collisions; existing collision refusal remains the final authority.
Missing submission identity never causes the controller to invent a timestamp.

One pure naming function serves publication and materialization. Fresh active
and final accounting records publish the planned renamed paths through
`scheduler_metadata.slurm_log_file_paths`; Job Manager continues to project that
list without new public Advisor fields or scheduler-specific gateway behavior.
`scheduler_metadata.app_log_link_targets` maps each renamed path to its original
source. It is an adapter-produced description, not authority for filesystem access.
Planning preserves original scheduler snapshots and caches and performs no
filesystem I/O. Unplannable or out-of-root paths retain their source metadata.

Published paths describe an eventually consistent plan, not successful file
creation. Filesystem work stays asynchronous and independently validates source
roots, ancestors, and ownership. Failures must not block job-state publication
or Advice. Generated names are verified during manifest recovery, including
after disabling the naming option. Adjacent application logs retain existing
basename behavior; a primary is not linked again by the adjacent scanner.

This first slice does not remove superseded attempt links, compare source
identity across attempts, or change retention. A requeued attempt can therefore
receive a new collector path to a file an earlier attempt also used, including a
previous attempt's path that Slurm briefly reports after a requeue, so log lines
can be attributed to the wrong attempt. This applies whether attempts reuse one
file or write distinct files. Production qualification for workloads that may be
requeued requires the follow-up replacement policy and collector-position/replay
tests; this option must remain disabled for them until that work is reviewed.

## Consequences

Arbitrary primary filenames can use existing collector filename conventions
without renaming source files. Operators must keep collector job-ID extraction,
file extensions, shared mounts, and backend filename matching compatible with
the generated namespace, including canonical array-task IDs.

Changing or disabling the option leaves existing links and collector positions
in place. Operators need a controlled transition to avoid duplicate readers or
replay. The reader accepts manifest versions 1 and 2. The writer uses version 2
whenever any record carries `submit_time`, and otherwise writes the original
version-1 schema. An older controller rejects the **entire** version-2 manifest,
losing ownership of legacy links as well as generated links; disabling renaming
does not restore compatibility while extended records remain.

Before downgrading the binary with a version-2 manifest, disable renaming and
keep the newer materializer running until normal cleanup retires the generated
records. Then stop all writers, back up the manifest and dedicated link directory,
and verify that the on-disk manifest is version 1 before enabling the older
materializer. If waiting for cleanup is not possible, keep materialization
disabled in the older deployment, or restore a verified matching version-1
manifest-and-link snapshot while all writers are stopped.
Do not merely change the version number or strip fields. These procedures concern
links and ownership metadata, never deletion of source logs.
This proposal records the implemented opt-in contract for review and does not
accept ADR 0004 or alter scheduler-action authorization.

## Alternatives Rejected

- Rename source files: interferes with workload-owned output paths.
- Rename links without publishing their names: breaks filename correlation.
- Wait for filesystem completion before publishing jobs: allows optional log
  compatibility work to delay recovery.
- Interpret the mapping in API Gateway: crosses the scheduler-neutral boundary.
- Change default naming immediately: silently changes collector streams and
  position behavior for existing installations.
