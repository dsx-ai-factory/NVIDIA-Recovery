# ADR 0028: Project Slurm Job Fields from Scheduler Metadata

Status: accepted

Date: 2026-09-03

## Context

The scheduler-neutral Advisor job API carries common reconciliation fields but
does not expose every scheduler-specific job attribute consumed by the legacy
Job Manager and dashboard. Slurm's `squeue --json` and `sacct --json` outputs
provide cluster, partition, user, command, state reason, and aggregate resource
values. Those values correspond to typed columns on the retained
`SlurmJobExecutionRecord` model.

Adding Slurm-shaped fixed fields to the Advisor API would couple every future
scheduler adapter to Slurm's representation. Leaving the values only in an
opaque JSON column would keep the dashboard's established typed fields empty.

The controller already publishes Slurm's job state reason as `slurm_reason`,
and Job Manager consumes it when resolving older Slurm's ambiguous
`COMPLETING` state. Introducing `slurm_state_reason` solely for typed
persistence would carry the same source value under two metadata keys.

## Decision

The Slurm Advisor Controller publishes the following Slurm-owned keys through
the existing `scheduler_metadata` field:

| Metadata key | `SlurmJobExecutionRecord` field |
| --- | --- |
| `slurm_cluster` | `cluster` |
| `slurm_partition` | `partition` |
| `slurm_user` | `user` |
| `slurm_command` | `command` |
| `slurm_reason` | `state_reason` |
| `slurm_node_count` | `node_count` |
| `slurm_total_gpu_count` | `total_gpu_count` |
| `slurm_total_cpu_count` | `total_cpu_count` |
| `slurm_total_memory_mb` | `total_memory_mb` |

The API Gateway continues to transport scheduler metadata opaquely. Job Manager
is the compatibility boundary that validates Slurm-owned values and projects
them into its typed persistence model while retaining the original metadata.
The existing `slurm_reason` key serves both recovery-state resolution and typed
state-reason persistence; a separate `slurm_state_reason` key is not published.
The opaque scheduler metadata does not need to use the same field name as the
typed persistence model.

For requeue-managed accounting records, the controller retains the accounting
reason on its internal job record for reconciliation but omits `slurm_reason`
from published metadata. The accounting reason can describe an older
transition, so Job Manager retains the last value observed through `squeue`.

Missing metadata leaves a typed field unset so an accounting-only update does
not erase richer data previously observed through `squeue`.

For active records, allocated TRES values override requested values one resource
at a time. For finalized accounting records, the allocated TRES collection is
authoritative when present; requested TRES is used only when the allocation
collection is absent. `sacct` does not replace the command learned from
`squeue`, matching the inherited persistence behavior.

## Consequences

The Advisor API remains scheduler-neutral while the existing dashboard columns
are populated. The metadata key names and validation rules form a compatibility
contract between the Slurm adapter and Job Manager and require cross-component
tests.

Scheduler metadata contains one Slurm state-reason key, and existing Job
Manager recovery logic and typed persistence consume the same value. Existing
producers and consumers of `slurm_reason` remain compatible. The controller's
internal job reason remains available for reconciliation even when a stale
accounting reason is not published.

Scheduler metadata temporarily duplicates values stored in typed Job Manager
columns. That duplication is confined to the legacy persistence boundary and
can be removed when consumers use a scheduler-neutral workload metadata model.

## Alternatives Rejected

### Add all fields to the Advisor API

This would make Slurm-specific concepts part of the fixed onboarding contract
before another scheduler demonstrates equivalent semantics.

### Read scheduler metadata directly in the dashboard

This would bypass the established `SlurmJobExecutionRecord` fields, duplicate
validation in a UI consumer, and leave other backend consumers inconsistent.

### Interpret Slurm keys in API Gateway

The gateway is a scheduler-neutral transport facade. Job Manager already owns
the Slurm-shaped persistence model and is the narrower compatibility boundary.

### Publish a separate `slurm_state_reason` key

Publishing `slurm_state_reason` alongside `slurm_reason` would duplicate one
source value and require consumers to understand an artificial distinction
between operational and persisted state reasons. Renaming the existing key
would instead require a compatibility migration for deployed producers, stored
records, and the Job Manager recovery consumer.
