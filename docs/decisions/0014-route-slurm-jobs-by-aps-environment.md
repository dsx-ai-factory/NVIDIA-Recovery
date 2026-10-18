# ADR 0014: Route Slurm Jobs by APS Environment

Status: accepted

Date: 2026-08-13

Related:
HEIMDALL-3059 requires environment routing in the Slurm Advisor path, and
[the Slurm Advisor Controller architecture](../architecture/slurm-advisor-controller.md)
defines the per-job mode decision that this routing gate precedes.

## Context

The inherited Workflow Controller allows multiple NVIDIA Recovery backends to watch
one Slurm cluster without claiming the same jobs. A job declares its target in
the optional `environment` field of its APS comment, and each controller admits
only the jobs routed to its configured APS environment. Filtering occurs while
the controller builds its `squeue` working set, before it publishes job state,
creates log links, or performs recovery work.

The inherited contract has two special controller values. `unspecified` is a
compatibility mode that admits every job with a valid APS specification.
`prod` owns valid APS jobs whose environment is absent as well as jobs that
explicitly name `prod`. Every other configured environment requires an exact
job value. The job field is an optional string and matching is case-sensitive.
The retired Workflow Controller's CLI restricted its own configuration to
`prod`, `staging`, `dev`, `test`, and `unspecified`, but neither the APS field
nor the matching algorithm requires that closed vocabulary.

The Slurm Advisor Controller already reads APS/HEIMDALL comments to resolve
per-job `ignore`, `observe`, and `act` modes, but does not read the environment
field. It therefore publishes and evaluates every Slurm job before its later
advice-ownership check can reject advice from another environment. Treating an
environment mismatch as `ignore` would still run mode resolution and would
conflate routing with job policy.

The existing `SLURM_ADVISOR_ENVIRONMENT` setting already identifies the
controller's backend environment. It accepts arbitrary values such as
`customer-slurm`, is written to scheduler metadata, and scopes returned advice.
That is the same controller-side identity that the APS comment must target. The
missing behavior is parsing and preserving the job's declared value, then
applying the inherited comparison before publication or mode resolution.

## Decision

### Preserve the inherited matching contract

The Slurm Advisor Controller will use the following admission matrix:

| Controller APS environment | Job APS `environment` | Admission |
| --- | --- | --- |
| `unspecified` | any string, absent, null, or empty | admit |
| `prod` | absent, null, empty, or exactly `prod` | admit |
| `prod` | any other non-empty string | reject |
| any other non-empty value | exact same string | admit |
| any other non-empty value | absent, null, empty, or any other string | reject |

Matching is exact and case-sensitive. The controller does not trim, lowercase,
alias, or otherwise normalize a job's value. For example, `DEV` and
`production` are valid strings but do not match `dev` or `prod`.

An absent field, JSON `null`, and an empty string all have the inherited
"environment not declared" meaning. A non-string value is malformed and is
not admitted. Unknown APS fields remain ignored.

Only a job with a valid object-valued APS/HEIMDALL specification participates
in this matrix. A missing, null, non-object, or malformed specification is
"not onboarded," not an environment mismatch, and is not admitted even when
the controller uses `unspecified`. This preserves the Workflow Controller's
admission boundary and keeps "not onboarded" distinguishable from "routed
elsewhere."

The Advisor Controller already intentionally gives the top-level `APS` and
`HEIMDALL` aliases identical comment semantics. Environment routing applies the
same matrix to both. This preserves the inherited environment contract without
copying the Workflow Controller poller's accidental failure to dereference a
HEIMDALL-only comment after recognizing it.

### Use the existing controller environment

Use `SLURM_ADVISOR_ENVIRONMENT` as both the existing backend identity and the
APS routing target. Keep its current arbitrary non-empty string behavior and
its `unspecified` default. Helm and other deployment surfaces continue to use
their existing environment setting; no parallel APS-environment setting is
introduced.

`prod` and `unspecified` retain their inherited special meanings. Any other
value is a named backend environment and uses exact string matching. This is
more permissive than the retired Workflow Controller CLI, but it preserves the
APS schema and matching semantics while remaining compatible with existing OSS
site values.

A configuration change takes effect when the controller is restarted or
redeployed with the new value. Jobs do not need to be resubmitted. A job
comment changed through Slurm is reevaluated from the next fresh Slurm poll.
Hot reloading process environment is not required.

### Gate all controller behavior before mode resolution

Parse and type the declared environment in the Slurm adapter for both active
`squeue` records and historical `sacct` records. Apply admission immediately
after conversion from fresh Slurm data and before the job is inserted into the
controller snapshot or receives an effective mode.

A rejected job must not:

- receive an `ignore`, `observe`, or `act` decision
- be marked in the local seen-job store
- be upserted or have a final/deleted state published to NVIDIA Recovery
- become a log-link or workload-metadata target
- participate in direct failed-job recovery or advice reconciliation
- cause a Slurm scheduler mutation

If a previously admitted job becomes rejected because the controller
configuration or job comment changes, forget its local seen state without
publishing a tombstone. Existing advice ownership checks remain as defense in
depth, but are not the admission mechanism.

### Restore the missing typed APS conversion

Treat the missing environment as a loss in the existing typed APS conversion,
not as a new scheduler-metadata convention. Parse the optional string into the
Slurm job model and carry it through the Advisor job configuration used by
`CreateOrUpdateJobRequest` and `Job`. API Gateway must map it in both directions
between that typed field and the legacy Job Manager `ApsSlurmJobSpec`.

The existing scheduler metadata `environment` remains the controller/backend
identity used for advice ownership. It is not substituted for the job's typed
APS value because `prod` may admit an absent value and `unspecified` may admit a
different explicit value. An absent APS value remains absent rather than being
rewritten to the effective `prod` fallback.

### Make admission observable without unbounded labels

Record a per-poll admission summary and a cumulative metric with a fixed set of
decision reasons, including at least `matched`, `controller_unspecified`,
`prod_default`, `environment_mismatch`, `not_onboarded`, and
`invalid_environment`. Raw environment values and job identifiers must not be
metric labels.

Structured logs include the controller APS environment and aggregate counts.
Per-job diagnostics may include the job identifier and declared value at debug
level. Operators must be able to distinguish no Slurm jobs, no onboarded jobs,
and jobs routed to another backend.

### Prove isolation through production interfaces

Automated coverage must include every row of the matching matrix; absent,
null, empty, case-mismatched, unknown, and non-string values; malformed or
missing APS/HEIMDALL specifications; and the existing APS/HEIMDALL alias
behavior.

Controller tests must prove that rejection occurs before mode resolution and
produces no upsert, seen-state retention, log-link work, recovery plan, advice
action, or Slurm mutation. The Slurm harness must run two controllers with
different APS environments against one job set and prove that each publishes
and acts only on its own jobs, with no job claimed by both.

## Consequences

NVIDIA Recovery preserves the deployed Workflow Controller routing behavior,
including its backward-compatible ownership of environment-less jobs by
`prod` and its explicit `unspecified` migration mode. Existing arbitrary
site/backend environment values remain valid and become exact-match routing
values.

`unspecified` is intentionally unsafe for two-controller isolation: every
valid APS/HEIMDALL job is admitted. Parallel backends must use distinct
non-`unspecified` routing values, and only `prod` may own jobs submitted without
an environment.

Jobs without a valid APS/HEIMDALL object no longer reach per-job mode
resolution. This is stricter than treating a missing comment as a safe
`observe` request, but it is required to preserve the inherited admission
boundary and the ordering required by HEIMDALL-3059.

The typed job configuration expands to preserve an APS field that the current
conversion drops. Generated API artifacts and the controller, gateway, and Job
Manager conversion tests must change together.

Changing routing can leave records previously published by another controller
in NVIDIA Recovery until the owning backend or its normal retention policy updates
them. The newly non-owning controller must not publish a deletion because the
ticket requires it to leave routed-away jobs entirely alone.

## Alternatives Rejected

### Add a separate APS environment setting

The existing environment already names the backend, stamps published job and
advice ownership, and accepts the site-specific values used by OSS deployments.
A second value could disagree with that ownership identity without improving
the inherited exact-match behavior.

### Treat an environment mismatch as ignore mode

Ignore is a per-job policy decision. It occurs after the job has entered the
controller snapshot and mode telemetry, while environment routing must prevent
the job from reaching that decision at all.

### Restrict controller environments to the retired CLI vocabulary

The five values were enforced by the Workflow Controller CLI, not by the APS
schema or `should_include_job` comparison. Carrying that UI restriction into
OSS would reject current site-specific values even though exact string matching
supports them.

### Normalize environment values

Case folding, whitespace trimming, or aliases such as `production` would make
jobs move between backends differently from the Workflow Controller.
Permissive values still use the inherited exact, case-sensitive comparison.

### Copy the Workflow Controller's HEIMDALL-only parser defect

The inherited poller recognizes a HEIMDALL-only comment and then accidentally
indexes the absent APS key. The Advisor Controller already defines APS and
HEIMDALL as aliases. Reproducing that defect would make environment behavior
inconsistent with the rest of the current comment contract.

### Carry the job value only in scheduler metadata

The value is already part of the typed legacy APS specification. Hiding it in
untyped metadata would duplicate the controller/backend `environment` field
and leave the existing APS conversion incomplete.
