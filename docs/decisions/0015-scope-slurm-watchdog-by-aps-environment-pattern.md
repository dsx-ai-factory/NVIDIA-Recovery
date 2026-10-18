# ADR 0015: Scope Slurm Watchdog by APS Environment Pattern

Status: accepted

Date: 2026-08-13

> **Implementation note (2026-09-29):** [ADR 0042](0042-align-sac-and-watchdog-job-action-policy.md)
> supersedes only the rule that watchdog ignores per-job modes, adding SAC's
> shared job action policy before release timers while preserving environment
> admission and the independent watchdog action gates; see [PR 193](https://github.com/NVIDIA/NVIDIA-Recovery/pull/193).

Related:
[ADR 0014](0014-route-slurm-jobs-by-aps-environment.md) defines the Slurm
Advisor Controller's APS environment admission contract.
[ADR 0033](0033-deliver-watchdog-audit-events-through-gateway.md) qualifies the
statement below about independence from NVIDIA Recovery: watchdog audit transport may use the
gateway, while scheduler eligibility and actions remain independent of it.

## Context

The standalone Slurm watchdog is a cluster-level fail-open process. It reads
the same converted `squeue` records as the Advisor Controller, but it currently
uses only APS/HEIMDALL onboarding, partition, held state, DAG identity, and
attempt index when deciding whether to release a job. Although the shared
Slurm adapter now preserves the job's optional APS `environment`, the watchdog
does not evaluate it and has no environment configuration.

That behavior is compatible with the inherited watchdog, which was
global to its Slurm scope. It is not a safe default when multiple
environment-scoped controllers and their watchdogs share one Slurm job set:
any acknowledged action-mode watchdog can release a held job routed to another
environment.

The watchdog remains intentionally independent of NVIDIA Recovery and does not consume
per-job `ignore`, `observe`, or `act` mode. Environment admission is a routing
boundary, not a job-mode decision, so applying it does not couple the watchdog
to the Advisor API.

Some deployments need the inherited cluster-global fail-open behavior, while
others need one watchdog to cover an environment family rather than one exact
value. Configuration therefore needs both controller-compatible literal
matching and a small, explicit wildcard form.

## Decision

### Add a watchdog environment pattern

Add `SLURM_WATCHDOG_ENVIRONMENT_PATTERN` to the standalone watchdog process.
An explicitly empty value is invalid. When the variable is absent, the binary
defaults it to `unspecified` so standalone systemd and direct-process usage
retain the inherited cluster-global behavior.

Expose the setting as `watchdog.environmentPattern` in the Helm chart. When
the Helm value is not explicitly set, render the controller's
`controller.environment` value into the watchdog. This makes a colocated
controller and watchdog share the same environment boundary by default. An
operator may set `watchdog.environmentPattern: "*"` to request cluster-global
watchdog coverage explicitly.

The watchdog setting is separate from `SLURM_ADVISOR_ENVIRONMENT`. The two
processes have independent configuration contracts and can be deployed
separately; the Helm chart owns their default relationship when it deploys
them together.

### Preserve literal controller semantics

A pattern containing no `*` is a literal selector and uses the ADR 0014
admission matrix:

| Watchdog selector | Job APS `environment` | Admission |
| --- | --- | --- |
| `unspecified` | any string, absent, null, or empty | admit |
| `prod` | absent, null, empty, or exactly `prod` | admit |
| `prod` | any other non-empty string | reject |
| any other non-empty literal | exact same string | admit |
| any other non-empty literal | absent, null, empty, or any other string | reject |

Literal comparison is exact and case-sensitive. Neither the selector nor the
job value is trimmed, lowercased, aliased, or otherwise normalized.

### Define a minimal wildcard language

When the configured selector contains `*`, interpret each `*` as zero or more
characters. All other characters are literals. Matching is case-sensitive,
and `*` matches every character, including `/`; this is not filesystem path
matching and is not a regular expression.

Examples:

| Pattern | Matching values | Non-matching values |
| --- | --- | --- |
| `*` | every string, including absent/null/empty represented as empty | none within a valid APS object |
| `dev-*` | `dev-blue`, `dev-green` | `dev`, `DEV-blue` |
| `customer/*` | `customer/a`, `customer/a/b` | `customer`, `other/a` |
| `prod` | ADR 0014 literal `prod` behavior | any other explicit non-empty value |

Only jobs with a valid object-valued APS or HEIMDALL specification participate
in matching. Missing, null, non-object, or malformed specifications remain
unmanaged. A non-string `environment` is invalid and never matches, including
under `*`. APS and HEIMDALL retain identical alias semantics.

### Gate watchdog state and scheduler actions

Apply environment admission to each fresh Slurm record before the watchdog:

- counts it as managed
- creates or refreshes process-local tracking state
- starts or retains a held timer
- reports a release candidate
- calls `scontrol release`

When a previously admitted job no longer matches on a later poll, disarm and
remove its watchdog tracking state. If it later matches again, its release
timeout starts from the new observation; stale held time from another
environment is not reused.

The existing watchdog mode and
`SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS` remain independent hard action
gates. Environment admission cannot enable scheduler mutation.

### Keep matching observable and tested

Expose aggregate environment-admission counts using only bounded decision
reasons. Raw patterns, declared environment values, job IDs, workflow IDs, and
user data must not be metric labels.

Automated coverage must include the ADR 0014 literal matrix, `*`, prefix and
slash-containing patterns, case sensitivity, absent/null/empty values,
non-string values, malformed comments, APS/HEIMDALL aliases, and live comment
changes that disarm and later restart tracking. Production-interface coverage
must run differently scoped action-capable watchdogs over one shared Slurm job
set and prove that neither releases a job owned only by the other selector.

## Consequences

Once implemented, Helm deployments with a named controller environment will
change from a global watchdog to a watchdog scoped to that controller by
default. This closes a cross-environment scheduler-action path. Deployments
that intentionally require global fail-open behavior must select `*` (or retain
the literal `unspecified` compatibility selector).

Standalone deployments that do not set the new variable retain the inherited
global behavior through the `unspecified` default. This avoids silently
disarming an existing watchdog during upgrade.

Wildcard selectors can overlap. Operators running multiple action-capable
watchdogs must choose non-overlapping selectors when exclusive ownership is
required. The chart documentation must call out that `*`, `unspecified`, and
overlapping patterns do not provide isolation.

The implementation can reuse the already parsed scheduler-neutral
environment-presence and validity data. No additional Advisor API or Job
Manager field is required.

## Alternatives Rejected

### Keep the watchdog cluster-global only

This preserves inherited behavior but lets a watchdog deployed for one
environment mutate jobs routed to another. It conflicts with environment
isolation at the scheduler-action boundary.

### Read `SLURM_ADVISOR_ENVIRONMENT` directly

The watchdog and controller are independent processes with separate systemd
environment files and deployment lifecycles. Reusing the controller variable
inside the watchdog would obscure that boundary and make standalone
configuration surprising. Helm can express inheritance explicitly when it
deploys both processes.

### Support full regular expressions

Regular expressions add escaping, invalid-pattern, and review complexity that
is unnecessary for environment families and the cluster-global use case. The
minimal `*` wildcard is sufficient and easier to explain safely.

### Use filesystem glob semantics

Filesystem glob implementations commonly give `/` special treatment and may
vary by platform. APS environments are opaque strings, not paths, so `*` must
match `/` consistently.

### Support only controller-compatible literals

Literal matching provides isolation but cannot express an explicit global
watchdog or one watchdog covering a named family without falling back to the
ambiguous `unspecified` compatibility value. The minimal wildcard makes that
intent visible.
