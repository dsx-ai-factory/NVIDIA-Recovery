# ADR 0042: Align SAC and Watchdog Job Action Policy

Status: accepted

Date: 2026-09-29

Discussion: [PR 193](https://github.com/NVIDIA/NVIDIA-Recovery/pull/193)

This decision supersedes only the statement in
[ADR 0015](0015-scope-slurm-watchdog-by-aps-environment-pattern.md) that the
watchdog does not consume per-job operating modes. Its environment admission
contract and independent process action gates remain in force.

## Context

SAC resolves job modes before acting, while the watchdog historically releases
eligible held jobs without consulting those modes. This permits watchdog
release of a job whose explicit `observe` mode prevents SAC from acting.
Selecting an initial rollout cohort by username must preserve job visibility
and must not introduce a second interpretation of job action permission.

The instructing human selected explicit job-mode precedence and the same
job-policy scope for SAC and watchdog. An explicit valid job mode expresses
the user's intention even when a rollout username list is configured.

## Decision

Resolve action policy through one shared implementation using fresh Slurm
metadata, after onboarding and environment admission:

1. An explicit valid job mode (`ignore`, `observe`, or `act`) wins. Invalid
   explicit modes resolve to `observe`.
2. With no explicit mode and a configured username list, listed users select
   `act`; unlisted or unknown users select `observe`, regardless of SAC default.
3. With neither an explicit mode nor a configured list, use SAC's default.
4. SAC's scheduler-action acknowledgement caps every selected `act` at
   `observe` when permission is absent.

The username list is action selection, not a hard action boundary: an unlisted
user's explicit `act` may authorize actions after the acknowledgement and
other safety checks pass. Username selection does not gate bootstrap,
accounting, enrichment, seen-state retention, publication, or Advice ownership.
Jobs otherwise admitted remain monitored unless their effective mode is
`ignore`. [ADR 0037](0037-filter-sac-bootstrap-by-aps-environment.md) continues
to govern environment filtering during bootstrap.

The watchdog requires `act` from the same job policy before arming held timers.
A later non-actionable observation discards its timer; renewed eligibility
starts a fresh timeout. It retains its independent process mode,
acknowledgement, environment and partition scopes, timeout, and initial-DAG
attempt protection. Scheduler decisions still depend on fresh Slurm data,
without requiring NVIDIA Recovery availability.

Helm supplies the controller default, acknowledgement, and username list to
both processes. Standalone deployments explicitly synchronize these inputs.
Operational configuration and examples live in the
[current architecture](../architecture/slurm-advisor-controller.md) and
[chart guide](../../deploy/helm/slurm-advisor-controller/README.md).

The username selector is **temporary, solely for the initial internal cluster
rollout; it will be deprecated soon and may be removed without notice**. Its
intended default is `observe`; default `act` combined with a nonempty list is
accepted with a bootstrap warning.

## Consequences

SAC and watchdog agree on the job cohort eligible for actions while retaining
their distinct recovery and release safeguards. Explicit `observe` and `ignore`
protect a job from either action path. Acknowledgement remains mandatory even
for explicit `act`.

Monitoring all otherwise eligible jobs increases backend publication and
analysis work compared with filtering them at discovery. No Advisor API or
database migration is required. Standalone operators must synchronize the
shared policy settings as well as existing environment and partition scopes;
watchdog job-policy defaults of `observe` and unacknowledged do not authorize
release on their own.

## Alternatives Rejected

- Keep username filtering at discovery: hides jobs that should remain visible
  while scheduler actions are limited during rollout.
- Make the list a hard action boundary: overrides explicit job-owner intent.
- Leave watchdog independent of job modes: permits release of a job SAC is
  explicitly instructed only to observe or ignore.
- Have watchdog consult SAC state or Advice: couples fail-open release to
  availability of the process and backend it is intended to supplement.
