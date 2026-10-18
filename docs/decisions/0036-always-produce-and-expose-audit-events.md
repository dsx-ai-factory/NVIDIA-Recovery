# ADR 0036: Always Produce and Expose Audit Events

Status: accepted

Date: 2026-09-22

> **Implementation note (2026-10-05):** Python Helm audit URL defaults now resolve
> Job Manager's configured notifications URL or the same-release Notifications
> Service for Watcher and ADI, preserving explicit receivers and logging-only
> overrides; see [PR #221](https://github.com/NVIDIA/NVIDIA-Recovery/pull/221)
> and the [Helm guidance](../../deploy/helm/README.md#retired-audit-enable-values).

## Compatibility retirement: 2026-09-23

The instructing human confirmed that existing Helm deployments have removed the
retired audit enable values and requested removal of the temporary compatibility
support. The charts now reject those values and no longer emit deprecation
warnings. The compatibility provisions below record the original transition;
they no longer apply. Destination selection and the active, default-on dashboard
UI flag remain unchanged. See the [Helm upgrade guidance](../../deploy/helm/README.md#retired-audit-enable-values).

## Context

Audit producers, Notifications ingestion, and the dashboard each have independent
feature flags. The instructing human requested removing producer and ingestion
enablement flags while retaining destination selection, accepting retired Helm
values with warnings, and keeping the dashboard visibility flag enabled by default.

## Decision

- Always start the bounded, best-effort audit buffers in the controller,
  watchdog, Job Manager, Watcher, and Anomaly Detection Isolator.
- Remove runtime producer enable settings and the controller and watchdog
  `*_AUDIT_TRAIL_ENABLED` environment variables. Retain retired Helm enable
  keys as optional, ignored booleans for upgrade compatibility; warn in standalone
  and umbrella install/upgrade release notes whenever a retired key is supplied,
  including `false`. Keep these keys out of the chart defaults.
- Keep destination selection and its existing defaults: Go producers accept
  `logging` or `gateway` and fall back to logging for an empty destination;
  Python producers log when no destination URL is configured. Helm retains
  its existing gateway and Notifications URL defaults.
- Always expose Notifications audit routes and remove the runtime
  `AUDIT_LOG_ENABLED` switch. Its Helm value `app.auditLogEnabled` follows the
  same compatibility policy as producer enable values.
- Retain the dashboard `AUDIT_EVENTS_ENABLED` flag, defaulting to on both in Helm
  and when the environment variable is omitted. Setting it to `false` hides Audit
  Events and Timeline for logging-only deployments without affecting delivery or
  ingestion.
- Keep existing gateway authentication, ingestion validation, buffering limits,
  retry behavior, and scheduler independence. Destination selection alone decides
  whether the watchdog opens its audit gateway connection.

This supersedes only the feature-flag requirements in
[ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md)
and [ADR 0033](0033-deliver-watchdog-audit-events-through-gateway.md). Their
remaining transport, authentication, persistence, and scheduler boundaries remain
in force.

## Consequences

Audit delivery and ingestion no longer depend on opt-in switches. Existing Helm
enable values do not block upgrades and no longer control runtime behavior.
Operators can remove them after seeing the release-note warning; plain
`helm template` consumers do not display release notes. Producers validate buffer
settings unconditionally. Operators can still choose local logging instead of
remote delivery; audit delivery remains asynchronous and best effort and cannot
gate recovery actions. Logging-only deployments can hide
audit UI pages rather than presenting an empty persisted-event history.

## Alternatives Rejected

### Keep flags with defaults set to true

That leaves independent disable switches and does not satisfy the requested
removal of the feature flags.

### Remove destination selection

That would force a single delivery topology and contradict the requirement to
retain configurable destinations.
