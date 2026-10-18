# ADR 0050: Expose authenticated whole-job audit reads

Status: accepted

Date: 2026-10-06

> **Implementation note (2026-10-06):** [The generated facade](../../api-gateway/protos/heimdall/v1/audit.proto)
> at `d111a203` implements these reads; deployment migration/backfill and live
> authentication qualification remain required before release acceptance.

## Context

The dashboard can read persisted audit events, but external consumers lack a
generated gateway read API. Exact-attempt queries cannot retrieve the complete
history of a job without separately enumerating its attempts. Audit history
must remain readable after Job Manager records have been removed.

This decision extends [ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md)
to reads, partially superseding its restriction of generated audit routes to
ingestion. Existing listener authentication, ingestion behavior, audit ownership,
and the always-produced audit policy in ADR 0036 remain unchanged.

## Decision

- Add `AuditService.GetAuditEvents` and generated `GET /v1/audit/events` on the
  existing authenticated gateway listeners. Introduce no new listener,
  authentication mechanism, tenant identity, or cluster identity.
- Notifications owns queries, filtering, pagination, storage, and retention.
  The gateway translates its generated contract and does not enumerate Job
  Manager attempts or expose internal storage metadata.
- Accept an exact `heimdall_job_id`, or `job_id` with optional `attempt_index`.
  Attempt zero is valid. A job without an attempt selects all its attempts;
  conflicting selectors and attempt-only requests are invalid. Preserve the
  existing deployment-wide query when no identity is supplied and the current
  access boundaries.
- Support event type, level, producer, timestamp bounds, ordering, and opaque
  pagination cursors. Keep the existing maximum page size of 100 and stable
  producer-timestamp/row-ID ordering. Filter changes reset dashboard cursors;
  existing filtered deep links remain valid.
- Preserve the 4 KiB ingestion-response bound and use a separate 4 MiB read
  bound. Oversized reads fail explicitly; they are never silently truncated.

## Migration and rollback

Add nullable `scheduler_job_id` and an index ordered by scheduler job ID,
producer timestamp, and row ID. Populate it only from valid existing payload
identity that agrees with the envelope's deterministic job-attempt identity.
This is a deployment-local projection, not a new externally supplied identity.

Deploy the additive schema first, then writers. Run bounded, rerunnable
backfill batches, drain old writers, and perform a final catch-up before enabling
whole-job consumers. Retain unmappable records and report their whole-job query
limitation; exact identity and global reads continue to expose retained history.

Rollback disables the new read consumers while retaining the additive schema
and all audit history. Production DDL cost and backfill duration must be measured
for the actual database; small local fixtures do not establish those bounds.

## Consequences

Public consumers and dashboard users can inspect every recorded attempt without
depending on Job Manager retention. Request and response bounds limit resource
use but do not replace existing authentication. This change adds no per-job
authorization policy; introducing one requires a separate product decision.

Historical data without trustworthy identity cannot safely be guessed into a
whole-job result. Audit delivery remains best effort, so an empty result is not
proof that no detection, decision, or scheduler action occurred.

## Alternatives rejected

- Enumerating attempts through Job Manager couples audit access to another
  service's availability and retention.
- A separate audit ingress duplicates the existing authentication boundary.
- Guessing malformed historical identities risks assigning evidence to the
  wrong job; deleting them would lose retained history.
