// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type {
  AuditEvent,
  AuditEventHumanSummaryGranularityClassKey,
} from "~/gen/notifications/types";

/** Granularity assigned by the audit API to events in its abbreviated timeline. */
export const COARSE_AUDIT_EVENT_GRANULARITY: AuditEventHumanSummaryGranularityClassKey = 1;

/** Columns shown in the compact, job-scoped audit event table. */
export const JOB_DETAILS_EVENT_COLUMN_IDS = [
  "produced_at",
  "event",
  "description",
] as const;

/** Initial visibility for optional columns on the full audit-event stream. */
export function getDefaultAuditEventColumnVisibility(): Record<
  string,
  boolean
> {
  return {
    component: false,
    level: false,
    payload: false,
  };
}

function getRuntimeSummary(
  event: AuditEvent,
): Partial<AuditEvent["summary"]> | undefined {
  const summary = (event as { summary?: unknown }).summary;
  return summary && typeof summary === "object"
    ? (summary as Partial<AuditEvent["summary"]>)
    : undefined;
}

function formatEventType(eventType: string): string {
  const label = eventType.replaceAll("_", " ");
  return label.charAt(0).toUpperCase() + label.slice(1);
}

/** Returns the human-readable event name supplied by the audit API. */
export function getAuditEventTitle(event: AuditEvent): string {
  return getRuntimeSummary(event)?.title || formatEventType(event.event_type);
}

/** Returns the concise event description supplied by the audit API. */
export function getAuditEventDescription(event: AuditEvent): string {
  return getRuntimeSummary(event)?.description || "Summary unavailable";
}

/** Reports whether an event belongs in a coarse audit timeline. */
export function isCoarseAuditEvent(event: AuditEvent): boolean {
  const granularity = getRuntimeSummary(event)?.granularity;
  return (
    granularity === undefined || granularity === COARSE_AUDIT_EVENT_GRANULARITY
  );
}

/** Returns the source occurrence time when a producer supplies one. */
export function getAuditEventOccurredAt(event: AuditEvent): number {
  const occurredAt = event.payload?.event_occurred_at;
  return typeof occurredAt === "number" &&
    Number.isSafeInteger(occurredAt) &&
    occurredAt >= 0
    ? occurredAt
    : event.event_produced_at;
}

/** Orders a timeline by source occurrence time, preserving emission time as a deterministic tie-breaker. */
export function sortAuditEventsByOccurrence(
  events: AuditEvent[],
): AuditEvent[] {
  return [...events].sort((left, right) => {
    const occurrenceDifference =
      getAuditEventOccurredAt(left) - getAuditEventOccurredAt(right);
    return (
      occurrenceDifference || left.event_produced_at - right.event_produced_at
    );
  });
}

function stringifiedPropOrNone(payload: unknown, key: string): string {
  if (payload === null || typeof payload !== "object") {
    return "--";
  }

  const value = (payload as Record<string, unknown>)[key];
  return value === null || value === undefined ? "--" : String(value);
}

/** Formats the scheduler job and attempt, preserving zero and marking missing identity. */
export function getAuditEventJobIdentity(event: { payload: unknown }): string {
  return `${stringifiedPropOrNone(event.payload, "job_id")} (${stringifiedPropOrNone(event.payload, "attempt_index")})`;
}
