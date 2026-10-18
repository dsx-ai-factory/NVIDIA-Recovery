// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  type AuditEvent,
  auditEventHumanSummaryGranularityClass,
  auditEventLevel,
} from "~/gen/notifications/types";

/** Build presentation metadata required by the audit-event query contract. */
export function createMockAuditEventSummary(
  eventType: string,
  granularity: AuditEvent["summary"]["granularity"] = auditEventHumanSummaryGranularityClass[
    "11"
  ],
): AuditEvent["summary"] {
  const title = eventType.replace(/_/g, " ");
  return {
    title,
    description: `Mock ${title} event`,
    granularity,
  };
}

export function decodeMockCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  const offset = Number(Buffer.from(cursor, "base64").toString("utf-8"));
  return Number.isFinite(offset) && offset >= 0 ? offset : 0;
}

export function encodeMockCursor(offset: number): string {
  return Buffer.from(String(offset)).toString("base64");
}

export function randomMockDelayMs(): number {
  return 500 + Math.floor(Math.random() * 500);
}

// Deterministic PRNG (mulberry32) so the mock dataset below is stable across
// requests within a process. Regenerating random data per-request would make
// the cursor-based slicing return a different underlying list each fetch,
// breaking Next/Prev pagination.
function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const HEIMDALL_JOB_IDS = Array.from({ length: 12 }, (_, i) => String(100 + i));
const EVENT_TYPES_BY_PRODUCER: Record<string, string[]> = {
  scheduler: ["job_scheduled", "job_queued"],
  advisor: ["recovery_action_taken", "recovery_action_failed"],
  event_producer_1: ["job_completed", "event_type_1"],
  notifier: ["notification_sent", "notification_failed"],
};
const EVENT_PRODUCERS = Object.keys(EVENT_TYPES_BY_PRODUCER);
const EVENT_LEVELS = Object.values(auditEventLevel);
const SUMMARY_GRANULARITIES = [1, 11] as const;

function generateMockAuditEvents(count: number): AuditEvent[] {
  const random = mulberry32(42);
  const pick = <T>(items: readonly T[]): T => {
    const item = items[Math.floor(random() * items.length)];
    if (item === undefined) throw new Error("empty pick pool");
    return item;
  };

  // event_produced_at is Unix microseconds per the API contract.
  let producedAt = 1719859200_000_000;
  return Array.from({ length: count }, () => {
    const eventProducer = pick(EVENT_PRODUCERS);
    const eventType = pick(EVENT_TYPES_BY_PRODUCER[eventProducer] ?? []);
    const eventLevel = pick(EVENT_LEVELS);
    producedAt += (Math.floor(random() * 120) + 30) * 1_000_000;
    return {
      heimdall_job_id: pick(HEIMDALL_JOB_IDS),
      event_producer: eventProducer,
      event_type: eventType,
      event_level: eventLevel,
      payload: { level: eventLevel, reason: eventType },
      event_produced_at: producedAt,
      summary: createMockAuditEventSummary(
        eventType,
        pick(SUMMARY_GRANULARITIES),
      ),
    };
  });
}

export const MOCK_AUDIT_EVENTS: AuditEvent[] = generateMockAuditEvents(150);
