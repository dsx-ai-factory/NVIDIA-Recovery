// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import type { AuditEvent } from "~/gen/notifications/types";
import {
  getAuditEventDescription,
  getAuditEventJobIdentity,
  getAuditEventOccurredAt,
  getAuditEventTitle,
  getDefaultAuditEventColumnVisibility,
  isCoarseAuditEvent,
  JOB_DETAILS_EVENT_COLUMN_IDS,
  sortAuditEventsByOccurrence,
} from "./eventPresentation";
import { getNextSelectedAuditEvent } from "./eventSelection";

function auditEvent(granularity: 1 | 11): AuditEvent {
  return {
    heimdall_job_id: "job-1",
    event_producer: "job-manager",
    event_type: "job_attempt_observed",
    event_level: "operational",
    event_produced_at: 1_725_000_000_000_000,
    payload: { state: "RUNNING" },
    summary: {
      title: "Job observed",
      description: "Job 42/0 was first observed in RUNNING",
      granularity,
    },
  };
}

function auditEventWithoutSummary(): AuditEvent {
  const { summary: _summary, ...event } = auditEvent(1);
  // Older Notifications deployments returned this runtime shape even though
  // the generated client contract now requires a summary.
  return event as AuditEvent;
}

describe("full audit event stream presentation", () => {
  it("hides diagnostic columns by default", () => {
    expect(getDefaultAuditEventColumnVisibility()).toEqual({
      component: false,
      level: false,
      payload: false,
    });
  });
});

describe("audit event row selection", () => {
  it("closes the panel when the selected row is clicked again", () => {
    const event = auditEvent(1);

    expect(getNextSelectedAuditEvent(event, event)).toBeNull();
  });

  it("switches the open panel when a different row is clicked", () => {
    const selectedEvent = auditEvent(1);
    const clickedEvent = { ...auditEvent(1), event_produced_at: 2 };

    expect(getNextSelectedAuditEvent(selectedEvent, clickedEvent)).toBe(
      clickedEvent,
    );
  });
});

describe("job details audit event presentation", () => {
  it("shows the summarized event and description without diagnostic columns", () => {
    expect(JOB_DETAILS_EVENT_COLUMN_IDS).toEqual([
      "produced_at",
      "event",
      "description",
    ]);
    expect(getAuditEventTitle(auditEvent(1))).toBe("Job observed");
    expect(getAuditEventDescription(auditEvent(1))).toBe(
      "Job 42/0 was first observed in RUNNING",
    );
  });

  it("includes only coarse-granularity audit events", () => {
    expect(isCoarseAuditEvent(auditEvent(1))).toBe(true);
    expect(isCoarseAuditEvent(auditEvent(11))).toBe(false);
  });

  it("keeps events from older APIs visible when their summary is absent", () => {
    const event = auditEventWithoutSummary();

    expect(isCoarseAuditEvent(event)).toBe(true);
    expect(getAuditEventTitle(event)).toBe("Job attempt observed");
    expect(getAuditEventDescription(event)).toBe("Summary unavailable");
  });

  it("uses source occurrence time when Watcher reports an event later", () => {
    const event = {
      ...auditEvent(1),
      event_producer: "watcher",
      event_type: "anomaly_detected",
      event_produced_at: 300,
      payload: {
        anomaly_type: "CHECKPOINTSAVE",
        event_occurred_at: 100,
      },
    };

    expect(getAuditEventOccurredAt(event)).toBe(100);
  });

  it("falls back to producer time for old or invalid occurrence payloads", () => {
    const oldEvent = { ...auditEvent(1), event_produced_at: 200 };
    const invalidEvent = {
      ...oldEvent,
      payload: { event_occurred_at: "100" },
    };

    expect(getAuditEventOccurredAt(oldEvent)).toBe(200);
    expect(getAuditEventOccurredAt(invalidEvent)).toBe(200);
  });

  it("places a checkpoint before completion using its source time", () => {
    const completed = {
      ...auditEvent(1),
      event_type: "job_attempt_updated",
      event_produced_at: 200,
    };
    const checkpoint = {
      ...auditEvent(1),
      event_producer: "watcher",
      event_type: "anomaly_detected",
      event_produced_at: 300,
      payload: {
        anomaly_type: "CHECKPOINTSAVE",
        event_occurred_at: 100,
      },
    };

    expect(sortAuditEventsByOccurrence([completed, checkpoint])).toEqual([
      checkpoint,
      completed,
    ]);
  });
});

describe("audit event job identity", () => {
  it.each([
    { payload: { job_id: "42", attempt_index: 2 }, expected: "42 (2)" },
    { payload: { job_id: "42", attempt_index: 0 }, expected: "42 (0)" },
    { payload: { job_id: "42" }, expected: "42 (--)" },
    { payload: { attempt_index: 0 }, expected: "-- (0)" },
    { payload: {}, expected: "-- (--)" },
    { payload: { job_id: null, attempt_index: null }, expected: "-- (--)" },
    { payload: null, expected: "-- (--)" },
    { payload: undefined, expected: "-- (--)" },
    { payload: "unexpected", expected: "-- (--)" },
    { payload: 42, expected: "-- (--)" },
    { payload: false, expected: "-- (--)" },
  ])("renders $payload as $expected", ({ payload, expected }) => {
    expect(getAuditEventJobIdentity({ payload })).toBe(expected);
  });
});
