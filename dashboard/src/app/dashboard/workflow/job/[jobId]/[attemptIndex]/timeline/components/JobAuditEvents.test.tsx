// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  auditEvents,
  eventsTableProps,
  sortAuditEventsByOccurrence,
  useGetAllAuditEvents,
} = vi.hoisted(() => {
  const auditEvents = [
    { event_type: "later", event_produced_at: 300 },
    { event_type: "earlier", event_produced_at: 400 },
  ];
  return {
    auditEvents,
    eventsTableProps: vi.fn(),
    sortAuditEventsByOccurrence: vi.fn((events: unknown[]) =>
      [...events].reverse(),
    ),
    useGetAllAuditEvents: vi.fn(() => ({
      data: {
        items: auditEvents,
        cursor: { prev_cursor: "", next_cursor: "" },
      },
      error: undefined,
      isLoading: false,
    })),
  };
});

vi.mock("~/app/dashboard/audit-events/hooks", () => ({
  useGetAllAuditEvents,
}));

vi.mock(
  "~/app/dashboard/audit-events/sort",
  () => import("../../../../../../audit-events/sort"),
);

vi.mock("~/app/dashboard/audit-events/components/eventPresentation", () => ({
  isCoarseAuditEvent: () => true,
  sortAuditEventsByOccurrence,
}));

vi.mock("~/app/dashboard/audit-events/components/EventsTable", () => ({
  EventsTable: (props: { auditEvents: unknown[] }) => {
    eventsTableProps(props);
    return <div>Events</div>;
  },
}));

vi.mock("~/lib/base-path", () => ({
  withBasePath: (path: string) => path,
}));

vi.mock("@nvidia/foundations-react-core", () => ({
  Banner: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));

import { JobAuditEvents } from "./JobAuditEvents";

describe("JobAuditEvents", () => {
  beforeEach(() => {
    useGetAllAuditEvents.mockClear();
    eventsTableProps.mockClear();
    sortAuditEventsByOccurrence.mockClear();
  });

  it("queries by Heimdall ID when the job record provides one", () => {
    const html = renderToStaticMarkup(
      <JobAuditEvents
        heimdallJobId="internal-id"
        jobId="12345"
        attemptIndex="2"
      />,
    );

    expect(useGetAllAuditEvents).toHaveBeenCalledWith({
      heimdall_job_id: "internal-id",
      limit: 100,
      sort_by: "event_produced_at:asc",
    });
    expect(html).toContain(
      "job_id=12345&amp;attempt_index=2&amp;sort_by=event_produced_at%3Aasc",
    );
    expect(html).not.toContain("heimdall_job_id=internal-id");
  });

  it("queries by scheduler identity when no Heimdall ID is available", () => {
    renderToStaticMarkup(<JobAuditEvents jobId="12345" attemptIndex="2" />);

    expect(useGetAllAuditEvents).toHaveBeenCalledWith(
      expect.objectContaining({
        job_id: "12345",
        attempt_index: 2,
        sort_by: "event_produced_at:asc",
      }),
    );
  });

  it("passes the globally occurrence-sorted events to the timeline table", () => {
    renderToStaticMarkup(<JobAuditEvents jobId="12345" attemptIndex="2" />);

    expect(sortAuditEventsByOccurrence).toHaveBeenCalledWith(auditEvents);
    expect(eventsTableProps).toHaveBeenCalledWith(
      expect.objectContaining({ auditEvents: [...auditEvents].reverse() }),
    );
  });
});
