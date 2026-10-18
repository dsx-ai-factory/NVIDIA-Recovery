// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { pageContent } = vi.hoisted(() => ({
  pageContent: vi.fn(() => <div>Events</div>),
}));

vi.mock("./components/PageContent", () => ({ PageContent: pageContent }));
vi.mock("../components/PageLayout", () => ({
  PageLayout: ({ children }: { children: React.ReactNode }) => children,
}));
vi.mock("~/utils/config", () => ({ isAuditEventsEnabled: () => true }));
vi.mock("~/lib/base-path", () => ({ withBasePath: (path: string) => path }));

import AuditEventsPage from "./page";

describe("audit event deep links", () => {
  beforeEach(() => pageContent.mockClear());

  it.each([
    undefined,
    "0",
    "2",
  ])("preserves a job filter with attempt %j and the other filters", async (attempt) => {
    renderToStaticMarkup(
      await AuditEventsPage({
        searchParams: Promise.resolve({
          job_id: "12345",
          attempt_index: attempt,
          event_type: "job_attempt_observed",
          event_producer: "job-manager",
          event_level: "operational",
          min_event_produced_at: "0",
          max_event_produced_at: "20",
          limit: "25",
          cursor: "next-page",
          sort_by: "event_produced_at:asc",
        }),
      }),
    );

    expect(pageContent).toHaveBeenCalledWith(
      expect.objectContaining({
        initialJobId: "12345",
        initialAttemptIndex:
          attempt === undefined ? undefined : Number(attempt),
        initialEventType: "job_attempt_observed",
        initialEventProducer: "job-manager",
        initialEventLevel: "operational",
        initialMinEventProducedAt: 0,
        initialMaxEventProducedAt: 20,
        initialLimit: 25,
        initialCursor: "next-page",
        initialSort: "event_produced_at:asc",
      }),
      undefined,
    );
  });

  it.each([
    "",
    "-1",
    "1.5",
    "nope",
  ])("rejects explicit malformed attempt %j without displaying broader results", async (attempt) => {
    const html = renderToStaticMarkup(
      await AuditEventsPage({
        searchParams: Promise.resolve({
          job_id: "12345",
          attempt_index: attempt,
        }),
      }),
    );

    expect(html).toContain('role="alert"');
    expect(pageContent).not.toHaveBeenCalled();
  });

  it("rejects repeated attempt parameters without displaying broader results", async () => {
    const html = renderToStaticMarkup(
      await AuditEventsPage({
        searchParams: Promise.resolve({
          job_id: "12345",
          attempt_index: ["0", "1"],
        }),
      }),
    );

    expect(html).toContain('role="alert"');
    expect(pageContent).not.toHaveBeenCalled();
  });
});
