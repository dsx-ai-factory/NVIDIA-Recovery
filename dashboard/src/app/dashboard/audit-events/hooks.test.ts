// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";

const { useQuery, fetchAuditEvents } = vi.hoisted(() => ({
  useQuery: vi.fn((options) => options),
  fetchAuditEvents: vi.fn(),
}));

vi.mock("@tanstack/react-query", () => ({
  useQuery,
  keepPreviousData: vi.fn(),
}));
vi.mock("./api", () => ({ fetchAuditEvents }));

import { useGetAllAuditEvents, useGetAuditEvents } from "./hooks";

function auditPage(items: unknown[], nextCursor = "") {
  return {
    items,
    cursor: { prev_cursor: "", next_cursor: nextCursor },
  };
}

describe("audit event query", () => {
  it("keeps event type in both the cache identity and the request", () => {
    const filters = {
      job_id: "12345",
      event_type: "job_attempt_observed",
      event_producer: "job-manager",
    };
    useGetAuditEvents(filters);

    const options = useQuery.mock.calls[0]![0];
    expect(options.queryKey).toEqual([
      "audit-events",
      expect.objectContaining(filters),
    ]);
    options.queryFn();
    expect(fetchAuditEvents).toHaveBeenCalledWith(
      expect.objectContaining(filters),
    );
  });

  it("loads every page for consumers that require global ordering", async () => {
    const firstPageEvent = { event_type: "first-page" };
    const secondPageEvent = { event_type: "second-page" };
    fetchAuditEvents
      .mockResolvedValueOnce(auditPage([firstPageEvent], "second-page"))
      .mockResolvedValueOnce(auditPage([secondPageEvent]));

    useGetAllAuditEvents({
      job_id: "scheduler-job",
      limit: 100,
      sort_by: "event_produced_at:asc",
    });

    const options = useQuery.mock.calls[0]![0];
    await expect(options.queryFn()).resolves.toEqual(
      auditPage([firstPageEvent, secondPageEvent]),
    );
    expect(fetchAuditEvents).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ cursor: undefined }),
    );
    expect(fetchAuditEvents).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ cursor: "second-page" }),
    );
  });

  it("stops when the backend repeats a cursor", async () => {
    fetchAuditEvents
      .mockResolvedValueOnce(auditPage([], "repeated"))
      .mockResolvedValueOnce(auditPage([], "repeated"));

    useGetAllAuditEvents({ heimdall_job_id: "internal-id", limit: 100 });

    const options = useQuery.mock.calls[0]![0];
    await expect(options.queryFn()).rejects.toThrow(
      "Audit event pagination returned a repeated cursor.",
    );
  });
});
