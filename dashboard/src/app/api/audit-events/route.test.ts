// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { auditEventsGetMock } = vi.hoisted(() => ({
  auditEventsGetMock: vi.fn(),
}));

vi.mock("~/gen/notifications", () => ({
  auditEventsGet: auditEventsGetMock,
}));

vi.mock("~/pino", () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("~/server/auth", () => ({
  getServerAuthSession: vi.fn(),
}));

vi.mock("~/utils/auth", () => ({
  isAuthAvailable: () => false,
}));

vi.mock("./mock", () => ({
  MOCK_AUDIT_EVENTS: [],
  decodeMockCursor: () => 0,
  encodeMockCursor: () => "",
  randomMockDelayMs: () => 0,
}));

import { GET } from "./route";

describe("GET /api/audit-events", () => {
  beforeEach(() => {
    auditEventsGetMock.mockReset();
    auditEventsGetMock.mockResolvedValue({
      data: {
        cursor: { prev_cursor: "", next_cursor: "" },
        items: [],
      },
    });
  });

  it.each([
    ["", undefined],
    ["?sort_by=", undefined],
    ["?sort_by=event_produced_at:asc", "event_produced_at:asc"],
    ["?sort_by=event_produced_at:desc", "event_produced_at:desc"],
  ])("forwards timestamp ordering for %s", async (query, sort) => {
    const response = await GET(
      new NextRequest(`http://dashboard.test/api/audit-events${query}`),
    );
    expect(response.status).toBe(200);
    expect(auditEventsGetMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.objectContaining({ sort_by: sort }),
      }),
    );
  });

  it("preserves the API validation error for unsupported ordering", async () => {
    auditEventsGetMock.mockRejectedValue(
      Object.assign(new Error("Request failed"), {
        response: { status: 422, data: { detail: "Invalid audit event sort" } },
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/audit-events?sort_by=job_id"),
    );
    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({
      detail: "Invalid audit event sort",
    });
    expect(auditEventsGetMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.objectContaining({ sort_by: "job_id" }),
      }),
    );
  });

  it.each([0, 2])("forwards an exact scheduler attempt %i", async (attempt) => {
    const response = await GET(
      new NextRequest(
        `http://dashboard.test/api/audit-events?job_id=12345&attempt_index=${attempt}`,
      ),
    );

    expect(response.status).toBe(200);
    expect(auditEventsGetMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.objectContaining({
          job_id: "12345",
          attempt_index: attempt,
        }),
      }),
    );
  });

  it("forwards a job-only filter with event type and existing filters", async () => {
    const response = await GET(
      new NextRequest(
        "http://dashboard.test/api/audit-events?job_id=12345&event_type=job_attempt_observed&event_producer=job-manager&event_level=operational&min_event_produced_at=10&max_event_produced_at=20&limit=25&cursor=next-page&sort_by=event_produced_at:asc",
      ),
    );

    expect(response.status).toBe(200);
    expect(auditEventsGetMock).toHaveBeenCalledWith({
      query: {
        job_id: "12345",
        attempt_index: undefined,
        heimdall_job_id: undefined,
        event_type: "job_attempt_observed",
        event_producer: "job-manager",
        event_level: "operational",
        min_event_produced_at: 10,
        max_event_produced_at: 20,
        limit: 25,
        cursor: "next-page",
        sort_by: "event_produced_at:asc",
      },
    });
  });

  it.each([
    "",
    " ",
    "-1",
    "1.5",
    "1e2",
    "9007199254740992",
  ])("rejects malformed explicit attempt index %j", async (attemptIndex) => {
    const response = await GET(
      new NextRequest(
        `http://dashboard.test/api/audit-events?job_id=12345&attempt_index=${encodeURIComponent(attemptIndex)}`,
      ),
    );

    expect(response.status).toBe(422);
    expect(auditEventsGetMock).not.toHaveBeenCalled();
  });

  it("preserves a Notifications validation error", async () => {
    auditEventsGetMock.mockRejectedValue(
      Object.assign(new Error("Request failed"), {
        response: { status: 422, data: { detail: "Invalid job identity" } },
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/audit-events?attempt_index=0"),
    );

    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({ detail: "Invalid job identity" });
  });

  it("handles a Notifications connection failure without throwing", async () => {
    auditEventsGetMock.mockRejectedValue(new Error("connect ECONNREFUSED"));

    const response = await GET(
      new NextRequest("http://dashboard.test/api/audit-events"),
    );

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({ detail: "connect ECONNREFUSED" });
  });
});
