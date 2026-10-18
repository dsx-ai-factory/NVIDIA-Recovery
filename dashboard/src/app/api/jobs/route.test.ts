// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { getAgentApiUrlMock, getAgentWebUrlMock, getJobsMock, agentLookupMock } =
  vi.hoisted(() => ({
    getAgentApiUrlMock: vi.fn(),
    getAgentWebUrlMock: vi.fn(),
    getJobsMock: vi.fn(),
    agentLookupMock: vi.fn(),
  }));

vi.mock("~/gen/job-manager", () => ({
  getJobs: getJobsMock,
}));

vi.mock("~/lib/baseUrls", () => ({
  getAgentApiUrl: getAgentApiUrlMock,
  getAgentWebUrl: getAgentWebUrlMock,
}));

vi.mock("~/pino", () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock("~/server/auth", () => ({
  getServerAuthSession: vi.fn(),
}));

vi.mock("~/utils/auth", () => ({
  isAuthAvailable: () => false,
}));

vi.mock("~/app/api/utils", async () => import("../utils"));

import { GET } from "./route";

describe("GET /api/jobs", () => {
  beforeEach(() => {
    getJobsMock.mockReset();
    getJobsMock.mockResolvedValue({ data: { statuses: [], item_count: 0 } });
    getAgentApiUrlMock.mockReset();
    getAgentWebUrlMock.mockReset();
    agentLookupMock.mockReset();
    vi.stubGlobal("fetch", agentLookupMock);
  });

  it("forwards the workflow ID verbatim", async () => {
    const workflowId = "  experiment-42  ";
    const url = new URL("http://dashboard.test/api/jobs");
    url.searchParams.set("workflowId", workflowId);

    const response = await GET(new NextRequest(url));

    expect(response.status).toBe(200);
    expect(getJobsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.objectContaining({ workflow_id: workflowId }),
      }),
    );
  });

  it("enriches visible attempts with ordered Agent lookup results", async () => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111/");
    getAgentWebUrlMock.mockReturnValue("https://agent.example.test/base/");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [
          { job_id: "42", job_attempt_index: 0, submit_time: "first" },
          { job_id: "42", job_attempt_index: 1, submit_time: "second" },
        ],
        item_count: 2,
      },
    });
    agentLookupMock.mockResolvedValue(
      Response.json({
        results: [
          {
            job_id: "42",
            job_attempt_index: 0,
            analysis: {
              session_id: "session-0",
              analysis_type: "root_cause",
              status: "completed",
            },
          },
          {
            job_id: "42",
            job_attempt_index: 1,
            analysis: null,
          },
        ],
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs?limit=100"),
    );

    expect(response.status).toBe(200);
    expect(agentLookupMock).toHaveBeenCalledOnce();
    expect(agentLookupMock).toHaveBeenCalledWith(
      "http://agent-api:8111/analyses/lookup",
      expect.objectContaining({ method: "POST" }),
    );
    const request = agentLookupMock.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(request.body as string)).toEqual({
      analysis_type: "root_cause",
      attempts: [
        { job_id: "42", job_attempt_index: 0 },
        { job_id: "42", job_attempt_index: 1 },
      ],
    });
    expect(await response.json()).toEqual({
      statuses: [
        {
          job_id: "42",
          job_attempt_index: 0,
          submit_time: "first",
          agent_analysis: {
            session_id: "session-0",
            analysis_type: "root_cause",
            status: "completed",
          },
        },
        {
          job_id: "42",
          job_attempt_index: 1,
          submit_time: "second",
          agent_analysis: null,
        },
      ],
      item_count: 2,
      agent_web_url: "https://agent.example.test/base/",
    });
  });

  it("resolves one group analysis for every visible array member", async () => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [
          { job_id: "42_1", job_attempt_index: 0, job_group_id: "42" },
          { job_id: "42_2", job_attempt_index: 0, job_group_id: "42" },
        ],
        item_count: 2,
      },
    });
    agentLookupMock.mockResolvedValue(
      Response.json({
        results: [
          {
            job_id: "42_1",
            job_attempt_index: 0,
            job_group_id: "42",
            analysis: {
              session_id: "group-session",
              analysis_type: "job_group_pattern",
              status: "completed",
            },
          },
          {
            job_id: "42_2",
            job_attempt_index: 0,
            job_group_id: "42",
            analysis: {
              session_id: "group-session",
              analysis_type: "job_group_pattern",
              status: "completed",
            },
          },
        ],
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    const request = agentLookupMock.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(request.body as string)).toEqual({
      analysis_type: "job_group_pattern",
      attempts: [
        { job_id: "42_1", job_attempt_index: 0, job_group_id: "42" },
        { job_id: "42_2", job_attempt_index: 0, job_group_id: "42" },
      ],
    });
    const payload = await response.json();
    expect(
      payload.statuses.map(
        (status: { agent_analysis: unknown }) => status.agent_analysis,
      ),
    ).toEqual([
      expect.objectContaining({ session_id: "group-session" }),
      expect.objectContaining({ session_id: "group-session" }),
    ]);
  });

  it.each([
    "root_cause",
    "job_group_pattern",
  ])("preserves the other analysis type when %s lookup fails", async (failedType) => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [
          { job_id: "standalone", job_attempt_index: 1 },
          { job_id: "42_1", job_attempt_index: 0, job_group_id: "42" },
        ],
        item_count: 2,
      },
    });
    agentLookupMock.mockImplementation(
      async (_url: string, init: RequestInit) => {
        const body = JSON.parse(init.body as string) as {
          analysis_type: "root_cause" | "job_group_pattern";
          attempts: Array<{
            job_id: string;
            job_attempt_index: number;
            job_group_id?: string;
          }>;
        };
        if (body.analysis_type === failedType) {
          return Response.json({ error: "unavailable" }, { status: 503 });
        }
        return Response.json({
          results: body.attempts.map((attempt) => ({
            ...attempt,
            analysis: {
              session_id: `${body.analysis_type}-session`,
              analysis_type: body.analysis_type,
              status: "completed",
            },
          })),
        });
      },
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    expect(agentLookupMock).toHaveBeenCalledTimes(2);
    const payload = await response.json();
    expect(payload.statuses[0].agent_analysis).toEqual(
      failedType === "root_cause"
        ? null
        : expect.objectContaining({ session_id: "root_cause-session" }),
    );
    expect(payload.statuses[1].agent_analysis).toEqual(
      failedType === "job_group_pattern"
        ? null
        : expect.objectContaining({
            session_id: "job_group_pattern-session",
          }),
    );
  });

  it("rejects a group lookup response for another group", async () => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [
          { job_id: "42_1", job_attempt_index: 0, job_group_id: "42" },
        ],
        item_count: 1,
      },
    });
    agentLookupMock.mockResolvedValue(
      Response.json({
        results: [
          {
            job_id: "42_1",
            job_attempt_index: 0,
            job_group_id: "99",
            analysis: {
              session_id: "wrong-group",
              analysis_type: "job_group_pattern",
              status: "completed",
            },
          },
        ],
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    expect((await response.json()).statuses[0].agent_analysis).toBeNull();
  });

  it("returns jobs without links when Agent lookup is unavailable", async () => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [{ job_id: "9", job_attempt_index: 3, submit_time: "now" }],
        item_count: 1,
      },
    });
    agentLookupMock.mockResolvedValue(
      Response.json({ error: "unavailable" }, { status: 503 }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    expect(response.status).toBe(200);
    expect((await response.json()).statuses[0].agent_analysis).toBeNull();
  });

  it("returns jobs without links when Agent lookup results are unordered", async () => {
    getAgentApiUrlMock.mockReturnValue("http://agent-api:8111");
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [
          { job_id: "9", job_attempt_index: 0, submit_time: "first" },
          { job_id: "10", job_attempt_index: 0, submit_time: "second" },
        ],
        item_count: 2,
      },
    });
    agentLookupMock.mockResolvedValue(
      Response.json({
        results: [
          { job_id: "10", job_attempt_index: 0, analysis: null },
          { job_id: "9", job_attempt_index: 0, analysis: null },
        ],
      }),
    );

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    expect(response.status).toBe(200);
    expect((await response.json()).statuses).toEqual([
      expect.objectContaining({ job_id: "9", agent_analysis: null }),
      expect.objectContaining({ job_id: "10", agent_analysis: null }),
    ]);
  });

  it("skips Agent lookup when its API URL is not configured", async () => {
    getJobsMock.mockResolvedValue({
      data: {
        statuses: [{ job_id: "9", job_attempt_index: 3, submit_time: "now" }],
        item_count: 1,
      },
    });

    const response = await GET(
      new NextRequest("http://dashboard.test/api/jobs"),
    );

    expect(response.status).toBe(200);
    expect(agentLookupMock).not.toHaveBeenCalled();
    expect((await response.json()).statuses[0].agent_analysis).toBeNull();
  });
});
