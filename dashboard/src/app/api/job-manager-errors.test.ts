// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { getJobGroupsMock, getNodeAnalysisMock } = vi.hoisted(() => ({
  getJobGroupsMock: vi.fn(),
  getNodeAnalysisMock: vi.fn(),
}));

vi.mock("~/gen/job-manager", () => ({
  getJobGroups: getJobGroupsMock,
  getNodeAnalysis: getNodeAnalysisMock,
}));
vi.mock("~/pino", () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));
vi.mock("~/server/auth", () => ({ getServerAuthSession: vi.fn() }));
vi.mock("~/utils/auth", () => ({ isAuthAvailable: () => false }));

import { GET as getJobGroups } from "./job-groups/route";
import { GET as getNodeAnalysis } from "./job-node-analysis/route";

describe.each([
  {
    path: "/api/job-groups",
    handler: getJobGroups,
    backend: getJobGroupsMock,
  },
  {
    path: "/api/job-node-analysis?expression=up",
    handler: getNodeAnalysis,
    backend: getNodeAnalysisMock,
  },
])("job-manager errors at $path", ({ path, handler, backend }) => {
  beforeEach(() => vi.resetAllMocks());

  it("returns a JSON 500 response for a network failure", async () => {
    backend.mockRejectedValue(new Error("Connection refused"));

    const response = await handler(
      new NextRequest(`http://dashboard.test${path}`),
    );

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({ detail: "" });
  });

  it.each([
    { data: undefined, detail: "" },
    { data: null, detail: "" },
    { data: "Bad Gateway", detail: "" },
    { data: {}, detail: "" },
    { data: { detail: "Backend unavailable" }, detail: "Backend unavailable" },
  ])(
    "preserves the upstream status for body $data",
    async ({ data, detail }) => {
      backend.mockRejectedValue({ response: { status: 503, data } });

      const response = await handler(
        new NextRequest(`http://dashboard.test${path}`),
      );

      expect(response.status).toBe(503);
      expect(await response.json()).toEqual({ detail });
    },
  );
});
