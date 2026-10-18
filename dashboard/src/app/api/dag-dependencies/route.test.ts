// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { getDagDependenciesMock } = vi.hoisted(() => ({
  getDagDependenciesMock: vi.fn(),
}));

vi.mock("~/gen/job-manager", () => ({
  getDagDependencies: getDagDependenciesMock,
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

import { GET } from "./route";

describe("GET /api/dag-dependencies", () => {
  beforeEach(() => {
    getDagDependenciesMock.mockReset();
    getDagDependenciesMock.mockResolvedValue({
      data: {
        dag_dependencies: [],
        item_count: 0,
      },
    });
  });

  it("requires a dependent job ID", async () => {
    const response = await GET(
      new NextRequest("http://dashboard.test/api/dag-dependencies"),
    );

    expect(response.status).toBe(400);
    expect(getDagDependenciesMock).not.toHaveBeenCalled();
  });

  it("fetches the dependency records for the selected job", async () => {
    const url = new URL("http://dashboard.test/api/dag-dependencies");
    url.searchParams.set("dependentJobId", "child-42");

    const response = await GET(new NextRequest(url));

    expect(response.status).toBe(200);
    expect(getDagDependenciesMock).toHaveBeenCalledWith({
      query: {
        dependent_job_id: "child-42",
        limit: 1000,
        offset: 0,
        sort_by: "dependency_job_identifier:asc",
      },
    });
  });
});
