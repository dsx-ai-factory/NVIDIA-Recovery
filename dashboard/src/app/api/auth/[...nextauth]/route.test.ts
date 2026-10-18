// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  delegateMock,
  getAuthConfigurationMock,
  getAuthOptionsMock,
  nextAuthMock,
} = vi.hoisted(() => {
  const delegate = vi.fn();
  return {
    delegateMock: delegate,
    getAuthConfigurationMock: vi.fn(),
    getAuthOptionsMock: vi.fn(),
    nextAuthMock: vi.fn(() => delegate),
  };
});

vi.mock("next-auth", () => ({
  default: nextAuthMock,
}));

vi.mock("~/server/auth", () => ({
  getAuthOptions: getAuthOptionsMock,
}));

vi.mock("~/utils/auth", () => ({
  getAuthConfiguration: getAuthConfigurationMock,
}));

describe("/api/auth/[...nextauth]", () => {
  beforeEach(() => {
    vi.resetModules();
    delegateMock.mockReset();
    getAuthConfigurationMock.mockReset();
    getAuthOptionsMock.mockReset();
    nextAuthMock.mockClear();
  });

  it.each([
    "GET",
    "POST",
  ] as const)("returns 404 for %s when authentication is unavailable", async (method) => {
    getAuthConfigurationMock.mockReturnValue({
      available: false,
      provider: null,
    });

    const route = await import("./route");
    const request = new NextRequest("http://dashboard.test/api/auth/session", {
      method,
    });
    const context = {
      params: Promise.resolve({ nextauth: ["session"] }),
    };

    const response = await route[method](request, context);

    expect(response.status).toBe(404);
    expect(nextAuthMock).not.toHaveBeenCalled();
    expect(delegateMock).not.toHaveBeenCalled();
  });

  it.each([
    "GET",
    "POST",
  ] as const)("delegates %s to NextAuth when authentication is available", async (method) => {
    const authOptions = { providers: [] };
    const delegatedResponse = new Response("delegated", { status: 200 });
    getAuthConfigurationMock.mockReturnValue({
      available: true,
      provider: "starfleet",
    });
    getAuthOptionsMock.mockReturnValue(authOptions);
    delegateMock.mockResolvedValue(delegatedResponse);

    const route = await import("./route");
    const request = new NextRequest("http://dashboard.test/api/auth/session", {
      method,
    });
    const context = {
      params: Promise.resolve({ nextauth: ["session"] }),
    };

    const response = await route[method](request, context);

    expect(nextAuthMock).toHaveBeenCalledWith(authOptions);
    expect(delegateMock).toHaveBeenCalledWith(request, context);
    expect(response).toBe(delegatedResponse);
  });
});
