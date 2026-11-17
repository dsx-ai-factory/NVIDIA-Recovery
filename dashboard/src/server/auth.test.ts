// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { getAuthConfigurationMock, getServerSessionMock } = vi.hoisted(() => ({
  getAuthConfigurationMock: vi.fn(),
  getServerSessionMock: vi.fn(),
}));

vi.mock("next-auth", () => ({
  getServerSession: getServerSessionMock,
}));

vi.mock("next-auth/providers/azure-ad", () => ({
  default: vi.fn(),
}));

vi.mock("next-auth/providers/credentials", () => ({
  default: vi.fn(),
}));

vi.mock("~/env", () => ({
  env: { API_GATEWAY_URL: "https://gateway.example.test" },
}));

vi.mock("~/pino", () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock("~/lib/base-path", () => ({
  withBasePath: (path: string) => path,
}));

vi.mock("~/utils/auth", () => ({
  getAuthConfiguration: getAuthConfigurationMock,
}));

import { getAuthOptions, getServerAuthSession } from "./auth";

describe("getServerAuthSession", () => {
  beforeEach(() => {
    getAuthConfigurationMock.mockReset();
    getServerSessionMock.mockReset();
  });

  it("returns null without invoking NextAuth when auth is unavailable", async () => {
    getAuthConfigurationMock.mockReturnValue({
      available: false,
      provider: null,
    });

    await expect(getServerAuthSession()).resolves.toBeNull();
    expect(getServerSessionMock).not.toHaveBeenCalled();
  });
});

describe("gateway session in the NextAuth JWT", () => {
  const expiring = {
    bearerToken: "old-bearer",
    idToken: "old-id",
    expiresAt: Date.now() + 1000,
    cookies: { idToken: "i", accessToken: "a", refreshToken: "r" },
  };

  beforeEach(() => {
    getAuthConfigurationMock.mockReturnValue({
      available: true,
      provider: "gateway",
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("refreshes a session that is about to expire", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            idToken: "new-id",
            bearerToken: "new-bearer",
            expiresAt: Math.floor(Date.now() / 1000) + 3600,
          }),
          { status: 200 },
        ),
      ),
    );

    const token = await getAuthOptions().callbacks!.jwt!({
      token: { gateway: expiring },
    } as never);

    expect(token.gateway?.bearerToken).toBe("new-bearer");
    expect(token.error).toBeUndefined();
  });

  it("drops the credential and marks the user signed out when refresh fails", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("revoked", { status: 401 })),
    );

    const token = await getAuthOptions().callbacks!.jwt!({
      token: { gateway: expiring },
    } as never);
    getServerSessionMock.mockResolvedValue({
      user: { error: token.error },
    });

    expect(token.gateway).toBeUndefined();
    await expect(getServerAuthSession()).resolves.toBeNull();
  });
});
