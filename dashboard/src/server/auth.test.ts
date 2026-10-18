// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from "vitest";

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
  env: {},
}));

vi.mock("~/pino", () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("~/lib/base-path", () => ({
  withBasePath: (path: string) => path,
}));

vi.mock("~/utils/auth", () => ({
  getAuthConfiguration: getAuthConfigurationMock,
}));

import { getServerAuthSession } from "./auth";

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
