// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("~/env", () => ({
  env: { API_GATEWAY_URL: "https://gateway.example.test/api" },
}));

import {
  GATEWAY_REFRESH_THRESHOLD_MS,
  isGatewaySessionExpiring,
  parseGatewayCookies,
  refreshGatewaySession,
  userFromIdToken,
} from "./gateway-auth";

const cookies = {
  idToken: "id-1",
  accessToken: "access-1",
  refreshToken: "refresh-1",
};

function idToken(claims: Record<string, unknown>) {
  const encode = (value: unknown) =>
    Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "none" })}.${encode(claims)}.signature`;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("parseGatewayCookies", () => {
  it("returns the three gateway cookies exactly as sent by the browser", () => {
    expect(
      parseGatewayCookies(
        "other=1; idToken=id%3D1; accessToken=access-1; refreshToken=refresh-1",
      ),
    ).toEqual({ ...cookies, idToken: "id%3D1" });
  });

  it("requires all three cookies", () => {
    expect(parseGatewayCookies("idToken=a; accessToken=b")).toBeUndefined();
    expect(parseGatewayCookies(undefined)).toBeUndefined();
  });
});

describe("refreshGatewaySession", () => {
  it("returns the bearer and the cookies the gateway rotated", async () => {
    const response = new Response(
      JSON.stringify({
        idToken: "raw-id",
        bearerToken: "bearer",
        expiresAt: 2000,
      }),
      { status: 200 },
    );
    response.headers.append("Set-Cookie", "idToken=id-2; Path=/; HttpOnly");
    response.headers.append("Set-Cookie", "refreshToken=refresh-2; Path=/");
    const fetchMock = vi.fn().mockResolvedValue(response);
    vi.stubGlobal("fetch", fetchMock);

    const session = await refreshGatewaySession(cookies);

    expect(session).toEqual({
      bearerToken: "bearer",
      idToken: "raw-id",
      expiresAt: 2_000_000,
      // The unrotated accessToken is kept; the rotated ones replace the old values.
      cookies: {
        idToken: "id-2",
        accessToken: "access-1",
        refreshToken: "refresh-2",
      },
    });
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe("https://gateway.example.test/api/v1/auth/refresh");
    expect(init.headers.Cookie).toBe(
      "idToken=id-1; accessToken=access-1; refreshToken=refresh-1",
    );
  });

  it("rejects when the gateway refuses the cookies", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("expired", { status: 401 })),
    );

    await expect(refreshGatewaySession(cookies)).rejects.toThrow(/401/);
  });
});

describe("isGatewaySessionExpiring", () => {
  it("flags sessions that expire inside the refresh window", () => {
    const now = 1_000_000;
    expect(
      isGatewaySessionExpiring(
        { expiresAt: now + GATEWAY_REFRESH_THRESHOLD_MS - 1 },
        now,
      ),
    ).toBe(true);
    expect(
      isGatewaySessionExpiring(
        { expiresAt: now + GATEWAY_REFRESH_THRESHOLD_MS + 1 },
        now,
      ),
    ).toBe(false);
  });
});

describe("userFromIdToken", () => {
  it("maps OIDC claims to the dashboard user", () => {
    const token = idToken({
      sub: "user-1",
      name: "Ada Lovelace",
      email: "ada@example.test",
      preferred_username: "ada",
      picture: "https://img.example.test/ada.png",
    });

    expect(userFromIdToken(token)).toEqual({
      id: "user-1",
      name: "Ada Lovelace",
      email: "ada@example.test",
      username: "ada",
      image: "https://img.example.test/ada.png",
    });
  });

  it("falls back to the email for the display name", () => {
    const token = idToken({ sub: "user-1", email: "ada@example.test" });

    expect(userFromIdToken(token).name).toBe("ada@example.test");
  });
});
