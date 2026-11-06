// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import {
  buildGatewayLoginUrl,
  DEFAULT_RETURN_PATH,
  returnPathFromCallbackUrl,
} from "./gatewayLogin";

const origin = "https://dash.example.test";

describe("returnPathFromCallbackUrl", () => {
  it("keeps same-site destinations, including the query", () => {
    expect(
      returnPathFromCallbackUrl(
        `${origin}/dashboard/jobs?state=FAILED`,
        origin,
      ),
    ).toBe("/dashboard/jobs?state=FAILED");
    expect(returnPathFromCallbackUrl("/dashboard/jobs", origin)).toBe(
      "/dashboard/jobs",
    );
  });

  it.each([
    "https://evil.example.test/dashboard",
    "//evil.example.test/dashboard",
    undefined,
    null,
  ])("does not leave the site for %s", (callbackUrl) => {
    expect(returnPathFromCallbackUrl(callbackUrl, origin)).toBe(
      DEFAULT_RETURN_PATH,
    );
  });
});

describe("buildGatewayLoginUrl", () => {
  it("sends the gateway back to the callback carrying the return path", () => {
    const login = new URL(
      buildGatewayLoginUrl(
        "https://gateway.example.test/api",
        "/dashboard/jobs?a=1&b=2",
      ),
    );
    const redirect = new URL(login.searchParams.get("redirectURI")!, origin);

    expect(login.origin + login.pathname).toBe(
      "https://gateway.example.test/api/v1/auth/login",
    );
    expect(redirect.pathname).toBe("/api/auth/callback");
    expect(redirect.searchParams.get("returnPath")).toBe(
      "/dashboard/jobs?a=1&b=2",
    );
  });
});
