// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { getUserMenuModel } from "./model";

describe("getUserMenuModel", () => {
  it("omits every identity and auth action when auth is unavailable", () => {
    expect(
      getUserMenuModel({
        authAvailable: false,
        isApiKeysEnabled: true,
        sessionStatus: "unauthenticated",
      }),
    ).toEqual({
      showApiKeys: false,
      showIdentity: false,
      showSignIn: false,
      showSignOut: false,
    });
  });

  it("shows only Sign In when auth is available without a session", () => {
    expect(
      getUserMenuModel({
        authAvailable: true,
        isApiKeysEnabled: true,
        sessionStatus: "unauthenticated",
      }),
    ).toEqual({
      showApiKeys: false,
      showIdentity: false,
      showSignIn: true,
      showSignOut: false,
    });
  });

  it("shows identity and Sign Out for an authenticated session", () => {
    expect(
      getUserMenuModel({
        authAvailable: true,
        isApiKeysEnabled: false,
        sessionStatus: "authenticated",
      }),
    ).toEqual({
      showApiKeys: false,
      showIdentity: true,
      showSignIn: false,
      showSignOut: true,
    });
  });

  it("shows API Keys only for an authenticated session with the feature enabled", () => {
    expect(
      getUserMenuModel({
        authAvailable: true,
        isApiKeysEnabled: true,
        sessionStatus: "authenticated",
      }),
    ).toEqual({
      showApiKeys: true,
      showIdentity: true,
      showSignIn: false,
      showSignOut: true,
    });
  });

  it("does not offer an auth action while the session is loading", () => {
    expect(
      getUserMenuModel({
        authAvailable: true,
        isApiKeysEnabled: true,
        sessionStatus: "loading",
      }),
    ).toEqual({
      showApiKeys: false,
      showIdentity: false,
      showSignIn: false,
      showSignOut: false,
    });
  });
});
