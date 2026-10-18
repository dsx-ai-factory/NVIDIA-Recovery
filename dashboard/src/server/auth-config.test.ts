// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import {
  AuthConfigurationError,
  resolveAuthConfiguration,
} from "./auth-config";

const starfleet = {
  STARFLEET_CLIENT_SECRET: "starfleet-secret",
  STARFLEET_CLIENT_ID: "starfleet-client",
  STARFLEET_OPENID_CONFIG_URL: "https://starfleet.example.test",
};

const azure = {
  AZURE_AD_CLIENT_SECRET: "azure-secret",
  AZURE_AD_CLIENT_ID: "azure-client",
  AZURE_AD_TENANT_ID: "azure-tenant",
};

const ldap = {
  LDAP_URI: "ldaps://ldap.example.test",
  LDAP_CA_PATH: "/certs/ca.pem",
  LDAP_KEY_PATH: "/certs/ldap.key",
  LDAP_CERT_PATH: "/certs/ldap.pem",
  LDAP_BIND_DN: "cn=bind,dc=example,dc=test",
  LDAP_BIND_PASSWORD: "ldap-secret",
  LDAP_SEARCH_BASE: "dc=example,dc=test",
};

describe("resolveAuthConfiguration", () => {
  it("keeps authentication unavailable when it is disabled", () => {
    expect(
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "false",
        ...starfleet,
      }),
    ).toEqual({ available: false, provider: null });
  });

  it.each([
    ["starfleet", starfleet],
    ["azure", azure],
    ["ldap", ldap],
  ] as const)("accepts a complete %s provider", (provider, configuration) => {
    expect(
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "true",
        ...configuration,
      }),
    ).toEqual({ available: true, provider });
  });

  it("ignores partial providers when a later provider is complete", () => {
    expect(
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "true",
        STARFLEET_CLIENT_ID: "partial-starfleet",
        ...azure,
      }),
    ).toEqual({ available: true, provider: "azure" });
  });

  it("preserves Starfleet, Azure, LDAP precedence", () => {
    expect(
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "TRUE",
        ...ldap,
        ...azure,
        ...starfleet,
      }),
    ).toEqual({ available: true, provider: "starfleet" });

    expect(
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "TRUE",
        ...ldap,
        ...azure,
      }),
    ).toEqual({ available: true, provider: "azure" });
  });

  it.each([
    {},
    { STARFLEET_CLIENT_ID: "partial-starfleet" },
    {
      AZURE_AD_CLIENT_ID: "azure-client",
      AZURE_AD_CLIENT_SECRET: "azure-secret",
    },
    {
      LDAP_URI: "ldaps://ldap.example.test",
      LDAP_BIND_DN: "cn=bind,dc=example,dc=test",
    },
  ])("fails clearly when enabled without a complete provider", (configuration) => {
    expect(() =>
      resolveAuthConfiguration({
        NEXTAUTH_ENABLED: "true",
        ...configuration,
      }),
    ).toThrow(AuthConfigurationError);
  });
});
