// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @typedef {"starfleet" | "azure" | "ldap"} AuthProviderName
 */

/**
 * @typedef {object} AuthEnvironment
 * @property {string | undefined} [NEXTAUTH_ENABLED]
 * @property {string | undefined} [STARFLEET_CLIENT_SECRET]
 * @property {string | undefined} [STARFLEET_CLIENT_ID]
 * @property {string | undefined} [STARFLEET_OPENID_CONFIG_URL]
 * @property {string | undefined} [AZURE_AD_CLIENT_SECRET]
 * @property {string | undefined} [AZURE_AD_CLIENT_ID]
 * @property {string | undefined} [AZURE_AD_TENANT_ID]
 * @property {string | undefined} [LDAP_URI]
 * @property {string | undefined} [LDAP_CA_PATH]
 * @property {string | undefined} [LDAP_KEY_PATH]
 * @property {string | undefined} [LDAP_CERT_PATH]
 * @property {string | undefined} [LDAP_BIND_DN]
 * @property {string | undefined} [LDAP_BIND_PASSWORD]
 * @property {string | undefined} [LDAP_SEARCH_BASE]
 */

/**
 * @typedef {{ available: false, provider: null } | {
 *   available: true,
 *   provider: AuthProviderName,
 * }} AuthConfiguration
 */

export class AuthConfigurationError extends Error {
  constructor() {
    super(
      "NEXTAUTH_ENABLED=true requires one complete authentication provider configuration: Starfleet, Azure, or LDAP",
    );
    this.name = "AuthConfigurationError";
  }
}

/**
 * @param {(string | undefined)[]} values
 */
function areAllStringsValid(values) {
  return values.every(
    (value) => typeof value === "string" && value.trim().length > 0,
  );
}

/** @type {{ name: AuthProviderName, isComplete: (environment: AuthEnvironment) => boolean }[]} */
const PROVIDERS = [
  {
    name: "starfleet",
    isComplete: (environment) =>
      areAllStringsValid([
        environment.STARFLEET_CLIENT_SECRET,
        environment.STARFLEET_CLIENT_ID,
        environment.STARFLEET_OPENID_CONFIG_URL,
      ]),
  },
  {
    name: "azure",
    isComplete: (environment) =>
      areAllStringsValid([
        environment.AZURE_AD_CLIENT_SECRET,
        environment.AZURE_AD_CLIENT_ID,
        environment.AZURE_AD_TENANT_ID,
      ]),
  },
  {
    name: "ldap",
    isComplete: (environment) =>
      areAllStringsValid([
        environment.LDAP_URI,
        environment.LDAP_CA_PATH,
        environment.LDAP_KEY_PATH,
        environment.LDAP_CERT_PATH,
        environment.LDAP_BIND_DN,
        environment.LDAP_BIND_PASSWORD,
        environment.LDAP_SEARCH_BASE,
      ]),
  },
];

/**
 * Resolve whether authentication is available and which provider owns it.
 * Provider selection preserves the established Starfleet, Azure, LDAP order.
 *
 * @param {AuthEnvironment} environment
 * @returns {AuthConfiguration}
 */
export function resolveAuthConfiguration(environment) {
  if (environment.NEXTAUTH_ENABLED?.toUpperCase() !== "TRUE") {
    return { available: false, provider: null };
  }

  for (const provider of PROVIDERS) {
    if (provider.isComplete(environment)) {
      return {
        available: true,
        provider: provider.name,
      };
    }
  }

  throw new AuthConfigurationError();
}
