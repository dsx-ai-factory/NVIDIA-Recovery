// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { createEnv } from "@t3-oss/env-nextjs";
import { z } from "zod";
import { normalizeBasePath } from "./lib/base-path.js";
import { grafanaUrlSchema } from "./lib/grafanaUrlSchema.js";
import { httpUrlSchema } from "./lib/httpUrlSchema.js";
import { resolveAuthConfiguration } from "./server/auth-config.js";

const skipValidation = !!process.env.SKIP_ENV_VALIDATION;

export const env = createEnv({
  /**
   * Specify your server-side environment variables schema here. This way you can ensure the app
   * isn't built with invalid env vars.
   */
  server: {
    DATABASE_URL: z.string().url(),
    NODE_ENV: z
      .enum(["development", "test", "production"])
      .default("development"),
    BASE_PATH: z.string().optional(),
    NEXTAUTH_ENABLED: z.string(),
    NEXTAUTH_SECRET:
      process.env.NODE_ENV === "production" &&
      process.env.NEXTAUTH_ENABLED?.toUpperCase() === "TRUE"
        ? z.string()
        : z.string().optional(),
    NEXTAUTH_URL: z.preprocess(
      // This makes Vercel deployments not fail if you don't set NEXTAUTH_URL
      // Since NextAuth.js automatically uses the VERCEL_URL if present.
      (str) => process.env.VERCEL_URL ?? str,
      // VERCEL_URL doesn't include `https` so it cant be validated as a URL
      process.env.VERCEL
        ? z.string()
        : process.env.NEXTAUTH_ENABLED?.toUpperCase() === "TRUE"
          ? z.string().url()
          : z.string().optional(),
    ),
    AZURE_AD_CLIENT_ID: z.string().optional(),
    AZURE_AD_CLIENT_SECRET: z.string().optional(),
    AZURE_AD_TENANT_ID: z.string().optional(),
    STARFLEET_OPENID_CONFIG_URL: z.string().optional(),
    STARFLEET_CLIENT_ID: z.string().optional(),
    STARFLEET_CLIENT_SECRET: z.string().optional(),
    LDAP_URI: z.string().optional(),
    LDAP_CA_PATH: z.string().optional(),
    LDAP_KEY_PATH: z.string().optional(),
    LDAP_CERT_PATH: z.string().optional(),
    LDAP_BIND_DN: z.string().optional(),
    LDAP_BIND_PASSWORD: z.string().optional(),
    LDAP_SEARCH_BASE: z.string().optional(),
    JOB_MANAGER_HOST:
      process.env.NODE_ENV === "production"
        ? z.string().url()
        : z.string().url().optional(),
    AGENT_API_URL: httpUrlSchema.optional(),
    AGENT_WEB_URL: httpUrlSchema.optional(),
    NOTIFICATIONS_HOST:
      process.env.NODE_ENV === "production"
        ? z.string().url()
        : z.string().url().optional(),
    API_GATEWAY_URL: z.string().url(),
    API_GATEWAY_SESSION_SECRET: z.string().optional(),
    DATADOG_WATCHER_URL: z.string().url().optional(),
    DATADOG_ENABLED: z.string(),
    FACT_ENABLED: z.string(),
    FACT_URL: z.string().url().optional(),
    COCKPIT_NAME: z.string().optional(),
    JOB_NODE_ANALYSIS_ENABLED: z.string().optional(),
    JOB_GROUPS_ENABLED: z.string().optional(),
    METRICS_ENABLED: z.string().optional(),
    API_KEYS_ENABLED: z.string().optional(),
    AUDIT_EVENTS_ENABLED: z.string().optional(),
    GRAFANA_TRACE_URL_ENABLED: z.string().optional(),
    GRAFANA_DATA_SOURCE_ID: z.string().optional(),
    GRAFANA_URL: grafanaUrlSchema.optional(),
    GRAFANA_ORG_ID: z
      .string()
      .regex(/^[1-9][0-9]*$/)
      .optional(),
  },

  /**
   * Specify your client-side environment variables schema here. This way you can ensure the app
   * isn't built with invalid env vars. To expose them to the client, prefix them with
   * `NEXT_PUBLIC_`.
   */
  client: {
    NEXT_PUBLIC_BASE_PATH: z.string().optional(),
  },

  /**
   * You can't destruct `process.env` as a regular object in the Next.js edge runtimes (e.g.
   * middlewares) or client-side so we need to destruct manually.
   */
  runtimeEnv: {
    DATABASE_URL: `${process.env.DATABASE_PROTOCOL}://${process.env.DATABASE_USER}:${encodeURIComponent(process.env.DATABASE_PASSWORD ?? "")}@${process.env.DATABASE_HOST}:${process.env.DATABASE_PORT}/${process.env.DATABASE_DATABASE}`,

    NODE_ENV: process.env.NODE_ENV,
    BASE_PATH: process.env.BASE_PATH,
    NEXT_PUBLIC_BASE_PATH: process.env.NEXT_PUBLIC_BASE_PATH,
    COCKPIT_NAME: process.env.COCKPIT_NAME,
    NEXTAUTH_ENABLED: process.env.NEXTAUTH_ENABLED,
    NEXTAUTH_SECRET: process.env.NEXTAUTH_SECRET,
    NEXTAUTH_URL: process.env.NEXTAUTH_URL,

    AZURE_AD_CLIENT_ID: process.env.AZURE_AD_CLIENT_ID,
    AZURE_AD_CLIENT_SECRET: process.env.AZURE_AD_CLIENT_SECRET,
    AZURE_AD_TENANT_ID: process.env.AZURE_AD_TENANT_ID,
    STARFLEET_OPENID_CONFIG_URL: process.env.STARFLEET_OPENID_CONFIG_URL,
    STARFLEET_CLIENT_ID: process.env.STARFLEET_CLIENT_ID,
    STARFLEET_CLIENT_SECRET: process.env.STARFLEET_CLIENT_SECRET,
    LDAP_URI: process.env.LDAP_URI,
    LDAP_CA_PATH: process.env.LDAP_CA_PATH,
    LDAP_KEY_PATH: process.env.LDAP_KEY_PATH,
    LDAP_CERT_PATH: process.env.LDAP_CERT_PATH,
    LDAP_BIND_DN: process.env.LDAP_BIND_DN,
    LDAP_BIND_PASSWORD: process.env.LDAP_BIND_PASSWORD,
    LDAP_SEARCH_BASE: process.env.LDAP_SEARCH_BASE,

    JOB_MANAGER_HOST: process.env.JOB_MANAGER_HOST,
    AGENT_API_URL: process.env.AGENT_API_URL,
    AGENT_WEB_URL: process.env.AGENT_WEB_URL,
    NOTIFICATIONS_HOST: process.env.NOTIFICATIONS_HOST,
    DATADOG_WATCHER_URL: process.env.DATADOG_WATCHER_URL,
    DATADOG_ENABLED: process.env.DATADOG_ENABLED,
    FACT_ENABLED: process.env.FACT_ENABLED,
    FACT_URL: process.env.FACT_URL,
    API_GATEWAY_URL: process.env.API_GATEWAY_URL,
    API_GATEWAY_SESSION_SECRET: process.env.API_GATEWAY_SESSION_SECRET,
    JOB_NODE_ANALYSIS_ENABLED: process.env.JOB_NODE_ANALYSIS_ENABLED,
    JOB_GROUPS_ENABLED: process.env.JOB_GROUPS_ENABLED,
    METRICS_ENABLED: process.env.METRICS_ENABLED,
    API_KEYS_ENABLED: process.env.API_KEYS_ENABLED,
    AUDIT_EVENTS_ENABLED: process.env.AUDIT_EVENTS_ENABLED,
    GRAFANA_TRACE_URL_ENABLED: process.env.GRAFANA_TRACE_URL_ENABLED,
    GRAFANA_DATA_SOURCE_ID: process.env.GRAFANA_DATA_SOURCE_ID,
    GRAFANA_URL: process.env.GRAFANA_URL,
    GRAFANA_ORG_ID: process.env.GRAFANA_ORG_ID,
  },

  /**
   * Run `build` or `dev` with `SKIP_ENV_VALIDATION` to skip env validation. This is especially
   * useful for Docker builds.
   */
  skipValidation,
  /**
   * Makes it so that empty strings are treated as undefined. `SOME_VAR: z.string()` and
   * `SOME_VAR=''` will throw an error.
   */
  emptyStringAsUndefined: true,
});

if (!skipValidation && typeof window === "undefined") {
  normalizeBasePath(env.BASE_PATH);
  resolveAuthConfiguration(env);
}
