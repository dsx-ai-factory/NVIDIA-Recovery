// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Run `build` or `dev` with `SKIP_ENV_VALIDATION` to skip env validation. This is especially useful
 * for Docker builds.
 */

await import("./src/env.js");

import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { normalizeBasePath } from "./src/lib/base-path.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const APP_ENV = process.env.NEXT_PUBLIC_APP_ENV;
if (APP_ENV !== process.env.NODE_ENV) {
  dotenv.config({ path: [`.env.${APP_ENV}`], override: true });
}
const basePath = normalizeBasePath(process.env.BASE_PATH);

/** @type {import("next").NextConfig} */
const config = {
  turbopack: {
    root: path.join(__dirname, "."),
  },
  logging: {
    fetches: {
      fullUrl: process.env.ENABLE_NEXT_FETCH_LOGGING === "true",
    },
  },
  serverExternalPackages: ["pino", "pino-pretty", "ldapjs", "prom-client"],
  productionBrowserSourceMaps: APP_ENV !== "production",
  output: "standalone",
  basePath,
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "picsum.photos",
        pathname: "/seed/**",
      },
    ],
  },
  async redirects() {
    return [
      {
        source: `/`,
        destination: `/dashboard/workflow`,
        permanent: false,
      },
      {
        source: `/dashboard`,
        destination: `/dashboard/workflow`,
        permanent: false,
      },
    ];
  },
};

export default config;
