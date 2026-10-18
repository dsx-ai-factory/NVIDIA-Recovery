// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Normalize the optional Next.js base path.
 *
 * @param {string | undefined} value
 * @returns {string}
 */
export function normalizeBasePath(value) {
  const candidate = value?.trim() ?? "";
  if (candidate === "" || candidate === "/") {
    return "";
  }
  if (!candidate.startsWith("/")) {
    throw new Error("BASE_PATH must start with '/'");
  }
  if (candidate.includes("?") || candidate.includes("#")) {
    throw new Error("BASE_PATH must not include a query string or fragment");
  }
  return candidate.replace(/\/+$/, "");
}

/**
 * Prefix an application path with the configured dashboard base path.
 *
 * @param {string} path
 * @param {string | undefined} [basePath]
 * @returns {string}
 */
export function withBasePath(
  path,
  basePath = process.env.NEXT_PUBLIC_BASE_PATH,
) {
  if (!path.startsWith("/") || path.startsWith("//")) {
    throw new Error("Application paths must start with '/'");
  }
  const normalizedBasePath = normalizeBasePath(basePath);
  if (
    normalizedBasePath &&
    (path === normalizedBasePath || path.startsWith(`${normalizedBasePath}/`))
  ) {
    return path;
  }
  return `${normalizedBasePath}${path}`;
}
