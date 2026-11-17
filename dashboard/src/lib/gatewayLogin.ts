// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { withBasePath } from "./base-path";

const GATEWAY_LOGIN_PATH = "/v1/auth/login";
const CALLBACK_PATH = "/api/auth/callback";
export const DEFAULT_RETURN_PATH = "/dashboard/workflow";

/**
 * Reduce a NextAuth `callbackUrl` (absolute or relative) to a same-site path.
 * Anything pointing elsewhere falls back to the default landing page, so a
 * crafted sign-in link cannot send users to another origin after login.
 */
export function returnPathFromCallbackUrl(
  callbackUrl: string | null | undefined,
  origin: string,
): string {
  if (!callbackUrl) {
    return DEFAULT_RETURN_PATH;
  }
  try {
    const url = new URL(callbackUrl, origin);
    if (url.origin !== new URL(origin).origin) {
      return DEFAULT_RETURN_PATH;
    }
    return `${url.pathname}${url.search}`;
  } catch {
    return DEFAULT_RETURN_PATH;
  }
}

/**
 * URL that starts the API Gateway login and returns the browser to the
 * dashboard callback, which carries `returnPath` through to the final redirect.
 */
export function buildGatewayLoginUrl(
  apiGatewayLoginUrl: string,
  returnPath: string,
): string {
  const callback = `${withBasePath(CALLBACK_PATH)}?${new URLSearchParams({ returnPath }).toString()}`;
  return `${apiGatewayLoginUrl}${GATEWAY_LOGIN_PATH}?${new URLSearchParams({ redirectURI: callback }).toString()}`;
}
