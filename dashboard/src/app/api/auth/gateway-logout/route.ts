// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { type NextRequest, NextResponse } from "next/server";
import { env } from "~/env";
import { withBasePath } from "~/lib/base-path";
import { getApiGatewayLoginUrl } from "~/lib/baseUrls";
import { getAuthConfiguration } from "~/utils/auth";

/**
 * Second half of sign-out. NextAuth has already cleared the dashboard session;
 * this ends the API Gateway session (its cookies and the IdP session) so the
 * next login prompts again rather than silently signing the user back in.
 * The browser must navigate here: the gateway clears cookies on its own host.
 */
export function GET(req: NextRequest) {
  const signInUrl = new URL(
    withBasePath("/auth/signin", env.BASE_PATH),
    req.nextUrl.origin,
  );

  // The gateway rejects logout without its cookies, so skip it when none exist.
  if (
    getAuthConfiguration().provider !== "gateway" ||
    !req.cookies.has("idToken")
  ) {
    return NextResponse.redirect(signInUrl);
  }
  return NextResponse.redirect(`${getApiGatewayLoginUrl()}/v1/auth/logout`);
}
