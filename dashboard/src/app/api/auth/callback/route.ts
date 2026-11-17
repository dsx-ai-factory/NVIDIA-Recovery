// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { redirect } from "next/navigation";
import type { NextRequest } from "next/server";
import { env } from "~/env";
import { withBasePath } from "~/lib/base-path";
import { isAuthAvailable } from "~/utils/auth";

/**
 * Landing point for the API Gateway after login. The gateway has just set its
 * session cookies on this host; NextAuth v4 can only start a session from a
 * `signIn()` POST, which a route handler cannot issue, so hand off to a page
 * that does.
 */
export function GET(req: NextRequest) {
  if (!isAuthAvailable()) {
    return new Response("Not Found", { status: 404 });
  }

  const requestedReturnPath = req.nextUrl.searchParams.get("returnPath");
  // Only same-site relative paths, so the callback cannot be used as an open redirect.
  const returnPath =
    requestedReturnPath?.startsWith("/") &&
    !requestedReturnPath.startsWith("//")
      ? requestedReturnPath
      : "/dashboard/workflow";
  const complete = new URLSearchParams({ returnPath });
  redirect(
    withBasePath(`/auth/complete?${complete.toString()}`, env.BASE_PATH),
  );
}
