// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextResponse } from "next/server";
import { withAuth } from "next-auth/middleware";
import { withBasePath } from "~/lib/base-path";
import { areMetricsEnabled } from "~/lib/metrics";
import { isAuthAvailable } from "~/utils/auth";
import { isApiKeysEnabled } from "~/utils/config";

export default withAuth(
  async function middleware(request) {
    const internalOrigin = `http://localhost:${process.env.PORT || "3001"}`;
    const authAvailable = isAuthAvailable();
    const apiKeysPath = withBasePath(
      "/dashboard/api-keys",
      process.env.BASE_PATH,
    );
    const isApiKeysPath =
      request.nextUrl.pathname === "/dashboard/api-keys" ||
      request.nextUrl.pathname === apiKeysPath;

    if (isApiKeysPath && (!authAvailable || !isApiKeysEnabled())) {
      return new NextResponse("Not Found", { status: 404 });
    }

    if (areMetricsEnabled()) {
      await fetch(
        `${internalOrigin}${withBasePath("/metrics/http-request/start")}`,
        {
          method: "POST",
          body: JSON.stringify({
            method: request.method,
            route: request.nextUrl.pathname,
          }),
        },
      );
    }

    const startTime = performance.now();

    // Continue with normal request processing
    const response = NextResponse.next();

    const stopTime = performance.now();

    const duration = stopTime - startTime;

    if (areMetricsEnabled()) {
      await fetch(
        `${internalOrigin}${withBasePath("/metrics/http-request/end")}`,
        {
          method: "POST",
          body: JSON.stringify({
            method: request.method,
            route: request.nextUrl.pathname,
            statusCode: response.status,
            durationMs: duration,
          }),
        },
      );
    }

    return response;
  },
  {
    callbacks: {
      authorized({ token }) {
        return !isAuthAvailable() || !!token;
      },
    },
    pages: {
      signIn: "/auth/signin",
    },
  },
);

export const config = {
  matcher: ["/dashboard/:path*"],
};
