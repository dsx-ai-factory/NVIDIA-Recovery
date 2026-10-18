// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import { redirect } from "next/navigation";
import type { NextRequest } from "next/server";
import { env } from "~/env";
import { getBearerTokenAndRefresh } from "~/lib/apiGatewaySession";
import { withBasePath } from "~/lib/base-path";
import { isAuthAvailable } from "~/utils/auth";

export async function GET(req: NextRequest) {
  if (!isAuthAvailable()) {
    return new Response("Not Found", { status: 404 });
  }

  // this whole route is necessary to modify a cookie in a next server action
  await getBearerTokenAndRefresh();
  const requestedReturnPath = req.nextUrl.searchParams.get("returnPath");
  const returnPath =
    requestedReturnPath?.startsWith("/") &&
    !requestedReturnPath.startsWith("//")
      ? requestedReturnPath
      : "/dashboard/workflow";
  redirect(withBasePath(returnPath, env.BASE_PATH));
}
