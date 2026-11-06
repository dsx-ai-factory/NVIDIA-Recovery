// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "server-only";
import { headers } from "next/headers";
import { NextRequest } from "next/server";
import { getToken } from "next-auth/jwt";
import { env } from "~/env";

/**
 * Bearer token for API Gateway calls made on behalf of the signed-in user, read
 * from the NextAuth session. It cannot refresh the token (server components
 * cannot set cookies); the `jwt` callback does that when the session is fetched.
 *
 * @returns undefined when signed out, or when the token has expired or could not be refreshed.
 */
export async function getGatewayBearerToken(): Promise<string | undefined> {
  const token = await getToken({
    req: new NextRequest("http://localhost", { headers: await headers() }),
    secret: env.NEXTAUTH_SECRET,
  });

  if (!token?.gateway || token.error || token.gateway.expiresAt <= Date.now()) {
    return undefined;
  }
  return token.gateway.bearerToken;
}
