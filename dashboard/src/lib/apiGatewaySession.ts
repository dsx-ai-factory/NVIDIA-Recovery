// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "server-only";
import { getIronSession } from "iron-session";
import { cookies } from "next/headers";
import { env } from "~/env";
import { logger } from "~/pino";

// 5 minutes in milliseconds
const REFRESH_TOKEN_THRESHOLD = 1000 * 60 * 5;

interface SessionPayload {
  token?: {
    idToken: string;
    bearerToken: string;
    expiresAt: number;
  };
}

const cookieName = "api-gateway-session";
const ironSessionPassword =
  env.API_GATEWAY_SESSION_SECRET ?? env.NEXTAUTH_SECRET;

async function getSession() {
  const cookieStore = await cookies();

  if (!ironSessionPassword) {
    throw new Error(`No valid password for API Gateway Session Cookie`);
  }

  const session = await getIronSession<SessionPayload>(cookieStore, {
    password: ironSessionPassword,
    cookieName: cookieName,
    cookieOptions: {
      httpOnly: true,
      secure: env.NODE_ENV === "production",
      sameSite: "lax",
      path: "/",
    },
  });
  return session;
}

async function refreshSession() {
  const cookieStore = await cookies();
  const idToken = cookieStore.get("idToken")?.value;
  const accessToken = cookieStore.get("accessToken")?.value;
  const refreshToken = cookieStore.get("refreshToken")?.value;

  if (!idToken || !accessToken || !refreshToken) {
    throw new Error("No idToken, accessToken, or refreshToken found");
  }

  logger.debug(
    `${process.env.API_GATEWAY_URL}/v1/auth/refresh`,
    "fetching API token",
  );

  const response = await fetch(
    `${process.env.API_GATEWAY_URL}/v1/auth/refresh`,
    {
      method: "GET",
      headers: {
        Cookie: `idToken=${idToken}; accessToken=${accessToken}; refreshToken=${refreshToken}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    },
  );

  if (response.status === 200) {
    const {
      idToken,
      bearerToken,
      expiresAt: expiresAtUnix,
    } = await response.json();

    const expiresAt = new Date(expiresAtUnix * 1000);

    return {
      idToken,
      bearerToken,
      expiresAt: expiresAt.getTime(),
    };
  } else {
    const body = await response.text();
    throw new Error(
      `Error from refresh endpoint: ${response.statusText} ${body}`,
    );
  }
}

export async function getBearerTokenAndRefresh() {
  const session = await getSession();

  if (
    !session.token ||
    session.token.expiresAt < Date.now() ||
    session.token.expiresAt - Date.now() < REFRESH_TOKEN_THRESHOLD
  ) {
    const newToken = await refreshSession();
    session.token = newToken;
    await session.save();
  }

  return session.token.bearerToken;
}

// This is used to check if the user is authenticated, but cannot refresh the session
// Server components should use this to check if the user is authenticated
export async function getBearerToken() {
  const session = await getSession();

  if (
    !session.token ||
    session.token.expiresAt < Date.now() ||
    session.token.expiresAt - Date.now() < REFRESH_TOKEN_THRESHOLD
  ) {
    return undefined;
  }

  return session.token.bearerToken;
}
