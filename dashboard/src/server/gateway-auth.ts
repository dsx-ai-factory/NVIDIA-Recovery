// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { env } from "~/env";

/** Refresh the gateway session when it expires within this window. */
export const GATEWAY_REFRESH_THRESHOLD_MS = 1000 * 60 * 5;

const GATEWAY_COOKIE_NAMES = [
  "idToken",
  "accessToken",
  "refreshToken",
] as const;

type GatewayCookieName = (typeof GATEWAY_COOKIE_NAMES)[number];

/**
 * The opaque cookies the API Gateway sets after login. Their values are signed by
 * the gateway and are only meaningful when sent back to its refresh endpoint.
 */
export type GatewayCookies = Record<GatewayCookieName, string>;

/**
 * Gateway session state kept inside the NextAuth JWT so there is a single
 * session cookie. `cookies` are replayed to the gateway to refresh.
 */
export interface GatewaySession {
  /** Credential for API Gateway calls. */
  bearerToken: string;
  /** Raw OIDC ID token, used to derive the user's identity. */
  idToken: string;
  /** Epoch milliseconds at which `bearerToken` stops being valid. */
  expiresAt: number;
  cookies: GatewayCookies;
}

export interface GatewayUser {
  id: string;
  name?: string;
  email?: string;
  image?: string;
  username?: string;
}

/**
 * Extract the gateway cookies from a `Cookie` request header.
 *
 * @returns undefined unless all three gateway cookies are present.
 */
export function parseGatewayCookies(
  header: string | undefined,
): GatewayCookies | undefined {
  if (!header) {
    return undefined;
  }
  const found = new Map<string, string>();
  for (const part of header.split(";")) {
    const separator = part.indexOf("=");
    if (separator < 0) {
      continue;
    }
    // Values stay exactly as the browser sent them: they are replayed verbatim
    // to the gateway, which verifies their signature.
    found.set(
      part.slice(0, separator).trim(),
      part.slice(separator + 1).trim(),
    );
  }
  const cookies = {} as GatewayCookies;
  for (const name of GATEWAY_COOKIE_NAMES) {
    const value = found.get(name);
    if (!value) {
      return undefined;
    }
    cookies[name] = value;
  }
  return cookies;
}

/**
 * Exchange gateway cookies for a bearer token.
 *
 * The gateway rotates its cookies on every refresh (including the refresh
 * token), so the rotated values are read from `Set-Cookie` and returned; reusing
 * the pre-refresh cookies a second time would fail once the IdP revokes them.
 *
 * @throws if the gateway rejects the cookies or returns an unusable response.
 */
export async function refreshGatewaySession(
  cookies: GatewayCookies,
): Promise<GatewaySession> {
  const response = await fetch(`${env.API_GATEWAY_URL}/v1/auth/refresh`, {
    method: "GET",
    headers: {
      Cookie: GATEWAY_COOKIE_NAMES.map(
        (name) => `${name}=${cookies[name]}`,
      ).join("; "),
      Accept: "application/json",
    },
    cache: "no-store",
  });

  if (response.status !== 200) {
    throw new Error(
      `Error from refresh endpoint: ${response.status} ${await response.text()}`,
    );
  }

  const body = (await response.json()) as {
    idToken?: string;
    bearerToken?: string;
    expiresAt?: number;
  };
  if (!body.idToken || !body.bearerToken || !body.expiresAt) {
    throw new Error("Refresh endpoint returned an incomplete session");
  }

  const rotated = { ...cookies };
  for (const setCookie of response.headers.getSetCookie()) {
    const [pair = ""] = setCookie.split(";");
    const separator = pair.indexOf("=");
    const name = pair.slice(0, separator).trim() as GatewayCookieName;
    if (GATEWAY_COOKIE_NAMES.includes(name)) {
      rotated[name] = pair.slice(separator + 1).trim();
    }
  }

  return {
    bearerToken: body.bearerToken,
    idToken: body.idToken,
    expiresAt: body.expiresAt * 1000,
    cookies: rotated,
  };
}

/** Whether the session is expired or close enough to expiry to warrant a refresh. */
export function isGatewaySessionExpiring(
  session: Pick<GatewaySession, "expiresAt">,
  now = Date.now(),
): boolean {
  return session.expiresAt - now < GATEWAY_REFRESH_THRESHOLD_MS;
}

function decodeJwtPayload(jwt: string): Record<string, unknown> {
  const payload = jwt.split(".")[1];
  if (!payload) {
    throw new Error("Malformed ID token");
  }
  return JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
}

/**
 * Derive the dashboard user from the gateway's ID token. The token is not
 * verified here: it was just returned by the gateway's refresh endpoint, which
 * verifies it, and it is only used for display and defaults, never for authz.
 */
export function userFromIdToken(idToken: string): GatewayUser {
  const claims = decodeJwtPayload(idToken);
  const str = (value: unknown) =>
    typeof value === "string" && value !== "" ? value : undefined;

  const email = str(claims.email);
  const username = str(claims.preferred_username);
  const id = str(claims.sub);
  if (!id) {
    throw new Error("ID token has no subject");
  }
  return {
    id,
    name: str(claims.name) ?? username ?? email,
    email,
    image: str(claims.picture),
    username,
  };
}
