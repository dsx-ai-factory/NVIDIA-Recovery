// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import fs from "fs";
import ldap from "ldapjs";
import {
  type DefaultSession,
  getServerSession,
  type NextAuthOptions,
  type User,
} from "next-auth";
import AzureADProvider from "next-auth/providers/azure-ad";
import CredentialsProvider from "next-auth/providers/credentials";
import type { Provider } from "next-auth/providers/index";
import { env } from "~/env";
import { withBasePath } from "~/lib/base-path";
import { logger } from "~/pino";
import { getAuthConfiguration } from "~/utils/auth";
import {
  type GatewaySession,
  isGatewaySessionExpiring,
  parseGatewayCookies,
  refreshGatewaySession,
  userFromIdToken,
} from "./gateway-auth";

const DEFAULT_STARFLEET_SCOPES = "openid profile email authz";

/**
 * Module augmentation for `next-auth` types.
 * @see https://next-auth.js.org/getting-started/typescript#module-augmentation
 */
declare module "next-auth" {
  interface User {
    name?: string;
    email?: string;
    id?: string;
    image?: string;
    [prop: string]: string | undefined;
  }

  interface Session {
    user: {
      id?: string;
      [prop: string]: string | undefined;
    } & DefaultSession["user"];
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    /** API Gateway session; server-only, never copied into the client `Session`. */
    gateway?: GatewaySession;
    /** Set when the gateway session could not be refreshed; the user must log in again. */
    error?: "GatewaySessionExpired";
  }
}

/**
 * Sign-in provider that completes a login already performed at the API Gateway.
 *
 * The browser is sent to the gateway's `/v1/auth/login`, which sets its session
 * cookies on this hostname and redirects back. This provider turns those
 * cookies into the NextAuth session, so the dashboard has a single session
 * rather than a NextAuth one beside a separate gateway one.
 */
function getGatewayProvider(): Provider {
  return CredentialsProvider({
    id: "nvlogin",
    name: "nvlogin",
    credentials: {},
    async authorize(_credentials, req) {
      const cookies = parseGatewayCookies(req.headers?.cookie);
      if (!cookies) {
        logger.debug("No API Gateway session cookies on sign-in request");
        return null;
      }

      try {
        const gateway = await refreshGatewaySession(cookies);
        // `User` only allows string props, so the gateway session rides along
        // untyped and is moved into the JWT by the `jwt` callback.
        return {
          ...userFromIdToken(gateway.idToken),
          gateway,
        } as unknown as User;
      } catch (err) {
        logger.error(`API Gateway sign-in failed: ${String(err)}`);
        return null;
      }
    },
  });
}

function getProvider(
  provider: "gateway" | "starfleet" | "azure" | "ldap",
): Provider {
  if (provider === "gateway") {
    return getGatewayProvider();
  }
  if (provider === "starfleet") {
    return {
      id: "nvlogin",
      name: "nvlogin",
      type: "oauth",
      version: "2.0",
      allowDangerousEmailAccountLinking: true,
      wellKnown:
        env.STARFLEET_OPENID_CONFIG_URL! + "/.well-known/openid-configuration",
      authorization: {
        params: {
          scope: DEFAULT_STARFLEET_SCOPES,
          response_type: "code",
        },
      },
      token: {
        params: {
          grant_type: "authorization_code",
        },
      },
      idToken: true,
      checks: "state",
      httpOptions: {
        timeout: 6000,
      },
      profile: (profile, _tokens) => {
        // You can use the tokens, in case you want to fetch more profile information
        // For example several OAuth providers do not return email by default.
        // Depending on your provider, will have tokens like `access_token`, `id_token` and or `refresh_token`
        return {
          id: profile.external_id,
          name: profile.name,
          email: profile.email,
          image: profile.image_url,
        };
      },
      clientId: env.STARFLEET_CLIENT_ID!,
      clientSecret: env.STARFLEET_CLIENT_SECRET!,
      client: {
        authorization_signed_response_alg: "ES256",
        id_token_signed_response_alg: "ES256",
      },
    } as Provider;
  }
  if (provider === "azure") {
    logger.debug("azure auth used.");
    return AzureADProvider({
      id: "nvlogin",
      name: "nvlogin",
      clientId: env.AZURE_AD_CLIENT_ID!,
      clientSecret: env.AZURE_AD_CLIENT_SECRET!,
      tenantId: env.AZURE_AD_TENANT_ID,
      allowDangerousEmailAccountLinking: true,
    });
  }

  logger.debug("bcm ldap auth used.");
  return CredentialsProvider({
    id: "nvlogin",
    name: "nvlogin",
    credentials: {
      username: {
        label: "Username",
        type: "text",
        placeholder: "Enter your username",
      },
      password: {
        label: "Password",
        type: "password",
        placeholder: "Enter your password",
      },
    },
    async authorize(
      credentials: { username: string; password: string } | undefined,
    ) {
      logger.debug("Starting LDAP authentication...");

      const client = ldap.createClient({
        url: env.LDAP_URI!, // e.g., ldaps://ldap.example.com
        tlsOptions: {
          rejectUnauthorized: false, // For testing only; ensure this is true in production
          ca: fs.readFileSync(env.LDAP_CA_PATH!),
          key: fs.readFileSync(env.LDAP_KEY_PATH!),
          cert: fs.readFileSync(env.LDAP_CERT_PATH!),
        },
      });

      return new Promise((resolve, reject) => {
        logger.debug(
          "Attempting to bind to LDAP server with service account...",
        );
        client.bind(env.LDAP_BIND_DN!, env.LDAP_BIND_PASSWORD!, (err) => {
          if (err || !credentials) {
            if (err) {
              logger.error("Service account bind failed:", err.message);
            }
            reject(new Error("Failed to connect to LDAP server"));
            return;
          }

          logger.debug("Service account bind successful.");

          const searchOptions = {
            filter: `(|(uid=${credentials.username})(cn=${credentials.username}))`,
            scope: "sub" as "sub" | "base" | "one" | undefined,
            attributes: "uid",
          };
          logger.debug(`LDAP search options: ${JSON.stringify(searchOptions)}`);

          logger.debug(
            `Searching for user in LDAP directory with username: ${credentials.username}`,
          );
          client.search(env.LDAP_SEARCH_BASE!, searchOptions, (err, res) => {
            if (err) {
              logger.error("LDAP search failed:", err.message);
              reject(new Error("Failed to search for user"));
              client.unbind();
              return;
            }

            let userDN: string | null = null;

            res.on("searchEntry", (entry) => {
              logger.debug(
                "Full LDAP search entry:",
                JSON.stringify(entry.pojo, null, 2),
              );
              if (entry?.pojo?.objectName) {
                userDN = entry.pojo.objectName; // Use objectName to get the DN
                logger.debug(`User DN found during search: ${userDN}`);
              } else {
                logger.error(
                  "LDAP search entry is missing the DN attribute or is undefined.",
                );
              }
            });

            res.on("error", (err) => {
              logger.error("Search error occurred:", err.message);
              reject(new Error("Error during LDAP search"));
              client.unbind();
            });

            res.on("end", () => {
              if (!userDN) {
                logger.error(
                  `User not found in LDAP for username: ${credentials.username}`,
                );
                reject(new Error("Invalid username or password"));
                client.unbind();
                return;
              }

              logger.debug(`Attempting to bind as user DN: ${userDN}`);
              client.bind(userDN, credentials.password, (err) => {
                if (err) {
                  logger.error("User authentication failed:", err.message);
                  reject(new Error("Invalid username or password"));
                } else {
                  logger.debug(
                    `User authenticated successfully. ${JSON.stringify(credentials)}`,
                  );
                  resolve({
                    id: credentials.username,
                    username: credentials.username,
                    name: credentials.username,
                  });
                }
                client.unbind(); // Close connection after authentication
              });
            });
          });
        });
      });
    },
  });
}

/**
 * Options for NextAuth.js used to configure adapters, providers, callbacks, etc.
 *
 * @see https://next-auth.js.org/configuration/options
 */
export function getAuthOptions(): NextAuthOptions {
  const authConfiguration = getAuthConfiguration();
  if (!authConfiguration.available) {
    throw new Error("Authentication is unavailable");
  }

  return {
    callbacks: {
      signIn: async () => {
        return true;
      },
      jwt: async ({ token, user, profile }) => {
        const signedInGateway = (
          user as { gateway?: GatewaySession } | undefined
        )?.gateway;
        if (signedInGateway) {
          token.gateway = signedInGateway;
          delete token.error;
        } else if (token.gateway && isGatewaySessionExpiring(token.gateway)) {
          // Refresh ahead of expiry so server-side gateway calls keep a valid
          // bearer. A failure drops the credential instead of keeping a stale
          // one; `token.error` makes the session count as signed out.
          try {
            token.gateway = await refreshGatewaySession(token.gateway.cookies);
          } catch (err) {
            logger.warn(`API Gateway session refresh failed: ${String(err)}`);
            delete token.gateway;
            token.error = "GatewaySessionExpired";
          }
        }

        // expose nid, nan, idpId to the token on login
        if (profile && typeof profile === "object") {
          const prof = profile as Record<string, unknown>;
          if (typeof prof.nid === "string") {
            token.nid = prof.nid;
          }
          if (typeof prof.nan === "string") {
            token.nan = prof.nan;
          }
          if (typeof prof.idp_id === "string") {
            token.idpId = prof.idp_id;
          }
          if (typeof prof.sub === "string") {
            token.sub = prof.sub;
          }
        }

        return token;
      },

      session: async ({ session, token }) => {
        if (token.error) {
          // Surface the failure so server code can treat the user as signed out.
          session.user.error = token.error;
        }
        // add nid, nan, idpId to the session
        session.user.nid = token.nid as string;
        session.user.nan = token.nan as string;
        session.user.idpId = token.idpId as string;
        session.user.sub = token.sub!;
        return session;
      },
    },
    providers: [getProvider(authConfiguration.provider)],
    session: {
      strategy: "jwt",
    },
    pages: {
      signIn: withBasePath("/auth/signin", env.BASE_PATH),
      error: withBasePath("/auth/error", env.BASE_PATH),
    },
    debug: false,
  };
}

/**
 * Wrapper for `getServerSession` so that you don't need to import the `authOptions` in every file.
 *
 * @see https://next-auth.js.org/configuration/nextjs
 */
export async function getServerAuthSession() {
  if (!getAuthConfiguration().available) {
    return null;
  }
  const session = await getServerSession(getAuthOptions());
  // A session whose gateway credential could not be refreshed is signed out.
  return session?.user.error ? null : session;
}
