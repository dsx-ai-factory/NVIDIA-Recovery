// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "~/styles/globals.css";

import type { Metadata } from "next";
import { withBasePath } from "~/lib/base-path";
import { ApsQueryClientProvider } from "~/provider/apsQueryClientProvider";
import { ApsSessionProvider } from "~/provider/apsSessionProvider";
import { UserPreferencesProvider } from "~/provider/userPreferencesProvider";
import { getAuthConfiguration } from "~/utils/auth";
import { getUserPreferences } from "./api/user-preferences/cookie";

const SYSTEM_MODE_FALLBACK = "light";

export const metadata: Metadata = {
  title: "NVIDIA Recovery Dashboard",
  description:
    "Monitor distributed AI workloads and recovery with NVIDIA Recovery.",
  icons: [
    {
      rel: "icon",
      url: withBasePath("/favicon.svg", process.env.BASE_PATH),
      type: "image/svg+xml",
      sizes: "any",
    },
  ],
};

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const userPreferences = await getUserPreferences();
  const authAvailable = getAuthConfiguration().available;
  const resolvedTheme =
    userPreferences.theme === "system"
      ? SYSTEM_MODE_FALLBACK
      : userPreferences.theme;

  return (
    <html
      id="root"
      lang="en"
      className={`nv-${resolvedTheme} nv-density-${userPreferences.density}`}
      style={{
        colorScheme: resolvedTheme,
      }}
    >
      <head>
        <link
          rel="stylesheet"
          href="https://webassets.nvidia.com/kaizen-ui-foundations/1.13.0/base-external.css"
        />
        <link
          rel="stylesheet"
          href="https://webassets.nvidia.com/kaizen-ui-foundations/1.13.0/components.css"
        />
      </head>
      <ApsSessionProvider authAvailable={authAvailable}>
        <body>
          <ApsQueryClientProvider>
            <UserPreferencesProvider userPreferences={userPreferences}>
              {children}
            </UserPreferencesProvider>
          </ApsQueryClientProvider>
        </body>
      </ApsSessionProvider>
    </html>
  );
}
