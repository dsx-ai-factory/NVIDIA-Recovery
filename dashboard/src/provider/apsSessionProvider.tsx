// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";
import { SessionProvider } from "next-auth/react";
import { createContext, useContext } from "react";
import { withBasePath } from "~/lib/base-path";

const AuthAvailabilityContext = createContext(false);

export function useAuthAvailability() {
  return useContext(AuthAvailabilityContext);
}

export function ApsSessionProvider({
  children,
  authAvailable,
}: {
  children: React.ReactNode;
  authAvailable: boolean;
}) {
  const content = authAvailable ? (
    // Refetching hits /api/auth/session, the one place the API Gateway token can be
    // refreshed and persisted. It must be shorter than the 5-minute refresh window.
    <SessionProvider basePath={withBasePath("/api/auth")} refetchInterval={60}>
      {children}
    </SessionProvider>
  ) : (
    children
  );

  return (
    <AuthAvailabilityContext.Provider value={authAvailable}>
      {content}
    </AuthAvailabilityContext.Provider>
  );
}
