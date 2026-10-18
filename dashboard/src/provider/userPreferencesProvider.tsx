// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { createContext, type ReactNode, useContext } from "react";
import type { UserPreferences } from "~/app/api/user-preferences/types";

const UserPreferencesContext = createContext<UserPreferences | undefined>(
  undefined,
);

interface UserPreferencesProviderProps {
  children: ReactNode;
  userPreferences: UserPreferences;
}

export function UserPreferencesProvider({
  children,
  userPreferences,
}: UserPreferencesProviderProps) {
  return (
    <UserPreferencesContext.Provider value={userPreferences}>
      {children}
    </UserPreferencesContext.Provider>
  );
}

/**
 * Hook to access user preferences context
 */
export function useUserPreferences(): UserPreferences {
  const context = useContext(UserPreferencesContext);
  if (context === undefined) {
    throw new Error(
      "useUserPreferences must be used within a UserPreferencesProvider",
    );
  }
  return context;
}
