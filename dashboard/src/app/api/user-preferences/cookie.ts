// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "server-only";

import { cookies } from "next/headers";
import { logger } from "~/pino";
import {
  defaultUserPreferences,
  type UserPreferences,
  UserPreferencesSchema,
  userPreferencesCookieName,
} from "./types";

export async function getUserPreferences(): Promise<UserPreferences> {
  const cookieStore = await cookies();
  const userPreferencesCookieValue = cookieStore.get(userPreferencesCookieName);

  if (!userPreferencesCookieValue?.value) {
    return defaultUserPreferences;
  }

  try {
    const parsedUserPreferences = UserPreferencesSchema.parse(
      JSON.parse(userPreferencesCookieValue.value),
    );
    return parsedUserPreferences;
  } catch (error) {
    logger.debug(
      `Issue parsing user preferences: ${error instanceof Error ? error.message : "Unknown error"}`,
    );
    return defaultUserPreferences;
  }
}

export async function setUserPreferences(
  userPreferences: UserPreferences,
): Promise<void> {
  const cookieStore = await cookies();
  cookieStore.set(userPreferencesCookieName, JSON.stringify(userPreferences));
}
