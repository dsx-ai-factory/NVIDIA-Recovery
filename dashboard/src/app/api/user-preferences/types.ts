// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as z from "zod";

export const userPreferencesCookieName = "heimdall-dashboard-user-preferences";

export const UserPreferencesSchema = z.object({
  // matches @kui/react type Theme
  theme: z.enum(["light", "dark", "system"]),
  // matches @kui/react type Density
  density: z.enum(["spacious", "standard", "compact"]),
  timezone: z.enum(["utc", "system"]).default("system"),
  autoRefresh: z.boolean(),
  pageSize: z.number(),
});

export type UserPreferences = z.infer<typeof UserPreferencesSchema>;

export const defaultUserPreferences: UserPreferences = {
  theme: "light",
  density: "standard",
  timezone: "system",
  autoRefresh: true,
  pageSize: 10,
};
