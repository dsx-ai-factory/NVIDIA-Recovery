// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { type NextRequest, NextResponse } from "next/server";
import { logger } from "~/pino";
import { getUserPreferences, setUserPreferences } from "./cookie";
import { UserPreferencesSchema } from "./types";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    // Get current preferences
    const currentPreferences = await getUserPreferences();

    // Validate the incoming data against the schema
    const validatedPreferences = UserPreferencesSchema.partial().parse(body);

    // Merge with current preferences
    const updatedPreferences = {
      ...currentPreferences,
      ...validatedPreferences,
    };

    // Set the updated preferences in cookies
    await setUserPreferences(updatedPreferences);

    logger.debug(
      `User preferences updated: ${JSON.stringify(updatedPreferences)}`,
    );

    return NextResponse.json(
      {
        success: true,
        preferences: updatedPreferences,
      },
      { status: 200 },
    );
  } catch (error) {
    logger.error("Error updating user preferences:", error);

    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Invalid request data",
      },
      { status: 400 },
    );
  }
}
