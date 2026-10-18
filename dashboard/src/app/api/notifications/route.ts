// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { type NextRequest, NextResponse } from "next/server";
import { notificationsGet } from "~/gen/notifications";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";

export async function GET(req: NextRequest): Promise<NextResponse> {
  try {
    if (isAuthAvailable()) {
      const session = await getServerAuthSession();
      if (!session) throw new Error("Unauthorized");
    }
  } catch (_err) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  try {
    const { searchParams } = req.nextUrl;
    const id = searchParams.get("id") || undefined;
    const recipient = searchParams.get("recipient") || undefined;
    const jobId = searchParams.get("jobId") || undefined;
    const limit = Math.min(Number(searchParams.get("limit")) || 50, 100);
    const offset = Number(searchParams.get("offset")) || 0;
    const createdAfter = searchParams.get("createdAfter") || undefined;
    const createdBefore = searchParams.get("createdBefore") || undefined;

    logger.debug(`GET /api/notifications?${searchParams.toString()}`);
    const { data: notificationsData } = await notificationsGet({
      query: {
        id,
        recipient,
        job_id: jobId,
        limit,
        offset,
        created_after: createdAfter,
        created_before: createdBefore,
      },
    });
    const { items, total_items } = notificationsData;
    return NextResponse.json({
      items,
      total_items,
    });
  } catch (error) {
    logger.error(
      `GET /api/notifications?${req.nextUrl.searchParams.toString()} failed`,
      error,
    );
    if (process.env.NODE_ENV !== "production") {
      return NextResponse.json(
        { error: "Failed to get jobs", errorDetails: error },
        { status: 500 },
      );
    } else {
      return NextResponse.json(
        { error: "Internal Server Error" },
        { status: 500 },
      );
    }
  }
}
