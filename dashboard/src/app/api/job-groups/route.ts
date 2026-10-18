// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { getJobGroups } from "~/gen/job-manager";
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
    const jobGroupId = searchParams.get("jobGroupId") || undefined;
    const user = searchParams.get("user") || undefined;
    const limit = Math.min(Number(searchParams.get("limit")) || 50, 100);
    const offset = Number(searchParams.get("offset")) || 0;
    const sortBy = searchParams.get("sortBy") || undefined;

    const { data: jobGroupsData } = await getJobGroups({
      query: {
        job_group_id: jobGroupId,
        user: user,
        limit: limit,
        offset: offset,
        sort_by: sortBy,
      },
    });

    logger.debug(
      `GET /api/job-groups?${searchParams.toString()} - fetched job groups: ${jobGroupsData.job_groups.map((jobGroup) => jobGroup.job_group_id).join(", ")}`,
    );

    return NextResponse.json(jobGroupsData);
  } catch (_error) {
    const error = _error as AxiosError;
    const publicErrorDetails = {
      code: error.code,
      name: error.name,
      message: error.message,
      cause: error.cause,
    };
    logger.error(
      `GET /api/job-groups?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      {
        detail:
          "detail" in (error.response?.data as { detail?: string })
            ? (error.response?.data as { detail: string }).detail
            : "",
      },
      { status: error.response?.status || 500 },
    );
  }
}
