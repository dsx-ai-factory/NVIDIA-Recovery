// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { getJobGroups, isResponseError } from "~/gen/job-manager";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";

/** Return job groups or a JSON error when the job-manager request fails. */
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
    const error = _error as Error;
    const publicErrorDetails = {
      code: (error as AxiosError).code,
      name: error.name,
      message: error.message,
      cause: error.cause,
    };
    const responseError = isResponseError(error) ? error : undefined;
    logger.error(
      `GET /api/job-groups?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    const responseData = responseError?.data;
    const detail =
      typeof responseData === "object" &&
      responseData !== null &&
      "detail" in responseData
        ? responseData.detail
        : "";
    const upstreamStatus = responseError?.status;
    // A rejected redirect or other non-error status is a bad gateway result;
    // statuses such as 304 cannot carry the JSON error body.
    const status =
      upstreamStatus === undefined
        ? 500
        : upstreamStatus >= 400 && upstreamStatus <= 599
          ? upstreamStatus
          : 502;
    return NextResponse.json(
      {
        detail: detail ?? "",
      },
      { status },
    );
  }
}
