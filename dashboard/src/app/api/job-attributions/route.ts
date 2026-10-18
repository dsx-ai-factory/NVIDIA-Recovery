// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import {
  type GetJobErrorAttributionsStatus200,
  getJobErrorAttributions,
} from "~/gen/job-manager";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";

export async function GET(
  req: NextRequest,
): Promise<NextResponse<GetJobErrorAttributionsStatus200 | { error: string }>> {
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
    const jobId = searchParams.get("jobId")!;
    const jobAttemptIndex = searchParams.get("jobAttemptIndex") || undefined;
    const anomalyid = searchParams.get("anomalyid") || undefined;

    const { data: JobErrorAttributionsData } = await getJobErrorAttributions({
      query: {
        job_id: jobId,
        job_attempt_index: jobAttemptIndex
          ? Number.parseInt(jobAttemptIndex, 10)
          : undefined,
        anomaly_uuid: anomalyid ? anomalyid : undefined,
      },
    });

    logger.debug(`GET /api/job-attributions?${searchParams.toString()}`);

    return NextResponse.json(JobErrorAttributionsData);
  } catch (_error) {
    const error = _error as AxiosError;
    const publicErrorDetails = {
      code: error.code,
      name: error.name,
      message: error.message,
      cause: error.cause,
    };
    logger.error(
      `GET /api/job-attributions?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      { error: "Failed to get job error attributions", ...publicErrorDetails },
      { status: 500 },
    );
  }
}
