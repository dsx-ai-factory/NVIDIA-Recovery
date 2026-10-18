// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { getDagDependencies } from "~/gen/job-manager";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";

export async function GET(req: NextRequest): Promise<NextResponse> {
  try {
    if (isAuthAvailable()) {
      const session = await getServerAuthSession();
      if (!session) throw new Error("Unauthorized");
    }
  } catch (_error) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = req.nextUrl;
  const dependentJobId = searchParams.get("dependentJobId");
  if (!dependentJobId) {
    return NextResponse.json(
      { error: "dependentJobId is required" },
      { status: 400 },
    );
  }

  try {
    const { data: dependencies } = await getDagDependencies({
      query: {
        dependent_job_id: dependentJobId,
        limit: 1000,
        offset: 0,
        sort_by: "dependency_job_identifier:asc",
      },
    });

    logger.debug(`GET /api/dag-dependencies?${searchParams.toString()}`);
    return NextResponse.json(dependencies);
  } catch (_error) {
    const error = _error as AxiosError;
    const publicErrorDetails = {
      code: error.code,
      name: error.name,
      message: error.message,
    };
    logger.error(
      `GET /api/dag-dependencies?${searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      { error: "Failed to get DAG dependencies", ...publicErrorDetails },
      { status: 500 },
    );
  }
}
