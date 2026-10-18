// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import {
  type GetWorkflowsWorkflowIdMatchKey,
  getWorkflows,
} from "~/gen/job-manager";
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
    const limit = Math.min(Number(searchParams.get("limit")) || 50, 100);
    const offset = Number(searchParams.get("offset")) || 0;
    const workflowId = searchParams.get("workflowId") || undefined;
    const workflowIdMatch: GetWorkflowsWorkflowIdMatchKey | undefined =
      searchParams.get("workflowIdMatch") as
        | GetWorkflowsWorkflowIdMatchKey
        | undefined;

    const { data: workflowsData } = await getWorkflows({
      query: {
        limit: limit,
        offset: offset,
        "workflow-id": workflowId,
        workflow_id_match: workflowIdMatch,
      },
    });

    logger.debug(
      `GET /api/workflows?${searchParams.toString()} - offset: ${offset}, limit: ${limit}, workflowId: ${workflowId}, workflowIdMatch: ${workflowIdMatch}, fetched workflows: ${workflowsData.workflows.map((x) => `workflow_id:${x.id},submit_time:${x.start_time}`).join(", ")}`,
    );

    const { workflows, item_count } = workflowsData;
    return NextResponse.json({
      workflows,
      item_count,
    });
  } catch (_error) {
    const error = _error as AxiosError;
    const publicErrorDetails = {
      code: error.code,
      name: error.name,
      message: error.message,
      cause: error.cause,
    };
    logger.error(
      `GET /api/workflows?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      { error: "Failed to get workflows", ...publicErrorDetails },
      { status: 500 },
    );
  }
}
