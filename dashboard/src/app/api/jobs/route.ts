// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { type GetJobsJobNameMatchKey, getJobs } from "~/gen/job-manager";
import { getAgentWebUrl } from "~/lib/baseUrls";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";
import { lookupAgentAnalyses } from "./agentAnalysis";

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
    const jobName = searchParams.get("jobName") || undefined;
    const workflowId = searchParams.get("workflowId") ?? undefined;
    const state = searchParams.get("state") || undefined;
    const jobId = searchParams.get("jobId") || undefined;
    const jobAttemptIndex = searchParams.get("jobAttemptIndex") || undefined;
    const completeData = searchParams.get("completeData") === "true";
    const user = searchParams.get("user") || undefined;
    const jobNameMatch =
      (searchParams.get("jobNameMatch") as GetJobsJobNameMatchKey) || undefined;
    const limit = Math.min(Number(searchParams.get("limit")) || 50, 100);
    const offset = Number(searchParams.get("offset")) || 0;
    const sortBy = searchParams.get("sortBy") || "job_id:desc";
    const jobGroupId = searchParams.get("jobGroupId") || undefined;

    const { data: jobsData } = await getJobs({
      query: {
        complete_data: completeData,
        job_id: jobId,
        job_attempt_index: jobAttemptIndex,
        user: user,
        job_name: jobName,
        workflow_id: workflowId,
        job_name_match: jobNameMatch,
        state: state,
        limit: limit,
        offset: offset,
        sort_by: sortBy,
        job_group_id: jobGroupId,
      },
    });

    logger.debug(
      `GET /api/jobs?${searchParams.toString()} - offset: ${offset}, limit: ${limit}, fetched jobs: ${jobsData.statuses.map((x) => `job_id:${x.job_id},submit_time:${x.submit_time}`).join(", ")}`,
    );

    const { statuses, item_count } = jobsData;
    const analyses = await lookupAgentAnalyses(statuses);
    return NextResponse.json({
      statuses: statuses.map((status, index) => ({
        ...status,
        agent_analysis: analyses[index] ?? null,
      })),
      item_count,
      agent_web_url: getAgentWebUrl(),
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
      `GET /api/jobs?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      { error: "Failed to get jobs", ...publicErrorDetails },
      { status: 500 },
    );
  }
}
