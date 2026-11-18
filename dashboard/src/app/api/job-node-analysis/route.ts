// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { getNodeAnalysis, isResponseError } from "~/gen/job-manager";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";

/** Return node analysis or a JSON error when the job-manager request fails. */
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
    const expression = searchParams.get("expression");
    const groupBy = searchParams.get("groupBy") || undefined;

    if (!expression) {
      return NextResponse.json(
        { error: "Expression is required" },
        { status: 400 },
      );
    }

    const { data: nodeAnalysisData } = await getNodeAnalysis({
      query: {
        expr: expression,
        group_by: groupBy,
      },
    });

    logger.debug(
      `GET /api/jobs-node-analysis?${searchParams.toString()} - fetched nodes: ${nodeAnalysisData.result.nodes.map((node) => `node:${node}`).join(", ")}`,
    );

    return NextResponse.json(nodeAnalysisData);
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
      `GET /api/jobs-node-analysis?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
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
        detail: detail || "unknown error",
      },
      { status },
    );
  }
}
