// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextResponse } from "next/server";
import { z } from "zod";
import { areMetricsEnabled } from "~/lib/metrics";
import { startHttpRequest } from "~/lib/prometheus/instrumentation";

const schema = z.object({
  method: z.string(),
  route: z.string(),
});

/**
 * POST /metrics/http-request/start
 *
 * Called by instrumentation to record the start of an HTTP request.
 * Expects JSON body: { method: string, route: string }
 */
export async function POST(request: Request): Promise<NextResponse> {
  /* 422 Unprocessable Entity is the best fit for an endpoint that is unable to process a correct request */
  if (!areMetricsEnabled()) {
    return NextResponse.json({ message: "Metrics disabled" }, { status: 422 });
  }

  const { success, data } = await schema.safeParseAsync(await request.json());

  if (!success) {
    return NextResponse.json(
      { message: "HTTP request start event failed to parse request body" },
      { status: 400 },
    );
  }

  startHttpRequest(data.method, data.route);

  return NextResponse.json(
    { message: "HTTP request start event received" },
    { status: 200 },
  );
}
