// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { NextResponse } from "next/server";
import { z } from "zod";
import { areMetricsEnabled } from "~/lib/metrics";
import { endHttpRequest } from "~/lib/prometheus/instrumentation";

const schema = z.object({
  method: z.string(),
  route: z.string(),
  statusCode: z.number(),
  durationMs: z.number(),
});

/**
 * POST /metrics/http-request/stop
 *
 * Called by instrumentation to record the end of an HTTP request.
 * Expects JSON body: { method: string, route: string, statusCode: number, durationMs: number }
 */
export async function POST(request: Request): Promise<NextResponse> {
  /* 422 Unprocessable Entity is the best fit for an endpoint that is unable to process a correct request */
  if (!areMetricsEnabled()) {
    return NextResponse.json({ message: "Metrics disabled" }, { status: 422 });
  }

  const { success, data } = await schema.safeParseAsync(await request.json());

  if (!success) {
    return NextResponse.json(
      { message: "HTTP request end event failed to parse request body" },
      { status: 400 },
    );
  }

  endHttpRequest(data.method, data.route, data.statusCode, data.durationMs);

  return NextResponse.json(
    { message: "HTTP request end event received" },
    { status: 200 },
  );
}
