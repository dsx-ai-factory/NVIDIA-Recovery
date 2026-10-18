// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Prometheus Metrics Endpoint
 *
 * Exposes application metrics in Prometheus format at /metrics
 * This endpoint does not require authentication to allow Prometheus scraping
 */

import { NextResponse } from "next/server";
import { areMetricsEnabled } from "~/lib/metrics";
import { logger } from "~/pino";
import { getMetrics, getMetricsContentType } from "~/server/prometheus";

/**
 * Disable body parsing for this endpoint
 */
export const dynamic = "force-dynamic";

/**
 * GET /metrics
 *
 * Returns metrics in Prometheus text format
 * This endpoint is intentionally unauthenticated to allow Prometheus scraping
 */
export async function GET(): Promise<NextResponse> {
  /* 422 Unprocessable Entity is the best fit for an endpoint that is unable to process a correct request */
  if (!areMetricsEnabled()) {
    return new NextResponse("Metrics disabled", { status: 422 });
  }

  try {
    const metrics = await getMetrics();
    const contentType = getMetricsContentType();

    return new NextResponse(metrics, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-store, no-cache, must-revalidate",
      },
    });
  } catch (error) {
    logger.error("Failed to generate Prometheus metrics", { error });

    return NextResponse.json(
      { error: "Failed to generate metrics" },
      { status: 500 },
    );
  }
}
