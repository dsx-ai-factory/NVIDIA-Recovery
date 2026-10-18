// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AxiosError } from "axios";
import { type NextRequest, NextResponse } from "next/server";
import { auditEventsGet } from "~/gen/notifications";
import type {
  AuditEventLevelKey,
  AuditEventsGetResponses,
} from "~/gen/notifications/types";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";
import { parseAttemptIndex } from "../../dashboard/audit-events/jobIdentity";
import { AUDIT_EVENT_SORT } from "../../dashboard/audit-events/sort";
import {
  decodeMockCursor,
  encodeMockCursor,
  MOCK_AUDIT_EVENTS,
  randomMockDelayMs,
} from "./mock";

const MOCK_ENABLED = false;

function errorDetail(error: AxiosError): string {
  const responseData = error.response?.data;
  if (
    responseData !== null &&
    typeof responseData === "object" &&
    "detail" in responseData
  ) {
    const detail = (responseData as { detail?: unknown }).detail;
    if (typeof detail === "string") {
      return detail;
    }
    if (detail !== undefined) {
      return JSON.stringify(detail);
    }
  }
  return error.message || "Notifications request failed.";
}

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
    const eventLevel = searchParams.get("event_level") || undefined;
    const eventProducer = searchParams.get("event_producer") || undefined;
    const eventType = searchParams.get("event_type") || undefined;
    const minEventProducedAt = searchParams.get("min_event_produced_at")
      ? Number(searchParams.get("min_event_produced_at"))
      : undefined;
    const maxEventProducedAt = searchParams.get("max_event_produced_at")
      ? Number(searchParams.get("max_event_produced_at"))
      : undefined;
    const sort_by = searchParams.get("sort_by") || undefined;
    const cursor = searchParams.get("cursor") || undefined;
    const heimdallJobId = searchParams.get("heimdall_job_id") || undefined;
    const jobId = searchParams.get("job_id") || undefined;
    const attemptIndex = parseAttemptIndex(searchParams.get("attempt_index"));
    if (searchParams.has("attempt_index") && attemptIndex === undefined) {
      return NextResponse.json(
        { detail: "attempt_index must be a non-negative integer." },
        { status: 422 },
      );
    }
    const limit = searchParams.get("limit")
      ? Number(searchParams.get("limit"))
      : undefined;

    console.log(
      "audit-events query params",
      eventLevel,
      eventProducer,
      minEventProducedAt,
      maxEventProducedAt,
      cursor,
      heimdallJobId,
      jobId,
      attemptIndex,
      limit,
    );

    if (MOCK_ENABLED) {
      // Mock pagination needs a concrete page size; fall back to the real
      // API's documented default (100) when the caller doesn't specify one.
      const mockLimit = limit ?? 100;

      const filteredAuditEvents = MOCK_AUDIT_EVENTS.filter((event) => {
        if (heimdallJobId && event.heimdall_job_id !== heimdallJobId) {
          return false;
        }
        if (eventProducer && event.event_producer !== eventProducer) {
          return false;
        }
        if (eventLevel && event.event_level !== eventLevel) {
          return false;
        }
        if (eventType && event.event_type !== eventType) {
          return false;
        }
        if (
          minEventProducedAt !== undefined &&
          event.event_produced_at < minEventProducedAt
        ) {
          return false;
        }
        if (
          maxEventProducedAt !== undefined &&
          event.event_produced_at > maxEventProducedAt
        ) {
          return false;
        }
        return true;
      });

      const direction = sort_by === AUDIT_EVENT_SORT.ascending ? 1 : -1;
      filteredAuditEvents.sort(
        (left, right) =>
          direction * (left.event_produced_at - right.event_produced_at),
      );
      const offset = decodeMockCursor(cursor);
      const pageItems = filteredAuditEvents.slice(offset, offset + mockLimit);

      const auditEventsData: AuditEventsGetResponses["200"] = {
        items: pageItems,
        cursor: {
          prev_cursor:
            offset > 0 ? encodeMockCursor(Math.max(0, offset - mockLimit)) : "",
          next_cursor:
            offset + mockLimit < filteredAuditEvents.length
              ? encodeMockCursor(offset + mockLimit)
              : "",
        },
      };

      logger.debug(
        `GET /api/audit-events?${searchParams.toString()} - fetched audit events groups: ${auditEventsData.items.map((auditEvent) => auditEvent.heimdall_job_id).join(", ")}`,
      );

      await new Promise((resolve) => setTimeout(resolve, randomMockDelayMs()));

      return NextResponse.json(auditEventsData);
    }

    const { data: auditEventsData } = await auditEventsGet({
      query: {
        event_level: eventLevel as AuditEventLevelKey | undefined,
        event_producer: eventProducer,
        event_type: eventType,
        min_event_produced_at: minEventProducedAt,
        max_event_produced_at: maxEventProducedAt,
        cursor,
        sort_by,
        heimdall_job_id: heimdallJobId,
        job_id: jobId,
        attempt_index: attemptIndex,
        limit,
      },
    });

    logger.debug(
      `GET /api/audit-events?${searchParams.toString()} - fetched audit events groups: ${auditEventsData.items.map((auditEvent) => auditEvent.heimdall_job_id).join(", ")}`,
    );

    return NextResponse.json(auditEventsData);
  } catch (_error) {
    const error = _error as AxiosError;
    const publicErrorDetails = {
      code: error.code,
      name: error.name,
      message: error.message,
      cause: error.cause,
    };
    logger.error(
      `GET /api/audit-events?${req.nextUrl.searchParams.toString()} failed: ${JSON.stringify({ ...publicErrorDetails, stack: error.stack?.substring(0, 200) })}`,
    );
    return NextResponse.json(
      {
        detail: errorDetail(error),
      },
      { status: error.response?.status || 500 },
    );
  }
}
