// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { AuditEventsGetQuery } from "~/gen/notifications/types";

import { fetchAuditEvents } from "./api";

type AllAuditEventsGetQuery = Omit<AuditEventsGetQuery, "cursor">;

/** Fetches every producer-time page so callers can apply a global presentation order. */
async function fetchAllAuditEventPages(params: AllAuditEventsGetQuery) {
  const items = [];
  const visitedCursors = new Set<string>();
  let cursor: string | undefined;

  while (true) {
    const page = await fetchAuditEvents({ ...params, cursor });
    items.push(...page.items);

    const nextCursor = page.cursor.next_cursor || undefined;
    if (!nextCursor) {
      return {
        items,
        cursor: { prev_cursor: "", next_cursor: "" },
      };
    }
    if (visitedCursors.has(nextCursor)) {
      throw new Error("Audit event pagination returned a repeated cursor.");
    }
    visitedCursors.add(nextCursor);
    cursor = nextCursor;
  }
}

export function useGetAuditEvents(
  params: AuditEventsGetQuery,
  enabled?: boolean,
) {
  const {
    event_level,
    event_producer,
    event_type,
    min_event_produced_at,
    max_event_produced_at,
    cursor,
    heimdall_job_id,
    job_id,
    attempt_index,
    limit,
    sort_by,
  } = params;
  return useQuery({
    queryKey: [
      "audit-events",
      {
        event_level,
        event_producer,
        event_type,
        min_event_produced_at,
        max_event_produced_at,
        cursor,
        heimdall_job_id,
        job_id,
        attempt_index,
        limit,
        sort_by,
      },
    ],
    queryFn: () =>
      fetchAuditEvents({
        event_level,
        event_producer,
        event_type,
        min_event_produced_at,
        max_event_produced_at,
        cursor,
        heimdall_job_id,
        job_id,
        attempt_index,
        limit,
        sort_by,
      }),
    placeholderData: keepPreviousData,
    enabled: enabled ?? true,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchOnMount: false,
    refetchInterval: false,
  });
}

/** Loads a complete filtered audit stream for views that require global ordering. */
export function useGetAllAuditEvents(
  params: AllAuditEventsGetQuery,
  enabled?: boolean,
) {
  const {
    event_level,
    event_producer,
    event_type,
    min_event_produced_at,
    max_event_produced_at,
    heimdall_job_id,
    job_id,
    attempt_index,
    limit,
    sort_by,
  } = params;
  return useQuery({
    queryKey: [
      "audit-events",
      "all-pages",
      {
        event_level,
        event_producer,
        event_type,
        min_event_produced_at,
        max_event_produced_at,
        heimdall_job_id,
        job_id,
        attempt_index,
        limit,
        sort_by,
      },
    ],
    queryFn: () => fetchAllAuditEventPages(params),
    enabled: enabled ?? true,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchOnMount: false,
    refetchInterval: false,
  });
}
