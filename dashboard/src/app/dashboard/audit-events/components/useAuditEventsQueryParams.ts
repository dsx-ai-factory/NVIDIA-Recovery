// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect } from "react";
import type { AuditEventsGetQuery } from "~/gen/notifications/types";

export function useAuditEventsQueryParams(filters: AuditEventsGetQuery) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();

  const {
    heimdall_job_id,
    job_id,
    attempt_index,
    event_level,
    event_producer,
    event_type,
    min_event_produced_at,
    max_event_produced_at,
    limit,
    cursor,
    sort_by,
  } = filters;

  useEffect(() => {
    const params = new URLSearchParams(searchParams);

    const setOrDelete = (
      key: string,
      value: string | number | null | undefined,
    ) => {
      if (value === undefined || value === null || value === "") {
        params.delete(key);
      } else {
        params.set(key, String(value));
      }
    };

    setOrDelete("heimdall_job_id", heimdall_job_id);
    setOrDelete("job_id", job_id);
    setOrDelete("attempt_index", attempt_index);
    setOrDelete("event_level", event_level);
    setOrDelete("event_producer", event_producer);
    setOrDelete("event_type", event_type);
    setOrDelete("min_event_produced_at", min_event_produced_at);
    setOrDelete("max_event_produced_at", max_event_produced_at);
    setOrDelete("limit", limit);
    setOrDelete("cursor", cursor);
    setOrDelete("sort_by", sort_by);

    if (params.toString() !== searchParams.toString()) {
      router.replace(`${pathname}?${params.toString()}`, { scroll: false });
    }
  }, [
    heimdall_job_id,
    job_id,
    attempt_index,
    event_level,
    event_producer,
    event_type,
    min_event_produced_at,
    max_event_produced_at,
    limit,
    cursor,
    sort_by,
    pathname,
    router,
    searchParams,
  ]);
}
