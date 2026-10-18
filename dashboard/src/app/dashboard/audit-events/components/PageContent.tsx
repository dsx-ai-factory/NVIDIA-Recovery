// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Banner } from "@nvidia/foundations-react-core";
import { useEffect, useRef, useState } from "react";
import type { AuditEventLevelKey } from "~/gen/notifications/types";
import { useGetAuditEvents } from "../hooks";
import type { AuditEventSort } from "../sort";
import { EventsTable } from "./EventsTable";
import type { AuditEventsFilters } from "./FiltersCard";
import { FiltersCard } from "./FiltersCard";
import { PaginationControls } from "./PaginationControls";
import { useAuditEventsQueryParams } from "./useAuditEventsQueryParams";

export function PageContent({
  initialHeimdallJobId,
  initialJobId,
  initialAttemptIndex,
  initialEventLevel,
  initialEventProducer,
  initialEventType,
  initialMinEventProducedAt,
  initialMaxEventProducedAt,
  initialLimit,
  initialCursor,
  initialSort,
}: {
  initialHeimdallJobId?: string;
  initialJobId?: string;
  initialAttemptIndex?: number;
  initialEventLevel?: string;
  initialEventProducer?: string;
  initialEventType?: string;
  initialMinEventProducedAt?: number;
  initialMaxEventProducedAt?: number;
  initialLimit?: number;
  initialCursor?: string;
  initialSort: AuditEventSort;
}) {
  const [filters, setFilters] = useState<AuditEventsFilters>({
    heimdall_job_id: initialHeimdallJobId,
    job_id: initialJobId,
    attempt_index: initialAttemptIndex,
    event_level: initialEventLevel as AuditEventLevelKey | undefined,
    event_producer: initialEventProducer,
    event_type: initialEventType,
    min_event_produced_at: initialMinEventProducedAt,
    max_event_produced_at: initialMaxEventProducedAt,
  });
  const [filtersExpanded, setFiltersExpanded] = useState(true);
  const [cursor, setCursor] = useState<string | undefined>(initialCursor);
  const [limit, setLimit] = useState<number | undefined>(initialLimit);
  const effectiveLimit = limit ?? 25;
  const [sort, setSort] = useState<AuditEventSort>(initialSort);

  useAuditEventsQueryParams({ ...filters, cursor, limit, sort_by: sort });

  const { data, error, isLoading, isFetching, isPlaceholderData } =
    useGetAuditEvents({
      ...filters,
      cursor,
      limit: effectiveLimit,
      sort_by: sort,
    });

  const handleLimitChange = (next: number) => {
    setLimit(next);
    setCursor(undefined);
  };

  const handlePaginate = (nextCursor: string) => {
    setCursor(nextCursor);
    setLimit(effectiveLimit);
  };

  const wasFetchingRef = useRef(isFetching);
  useEffect(() => {
    if (wasFetchingRef.current && !isFetching) {
      window.scrollTo({ top: 0, behavior: "smooth" });
    }
    wasFetchingRef.current = isFetching;
  }, [isFetching]);

  return (
    <div className="w-full">
      {error && (
        <Banner kind="global" status="error" className="mb-4">
          {error.name} - {error.message}
        </Banner>
      )}
      <div className="flex w-full items-start gap-6 py-6">
        <div
          className={`flex flex-shrink-0 flex-col gap-4 ${filtersExpanded ? "w-[350px]" : "w-fit"}`}
        >
          <FiltersCard
            filters={filters}
            isFetching={isFetching && !isLoading}
            expanded={filtersExpanded}
            onExpandedChange={setFiltersExpanded}
            onApply={(next) => {
              setFilters(next);
              setCursor(undefined);
            }}
          />
        </div>
        <div className="min-w-0 flex-1">
          <EventsTable
            auditEvents={data?.items ?? []}
            isLoading={isLoading || isPlaceholderData}
            sort={sort}
            onSortChange={(next) => {
              setSort(next);
              setCursor(undefined);
            }}
          />
          <PaginationControls
            limit={effectiveLimit}
            onLimitChange={handleLimitChange}
            prevCursor={data?.cursor.prev_cursor}
            nextCursor={data?.cursor.next_cursor}
            isFetching={isFetching && !isLoading}
            onPrev={handlePaginate}
            onNext={handlePaginate}
          />
        </div>
      </div>
    </div>
  );
}
