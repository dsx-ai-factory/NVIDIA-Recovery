// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Banner } from "@nvidia/foundations-react-core";
import Link from "next/link";
import { EventsTable } from "~/app/dashboard/audit-events/components/EventsTable";
import {
  isCoarseAuditEvent,
  sortAuditEventsByOccurrence,
} from "~/app/dashboard/audit-events/components/eventPresentation";
import { useGetAllAuditEvents } from "~/app/dashboard/audit-events/hooks";
import { AUDIT_EVENT_SORT } from "~/app/dashboard/audit-events/sort";
import type { AuditEventsGetQuery } from "~/gen/notifications/types";
import { withBasePath } from "~/lib/base-path";

const AUDIT_EVENTS_PAGE_SIZE = 100;

/** Select the most compatible audit query identity for a job record. */
export function auditQueryForJob({
  heimdallJobId,
  jobId,
  attemptIndex,
}: {
  heimdallJobId?: string | null;
  jobId: string;
  attemptIndex: string;
}): AuditEventsGetQuery {
  if (heimdallJobId) {
    return { heimdall_job_id: heimdallJobId };
  }
  return { job_id: jobId, attempt_index: Number(attemptIndex) };
}

/** Renders the coarse audit timeline for one scheduler job attempt. */
export function JobAuditEvents({
  heimdallJobId,
  jobId,
  attemptIndex,
}: {
  heimdallJobId?: string | null;
  jobId: string;
  attemptIndex: string;
}) {
  const { data, error, isLoading } = useGetAllAuditEvents({
    ...auditQueryForJob({ heimdallJobId, jobId, attemptIndex }),
    limit: AUDIT_EVENTS_PAGE_SIZE,
    sort_by: AUDIT_EVENT_SORT.ascending,
  });
  const auditEvents = sortAuditEventsByOccurrence(
    (data?.items ?? []).filter(isCoarseAuditEvent),
  );

  return (
    <div className="min-w-0">
      <div className="mb-2 flex justify-end">
        <Link
          href={withBasePath(
            `/dashboard/audit-events?${new URLSearchParams({
              job_id: jobId,
              attempt_index: attemptIndex,
              sort_by: AUDIT_EVENT_SORT.ascending,
            })}`,
          )}
          className="text-12 text-green-700 underline"
        >
          View all audit events
        </Link>
      </div>
      {error && (
        <Banner kind="inline" status="error" className="mb-4">
          Unable to load audit events: {error.message}
        </Banner>
      )}
      <EventsTable
        auditEvents={auditEvents}
        isLoading={isLoading}
        mode="job-details"
        emptyMessage="No coarse audit events"
      />
    </div>
  );
}
