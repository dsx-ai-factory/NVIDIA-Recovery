// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";
import { isAuditEventsEnabled } from "~/utils/config";
import { PageLayout } from "../components/PageLayout";
import { PageContent } from "./components/PageContent";
import { parseAttemptIndex } from "./jobIdentity";
import { parseAuditEventSort } from "./sort";

export default async function AuditEventsPage(props: {
  searchParams?: Promise<{
    heimdall_job_id?: string;
    job_id?: string;
    attempt_index?: string | string[];
    event_level?: string;
    event_producer?: string;
    event_type?: string;
    min_event_produced_at?: string;
    max_event_produced_at?: string;
    limit?: string;
    cursor?: string;
    sort_by?: string;
  }>;
}) {
  if (!isAuditEventsEnabled()) {
    return redirect(withBasePath("/dashboard/workflow", process.env.BASE_PATH));
  }
  const searchParams = (await props.searchParams) ?? {};
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
  } = searchParams;
  const initialSort = parseAuditEventSort(sort_by);
  const initialAttemptIndex = parseAttemptIndex(attempt_index);
  const invalidAttempt =
    attempt_index !== undefined && initialAttemptIndex === undefined;

  return (
    <div className="bg-surface-sunken h-full">
      <PageLayout
        breadcrumbs={[{ children: "Audit Events" }]}
        title="Audit Events"
      >
        {invalidAttempt ? (
          <p role="alert" className="text-red-700">
            Invalid attempt index. Use a non-negative integer, or remove
            attempt_index to view all attempts of a job.
          </p>
        ) : (
          <PageContent
            key={JSON.stringify([
              heimdall_job_id,
              job_id,
              attempt_index,
              event_level,
              event_producer,
              event_type,
              min_event_produced_at,
              max_event_produced_at,
              initialSort,
            ])}
            initialHeimdallJobId={heimdall_job_id}
            initialJobId={job_id}
            initialAttemptIndex={initialAttemptIndex}
            initialEventLevel={event_level}
            initialEventProducer={event_producer}
            initialEventType={event_type}
            initialMinEventProducedAt={
              min_event_produced_at ? Number(min_event_produced_at) : undefined
            }
            initialMaxEventProducedAt={
              max_event_produced_at ? Number(max_event_produced_at) : undefined
            }
            initialLimit={Number(limit) || undefined}
            initialCursor={cursor}
            initialSort={initialSort}
          />
        )}
      </PageLayout>
    </div>
  );
}
