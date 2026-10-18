// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Banner } from "@nvidia/foundations-react-core";
import { notFound } from "next/navigation";
import { useGetJob } from "~/app/dashboard/workflow/hooks";
import Loading from "~/components/page/Loading";
import { JobAuditEvents } from "./JobAuditEvents";

/** Loads the selected job and renders its coarse audit-event timeline. */
export function JobTimeline({
  jobId,
  attemptIndex,
}: {
  jobId: string;
  attemptIndex: string;
}) {
  const { data: job, error, isLoading } = useGetJob({ attemptIndex, jobId });

  if (isLoading) {
    return <Loading />;
  }

  if (!job) {
    if (error) {
      return (
        <Banner kind="global" status="error">
          {error.name} - {error.message}
        </Banner>
      );
    }
    return notFound();
  }

  return (
    <div className="flex min-w-0 flex-col gap-4">
      {error && (
        <Banner kind="global" status="error">
          {error.name} - {error.message}
        </Banner>
      )}
      <JobAuditEvents
        heimdallJobId={job.heimdall_job_id}
        jobId={jobId}
        attemptIndex={attemptIndex}
      />
    </div>
  );
}
