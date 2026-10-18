// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";
import { isAuditEventsEnabled } from "~/utils/config";
import { JobTimeline } from "./components/JobTimeline";

export default async function JobTimelinePage({
  params,
}: {
  params: Promise<{ jobId: string; attemptIndex: string }>;
}) {
  const { jobId, attemptIndex } = await params;

  if (!isAuditEventsEnabled()) {
    return redirect(
      withBasePath(
        `/dashboard/workflow/job/${jobId}/${attemptIndex}/details`,
        process.env.BASE_PATH,
      ),
    );
  }

  return <JobTimeline jobId={jobId} attemptIndex={attemptIndex} />;
}
