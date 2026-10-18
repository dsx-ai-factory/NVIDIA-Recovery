// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Banner } from "@nvidia/foundations-react-core";
import {
  useGetJob,
  useGetJobErrorAttributions,
} from "~/app/dashboard/workflow/hooks";

export function AttributionsBanner({
  jobId,
  attemptIndex,
}: {
  jobId: string;
  attemptIndex: string;
}) {
  const { data: job } = useGetJob({ attemptIndex, jobId });
  const { data: jobAttributions } = useGetJobErrorAttributions({
    jobId,
    jobAttemptIndex: attemptIndex,
  });

  const showBanner =
    job?.state !== "RUNNING" &&
    jobAttributions?.attribution_summary &&
    jobAttributions?.attribution_insight &&
    jobAttributions?.attribution_summary.toUpperCase() !== "UNKNOWN";

  const not_infra = jobAttributions?.attribution_summary?.includes("not");

  if (showBanner) {
    return (
      <Banner
        kind="header"
        status={not_infra ? "error" : "warning"}
        slotSubheading={jobAttributions?.attribution_insight}
      >
        {jobAttributions?.attribution_summary}
      </Banner>
    );
  }

  return null;
}
