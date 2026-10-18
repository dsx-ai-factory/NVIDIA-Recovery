// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { V1Job } from "~/gen/api-gateway/types/V1Job";

const JOB_RUN_ID_REGEX = /^(\d+)-(\d+)$/;

export const getJobId = (job: V1Job): string | undefined => {
  const match = job.jobRunId?.match(JOB_RUN_ID_REGEX)?.[1];
  return match ? match : undefined;
};

export const getJobAttemptIndex = (job: V1Job): number | undefined => {
  const match = job.jobRunId?.match(JOB_RUN_ID_REGEX)?.[2];
  return match ? parseInt(match, 10) : undefined;
};
