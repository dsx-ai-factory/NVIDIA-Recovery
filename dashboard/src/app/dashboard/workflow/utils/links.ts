// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";

type JobIdentity = Pick<
  SlurmJobExecutionStatusOutput,
  "job_id" | "job_attempt_index"
>;

/** Build the dashboard route and label for a job attempt. */
export function getJobLink(job: JobIdentity): {
  href: string;
  label: string;
};
export function getJobLink(
  job: JobIdentity | undefined,
): { href: string; label: string } | undefined;
export function getJobLink(job: JobIdentity | undefined) {
  if (!job) {
    return undefined;
  }
  return {
    href: `/dashboard/workflow/job/${encodeURIComponent(job.job_id)}/${job.job_attempt_index}/details`,
    label: `${job.job_id} (${job.job_attempt_index})`,
  };
}
