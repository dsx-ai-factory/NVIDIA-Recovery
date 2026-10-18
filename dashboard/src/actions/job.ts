// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";
import { isAuthAvailableAsync } from "~/actions/server_var";
import { DATADOG_WATCHER_URL_PATTERN } from "~/const/statics";
import { env } from "~/env";
import { getJobStatus } from "~/gen/job-manager";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";

export async function GetJob({
  jobId,
}: {
  jobId: string;
}): Promise<SlurmJobExecutionStatusOutput> {
  const authEnabled = await isAuthAvailableAsync();
  const session = await getServerAuthSession();
  if (authEnabled && !session) throw new Error(`Unauthorized(401)`);
  if (!jobId) throw new Error(`JobID could not be empty`);

  const { data: jobStatusData } = await getJobStatus({
    path: {
      job_id: jobId,
    },
  });
  logger.info(`Got job ${JSON.stringify(jobStatusData)} for JobID ${jobId}`);
  return jobStatusData;
}

export async function GetDatadogWatcherURL({
  jobId,
}: {
  jobId: string | undefined;
}): Promise<string> {
  const authEnabled = await isAuthAvailableAsync();
  const session = await getServerAuthSession();
  if (authEnabled && !session) throw new Error(`Unauthorized(401)`);

  if (!jobId) throw new Error(`JobID could not be empty`);
  if (!env.DATADOG_WATCHER_URL) {
    logger.warn(`No datadog URL is provided`);
    return "";
  }
  return env.DATADOG_WATCHER_URL.replace(DATADOG_WATCHER_URL_PATTERN, jobId);
}
