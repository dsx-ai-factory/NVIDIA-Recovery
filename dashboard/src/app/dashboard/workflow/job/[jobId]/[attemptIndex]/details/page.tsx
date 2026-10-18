// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import {
  getCockpitNameAsync,
  getFactUrl,
  isDatadogEnabledAsync,
  isFACTEnabledAsync,
} from "~/actions/server_var";
import { DATADOG_WATCHER_URL_PATTERN } from "~/const/statics";
import { env } from "~/env";
import { logger } from "~/pino";
import { getServerAuthSession } from "~/server/auth";
import { isAuthAvailable } from "~/utils/auth";
import {
  getGrafanaDataSourceId,
  isGrafanaTraceUrlEnabled,
} from "~/utils/config";
import { JobDetails } from "./components/JobDetails";

async function getDataDogUrl(jobId: string | undefined) {
  if (!jobId) throw new Error(`JobID could not be empty`);
  if (!env.DATADOG_WATCHER_URL) {
    logger.warn(`No datadog URL is provided`);
    return undefined;
  }
  const isDatadogEnabled = await isDatadogEnabledAsync();
  if (!isDatadogEnabled) {
    logger.warn(`Datadog is not enabled`);
    return undefined;
  }
  const authEnabled = isAuthAvailable();
  const session = await getServerAuthSession();
  if (authEnabled && !session) throw new Error(`Unauthorized(401)`);

  return env.DATADOG_WATCHER_URL.replace(DATADOG_WATCHER_URL_PATTERN, jobId);
}

export default async function JobDetailsPage({
  params,
}: {
  params: Promise<{ jobId: string; attemptIndex: string }>;
}) {
  const { jobId, attemptIndex } = await params;
  const dataDogUrl = await getDataDogUrl(jobId);
  const factEnabled = await isFACTEnabledAsync();
  const factUrl = await getFactUrl();
  const cockpitName = await getCockpitNameAsync();
  const grafanaTraceUrlEnabled = isGrafanaTraceUrlEnabled();
  const grafanaDataSourceId = getGrafanaDataSourceId();

  return (
    <JobDetails
      jobId={jobId}
      attemptIndex={attemptIndex}
      cockpitName={cockpitName}
      datadogLink={dataDogUrl}
      factUrl={factEnabled && factUrl}
      grafanaTraceUrlEnabled={grafanaTraceUrlEnabled}
      grafanaDataSourceId={grafanaDataSourceId}
      grafanaUrl={env.GRAFANA_URL}
      grafanaOrgId={env.GRAFANA_ORG_ID}
    />
  );
}
