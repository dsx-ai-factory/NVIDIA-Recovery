// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { DEFAULT_AUTO_REFRESH_INTERVAL } from "~/const/statics";
import type { GetWorkflowsWorkflowIdMatchKey } from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import {
  fetchDagDependencies,
  fetchJobs,
  fetchWorkflows,
  getJobErrorAttributions,
  type JobKey,
} from "./api";

const JOBS_KEY = "JOBS";

export function useGetWorkflows({
  workflowId,
  workflowIdMatch,
  pageIndex = 0,
  pageSize = 10,
}: {
  workflowId?: string;
  workflowIdMatch?: GetWorkflowsWorkflowIdMatchKey;
  pageIndex?: number;
  pageSize?: number;
}) {
  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  return useQuery({
    queryKey: [
      "workflows",
      { workflowId, workflowIdMatch, pageIndex, pageSize },
    ],
    queryFn: () =>
      fetchWorkflows({
        workflowId,
        workflowIdMatch,
        offset: pageIndex * pageSize,
        limit: pageSize,
      }),
    refetchInterval: autoRefreshEnabled
      ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
      : false,
  });
}

export function useGetJobs({
  jobKeys,
  pageIndex = 0,
  pageSize = 10,
  sortBy,
  state,
  user,
  jobGroupId,
}: {
  jobKeys: JobKey[];
  pageIndex?: number;
  pageSize?: number;
  sortBy?: string;
  state?: string;
  user?: string;
  jobGroupId?: string;
}) {
  const queryClient = useQueryClient();
  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  return useQuery({
    enabled: jobKeys && jobKeys.length > 0,
    queryKey: [
      JOBS_KEY,
      { jobKeys, pageIndex, pageSize, sortBy, state, user, jobGroupId },
    ],
    queryFn: async () => {
      const results = await Promise.all(
        jobKeys.map((key) =>
          queryClient.fetchQuery({
            queryKey: [
              JOBS_KEY,
              key,
              pageSize,
              pageIndex,
              sortBy,
              state,
              user,
              jobGroupId,
            ],
            queryFn: () =>
              fetchJobs({
                jobName: key.jobName,
                jobId: key.jobId,
                jobAttemptIndex: key.attemptIndex,
                jobNameMatch: key.jobNameMatch,
                limit: pageSize,
                offset: pageIndex * pageSize,
                sortBy,
                state,
                user,
                jobGroupId,
              }),
          }),
        ),
      );
      return results.reduce((acc, currentValue) => ({
        item_count: acc.item_count + currentValue.item_count,
        statuses: acc.statuses.concat(currentValue.statuses),
        agent_web_url: acc.agent_web_url ?? currentValue.agent_web_url,
      }));
    },
    refetchInterval: autoRefreshEnabled
      ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
      : false,
  });
}

export function useGetJob({
  attemptIndex,
  jobId,
}: {
  attemptIndex: string;
  jobId: string;
}) {
  const { data, ...rest } = useGetJobs({
    jobKeys: [{ jobId, attemptIndex }],
  });
  return {
    data: data ? data.statuses[0] : undefined,
    ...rest,
  };
}

export function useGetDagDependencies({
  dependentJobId,
}: {
  dependentJobId?: string;
}) {
  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  return useQuery({
    enabled: !!dependentJobId,
    queryKey: ["dag-dependencies", { dependentJobId }],
    queryFn: () => fetchDagDependencies({ dependentJobId: dependentJobId! }),
    refetchInterval: autoRefreshEnabled
      ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
      : false,
  });
}

export function useGetJobErrorAttributions({
  jobId,
  jobAttemptIndex,
  anomalyid,
}: {
  jobId: string;
  jobAttemptIndex?: string;
  anomalyid?: string;
}) {
  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  return useQuery({
    enabled: !!jobId,
    queryKey: ["job-attributions", { jobId, jobAttemptIndex, anomalyid }],
    queryFn: () =>
      getJobErrorAttributions({
        jobId,
        jobAttemptIndex,
        anomalyid,
      }),
    refetchInterval: autoRefreshEnabled
      ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
      : false,
  });
}
