// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchJobs } from "~/app/dashboard/workflow/api";
import type { JobExecutionStatuses } from "~/gen/job-manager/types";

const JOBS_PAGE_LIMIT = 100;

import { fetchJobGroups } from "./api";

export function useGetJobGroups({
  jobGroupId,
  user,
  pageIndex = 0,
  pageSize = 10,
  sortBy,
}: {
  jobGroupId?: string;
  user?: string;
  pageIndex?: number;
  pageSize?: number;
  sortBy?: string;
}) {
  return useQuery({
    queryKey: ["job-groups", { jobGroupId, user, pageIndex, pageSize, sortBy }],
    queryFn: () =>
      fetchJobGroups({
        jobGroupId,
        user,
        limit: pageSize,
        offset: pageIndex * pageSize,
        sortBy,
      }),
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchOnMount: false,
    refetchInterval: false,
  });
}

export function useGetJobs({
  jobGroupId,
  enabled,
}: {
  jobGroupId?: string;
  enabled?: boolean;
}) {
  return useQuery<JobExecutionStatuses>({
    queryKey: ["jobs-by-group-id", { jobGroupId }],
    queryFn: async () => {
      const statuses: JobExecutionStatuses["statuses"] = [];
      let offset = 0;
      let itemCount: number | undefined;
      while (true) {
        const page = await fetchJobs({
          jobGroupId,
          limit: JOBS_PAGE_LIMIT,
          offset,
        });
        const pageStatuses = page.statuses ?? [];
        statuses.push(...pageStatuses);
        itemCount = page.item_count;
        if (pageStatuses.length < JOBS_PAGE_LIMIT) {
          break;
        }
        if (page.item_count != null && statuses.length >= page.item_count) {
          break;
        }
        offset += JOBS_PAGE_LIMIT;
      }
      return { statuses, item_count: itemCount ?? statuses.length };
    },
    enabled: Boolean(jobGroupId) && (enabled ?? true),
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchOnMount: false,
    refetchInterval: false,
  });
}
