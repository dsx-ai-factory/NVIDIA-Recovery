// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Banner, Flex, Pagination } from "@nvidia/foundations-react-core";
import { useMemo } from "react";
import {
  getJobKeysFromId,
  getJobKeysFromName,
  type JobKey,
} from "~/app/dashboard/workflow/api";
import { useGetJobs } from "~/app/dashboard/workflow/hooks";
import Loading from "~/components/page/Loading";
import type { GetJobsJobNameMatchKey } from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { DEFAULT_PAGE_SIZE_OPTIONS } from "~/utils/pagination";
import { AppliedFilters } from "./AppliedFilters";
import { makeColumns } from "./Columns";
import { useWorkflowsTableState } from "./tableState";
import {
  sortByToString,
  usePersistTableStateQuery,
  useUserPageSize,
} from "./tableUrl";
import { WorkflowsTableView } from "./WorkflowsTableView";
import { WorkflowsToolbar } from "./WorkflowsToolbar";

interface WorkflowsTableProps {
  initialSortBy: string | undefined;
  initialJobIdFilter: string | undefined;
  initialJobGroupIdFilter: string | undefined;
  initialPage: number | undefined;
  initialStateFilter: string | undefined;
  initialUserFilter: string | undefined;
  initialJobNameMatchFilter: GetJobsJobNameMatchKey;
  initialQuery: string | undefined;
  jobGroupsEnabled: boolean;
}

export function WorkflowsTable({
  initialSortBy,
  initialJobIdFilter,
  initialJobGroupIdFilter,
  initialPage,
  initialStateFilter,
  initialUserFilter,
  initialJobNameMatchFilter,
  initialQuery,
  jobGroupsEnabled,
}: WorkflowsTableProps) {
  const { pageSize: userPageSize } = useUserPreferences();

  const tableState = useWorkflowsTableState({
    initialSortBy,
    initialJobIdFilter,
    initialJobGroupIdFilter,
    initialStateFilter,
    initialUserFilter,
    initialPage,
    initialPageSize: userPageSize,
  });

  const jobIdFilter = tableState.filters.job_id;
  const jobGroupIdFilter = tableState.filters.job_group_id;
  const stateFilter = tableState.filters.state;
  const userFilter = tableState.filters.user;

  const jobKeys = useMemo<JobKey[]>(() => {
    const keys = getJobKeysFromId(jobIdFilter).concat(
      getJobKeysFromName(initialQuery).map((key) => ({
        ...key,
        jobNameMatch: initialJobNameMatchFilter,
      })),
    );
    return keys.length > 0 ? keys : [{}];
  }, [jobIdFilter, initialQuery, initialJobNameMatchFilter]);

  const sortByString = sortByToString(tableState.sort, jobIdFilter);

  const { data, error, isFetching, isLoading, refetch } = useGetJobs({
    jobKeys,
    pageIndex: tableState.pageIndex,
    pageSize: tableState.pageSize,
    sortBy: sortByString,
    state: stateFilter,
    user: userFilter,
    jobGroupId: jobGroupIdFilter,
  });

  const columns = useMemo(
    () => makeColumns(jobGroupsEnabled, data?.agent_web_url),
    [jobGroupsEnabled, data?.agent_web_url],
  );

  const rows = useMemo(
    () =>
      (data?.statuses ?? []).map((job) => ({
        id: `${job.job_id}_${job.job_attempt_index}`,
        ...job,
      })),
    [data],
  );

  useUserPageSize(userPageSize, tableState.pageSize);
  usePersistTableStateQuery({
    page: String(tableState.pageIndex + 1),
    jobIdFilter,
    jobGroupIdFilter,
    stateFilter,
    userFilter,
    sortBy: sortByString,
  });

  return (
    <Flex direction="col" gap="2">
      {error && (
        <Banner kind="global" status="error">
          {error?.name} - {error?.message}
        </Banner>
      )}
      <WorkflowsToolbar
        columns={columns}
        state={tableState}
        totalCount={data?.item_count}
        query={initialQuery}
        jobNameMatch={initialJobNameMatchFilter}
        isFetching={isFetching}
        onRefresh={() => {
          void refetch();
        }}
      />
      <AppliedFilters
        filters={tableState.filters}
        onClear={(id) => tableState.setFilter(id, undefined)}
        onClearAll={tableState.clearAllFilters}
      />
      {isLoading ? (
        <Loading />
      ) : (
        <>
          <WorkflowsTableView
            columns={columns}
            rows={rows}
            state={tableState}
          />
          <Pagination
            displayControls={true}
            page={tableState.pageIndex + 1}
            kind="tabs"
            pageSize={tableState.pageSize}
            pageSizeOptions={DEFAULT_PAGE_SIZE_OPTIONS}
            totalItems={data?.item_count ?? 0}
            onPageChange={(next) => tableState.setPageIndex(next - 1)}
            onPageSizeChange={(next) => {
              tableState.setPageSize(next);
              tableState.setPageIndex(0);
            }}
          />
        </>
      )}
    </Flex>
  );
}
