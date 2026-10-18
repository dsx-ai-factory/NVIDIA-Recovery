// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Banner,
  Button,
  Dropdown,
  Flex,
  Pagination,
  Text,
  TextInput,
} from "@nvidia/foundations-react-core";
import { Close, Filter } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import Loading from "~/components/page/Loading";
import { useUpdateUserPreferences } from "~/hooks/useUpdateUserPreferences";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { DEFAULT_PAGE_SIZE_OPTIONS } from "~/utils/pagination";
import { pluralize } from "~/utils/strings";
import { useGetJobGroups } from "../hooks";
import { JobGroupCard } from "./JobGroupCard";
import { useJobGroupsQueryParams } from "./useJobGroupsQueryParams";

const SORT_OPTIONS: Array<{ value: string | undefined; label: string }> = [
  { value: undefined, label: "Default order" },
  { value: "job_group_id:asc", label: "Job group ID (A → Z)" },
  { value: "job_group_id:desc", label: "Job group ID (Z → A)" },
  { value: "unique_job_count:asc", label: "Unique jobs (low → high)" },
  { value: "unique_job_count:desc", label: "Unique jobs (high → low)" },
  {
    value: "total_job_attempt_count:asc",
    label: "Total attempts (low → high)",
  },
  {
    value: "total_job_attempt_count:desc",
    label: "Total attempts (high → low)",
  },
];

export function PageContent({
  initialPage,
  initialSort,
  initialJobGroupId,
}: {
  initialPage?: number;
  initialSort?: string;
  initialJobGroupId?: string;
}) {
  const { pageSize: userPageSize } = useUserPreferences();
  const updateUserPreferences = useUpdateUserPreferences();
  const [pageIndex, setPageIndex] = useState(
    Math.max(0, (initialPage ?? 1) - 1),
  );
  const [pageSize, setPageSize] = useState(userPageSize);
  const initialSortBy = SORT_OPTIONS.some((o) => o.value === initialSort)
    ? initialSort
    : undefined;
  const [sortBy, setSortBy] = useState<string | undefined>(initialSortBy);
  const [jobGroupIdFilter, setJobGroupIdFilter] = useState<string>(
    initialJobGroupId ?? "",
  );
  const [jobGroupIdInput, setJobGroupIdInput] = useState<string>(
    initialJobGroupId ?? "",
  );

  useJobGroupsQueryParams({
    page: pageIndex + 1,
    sort: sortBy,
    jobGroupId: jobGroupIdFilter || undefined,
  });

  const { data, error, isLoading, isFetching } = useGetJobGroups({
    pageIndex,
    pageSize,
    sortBy,
    jobGroupId: jobGroupIdFilter || undefined,
  });
  const jobGroups = data?.job_groups ?? [];

  const activeSortLabel =
    SORT_OPTIONS.find((o) => o.value === sortBy)?.label ?? "Default order";

  return (
    <Flex direction="col" gap="2">
      {error && (
        <Banner kind="global" status="error">
          {error.name} - {error.message}
        </Banner>
      )}
      <Flex
        direction="row"
        justify="between"
        align="center"
        gap="2"
        className="mb-2"
      >
        <TextInput
          id="job-group-id-filter"
          placeholder="Filter by Job Group ID"
          value={jobGroupIdInput}
          onValueChange={(value) => setJobGroupIdInput(value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              setJobGroupIdFilter(jobGroupIdInput);
              setPageIndex(0);
            }
          }}
          slotEnd={
            jobGroupIdInput.length > 0 ? (
              <Button
                kind="tertiary"
                size="small"
                onClick={() => {
                  setJobGroupIdInput("");
                  setJobGroupIdFilter("");
                  setPageIndex(0);
                }}
              >
                <Close />
              </Button>
            ) : undefined
          }
        />
        {data?.job_groups && (
          <Text kind="body/regular/md" className="whitespace-nowrap ml-2">
            {pluralize(data?.item_count ?? 0, "result")}
          </Text>
        )}
        <Dropdown
          align="end"
          items={SORT_OPTIONS.map((option) => ({
            children: option.label,
            onSelect: () => {
              setSortBy(option.value);
              setPageIndex(0);
            },
          }))}
        >
          Sort: {activeSortLabel}
        </Dropdown>
      </Flex>
      {isLoading || isFetching ? (
        <Loading />
      ) : (
        <Flex direction="col" gap="4">
          {jobGroups.length === 0 ? (
            <Flex
              direction="col"
              align="center"
              justify="center"
              className="pt-4 pb-6"
            >
              <Filter className="size-12" />
              <Text kind="body/regular/2xl" fontWeight="bold">
                No results
              </Text>
              <div className="text-muted text-center">
                No job groups match your filters
              </div>
            </Flex>
          ) : (
            jobGroups.map((group) => (
              <JobGroupCard key={group.job_group_id} jobGroup={group} />
            ))
          )}
          <Pagination
            displayControls
            page={pageIndex + 1}
            kind="tabs"
            pageSize={pageSize}
            pageSizeOptions={DEFAULT_PAGE_SIZE_OPTIONS}
            totalItems={data?.item_count ?? 0}
            onPageChange={(next) => setPageIndex(next - 1)}
            onPageSizeChange={(next) => {
              setPageSize(next);
              setPageIndex(0);
              updateUserPreferences.mutate({ pageSize: next });
            }}
          />
        </Flex>
      )}
    </Flex>
  );
}
