// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useRef } from "react";
import { useUpdateUserPreferences } from "~/hooks/useUpdateUserPreferences";

export type SortState = { id: string; desc: boolean } | undefined;

export function sortByToString(
  sort: SortState,
  jobIdFilter: string | undefined,
): string | undefined {
  if (!sort) {
    return undefined;
  }
  // when filtering by a specific job_id, sort by job_attempt_index instead
  const sortKey =
    jobIdFilter !== undefined && sort.id === "job_id"
      ? "job_attempt_index"
      : sort.id;
  const sortOrder = sort.desc ? "desc" : "asc";
  const primarySort = `${sortKey}:${sortOrder}`;

  // append job_attempt_index:desc as secondary sort when primary sort is job_id
  if (sortKey === "job_id") {
    return `${primarySort},job_attempt_index:desc`;
  }
  return primarySort;
}

export const DEFAULT_SORT = { id: "job_id", desc: true } as const;

const DEFAULT_SORT_STRING = sortByToString(DEFAULT_SORT, undefined);

export function sortByStringToSortBy(
  sortByString: string | undefined,
): SortState {
  if (!sortByString) return undefined;
  const primarySort = sortByString.split(",")[0] ?? sortByString;
  const [sortBy, sortOrder] = primarySort.split(":");
  // Reverse sortByToString's job_attempt_index substitution so the round-trip
  // preserves the user-facing sort key when a job_id filter is active.
  const id = sortBy === "job_attempt_index" ? "job_id" : (sortBy ?? "");
  return {
    id,
    desc: sortOrder === "desc",
  };
}

export function usePersistTableStateQuery(state: {
  page: string;
  jobIdFilter: string | undefined;
  jobGroupIdFilter: string | undefined;
  stateFilter: string | undefined;
  userFilter: string | undefined;
  sortBy: string | undefined;
}) {
  const {
    page,
    jobIdFilter,
    jobGroupIdFilter,
    stateFilter,
    userFilter,
    sortBy,
  } = state;
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();

  useEffect(() => {
    const params = new URLSearchParams(searchParams);

    if (params.get("page") !== page) {
      if (page === "1") {
        params.delete("page");
      } else {
        params.set("page", page);
      }
    }

    if (params.get("jobId") !== jobIdFilter) {
      if (jobIdFilter === undefined) {
        params.delete("jobId");
      } else {
        params.set("jobId", jobIdFilter);
      }
    }

    if (params.get("jobGroupId") !== jobGroupIdFilter) {
      if (jobGroupIdFilter === undefined) {
        params.delete("jobGroupId");
      } else {
        params.set("jobGroupId", jobGroupIdFilter);
      }
    }

    if (params.get("state") !== stateFilter) {
      if (stateFilter === undefined) {
        params.delete("state");
      } else {
        params.set("state", stateFilter);
      }
    }

    if (params.get("user") !== userFilter) {
      if (userFilter === undefined) {
        params.delete("user");
      } else {
        params.set("user", userFilter);
      }
    }

    if (params.get("sortBy") !== sortBy) {
      if (sortBy === undefined || sortBy === DEFAULT_SORT_STRING) {
        params.delete("sortBy");
      } else {
        params.set("sortBy", sortBy);
      }
    }

    if (params.toString() !== searchParams.toString()) {
      router.replace(`${pathname}?${params.toString()}`);
    }
  }, [
    jobIdFilter,
    jobGroupIdFilter,
    page,
    pathname,
    router,
    searchParams,
    sortBy,
    stateFilter,
    userFilter,
  ]);
}

export function useUserPageSize(userPageSize: number, tablePageSize: number) {
  const updatePreferences = useUpdateUserPreferences();
  const mutateRef = useRef(updatePreferences.mutate);
  mutateRef.current = updatePreferences.mutate;

  useEffect(() => {
    if (userPageSize !== tablePageSize) {
      mutateRef.current({ pageSize: tablePageSize });
    }
  }, [tablePageSize, userPageSize]);
}
