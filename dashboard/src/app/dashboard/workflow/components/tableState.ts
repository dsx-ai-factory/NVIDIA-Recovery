// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { useCallback, useState } from "react";
import { DEFAULT_SORT, type SortState, sortByStringToSortBy } from "./tableUrl";

export type FilterMap = {
  job_id?: string;
  job_group_id?: string;
  user?: string;
  state?: string;
};

export type Visibility = Record<string, boolean>;

export type RowSelection = Record<string, boolean>;

const DEFAULT_HIDDEN_COLUMNS: Visibility = {
  job_name: false,
  job_group_id: false,
  user: false,
  submit_time: false,
  last_updated_at: false,
};

export interface UseWorkflowsTableStateArgs {
  initialSortBy: string | undefined;
  initialJobIdFilter: string | undefined;
  initialJobGroupIdFilter: string | undefined;
  initialStateFilter: string | undefined;
  initialUserFilter: string | undefined;
  initialPage: number | undefined;
  initialPageSize: number;
}

export interface WorkflowsTableState {
  sort: SortState;
  setSort: (next: SortState) => void;
  cycleSort: (id: string) => void;

  filters: FilterMap;
  setFilter: (id: keyof FilterMap, value: string | undefined) => void;
  clearAllFilters: () => void;

  pageIndex: number;
  pageSize: number;
  setPageIndex: (next: number) => void;
  setPageSize: (next: number) => void;

  visibility: Visibility;
  toggleColumn: (id: string) => void;
  setColumnVisible: (id: string, visible: boolean) => void;
  isColumnVisible: (id: string) => boolean;

  rowSelection: RowSelection;
  toggleRow: (id: string) => void;
  selectAll: (ids: string[]) => void;
  clearSelection: () => void;
  getSelectedIds: () => string[];
}

export function useWorkflowsTableState({
  initialSortBy,
  initialJobIdFilter,
  initialJobGroupIdFilter,
  initialStateFilter,
  initialUserFilter,
  initialPage,
  initialPageSize,
}: UseWorkflowsTableStateArgs): WorkflowsTableState {
  const externalSort = sortByStringToSortBy(initialSortBy) ?? DEFAULT_SORT;
  const externalSortKey = `${externalSort.id}:${externalSort.desc}`;
  const [sort, setSortRaw] = useState<SortState>(externalSort);
  const [trackedSortKey, setTrackedSortKey] = useState<string>(externalSortKey);
  if (trackedSortKey !== externalSortKey) {
    setTrackedSortKey(externalSortKey);
    setSortRaw(externalSort);
  }

  const externalFilters: FilterMap = {
    job_id: initialJobIdFilter,
    job_group_id: initialJobGroupIdFilter,
    state: initialStateFilter,
    user: initialUserFilter,
  };
  const externalFilterKey = JSON.stringify(externalFilters);
  const [filters, setFilters] = useState<FilterMap>(externalFilters);
  const [trackedFilterKey, setTrackedFilterKey] =
    useState<string>(externalFilterKey);
  if (trackedFilterKey !== externalFilterKey) {
    setTrackedFilterKey(externalFilterKey);
    setFilters(externalFilters);
  }

  const initialPageIndex = Math.max(0, (initialPage ?? 1) - 1);
  const [pageIndex, setPageIndex] = useState<number>(initialPageIndex);
  const [trackedInitialPage, setTrackedInitialPage] =
    useState<number>(initialPageIndex);
  if (trackedInitialPage !== initialPageIndex) {
    setTrackedInitialPage(initialPageIndex);
    setPageIndex(initialPageIndex);
  }

  const [pageSize, setPageSize] = useState<number>(initialPageSize);

  const [visibility, setVisibility] = useState<Visibility>(
    DEFAULT_HIDDEN_COLUMNS,
  );

  const [rowSelection, setRowSelection] = useState<RowSelection>({});

  const setFilter = useCallback(
    (id: keyof FilterMap, value: string | undefined) => {
      setFilters((prev) => {
        const next: FilterMap = { ...prev };
        if (value === undefined || value === "") {
          delete next[id];
        } else {
          next[id] = value;
        }
        return next;
      });
      // changing a filter should reset to first page
      setPageIndex(0);
    },
    [],
  );

  const clearAllFilters = useCallback(() => {
    setFilters({});
    setPageIndex(0);
  }, []);

  const setSort = useCallback((next: SortState) => {
    setSortRaw(next);
    setPageIndex(0);
  }, []);

  const cycleSort = useCallback((id: string) => {
    setSortRaw((prev) => {
      if (!prev || prev.id !== id) {
        return { id, desc: true };
      }
      if (prev.desc) {
        return { id, desc: false };
      }
      return undefined;
    });
    setPageIndex(0);
  }, []);

  const toggleColumn = useCallback((id: string) => {
    setVisibility((prev) => ({ ...prev, [id]: prev[id] === false }));
  }, []);

  const setColumnVisible = useCallback((id: string, visible: boolean) => {
    setVisibility((prev) => ({ ...prev, [id]: visible }));
  }, []);

  const isColumnVisible = useCallback(
    (id: string) => visibility[id] !== false,
    [visibility],
  );

  const toggleRow = useCallback((id: string) => {
    setRowSelection((prev) => {
      const next = { ...prev };
      if (next[id]) {
        delete next[id];
      } else {
        next[id] = true;
      }
      return next;
    });
  }, []);

  const selectAll = useCallback((ids: string[]) => {
    setRowSelection(() => {
      const next: RowSelection = {};
      for (const id of ids) {
        next[id] = true;
      }
      return next;
    });
  }, []);

  const clearSelection = useCallback(() => {
    setRowSelection({});
  }, []);

  const getSelectedIds = useCallback(
    () => Object.keys(rowSelection).filter((k) => rowSelection[k] === true),
    [rowSelection],
  );

  return {
    sort,
    setSort,
    cycleSort,
    filters,
    setFilter,
    clearAllFilters,
    pageIndex,
    pageSize,
    setPageIndex,
    setPageSize,
    visibility,
    toggleColumn,
    setColumnVisible,
    isColumnVisible,
    rowSelection,
    toggleRow,
    selectAll,
    clearSelection,
    getSelectedIds,
  };
}
