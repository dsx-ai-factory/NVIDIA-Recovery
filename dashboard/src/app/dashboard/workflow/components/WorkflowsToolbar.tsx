// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  DropdownContent,
  DropdownRoot,
  DropdownSub,
  DropdownSubContent,
  DropdownSubTrigger,
  DropdownTrigger,
  Flex,
  Text,
} from "@nvidia/foundations-react-core";
import { Filter, Refresh } from "@nvidia/react-gui-icons/icons";
import { useRouter } from "next/navigation";
import { useState } from "react";
import type { GetJobsJobNameMatchKey } from "~/gen/job-manager/types";
import type { WorkflowColumn } from "./Columns";
import { EditColumnsMenu } from "./EditColumnsMenu";
import { StateFilterSubmenu, TextFilterSubmenu } from "./Filters";
import { SearchBar } from "./SearchBar";
import type { FilterMap, WorkflowsTableState } from "./tableState";

function getCompareUrl(selected: string[]) {
  const queryParamValue = encodeURI(selected.join(","));
  return `/dashboard/workflow/job/compare?jobs=${queryParamValue}`;
}

export function WorkflowsToolbar({
  columns,
  state,
  totalCount,
  query,
  jobNameMatch,
  isFetching,
  onRefresh,
}: {
  columns: WorkflowColumn[];
  state: WorkflowsTableState;
  totalCount: number | undefined;
  query?: string;
  jobNameMatch: GetJobsJobNameMatchKey;
  isFetching: boolean;
  onRefresh: () => void;
}) {
  const router = useRouter();
  const [filtersOpen, setFiltersOpen] = useState<boolean>(false);
  const selected = state.getSelectedIds();
  const total = totalCount ?? 0;

  const filterableColumns = columns.filter((c) => c.filter !== undefined);
  const activeFilterCount = Object.values(state.filters).filter(
    (v) => v !== undefined && v !== "",
  ).length;

  return (
    <Flex direction="row" align="center" gap="2">
      <DropdownRoot open={filtersOpen} onOpenChange={setFiltersOpen}>
        <DropdownTrigger asChild>
          <Button kind="tertiary">
            <Filter />
            Filters
            {activeFilterCount > 0 ? ` (${activeFilterCount})` : ""}
          </Button>
        </DropdownTrigger>
        <DropdownContent align="start">
          {filterableColumns.map((col) => {
            const filter = col.filter!;
            const id = col.id as keyof FilterMap;
            const current = state.filters[id];
            const triggerLabel = current
              ? `${col.header}: ${current}`
              : col.header;
            return (
              <DropdownSub key={col.id}>
                <DropdownSubTrigger>{triggerLabel}</DropdownSubTrigger>
                <DropdownSubContent className="p-2">
                  {filter.type === "text" ? (
                    <TextFilterSubmenu
                      label={col.header}
                      value={current}
                      onApply={(next) => {
                        state.setFilter(id, next);
                        setFiltersOpen(false);
                      }}
                    />
                  ) : (
                    <StateFilterSubmenu
                      value={current}
                      options={filter.options}
                      onApply={(next) => {
                        state.setFilter(id, next);
                        setFiltersOpen(false);
                      }}
                    />
                  )}
                </DropdownSubContent>
              </DropdownSub>
            );
          })}
        </DropdownContent>
      </DropdownRoot>
      <SearchBar defaultValue={query} defaultJobNameMatch={jobNameMatch} />
      <Text className="ml-2 whitespace-nowrap">{total} Results</Text>

      <Button kind="tertiary" onClick={onRefresh} disabled={isFetching}>
        <Refresh className={isFetching ? "animate-spin" : undefined} />
        Refresh
      </Button>
      <EditColumnsMenu
        columns={columns}
        isVisible={state.isColumnVisible}
        onSetVisible={state.setColumnVisible}
      />
      {selected.length > 0 && (
        <Button
          kind="primary"
          size="small"
          onClick={() => router.push(getCompareUrl(selected))}
        >
          Compare Jobs ({selected.length})
        </Button>
      )}
    </Flex>
  );
}
