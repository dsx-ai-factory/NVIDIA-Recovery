// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Checkbox,
  Flex,
  TableBody,
  TableDataCell,
  TableHead,
  TableHeaderCell,
  TableRoot,
  TableRow,
  Text,
} from "@nvidia/foundations-react-core";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Filter,
} from "@nvidia/react-gui-icons/icons";
import type { JobRow, WorkflowColumn } from "./Columns";
import type { WorkflowsTableState } from "./tableState";

export function WorkflowsTableView({
  columns,
  rows,
  state,
}: {
  columns: WorkflowColumn[];
  rows: JobRow[];
  state: WorkflowsTableState;
}) {
  const visibleColumns = columns.filter((c) => state.isColumnVisible(c.id));
  const allRowIds = rows.map((r) => r.id);
  const selectedCount = allRowIds.filter((id) => state.rowSelection[id]).length;
  const allSelected =
    allRowIds.length > 0 && selectedCount === allRowIds.length;
  const someSelected = selectedCount > 0 && !allSelected;

  return (
    <TableRoot className="w-full">
      <TableHead>
        <TableRow>
          <TableHeaderCell className="w-8">
            <Checkbox
              checked={
                allSelected ? true : someSelected ? "indeterminate" : false
              }
              onCheckedChange={(next) => {
                if (next === true) {
                  state.selectAll(allRowIds);
                } else {
                  state.clearSelection();
                }
              }}
              aria-label="Select all rows"
            />
          </TableHeaderCell>
          {visibleColumns.map((col) => {
            if (!col.sortable) {
              return (
                <TableHeaderCell key={col.id}>{col.header}</TableHeaderCell>
              );
            }
            const isActive = state.sort?.id === col.id;
            const sortIcon = !isActive ? (
              <ArrowUpDown />
            ) : state.sort?.desc ? (
              <ArrowDown />
            ) : (
              <ArrowUp />
            );
            return (
              <TableHeaderCell key={col.id}>
                <button
                  type="button"
                  className="flex cursor-pointer items-center gap-1"
                  onClick={() => state.cycleSort(col.id)}
                >
                  <span>{col.header}</span>
                  {sortIcon}
                </button>
              </TableHeaderCell>
            );
          })}
        </TableRow>
      </TableHead>
      <TableBody>
        {rows.length === 0 ? (
          <TableRow>
            <TableDataCell colSpan={visibleColumns.length + 1}>
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
                  No jobs match your filters
                </div>
              </Flex>
            </TableDataCell>
          </TableRow>
        ) : (
          rows.map((row) => (
            <TableRow key={row.id}>
              <TableDataCell className="w-8">
                <Checkbox
                  checked={state.rowSelection[row.id] === true}
                  onCheckedChange={() => state.toggleRow(row.id)}
                  aria-label={`Select row ${row.id}`}
                />
              </TableDataCell>
              {visibleColumns.map((col) => (
                <TableDataCell key={col.id}>{col.render(row)}</TableDataCell>
              ))}
            </TableRow>
          ))
        )}
      </TableBody>
    </TableRoot>
  );
}
