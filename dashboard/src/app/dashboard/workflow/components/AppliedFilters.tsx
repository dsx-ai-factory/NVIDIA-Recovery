// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button, Flex, Group, Tag } from "@nvidia/foundations-react-core";
import { Close } from "@nvidia/react-gui-icons/icons";
import type { FilterMap } from "./tableState";

const FILTER_LABELS: Record<keyof FilterMap, string> = {
  job_id: "Job ID",
  job_group_id: "Job Group ID",
  state: "State",
  user: "User",
};

export function AppliedFilters({
  filters,
  onClear,
  onClearAll,
}: {
  filters: FilterMap;
  onClear: (id: keyof FilterMap) => void;
  onClearAll: () => void;
}) {
  const entries = (Object.keys(filters) as (keyof FilterMap)[]).filter(
    (id) => filters[id] !== undefined && filters[id] !== "",
  );
  if (entries.length === 0) return null;

  return (
    <Flex gap="2" align="center" wrap="wrap">
      {entries.map((id) => (
        <Group key={id}>
          <Tag color="gray" kind="outline" readOnly>
            {FILTER_LABELS[id]}: {filters[id]}
          </Tag>
          <Tag color="gray" kind="outline" onClick={() => onClear(id)}>
            <Close />
          </Tag>
        </Group>
      ))}
      {entries.length >= 1 && (
        <Button kind="tertiary" size="small" onClick={onClearAll}>
          <Close />
          Clear Filters
        </Button>
      )}
    </Flex>
  );
}
