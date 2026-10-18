// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Badge,
  Button,
  Dropdown,
  type DropdownEntry,
  type DropdownRadioGroupEntry,
  Flex,
  TableBody,
  TableDataCell,
  TableHead,
  TableHeaderCell,
  TableRoot,
  TableRow,
  Text,
  Tooltip,
} from "@nvidia/foundations-react-core";
import { Sort, Trash } from "@nvidia/react-gui-icons/icons";
import { format } from "date-fns";
import { useMemo, useState } from "react";
import { type V1ApiKey, v1ApiKeyStatus } from "~/gen/api-gateway/types";

type SortKey = "description" | "value" | "status" | "createdAt" | "expiresAt";
type SortState = { id: SortKey; desc: boolean };
const DEFAULT_SORT: SortState = { id: "createdAt", desc: true };

const SORTABLE_COLUMNS: { id: SortKey; header: string }[] = [
  { id: "description", header: "Description" },
  { id: "value", header: "API Key" },
  { id: "status", header: "Status" },
  { id: "createdAt", header: "Created" },
  { id: "expiresAt", header: "Expires" },
];

function formatDate(dateString: string | undefined): string {
  if (!dateString) return "";
  try {
    return format(new Date(dateString), "MMM dd, yyyy");
  } catch {
    return new Date(dateString).toLocaleDateString();
  }
}

interface ApiKeysTableProps {
  apiKeys: V1ApiKey[];
  setShowDeleteModal: (show: boolean) => void;
  setSelectedApiKey: (apiKey: V1ApiKey | null) => void;
}

export function ApiKeysTable({
  apiKeys,
  setShowDeleteModal,
  setSelectedApiKey,
}: ApiKeysTableProps) {
  const [sort, setSort] = useState<SortState>(DEFAULT_SORT);

  const sortOptions = SORTABLE_COLUMNS.flatMap((c) => [
    { value: `${c.id}:desc`, children: `${c.header} (desc)` },
    { value: `${c.id}:asc`, children: `${c.header} (asc)` },
  ]);

  const sortValue = `${sort.id}:${sort.desc ? "desc" : "asc"}`;
  const activeSortLabel =
    sortOptions.find((o) => o.value === sortValue)?.children ??
    "Created (desc)";

  const sortDropdownItems: DropdownEntry[] = [
    {
      kind: "radio",
      radioKind: "check",
      slotHeading: "Sort by",
      value: sortValue,
      onValueChange: (next) => {
        const [id, dir] = next.split(":");
        setSort({ id: (id ?? "createdAt") as SortKey, desc: dir !== "asc" });
      },
      items: sortOptions.map((o) => ({ value: o.value, children: o.children })),
    } as DropdownRadioGroupEntry,
  ];

  const sortedKeys = useMemo(() => {
    const arr = [...apiKeys];
    arr.sort((a, b) => {
      const av = a[sort.id] ?? "";
      const bv = b[sort.id] ?? "";
      return sort.desc ? bv.localeCompare(av) : av.localeCompare(bv);
    });
    return arr;
  }, [apiKeys, sort]);

  return (
    <Flex direction="col" gap="2">
      <Flex>
        <Dropdown items={sortDropdownItems} size="small">
          <span className="inline-flex items-center gap-1">
            <Sort />
            Sort: {activeSortLabel}
          </span>
        </Dropdown>
      </Flex>
      <TableRoot className="w-full">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Description</TableHeaderCell>
            <TableHeaderCell className="max-w-[300px]">API Key</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
            <TableHeaderCell>Created</TableHeaderCell>
            <TableHeaderCell>Expires</TableHeaderCell>
            <TableHeaderCell className="w-[80px]" />
          </TableRow>
        </TableHead>
        <TableBody>
          {sortedKeys.map((key) => (
            <TableRow key={key.id ?? `${key.value}-${key.createdAt}`}>
              <TableDataCell>
                <div className="max-w-xs">
                  <Text className="break-words whitespace-normal">
                    {key.description}
                  </Text>
                </div>
              </TableDataCell>
              <TableDataCell className="max-w-[300px]">
                <Flex align="center" gap="1" wrap="wrap">
                  <Text fontFamily="mono" className="text-12 break-all">
                    {key.value}
                  </Text>
                </Flex>
              </TableDataCell>
              <TableDataCell>
                {key.status === v1ApiKeyStatus.API_KEY_STATUS_ACTIVE ? (
                  <Badge color="green">Active</Badge>
                ) : (
                  <Badge color="red">Expired</Badge>
                )}
              </TableDataCell>
              <TableDataCell>
                <Text className="text-12 whitespace-nowrap">
                  {formatDate(key.createdAt)}
                </Text>
              </TableDataCell>
              <TableDataCell>
                <Text className="text-12 whitespace-nowrap">
                  {formatDate(key.expiresAt)}
                </Text>
              </TableDataCell>
              <TableDataCell className="w-[80px] whitespace-nowrap text-right">
                <Tooltip slotContent="Delete API Key">
                  <Button
                    kind="tertiary"
                    size="small"
                    color="danger"
                    onClick={() => {
                      setSelectedApiKey(key);
                      setShowDeleteModal(true);
                    }}
                    aria-label="Delete API Key"
                  >
                    <Trash />
                  </Button>
                </Tooltip>
              </TableDataCell>
            </TableRow>
          ))}
        </TableBody>
      </TableRoot>
    </Flex>
  );
}
