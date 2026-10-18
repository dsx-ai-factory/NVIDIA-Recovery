// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Flex,
  Select,
  Spinner,
  Text,
} from "@nvidia/foundations-react-core";

const PAGE_SIZE_OPTIONS = ["10", "25", "100"];

export function PaginationControls({
  limit,
  onLimitChange,
  prevCursor,
  nextCursor,
  isFetching,
  onPrev,
  onNext,
  showLimitSelector = true,
}: {
  limit: number;
  onLimitChange?: (limit: number) => void;
  prevCursor?: string;
  nextCursor?: string;
  isFetching: boolean;
  onPrev: (cursor: string) => void;
  onNext: (cursor: string) => void;
  showLimitSelector?: boolean;
}) {
  return (
    <Flex direction="row" justify="between" align="center" className="pt-4">
      {showLimitSelector ? (
        <Flex direction="row" align="center" gap="2">
          <Text kind="body/regular/md" color="" className="whitespace-nowrap">
            Items per page
          </Text>
          <Select
            items={PAGE_SIZE_OPTIONS}
            value={String(limit)}
            onValueChange={(value) => onLimitChange?.(Number(value))}
          />
        </Flex>
      ) : (
        <div />
      )}
      <Flex direction="row" align="center" gap="2">
        {isFetching && <Spinner size="small" aria-label="Loading" />}
        <Button
          kind="secondary"
          size="small"
          disabled={isFetching || !prevCursor}
          onClick={() => prevCursor && onPrev(prevCursor)}
        >
          Previous
        </Button>
        <Button
          kind="secondary"
          size="small"
          disabled={isFetching || !nextCursor}
          onClick={() => nextCursor && onNext(nextCursor)}
        >
          Next
        </Button>
      </Flex>
    </Flex>
  );
}
