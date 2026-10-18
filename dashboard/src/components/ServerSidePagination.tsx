// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Pagination } from "@nvidia/foundations-react-core";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import {
  DEFAULT_PAGE_SIZE_OPTIONS,
  INITIAL_PAGE,
  INITIAL_PAGE_SIZE,
} from "~/utils/pagination";

interface ServerSidePaginationProps {
  page: number;
  totalItems: number;
  pageSize: number;
}

function usePersistPageStateQuery(page: number, pageSize: number) {
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    const params = new URLSearchParams();

    if (page !== INITIAL_PAGE) {
      params.set("page", page.toString());
    }
    if (pageSize !== INITIAL_PAGE_SIZE) {
      params.set("pageSize", pageSize.toString());
    }

    router.replace(
      `${pathname}${params.toString() ? `?${params.toString()}` : ""}`,
    );
  }, [page, pathname, router, pageSize]);
}

export function ServerSidePagination({
  page,
  pageSize,
  totalItems,
}: ServerSidePaginationProps) {
  const [currentPage, setCurrentPage] = useState(page);
  const [currentPageSize, setCurrentPageSize] = useState(pageSize);

  usePersistPageStateQuery(currentPage, currentPageSize);

  return (
    <Pagination
      displayControls
      page={page}
      pageSize={pageSize}
      pageSizeOptions={DEFAULT_PAGE_SIZE_OPTIONS}
      totalItems={totalItems}
      onPageChange={(page) => {
        setCurrentPage(page);
      }}
      onPageSizeChange={(newPageSize) => {
        setCurrentPageSize(newPageSize);
      }}
    />
  );
}
