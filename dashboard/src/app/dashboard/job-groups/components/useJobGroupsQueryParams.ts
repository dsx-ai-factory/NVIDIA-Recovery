// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect } from "react";

export function useJobGroupsQueryParams({
  page,
  sort,
  jobGroupId,
}: {
  page: number;
  sort: string | undefined;
  jobGroupId: string | undefined;
}) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();

  useEffect(() => {
    const params = new URLSearchParams(searchParams);

    if (page === 1) {
      params.delete("page");
    } else {
      params.set("page", String(page));
    }

    if (sort === undefined) {
      params.delete("sort");
    } else {
      params.set("sort", sort);
    }

    if (!jobGroupId) {
      params.delete("job_group_id");
    } else {
      params.set("job_group_id", jobGroupId);
    }

    if (params.toString() !== searchParams.toString()) {
      router.replace(`${pathname}?${params.toString()}`);
    }
  }, [page, sort, jobGroupId, pathname, router, searchParams]);
}
