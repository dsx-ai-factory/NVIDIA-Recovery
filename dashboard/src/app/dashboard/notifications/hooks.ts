// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from "@tanstack/react-query";
import { DEFAULT_AUTO_REFRESH_INTERVAL } from "~/const/statics";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { getNotifications } from "./api";

export function useGetNotifications({
  pageIndex = 0,
  pageSize = 10,
  recipient,
}: {
  pageIndex?: number;
  pageSize?: number;
  recipient?: string;
} = {}) {
  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  return useQuery({
    queryKey: ["notifications", { pageIndex, pageSize, recipient }],
    queryFn: () =>
      getNotifications({
        offset: pageIndex * pageSize,
        limit: pageSize,
        recipient,
      }),
    refetchInterval: autoRefreshEnabled
      ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
      : false,
  });
}
