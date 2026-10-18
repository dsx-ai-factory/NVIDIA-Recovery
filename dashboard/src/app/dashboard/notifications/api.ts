// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { toQueryParamString } from "~/app/api/utils";
import type { ItemsNotificationPublic } from "~/gen/notifications/types";
import { withBasePath } from "~/lib/base-path";

export async function getNotifications(params: {
  id?: string;
  recipient?: string;
  limit?: number;
  offset?: number;
  createdAfter?: string;
  createdBefore?: string;
}): Promise<ItemsNotificationPublic> {
  const route = withBasePath(
    `/api/notifications?${toQueryParamString(params)}`,
  );
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<ItemsNotificationPublic>;
}
