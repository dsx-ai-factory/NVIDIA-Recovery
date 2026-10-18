// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { toQueryParamString } from "~/app/api/utils";
import type {
  AuditEventsGetQuery,
  AuditEventsGetStatus200,
} from "~/gen/notifications/types";
import { withBasePath } from "~/lib/base-path";

export async function fetchAuditEvents(
  params: AuditEventsGetQuery,
): Promise<AuditEventsGetStatus200> {
  const route = withBasePath(
    `/api/audit-events?${toQueryParamString(params as Record<string, string | number | boolean | null | undefined>)}`,
  );
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<AuditEventsGetStatus200>;
}
