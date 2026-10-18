// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { toQueryParamString } from "~/app/api/utils";
import type { JobGroupStatuses } from "~/gen/job-manager/types";
import { withBasePath } from "~/lib/base-path";

export async function fetchJobGroups(params: {
  jobGroupId?: string;
  user?: string;
  limit?: number;
  offset?: number;
  sortBy?: string;
}): Promise<JobGroupStatuses> {
  const route = withBasePath(`/api/job-groups?${toQueryParamString(params)}`);
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
  return response.json() as Promise<JobGroupStatuses>;
}
