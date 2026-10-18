// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Flex } from "@nvidia/foundations-react-core";
import type { GetJobsJobNameMatchKey } from "~/gen/job-manager/types";

import { isJobGroupsEnabled } from "~/utils/config";
import { PageLayout } from "../components/PageLayout";
import { WorkflowsTable } from "./components/WorkflowsTable";

export default async function WorkflowPage(props: {
  searchParams?: Promise<{
    query?: string;
    page?: string;
    sortBy?: string;
    jobNameMatch?: GetJobsJobNameMatchKey;
    jobId?: string;
    jobGroupId?: string;
    state?: string;
    user?: string;
  }>;
}) {
  const searchParams = (await props.searchParams) ?? {};
  const {
    query,
    page,
    sortBy,
    jobId,
    jobGroupId,
    state,
    user,
    jobNameMatch = "prefix",
  } = searchParams;
  const initialPage = Number(page) || undefined;

  return (
    <PageLayout
      breadcrumbs={[{ children: "Workflows Overview" }]}
      title="Workflows Overview"
    >
      <Flex direction="col" gap="2">
        <WorkflowsTable
          initialSortBy={sortBy}
          initialJobIdFilter={jobId}
          initialJobGroupIdFilter={jobGroupId}
          initialPage={initialPage}
          initialStateFilter={state}
          initialUserFilter={user}
          initialJobNameMatchFilter={jobNameMatch}
          initialQuery={query}
          jobGroupsEnabled={isJobGroupsEnabled()}
        />
      </Flex>
    </PageLayout>
  );
}
