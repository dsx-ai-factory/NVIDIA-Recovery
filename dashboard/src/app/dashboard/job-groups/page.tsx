// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";
import { isJobGroupsEnabled } from "~/utils/config";
import { PageLayout } from "../components/PageLayout";
import { PageContent } from "./components/PageContent";

export default async function JobGroupsPage(props: {
  searchParams?: Promise<{
    page?: string;
    sort?: string;
    job_group_id?: string;
  }>;
}) {
  if (!isJobGroupsEnabled()) {
    return redirect(withBasePath("/dashboard/workflow", process.env.BASE_PATH));
  }
  const searchParams = (await props.searchParams) ?? {};
  const { page, sort, job_group_id } = searchParams;
  const initialPage = Number(page) || undefined;

  return (
    <PageLayout breadcrumbs={[{ children: "Job Groups" }]} title="Job Groups">
      <PageContent
        initialPage={initialPage}
        initialSort={sort}
        initialJobGroupId={job_group_id}
      />
    </PageLayout>
  );
}
