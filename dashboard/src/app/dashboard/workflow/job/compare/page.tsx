// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";
import { CompareTable } from "./components/CompareTable";

export default async function ComparePage(props: {
  searchParams?: Promise<{
    jobs?: string;
  }>;
}) {
  const searchParams = await props.searchParams;
  const jobCompoundKeys = (searchParams?.jobs ?? "")
    .split(",")
    .map((jobId) => jobId.trim())
    .filter((id) => !!id);

  if (jobCompoundKeys.length === 0) {
    redirect(withBasePath("/dashboard/workflow", process.env.BASE_PATH));
  }

  return <CompareTable jobCompoundKeys={jobCompoundKeys} />;
}
