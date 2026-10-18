// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";

export default async function JobIndex({
  params,
}: {
  params: Promise<{ jobId: string; attemptIndex: string }>;
}) {
  const { jobId, attemptIndex } = await params;
  redirect(
    withBasePath(
      `/dashboard/workflow/job/${jobId}/${attemptIndex}/details`,
      process.env.BASE_PATH,
    ),
  );
}
