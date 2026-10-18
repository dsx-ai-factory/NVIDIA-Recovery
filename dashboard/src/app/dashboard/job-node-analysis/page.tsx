// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import { redirect } from "next/navigation";
import { withBasePath } from "~/lib/base-path";
import { isJobNodeAnalysisEnabled } from "~/utils/config";
import { NodeAnalysisPageContent } from "./components/PageContent";
import { NodeAnalysisHeader } from "./components/PageHeader";

export default async function JobsNodeAnalysisPage() {
  if (!isJobNodeAnalysisEnabled()) {
    return redirect(withBasePath("/dashboard/workflow", process.env.BASE_PATH));
  }

  return (
    <div className="bg-surface-sunken h-full">
      <NodeAnalysisHeader />
      <NodeAnalysisPageContent />
    </div>
  );
}
