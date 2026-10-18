// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Breadcrumbs, PageHeader } from "@nvidia/foundations-react-core";
import Link from "next/link";

export function NodeAnalysisHeader() {
  return (
    <PageHeader
      className="border-base bg-surface-navigation border-b px-8 py-3"
      slotHeading="Job Node Analysis"
      slotBreadcrumbs={
        <Breadcrumbs
          items={[
            {
              children: (
                <Link href={"/dashboard/workflow"}>NVIDIA Recovery</Link>
              ),
            },
            {
              children: (
                <Link href={`/dashboard/job-node-analysis`}>
                  Job Node Analysis
                </Link>
              ),
            },
          ]}
        />
      }
    />
  );
}
