// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import Link from "next/link";
import type { ReactNode } from "react";
import { PageLayout } from "~/app/dashboard/components/PageLayout";
import { isAuditEventsEnabled } from "~/utils/config";
import { AttributionsBanner } from "./components/AttributionsBanner";
import { TabsLayout } from "./components/TabsLayout";

export default async function JobLayout({
  params,
  children,
}: {
  params: Promise<{ jobId: string; attemptIndex: string }>;
  children: ReactNode;
}) {
  const { jobId, attemptIndex } = await params;
  const auditEventsEnabled = isAuditEventsEnabled();

  return (
    <PageLayout
      breadcrumbs={[
        {
          children: <Link href="/dashboard/workflow">Workflows Overview</Link>,
        },
        { children: `Job ${jobId} (${attemptIndex})` },
      ]}
      title={`Job ${jobId} (${attemptIndex})`}
    >
      <div className="flex flex-col gap-2">
        <AttributionsBanner jobId={jobId} attemptIndex={attemptIndex} />
        <TabsLayout
          jobId={jobId}
          attemptIndex={attemptIndex}
          auditEventsEnabled={auditEventsEnabled}
        >
          {children}
        </TabsLayout>
      </div>
    </PageLayout>
  );
}
