// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Tabs } from "@nvidia/foundations-react-core";
import { usePathname, useRouter } from "next/navigation";
import type { ReactNode } from "react";

export function TabsLayout({
  jobId,
  attemptIndex,
  auditEventsEnabled,
  children,
}: {
  jobId: string;
  attemptIndex: string;
  auditEventsEnabled: boolean;
  children: ReactNode;
}) {
  const pathname = usePathname();
  const router = useRouter();

  const basePath = `/dashboard/workflow/job/${jobId}/${attemptIndex}`;
  const active = pathname?.endsWith("/timeline") ? "timeline" : "details";

  return (
    <div className="flex flex-col gap-2">
      <Tabs
        className="[&>.nv-tabs-content]:w-full [&>.nv-tabs-content]:p-0"
        kind="primary"
        items={[
          { children: "Job Details", value: "details" },
          ...(auditEventsEnabled
            ? [{ children: "Timeline", value: "timeline" }]
            : []),
        ]}
        value={active}
        onValueChange={(value) => {
          router.push(`${basePath}/${value}`);
        }}
      />
      <div className="w-full">{children}</div>
    </div>
  );
}
