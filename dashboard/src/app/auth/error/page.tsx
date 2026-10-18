// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { notFound } from "next/navigation";
import { DashboardContainer } from "~/app/dashboard/components/DashboardContainer";
import { env } from "~/env";
import { getAuthConfiguration } from "~/utils/auth";
import {
  isApiKeysEnabled,
  isAuditEventsEnabled,
  isJobGroupsEnabled,
  isJobNodeAnalysisEnabled,
} from "~/utils/config";

export const dynamic = "force-dynamic";

export default async function ErrorPage(props: {
  searchParams?: Promise<{
    error?: string;
  }>;
}) {
  if (!getAuthConfiguration().available) {
    notFound();
  }

  const parameters = await props.searchParams;
  return (
    <DashboardContainer
      title={env.COCKPIT_NAME ?? "NVIDIA Recovery"}
      authAvailable
      isApiKeysEnabled={isApiKeysEnabled()}
      isJobNodeAnalysisEnabled={isJobNodeAnalysisEnabled()}
      isJobGroupsEnabled={isJobGroupsEnabled()}
      isAuditEventsEnabled={isAuditEventsEnabled()}
    >
      <div className="flex h-full flex-col items-center justify-center gap-2">
        <h1 className="leading-heading text-18 text-primary font-medium">
          Authentication Failed
        </h1>
        <p className="leading-body text-14 text-primary text-center">
          {parameters?.error ?? "Please check the username or password"}
        </p>
      </div>
    </DashboardContainer>
  );
}
