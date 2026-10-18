// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { ReactNode } from "react";
import { env } from "~/env";
import { getAuthConfiguration } from "~/utils/auth";
import {
  isApiKeysEnabled,
  isAuditEventsEnabled,
  isJobGroupsEnabled,
  isJobNodeAnalysisEnabled,
} from "~/utils/config";
import { DashboardContainer } from "./components/DashboardContainer";

export const dynamic = "force-dynamic";

export default async function DashboardLayout({
  children,
}: Readonly<{
  children: ReactNode;
}>) {
  const authAvailable = getAuthConfiguration().available;

  return (
    <DashboardContainer
      title={env.COCKPIT_NAME ?? "NVIDIA Recovery"}
      authAvailable={authAvailable}
      isApiKeysEnabled={authAvailable && isApiKeysEnabled()}
      agentWebUrl={env.AGENT_WEB_URL}
      isJobNodeAnalysisEnabled={isJobNodeAnalysisEnabled()}
      isJobGroupsEnabled={isJobGroupsEnabled()}
      isAuditEventsEnabled={isAuditEventsEnabled()}
    >
      {children}
    </DashboardContainer>
  );
}
