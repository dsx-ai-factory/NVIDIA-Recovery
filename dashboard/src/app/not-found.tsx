// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Link from "next/link";
import { env } from "~/env";
import { getAuthConfiguration } from "~/utils/auth";
import {
  isApiKeysEnabled,
  isAuditEventsEnabled,
  isJobGroupsEnabled,
  isJobNodeAnalysisEnabled,
} from "~/utils/config";
import { DashboardContainer } from "./dashboard/components/DashboardContainer";

export const dynamic = "force-dynamic";

export default async function NotFound() {
  const authAvailable = getAuthConfiguration().available;

  return (
    <DashboardContainer
      title={env.COCKPIT_NAME ?? "NVIDIA Recovery"}
      authAvailable={authAvailable}
      isApiKeysEnabled={authAvailable && isApiKeysEnabled()}
      isJobNodeAnalysisEnabled={isJobNodeAnalysisEnabled()}
      isJobGroupsEnabled={isJobGroupsEnabled()}
      isAuditEventsEnabled={isAuditEventsEnabled()}
    >
      <div className="flex h-full flex-col items-center justify-center gap-2">
        <h1 className="leading-heading text-18 text-primary font-medium">
          404 | Not found
        </h1>
        <p className="leading-body text-14 text-primary text-center">
          The page you are looking for does not exist. Please check the URL or
          click on the link below to go back to the home page.
        </p>
        <Link className="underline" href="/">
          Go back to home page
        </Link>
      </div>
    </DashboardContainer>
  );
}
