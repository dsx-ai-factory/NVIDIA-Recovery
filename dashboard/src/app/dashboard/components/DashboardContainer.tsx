// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { AppBar, Button, Text } from "@nvidia/foundations-react-core";
import { Menu } from "@nvidia/react-gui-icons/icons";
import type React from "react";
import { useState } from "react";
import { HorizontalLogo } from "~/components/HorizontalLogo";
import { Sidebar } from "./Sidebar";
import { UserDropdown } from "./UserDropdown";
import {
  APP_BAR_HEIGHT,
  SIDEBAR_WIDTH,
  SIDEBAR_WIDTH_COLLAPSED,
} from "./utils";

export function DashboardContainer({
  title,
  children,
  authAvailable,
  isApiKeysEnabled,
  agentWebUrl,
  isJobNodeAnalysisEnabled,
  isJobGroupsEnabled,
  isAuditEventsEnabled,
}: {
  title: string;
  children: React.ReactNode;
  authAvailable: boolean;
  isApiKeysEnabled: boolean;
  agentWebUrl?: string;
  isJobNodeAnalysisEnabled: boolean;
  isJobGroupsEnabled: boolean;
  isAuditEventsEnabled: boolean;
}) {
  const [collapsed, setCollapsed] = useState(false);
  return (
    <>
      <AppBar
        className="sticky top-0 z-[100]"
        slotStart={
          <>
            <Button
              kind="tertiary"
              color="neutral"
              size="small"
              onClick={() => setCollapsed(!collapsed)}
            >
              <Menu />
            </Button>
            <HorizontalLogo className="h-[16px] w-auto" />
            <Text kind="label/bold/md" className="!mr-4">
              {title}
            </Text>
          </>
        }
        slotEnd={
          <UserDropdown
            authAvailable={authAvailable}
            isApiKeysEnabled={isApiKeysEnabled}
          />
        }
      />
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "min-content 1fr",
          minHeight: `calc(100vh - ${APP_BAR_HEIGHT}px)`,
        }}
      >
        <div
          style={{
            position: "sticky",
            top: APP_BAR_HEIGHT,
            left: 0,
            bottom: 0,
            alignSelf: "start",
            width: collapsed ? SIDEBAR_WIDTH_COLLAPSED : SIDEBAR_WIDTH,
            transition: "width 300ms cubic-bezier(0.23, 1, 0.32, 1)",
            minHeight: `calc(100vh - ${APP_BAR_HEIGHT}px)`,
          }}
          className="border-base bg-surface-navigation border-r"
        >
          <Sidebar
            collapsed={collapsed}
            agentWebUrl={agentWebUrl}
            isJobNodeAnalysisEnabled={isJobNodeAnalysisEnabled}
            isJobGroupsEnabled={isJobGroupsEnabled}
            isAuditEventsEnabled={isAuditEventsEnabled}
          />
        </div>
        <div className="min-w-0">{children}</div>
      </div>
    </>
  );
}
