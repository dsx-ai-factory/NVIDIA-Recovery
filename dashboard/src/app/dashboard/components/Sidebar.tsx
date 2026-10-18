// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Badge, Button, Tooltip } from "@nvidia/foundations-react-core";
import {
  Bell,
  ChatSingle,
  LayoutRows,
  Megaphone,
  MirrorVertical,
  Workspace,
} from "@nvidia/react-gui-icons/icons";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

interface SidebarLinkItem {
  icon: ReactNode;
  children: ReactNode;
  linkHref: string;
  linkActive: boolean;
  target?: string;
}

type SidebarLinkItemProps = SidebarLinkItem & {
  collapsed: boolean;
};

function SidebarLinkItem({
  icon,
  children,
  linkHref,
  linkActive,
  target = "_self",
  collapsed,
}: SidebarLinkItemProps) {
  return (
    <Tooltip
      slotContent={children}
      style={{ visibility: collapsed ? "visible" : "hidden" }}
      side="right"
    >
      <Link
        href={linkHref}
        className="relative block w-full"
        target={target}
        rel={target === "_blank" ? "noopener noreferrer" : undefined}
      >
        {linkActive && (
          <div className="bg-brand absolute top-0 left-0 z-10 h-full w-1" />
        )}
        <Button
          kind="tertiary"
          color="neutral"
          className="!w-full !justify-start !gap-4 !overflow-hidden !rounded-none !pl-4 !whitespace-nowrap"
        >
          {icon} {children}
        </Button>
      </Link>
    </Tooltip>
  );
}

export function Sidebar({
  agentWebUrl,
  isJobNodeAnalysisEnabled,
  isJobGroupsEnabled,
  isAuditEventsEnabled,
  collapsed,
}: {
  agentWebUrl?: string;
  isJobNodeAnalysisEnabled: boolean;
  isJobGroupsEnabled: boolean;
  isAuditEventsEnabled: boolean;
  collapsed: boolean;
}) {
  const pathname = usePathname();
  const sidebarLinkItems: SidebarLinkItem[] = [
    {
      icon: <Workspace />,
      children: "Workflows Overview",
      linkHref: "/dashboard/workflow",
      linkActive: pathname?.startsWith("/dashboard/workflow"),
    },
    ...(isJobGroupsEnabled
      ? [
          {
            icon: <LayoutRows />,
            children: <>Job Groups</>,
            linkHref: "/dashboard/job-groups",
            linkActive: pathname?.startsWith("/dashboard/job-groups"),
          },
        ]
      : []),
    ...(isAuditEventsEnabled
      ? [
          {
            icon: <Bell />,
            children: <>Audit Events</>,
            linkHref: "/dashboard/audit-events",
            linkActive: pathname?.startsWith("/dashboard/audit-events"),
          },
        ]
      : []),
    {
      icon: <Megaphone />,
      children: "Notifications",
      linkHref: "/dashboard/notifications",
      linkActive: pathname?.startsWith("/dashboard/notifications"),
    },
    ...(agentWebUrl
      ? [
          {
            icon: <ChatSingle />,
            children: (
              <>
                Agent <Badge kind="solid">Beta</Badge>
              </>
            ),
            linkHref: agentWebUrl,
            linkActive: false,
            target: "_blank",
          },
        ]
      : []),
    ...(isJobNodeAnalysisEnabled
      ? [
          {
            icon: <MirrorVertical />,
            children: <>Job Node Analysis</>,
            linkHref: "/dashboard/job-node-analysis",
            linkActive: pathname?.startsWith("/dashboard/job-node-analysis"),
          },
        ]
      : []),
  ];

  return (
    <div>
      {sidebarLinkItems.map((item) => (
        <SidebarLinkItem key={item.linkHref} {...item} collapsed={collapsed} />
      ))}
    </div>
  );
}
