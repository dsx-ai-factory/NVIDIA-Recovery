// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  usePathname: () => "/dashboard/workflow",
}));
vi.mock("@nvidia/react-gui-icons/icons", () => ({
  Bell: () => null,
  ChatSingle: () => null,
  LayoutRows: () => null,
  Megaphone: () => null,
  MirrorVertical: () => null,
  Workspace: () => null,
}));

import { Sidebar } from "./Sidebar";

function sidebarMarkup(
  agentWebUrl?: string,
  auditEventsEnabled = true,
): string {
  return renderToStaticMarkup(
    <Sidebar
      agentWebUrl={agentWebUrl}
      collapsed={false}
      isAuditEventsEnabled={auditEventsEnabled}
      isJobGroupsEnabled={false}
      isJobNodeAnalysisEnabled={false}
    />,
  );
}

describe("Agent sidebar navigation", () => {
  it("opens the configured Agent frontend in a new tab", () => {
    const html = sidebarMarkup("https://agent.example.test/base");

    expect(html).toContain("Agent");
    expect(html).toContain('href="https://agent.example.test/base"');
    expect(html).toContain('target="_blank"');
    expect(html).toContain('rel="noopener noreferrer"');
    expect(html).toContain("Beta");
  });

  it("hides the entry when the Agent frontend is not configured", () => {
    expect(sidebarMarkup()).not.toContain("Agent");
  });
});

describe("audit sidebar navigation", () => {
  it("links to audit events when the audit UI is enabled", () => {
    const html = sidebarMarkup();

    expect(html).toContain("Audit Events");
    expect(html).toContain('href="/dashboard/audit-events"');
  });

  it("hides audit events when the audit UI is disabled", () => {
    const html = sidebarMarkup(undefined, false);

    expect(html).not.toContain("Audit Events");
    expect(html).not.toContain('href="/dashboard/audit-events"');
    expect(html).toContain("Notifications");
  });
});
