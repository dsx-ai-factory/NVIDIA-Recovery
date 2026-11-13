// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  usePathname: () => "/dashboard/workflow",
}));
vi.mock("~/components/HorizontalLogo", () => ({
  HorizontalLogo: () => null,
}));
vi.mock("~/env", () => ({
  env: {
    AGENT_WEB_URL: "https://agent.example.test/base",
    COCKPIT_NAME: "NVIDIA Recovery",
    AUDIT_TRACE_URL_TEMPLATE: "https://traces.example.test/{trace_id}",
  },
}));
vi.mock("@nvidia/react-gui-icons/icons", () => ({
  Bell: () => null,
  ChatSingle: () => null,
  LayoutRows: () => null,
  Megaphone: () => null,
  Menu: () => null,
  MirrorVertical: () => null,
  Workspace: () => null,
}));
vi.mock("~/utils/auth", () => ({
  getAuthConfiguration: () => ({ available: false }),
}));
vi.mock("~/utils/config", async () =>
  vi.importActual<typeof import("../../utils/config")>("../../utils/config"),
);
vi.mock("./components/UserDropdown", () => ({
  UserDropdown: () => null,
}));

import { AuditTraceLink } from "./audit-events/components/AuditTraceLink";
import DashboardLayout from "./layout";

describe("Dashboard layout Agent navigation", () => {
  it("propagates the configured Agent URL to the sidebar", async () => {
    const html = renderToStaticMarkup(
      await DashboardLayout({ children: <main>Dashboard</main> }),
    );

    expect(html).toContain("Agent");
    expect(html).toContain('href="https://agent.example.test/base"');
    expect(html).toContain('target="_blank"');
  });
});

it("propagates runtime trace navigation configuration to audit content", async () => {
  const trace_id = "0123456789abcdef0123456789abcdef";
  const html = renderToStaticMarkup(
    await DashboardLayout({
      children: <AuditTraceLink payload={{ trace_id }} />,
    }),
  );
  expect(html).toContain(`href="https://traces.example.test/${trace_id}"`);
});
