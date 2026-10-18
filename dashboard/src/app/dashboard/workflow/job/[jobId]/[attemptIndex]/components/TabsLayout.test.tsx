// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const navigation = vi.hoisted(() => ({
  pathname: "",
  push: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  usePathname: () => navigation.pathname,
  useRouter: () => ({ push: navigation.push }),
}));

vi.mock("@nvidia/foundations-react-core", () => ({
  Tabs: ({
    items,
    value,
  }: {
    items: Array<{ children: string; value: string }>;
    value: string;
  }) => (
    <div data-active-tab={value}>
      {items.map((item) => (
        <span key={item.value}>{item.children}</span>
      ))}
    </div>
  ),
}));

import { TabsLayout } from "./TabsLayout";

function renderTabs(auditEventsEnabled: boolean): string {
  return renderToStaticMarkup(
    <TabsLayout
      jobId="42"
      attemptIndex="3"
      auditEventsEnabled={auditEventsEnabled}
    >
      <div>Tab content</div>
    </TabsLayout>,
  );
}

describe("job tabs", () => {
  beforeEach(() => {
    navigation.pathname = "/dashboard/workflow/job/42/3/details";
  });

  it("shows and selects the Timeline tab on the timeline route", () => {
    navigation.pathname = "/dashboard/workflow/job/42/3/timeline";

    const html = renderTabs(true);

    expect(html).toContain('data-active-tab="timeline"');
    expect(html).toContain("Job Details");
    expect(html).toContain("Timeline");
  });

  it("keeps Job Details selected on the details route", () => {
    const html = renderTabs(true);

    expect(html).toContain('data-active-tab="details"');
  });

  it("hides Timeline when audit events are disabled", () => {
    const html = renderTabs(false);

    expect(html).toContain("Job Details");
    expect(html).not.toContain("Timeline");
  });
});
