// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

vi.mock("@nvidia/foundations-react-core", () => ({
  Tooltip: ({
    children,
    slotContent,
  }: {
    children: ReactNode;
    slotContent: string;
  }) => <div title={slotContent}>{children}</div>,
}));
vi.mock("@nvidia/react-gui-icons/icons", () => ({
  Warning: () => <span role="img" aria-label="Anomalies" />,
  InfoCircle: () => <span role="img" aria-label="Job reason" />,
  IconError: () => <span role="img" aria-label="Attributions" />,
}));
vi.mock("~/const/slurm_job", () => import("../../const/slurm_job"));
vi.mock("~/provider/userPreferencesProvider", () => ({}));
vi.mock("~/utils/time", () => ({}));
vi.mock("./Datetime", () => ({ Datetime: () => null }));

import { ApsRenderAlertBadges as CompactAlertBadges } from "./ApsAlertBadges";
import { ApsRenderAlertBadges as WorkflowAlertBadges } from "./apsJobRender";

// A response from an older server can still contain the retired enrichment.
const anomalies = [
  {
    anomaly_type: "CRASH",
    timestamp: "2026-10-08T00:00:00Z",
    node_events: [{ error_attribution: "GPU failure" }],
    global_events: [{ error_attribution: "Network failure" }],
  },
];

describe.each([
  ["compact", CompactAlertBadges],
  ["workflow", WorkflowAlertBadges],
] as const)("%s job alert badges", (_name, AlertBadges) => {
  it("shows the anomaly count without an attribution badge for legacy responses", () => {
    const html = renderToStaticMarkup(
      <AlertBadges anomalies={anomalies} reason={undefined} />,
    );

    expect(html).toContain('aria-label="Anomalies"');
    expect(html).toContain('title="Crash"');
    expect(html).toMatch(/>1</);
    expect(html).not.toContain('aria-label="Attributions"');
    expect(html).not.toContain("GPU failure");
    expect(html).not.toContain("Network failure");
  });

  it("renders anomalies without embedded attribution records", () => {
    const html = renderToStaticMarkup(
      <AlertBadges
        anomalies={[
          { anomaly_type: "CRASH", timestamp: "2026-10-08T00:00:00Z" },
          { anomaly_type: "HANG", timestamp: "2026-10-08T01:00:00Z" },
        ]}
        reason={undefined}
      />,
    );

    expect(html).toContain('title="Crash, Hang"');
    expect(html).toMatch(/>2</);
  });

  it("keeps scheduler reasons ahead of anomaly badges", () => {
    const html = renderToStaticMarkup(
      <AlertBadges anomalies={anomalies} reason="JobHeldAdmin" />,
    );

    expect(html).toContain('aria-label="Job reason"');
    expect(html).toContain('title="JobHeldAdmin"');
    expect(html).not.toContain('aria-label="Anomalies"');
  });

  it("renders no badges when the job has no anomalies or reason", () => {
    expect(
      renderToStaticMarkup(
        <AlertBadges anomalies={undefined} reason={undefined} />,
      ),
    ).toBe("");
  });
});
