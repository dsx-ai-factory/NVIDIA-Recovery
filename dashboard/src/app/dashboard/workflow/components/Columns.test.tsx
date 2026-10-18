// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

vi.mock("~/app/dashboard/workflow/utils/links", () => ({
  getJobLink: () => ({ href: "/job", label: "job" }),
}));
vi.mock("~/components/utils/ApsDuration", () => ({
  ApsDuration: () => null,
}));
vi.mock("~/components/utils/ApsRenderState", () => ({
  ApsRenderState: () => null,
}));
vi.mock("~/components/utils/apsJobRender", () => ({
  ApsRenderAlertBadges: () => null,
  ApsRenderCheckpoints: () => null,
  ApsRenderTimestamp: () => null,
}));
vi.mock("~/const/slurm_job", () => ({ SLURM_JOB_STATE: {} }));
vi.mock("~/utils/time", () => ({ CalTimeDiffInSeconds: () => 0 }));

import { type JobRow, makeColumns } from "./Columns";

function troubleshootingMarkup(
  agentAnalysis: JobRow["agent_analysis"],
  agentWebUrl?: string,
): string {
  const column = makeColumns(false, agentWebUrl).find(
    ({ id }) => id === "agent_analysis",
  );
  const row = {
    id: "42_1",
    job_id: "42",
    job_attempt_index: 1,
    agent_analysis: agentAnalysis,
  } as JobRow;

  return renderToStaticMarkup(column?.render(row));
}

describe("Agent Analysis workflow column", () => {
  it("links Agent-owned session metadata to the Agent frontend", () => {
    const html = troubleshootingMarkup(
      {
        session_id: "session/1?view=full",
        analysis_type: "root_cause",
        status: "completed",
      },
      "https://agent.example.test/base/",
    );

    expect(html).toContain("Agent Analysis");
    expect(html).toContain(
      'href="https://agent.example.test/base/agent/session%2F1%3Fview%3Dfull#final-rca"',
    );
    expect(html).toContain('target="_blank"');
  });

  it("links the latest failed group analysis", () => {
    const html = troubleshootingMarkup(
      {
        session_id: "group-session",
        analysis_type: "job_group_pattern",
        status: "failed",
      },
      "https://agent.example.test/base///",
    );

    expect(html).toContain(
      'href="https://agent.example.test/base/agent/group-session#final-rca"',
    );
  });

  it("renders no link when no matching analysis exists", () => {
    const html = troubleshootingMarkup(null, "https://agent.example.test/base");

    expect(html).toBe("--");
  });

  it("renders no link when the Agent web URL is not configured", () => {
    const html = troubleshootingMarkup({
      session_id: "session-1",
      analysis_type: "root_cause",
      status: "completed",
    });

    expect(html).toBe("--");
  });
});
