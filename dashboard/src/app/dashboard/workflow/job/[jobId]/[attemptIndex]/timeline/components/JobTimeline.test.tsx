// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { notFound } from "next/navigation";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const jobState = vi.hoisted(() => ({
  data: { heimdall_job_id: null } as
    | { heimdall_job_id: string | null }
    | undefined,
  error: undefined as Error | undefined,
  isLoading: false,
}));

vi.mock("next/navigation", () => ({
  notFound: vi.fn(() => {
    throw new Error("Job not found");
  }),
}));

vi.mock("~/app/dashboard/workflow/hooks", () => ({
  useGetJob: () => jobState,
}));

vi.mock("@nvidia/foundations-react-core", () => ({
  Banner: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));

vi.mock("~/components/page/Loading", () => ({
  default: () => <div>Loading</div>,
}));

vi.mock("./JobAuditEvents", () => ({
  JobAuditEvents: ({
    heimdallJobId,
    jobId,
    attemptIndex,
  }: {
    heimdallJobId?: string;
    jobId: string;
    attemptIndex: string;
  }) => <div>Timeline for {heimdallJobId ?? `${jobId}/${attemptIndex}`}</div>,
}));

import { JobTimeline } from "./JobTimeline";

describe("JobTimeline", () => {
  beforeEach(() => {
    jobState.data = { heimdall_job_id: null };
    jobState.error = undefined;
    jobState.isLoading = false;
    vi.clearAllMocks();
  });

  it("shows a request error instead of reporting a missing job", () => {
    jobState.data = undefined;
    jobState.error = new Error("Service unavailable");

    const html = renderToStaticMarkup(
      <JobTimeline jobId="42" attemptIndex="3" />,
    );

    expect(html).toContain("Service unavailable");
    expect(notFound).not.toHaveBeenCalled();
    expect(html).not.toContain("Audit timeline is unavailable");
  });

  it("reports a missing job after a successful empty lookup", () => {
    jobState.data = undefined;

    expect(() =>
      renderToStaticMarkup(<JobTimeline jobId="42" attemptIndex="3" />),
    ).toThrow("Job not found");
    expect(notFound).toHaveBeenCalledOnce();
  });

  it("keeps the timeline visible alongside a refresh error", () => {
    jobState.data = { heimdall_job_id: "job-42" };
    jobState.error = new Error("Service unavailable");

    const html = renderToStaticMarkup(
      <JobTimeline jobId="42" attemptIndex="3" />,
    );

    expect(html).toContain("Service unavailable");
    expect(html).toContain("Timeline for job-42");
    expect(notFound).not.toHaveBeenCalled();
  });

  it("shows loading while the job lookup is pending", () => {
    jobState.data = undefined;
    jobState.isLoading = true;

    const html = renderToStaticMarkup(
      <JobTimeline jobId="42" attemptIndex="3" />,
    );

    expect(html).toContain("Loading");
    expect(notFound).not.toHaveBeenCalled();
  });

  it("falls back to the scheduler job attempt when the job has no audit identifier", () => {
    const html = renderToStaticMarkup(
      <JobTimeline jobId="42" attemptIndex="3" />,
    );

    expect(html).toContain("Timeline for 42/3");
  });
});
