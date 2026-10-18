// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterAll, describe, expect, it, vi } from "vitest";
import type { NotificationPublic } from "~/gen/notifications/types";

// Next.js embeds this value from next.config.js when building Link.
vi.hoisted(() => {
  vi.stubEnv("__NEXT_ROUTER_BASEPATH", "/mission-control/recovery-engine");
});
afterAll(() => vi.unstubAllEnvs());

vi.mock("@nvidia/foundations-react-core", () => {
  const Container = ({ children }: { children: ReactNode }) => (
    <div>{children}</div>
  );
  return {
    Button: Container,
    NotificationContent: Container,
    NotificationFooter: Container,
    NotificationHeader: Container,
    NotificationHeading: Container,
    NotificationIcon: () => null,
    NotificationRoot: Container,
    NotificationSubheading: Container,
  };
});
vi.mock("~/components/utils/Datetime", () => ({ Datetime: () => null }));
vi.mock(
  "~/app/dashboard/workflow/utils/links",
  () => import("../../workflow/utils/links"),
);

import { NotificationCard } from "./NotificationCard";

const jobData = {
  template_id: "/templates/JobRequeued",
  job_id: "456",
  job_attempt_index: 2,
  job_name: "training",
  event_type: "CRASH",
  user: "alice",
};

function renderNotification(templateData: unknown): string {
  return renderToStaticMarkup(
    <NotificationCard
      notification={{
        recipient: "alice",
        title: "Stored notification title",
        message: "Stored notification body",
        type: "info",
        template_data: templateData as NotificationPublic["template_data"],
      }}
    />,
  );
}

describe("notification job links", () => {
  it.each([
    0, 2,
  ])("links target attempt %i under the configured dashboard base path", (attemptIndex) => {
    const html = renderNotification({
      ...jobData,
      job_attempt_index: attemptIndex,
    });

    expect(html).toContain(
      `href="/mission-control/recovery-engine/dashboard/workflow/job/456/${attemptIndex}/details"`,
    );
    expect(html).toContain("View Job Details");
    expect(html).toContain("Stored notification body");
  });

  it("encodes job identity as one route segment", () => {
    const html = renderNotification({ ...jobData, job_id: "job/1?#" });

    expect(html).toContain(
      'href="/mission-control/recovery-engine/dashboard/workflow/job/job%2F1%3F%23/2/details"',
    );
  });

  it.each([
    undefined,
    null,
    "not structured data",
    {},
    { template_id: "/partials/GlobalEvent", error_attribution: "global" },
    { ...jobData, job_id: undefined },
    { ...jobData, job_id: "" },
    { ...jobData, job_id: "   " },
    { ...jobData, job_id: "." },
    { ...jobData, job_id: ".." },
    { ...jobData, job_id: 456 },
    { ...jobData, job_attempt_index: undefined },
    { ...jobData, job_attempt_index: null },
    { ...jobData, job_attempt_index: "0" },
    { ...jobData, job_attempt_index: -1 },
    { ...jobData, job_attempt_index: 0.5 },
    { ...jobData, job_attempt_index: Number.NaN },
    { ...jobData, job_attempt_index: Number.MAX_SAFE_INTEGER + 1 },
  ])("keeps notifications without a valid target readable: %j", (data) => {
    const html = renderNotification(data);

    expect(html).toContain("Stored notification title");
    expect(html).toContain("Stored notification body");
    expect(html).not.toContain("View Job Details");
    expect(html).not.toContain("href=");
  });
});
