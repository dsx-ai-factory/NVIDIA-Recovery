// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

vi.mock(
  "~/gen/notifications/types",
  () => import("../../../../gen/notifications/types/AuditEventLevel"),
);

vi.mock("@nvidia/foundations-react-core", () => {
  const Container = ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  );
  return {
    Button: ({
      children,
      disabled,
    }: {
      children: React.ReactNode;
      disabled?: boolean;
    }) => (
      <button type="button" disabled={disabled}>
        {children}
      </button>
    ),
    Card: Container,
    Flex: Container,
    Text: Container,
    Select: () => null,
    TextInput: ({ id, value }: { id: string; value: string }) => (
      <input id={id} value={value} readOnly />
    ),
  };
});

import { FiltersCard } from "./FiltersCard";

describe("audit filters", () => {
  it.each([
    "12345",
    "job/2",
    "job/with/slash",
  ])("preserves job-only deep link %j without inventing an attempt identity", (jobId) => {
    const html = renderToStaticMarkup(
      <FiltersCard
        filters={{ job_id: jobId, event_type: "job_attempt_observed" }}
        onApply={vi.fn()}
        expanded
        onExpandedChange={vi.fn()}
        isFetching={false}
      />,
    );

    expect(html).toContain(`value="${jobId}"`);
    expect(html).toContain('value="job_attempt_observed"');
    expect(html).not.toContain("Heimdall Job ID");
    expect(html).not.toContain("disabled");
  });
});
