// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { grafanaUrlSchema } from "../lib/grafanaUrlSchema.js";
import { buildGrafanaTraceUrl } from "./grafana";

describe("buildGrafanaTraceUrl", () => {
  it("uses the operator endpoint, subpath, datasource and organization", () => {
    const url = new URL(
      buildGrafanaTraceUrl(
        "trace-123",
        "tempo-local",
        "https://grafana.example.com/monitoring/",
        "42",
      ),
    );
    expect(url.origin).toBe("https://grafana.example.com");
    expect(url.pathname).toBe("/monitoring/explore");
    expect(url.searchParams.get("orgId")).toBe("42");
    const pane = JSON.parse(url.searchParams.get("panes") ?? "{}").otm;
    expect(pane.datasource).toBe("tempo-local");
    expect(pane.queries[0].query).toBe("trace-123");
  });
  it("does not select an organization unless configured", () => {
    const url = new URL(
      buildGrafanaTraceUrl("trace", "tempo", "http://localhost:3000"),
    );
    expect(url.searchParams.has("orgId")).toBe(false);
  });
  it("does not create links without valid endpoint and datasource configuration", () => {
    expect(buildGrafanaTraceUrl("trace", "tempo", undefined)).toBe("");
    expect(
      buildGrafanaTraceUrl("trace", undefined, "https://grafana.example.com"),
    ).toBe("");
    expect(buildGrafanaTraceUrl("trace", "tempo", "invalid")).toBe("");
    expect(buildGrafanaTraceUrl("trace", "tempo", "javascript:alert(1)")).toBe(
      "",
    );
  });
});

describe("Grafana configuration", () => {
  it.each([
    "https://user:password@grafana.example.com",
    "https://user@grafana.example.com",
    "https://:password@grafana.example.com",
  ])("rejects credentials before exposing configuration or links: %s", (value) => {
    expect(grafanaUrlSchema.optional().safeParse(value).success).toBe(false);
    expect(buildGrafanaTraceUrl("trace", "tempo", value)).toBe("");
  });
  it("accepts absent configuration and credential-free HTTP endpoints", () => {
    expect(grafanaUrlSchema.optional().safeParse(undefined).success).toBe(true);
    expect(grafanaUrlSchema.safeParse("http://localhost:3000").success).toBe(
      true,
    );
    expect(
      grafanaUrlSchema.safeParse("https://grafana.example.com/monitoring")
        .success,
    ).toBe(true);
  });
});
