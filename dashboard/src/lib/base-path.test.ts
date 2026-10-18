// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { normalizeBasePath, withBasePath } from "./base-path";

describe("normalizeBasePath", () => {
  it.each([undefined, "", " ", "/"])("uses the root path for %j", (value) => {
    expect(normalizeBasePath(value)).toBe("");
  });

  it("normalizes trailing slashes", () => {
    expect(normalizeBasePath("/mission-control/recovery-engine///")).toBe(
      "/mission-control/recovery-engine",
    );
  });

  it.each([
    "dashboard",
    "https://example.test/dashboard",
    "/dashboard?x=1",
  ])("rejects invalid base path %s", (value) => {
    expect(() => normalizeBasePath(value)).toThrow();
  });
});

describe("withBasePath", () => {
  it("keeps application URLs root-relative by default", () => {
    expect(withBasePath("/dashboard/workflow", "")).toBe("/dashboard/workflow");
  });

  it("prefixes application URLs when configured", () => {
    expect(withBasePath("/api/auth", "/mission-control/recovery-engine")).toBe(
      "/mission-control/recovery-engine/api/auth",
    );
  });

  it("does not duplicate an existing configured prefix", () => {
    expect(
      withBasePath(
        "/mission-control/recovery-engine/dashboard/workflow",
        "/mission-control/recovery-engine",
      ),
    ).toBe("/mission-control/recovery-engine/dashboard/workflow");
  });

  it.each([
    "api/jobs",
    "//example.test/api/jobs",
  ])("rejects path that is not safely root-relative: %s", (path) => {
    expect(() => withBasePath(path, "/dashboard")).toThrow(
      "Application paths must start with '/'",
    );
  });
});
