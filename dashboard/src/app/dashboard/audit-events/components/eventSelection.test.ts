// @vitest-environment jsdom

// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { isAuditEventRowInteraction } from "./eventSelection";

describe("audit event row interactions", () => {
  it("recognizes interactions with a row and its descendants", () => {
    const row = document.createElement("tr");
    row.dataset.auditEventRow = "";
    const cell = document.createElement("td");
    const content = document.createElement("span");
    row.append(cell);
    cell.append(content);

    expect(isAuditEventRowInteraction(row)).toBe(true);
    expect(isAuditEventRowInteraction(content)).toBe(true);
  });

  it("does not treat other outside interactions as row selections", () => {
    expect(isAuditEventRowInteraction(document.createElement("div"))).toBe(
      false,
    );
    expect(isAuditEventRowInteraction(null)).toBe(false);
  });
});
