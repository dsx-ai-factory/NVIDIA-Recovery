// @vitest-environment jsdom

// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ExpandableLogFileList } from "./ExpandableLogFileList";

describe("log file paths", () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(() => {
    flushSync(() => root.unmount());
    container.remove();
  });

  function render(paths: string[]) {
    flushSync(() => root.render(<ExpandableLogFileList paths={paths} />));
  }

  function visiblePaths() {
    return Array.from(
      container.querySelectorAll("li"),
      (item) => item.textContent,
    );
  }

  it("shows a placeholder when no paths are available", () => {
    render([]);
    expect(container.textContent).toBe("-");
    expect(container.querySelector("button")).toBeNull();
  });

  it.each([
    1, 2, 5,
  ])("shows all %i paths without an expansion control", (count) => {
    const paths = Array.from(
      { length: count },
      (_, index) => `/logs/job-${index}.out`,
    );
    render(paths);
    expect(visiblePaths()).toEqual(paths);
    expect(container.querySelector("button")).toBeNull();
  });

  it("limits long lists to five paths and lets users expand and collapse them", () => {
    const firstFive = [
      "/logs/stdout",
      "/logs/stderr",
      "/logs/rank-0",
      "/logs/rank-1",
      "/logs/rank-2",
    ];
    const paths = [...firstFive, "/logs/rank-3", "/logs/rank-4"];
    render(paths);
    const toggle = container.querySelector("button")!;
    expect(visiblePaths()).toEqual(firstFive);
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(
      document.getElementById(toggle.getAttribute("aria-controls")!),
    ).not.toBeNull();

    flushSync(() => toggle.click());
    expect(visiblePaths()).toEqual(paths);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");

    flushSync(() => toggle.click());
    expect(visiblePaths()).toEqual(firstFive);
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
  });
});
