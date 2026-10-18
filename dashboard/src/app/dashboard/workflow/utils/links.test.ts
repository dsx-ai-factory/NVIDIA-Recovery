// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { getJobLink } from "./links";

describe("job attempt links", () => {
  it("uses the job details route and supports the first attempt", () => {
    expect(getJobLink({ job_id: "456", job_attempt_index: 0 })).toEqual({
      href: "/dashboard/workflow/job/456/0/details",
      label: "456 (0)",
    });
  });

  it("encodes reserved characters without changing the displayed identity", () => {
    expect(getJobLink({ job_id: "job/1?#", job_attempt_index: 2 })).toEqual({
      href: "/dashboard/workflow/job/job%2F1%3F%23/2/details",
      label: "job/1?# (2)",
    });
  });

  it("omits the link when no job record is available", () => {
    expect(getJobLink(undefined)).toBeUndefined();
  });
});
