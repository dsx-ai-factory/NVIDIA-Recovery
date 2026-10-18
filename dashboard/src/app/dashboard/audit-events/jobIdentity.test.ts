// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import {
  formatJobAttempt,
  generateHeimdallJobId,
  JOB_ATTEMPT_FORMAT_ERROR,
  jobAttemptValidationError,
  parseAttemptIndex,
  parseJobAttempt,
} from "./jobIdentity";

describe("audit job identity", () => {
  it("parses and formats job attempt notation", () => {
    expect(parseJobAttempt(" 12345/2 ")).toEqual({
      jobId: "12345",
      attemptIndex: 2,
    });
    expect(parseJobAttempt("job/with/slash/0")).toEqual({
      jobId: "job/with/slash",
      attemptIndex: 0,
    });
    expect(formatJobAttempt("12345", 2)).toBe("12345/2");
    expect(formatJobAttempt("12345", 0)).toBe("12345/0");
  });

  it("keeps job-only filters across all attempts", () => {
    expect(parseJobAttempt(" 12345 ")).toEqual({ jobId: "12345" });
    expect(formatJobAttempt("12345", undefined)).toBe("12345");
    expect(formatJobAttempt("12345", null)).toBe("12345");
  });

  it.each([
    "",
    " ",
    "/2",
    "12345/",
    "12345/-1",
    "12345/1.5",
    "12345/1e2",
    "12345/9007199254740992",
  ])("rejects invalid notation %j", (value) => {
    expect(parseJobAttempt(value)).toBeUndefined();
  });

  it.each([
    ["0", 0],
    ["2", 2],
    ["0002", 2],
  ])("parses decimal attempt index %j", (value, expected) => {
    expect(parseAttemptIndex(value)).toBe(expected);
  });

  it.each([
    undefined,
    null,
    "",
    " ",
    "-1",
    "1.5",
    "1e2",
    "9007199254740992",
  ])("rejects malformed attempt index %j", (value) => {
    expect(parseAttemptIndex(value)).toBeUndefined();
  });

  it("rejects repeated attempt parameters", () => {
    expect(parseAttemptIndex(["2"])).toBeUndefined();
  });

  it("returns a clear validation error only for malformed non-empty notation", () => {
    expect(jobAttemptValidationError("")).toBeUndefined();
    expect(jobAttemptValidationError("12345/2")).toBeUndefined();
    expect(jobAttemptValidationError("12345")).toBeUndefined();
    expect(jobAttemptValidationError("12345/nope")).toBe(
      JOB_ATTEMPT_FORMAT_ERROR,
    );
  });

  it("matches the shared Python and Go canonical identity", async () => {
    await expect(generateHeimdallJobId("12345", 2)).resolves.toBe(
      "7afd1bcf-df8f-5b70-a845-571e8b2e7e16",
    );
    await expect(generateHeimdallJobId("job/with/slash", 0)).resolves.toBe(
      "ebefc021-2021-5fd1-8cf1-6db91d3f6bc0",
    );
  });
});
