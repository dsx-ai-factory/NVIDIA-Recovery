// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import type { V1Job } from "~/gen/api-gateway/types/V1Job";
import { getJobAttemptIndex, getJobId } from "./jobs";

const baseJob: V1Job = { workflowId: "test-workflow-id" };

describe("jobs utilities", () => {
  describe("getJobAttemptIndex", () => {
    it("should extract the second number from a valid jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-456" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(456);
    });

    it("should extract the second number from a jobRunId with leading zeros", () => {
      const job: V1Job = { ...baseJob, jobRunId: "001-002" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(2);
    });

    it("should extract the second number from a jobRunId with large numbers", () => {
      const job: V1Job = { ...baseJob, jobRunId: "999999-123456789" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(123456789);
    });

    it("should return undefined for undefined jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: undefined };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for job with no jobRunId property", () => {
      const job: V1Job = { ...baseJob };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123456" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - only hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "-" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - text with hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "abc-def" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing second number", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing first number", () => {
      const job: V1Job = { ...baseJob, jobRunId: "-456" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for empty jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: "" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for jobRunId with extra content after valid pattern", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-456-extra" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for jobRunId with extra content before valid pattern", () => {
      const job: V1Job = { ...baseJob, jobRunId: "prefix-123-456" };
      const result = getJobAttemptIndex(job);
      expect(result).toBe(undefined);
    });
  });

  describe("getJobId", () => {
    it("should extract the first number from a valid jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-456" };
      const result = getJobId(job);
      expect(result).toBe("123");
    });

    it("should extract the first number from a jobRunId with leading zeros", () => {
      const job: V1Job = { ...baseJob, jobRunId: "001-002" };
      const result = getJobId(job);
      expect(result).toBe("001");
    });

    it("should extract the first number from a jobRunId with large numbers", () => {
      const job: V1Job = { ...baseJob, jobRunId: "999999-123456789" };
      const result = getJobId(job);
      expect(result).toBe("999999");
    });

    it("should return undefined for undefined jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: undefined };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for job with no jobRunId property", () => {
      const job: V1Job = { ...baseJob };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123456" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - only hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "-" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - text with hyphen", () => {
      const job: V1Job = { ...baseJob, jobRunId: "abc-def" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing second number", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for invalid format - missing first number", () => {
      const job: V1Job = { ...baseJob, jobRunId: "-456" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for empty jobRunId", () => {
      const job: V1Job = { ...baseJob, jobRunId: "" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for jobRunId with extra content after valid pattern", () => {
      const job: V1Job = { ...baseJob, jobRunId: "123-456-extra" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for jobRunId with extra content before valid pattern", () => {
      const job: V1Job = { ...baseJob, jobRunId: "prefix-123-456" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });

    it("should return undefined for jobRunId with multiple hyphen-separated values", () => {
      const job: V1Job = { ...baseJob, jobRunId: "111-222-333-444" };
      const result = getJobId(job);
      expect(result).toBe(undefined);
    });
  });
});
