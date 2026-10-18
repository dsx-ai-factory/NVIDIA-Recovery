// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from "node:fs";
import { describe, expect, expectTypeOf, it } from "vitest";
import type { V1CreateOrUpdateJobRequest } from "./gen/api-gateway/types/V1CreateOrUpdateJobRequest.ts";
import type { V1Job } from "./gen/api-gateway/types/V1Job.ts";
import type { V1Workflow } from "./gen/api-gateway/types/V1Workflow.ts";
import { heimdallServiceGetJobsQueryWorkflowIdsSchema } from "./gen/api-gateway/zod/heimdallServiceGetJobsSchema.ts";
import { heimdallServiceGetWorkflow2PathWorkflowIdSchema } from "./gen/api-gateway/zod/heimdallServiceGetWorkflow2Schema.ts";
import { heimdallServiceGetWorkflowQueryWorkflowIdSchema } from "./gen/api-gateway/zod/heimdallServiceGetWorkflowSchema.ts";
import { v1CreateOrUpdateJobRequestSchema } from "./gen/api-gateway/zod/v1CreateOrUpdateJobRequestSchema.ts";
import { v1CreateOrUpdateJobResponseSchema } from "./gen/api-gateway/zod/v1CreateOrUpdateJobResponseSchema.ts";
import { v1GetJobResponseSchema } from "./gen/api-gateway/zod/v1GetJobResponseSchema.ts";
import { v1GetJobsBySchedulerIdResponseSchema } from "./gen/api-gateway/zod/v1GetJobsBySchedulerIdResponseSchema.ts";
import { v1GetJobsResponseSchema } from "./gen/api-gateway/zod/v1GetJobsResponseSchema.ts";
import { v1GetWorkflowResponseSchema } from "./gen/api-gateway/zod/v1GetWorkflowResponseSchema.ts";
import { v1GetWorkflowsResponseSchema } from "./gen/api-gateway/zod/v1GetWorkflowsResponseSchema.ts";
import { v1JobSchema } from "./gen/api-gateway/zod/v1JobSchema.ts";
import { v1WorkflowSchema } from "./gen/api-gateway/zod/v1WorkflowSchema.ts";
import { getJobsQueryWorkflowIdSchema } from "./gen/job-manager/zod/getJobsSchema.ts";
import { getWorkflowsResponseSchema } from "./gen/job-manager/zod/getWorkflowsResponseSchema.ts";
import { getWorkflowsQueryWorkflowIdSchema } from "./gen/job-manager/zod/getWorkflowsSchema.ts";
import { slurmJobExecutionStatusInputSchema } from "./gen/job-manager/zod/slurmJobExecutionStatusInputSchema.ts";
import { slurmJobExecutionStatusOutputSchema } from "./gen/job-manager/zod/slurmJobExecutionStatusOutputSchema.ts";
import { workflowSchema } from "./gen/job-manager/zod/workflowSchema.ts";

type WorkflowIDSchema = {
  name: string;
  parse: (workflowId: string) => { success: boolean; data?: unknown };
  read: (data: unknown) => string | undefined;
};

const workflowIDSchemas: WorkflowIDSchema[] = [
  {
    name: "API Gateway job",
    parse: (workflowId: string) =>
      v1JobSchema.safeParse({ jobId: "job-1", workflowId }),
    read: (data) => (data as { workflowId?: string }).workflowId,
  },
  {
    name: "API Gateway create request",
    parse: (workflowId: string) =>
      v1CreateOrUpdateJobRequestSchema.safeParse({
        jobId: "job-1",
        attemptIndex: 0,
        workflowId,
      }),
    read: (data) => (data as { workflowId?: string }).workflowId,
  },
  {
    name: "API Gateway workflow",
    parse: (workflowId: string) => v1WorkflowSchema.safeParse({ workflowId }),
    read: (data) => (data as { workflowId?: string }).workflowId,
  },
  {
    name: "API Gateway workflow lookup query",
    parse: (workflowId: string) =>
      heimdallServiceGetWorkflowQueryWorkflowIdSchema.safeParse(workflowId),
    read: (data) => data as string,
  },
  {
    name: "API Gateway legacy workflow lookup path",
    parse: (workflowId: string) =>
      heimdallServiceGetWorkflow2PathWorkflowIdSchema.safeParse(workflowId),
    read: (data) => data as string,
  },
  {
    name: "API Gateway jobs filter",
    parse: (workflowId: string) =>
      heimdallServiceGetJobsQueryWorkflowIdsSchema.safeParse([workflowId]),
    read: (data) => (data as string[])[0],
  },
  {
    name: "API Gateway create job response",
    parse: (workflowId: string) =>
      v1CreateOrUpdateJobResponseSchema.safeParse({
        job: { jobId: "job-1", workflowId },
      }),
    read: (data) => (data as { job?: { workflowId?: string } }).job?.workflowId,
  },
  {
    name: "API Gateway get job response",
    parse: (workflowId: string) =>
      v1GetJobResponseSchema.safeParse({
        job: { jobId: "job-1", workflowId },
      }),
    read: (data) => (data as { job?: { workflowId?: string } }).job?.workflowId,
  },
  {
    name: "API Gateway get jobs by scheduler ID response",
    parse: (workflowId: string) =>
      v1GetJobsBySchedulerIdResponseSchema.safeParse({
        jobs: [{ jobId: "job-1", workflowId }],
      }),
    read: (data) =>
      (data as { jobs?: { workflowId?: string }[] }).jobs?.[0]?.workflowId,
  },
  {
    name: "API Gateway get jobs response",
    parse: (workflowId: string) =>
      v1GetJobsResponseSchema.safeParse({
        jobs: [{ jobId: "job-1", workflowId }],
      }),
    read: (data) =>
      (data as { jobs?: { workflowId?: string }[] }).jobs?.[0]?.workflowId,
  },
  {
    name: "API Gateway get workflow response",
    parse: (workflowId: string) =>
      v1GetWorkflowResponseSchema.safeParse({ workflow: { workflowId } }),
    read: (data) =>
      (data as { workflow?: { workflowId?: string } }).workflow?.workflowId,
  },
  {
    name: "API Gateway get workflows response",
    parse: (workflowId: string) =>
      v1GetWorkflowsResponseSchema.safeParse({
        workflows: [{ workflowId }],
      }),
    read: (data) =>
      (data as { workflows?: { workflowId?: string }[] }).workflows?.[0]
        ?.workflowId,
  },
  {
    name: "Job Manager record",
    parse: (workflowId: string) =>
      slurmJobExecutionStatusOutputSchema.safeParse({
        job_id: "job-1",
        job_attempt_index: 0,
        workflow_id: workflowId,
      }),
    read: (data) => (data as { workflow_id?: string }).workflow_id,
  },
  {
    name: "Job Manager input record",
    parse: (workflowId: string) =>
      slurmJobExecutionStatusInputSchema.safeParse({
        job_id: "job-1",
        job_attempt_index: 0,
        workflow_id: workflowId,
      }),
    read: (data) => (data as { workflow_id?: string }).workflow_id,
  },
  {
    name: "Job Manager filter",
    parse: (workflowId: string) =>
      getJobsQueryWorkflowIdSchema.safeParse(workflowId),
    read: (data) => data as string,
  },
  {
    name: "Job Manager workflow",
    parse: (workflowId: string) => workflowSchema.safeParse({ id: workflowId }),
    read: (data) => (data as { id?: string }).id,
  },
  {
    name: "Job Manager workflows filter",
    parse: (workflowId: string) =>
      getWorkflowsQueryWorkflowIdSchema.safeParse(workflowId),
    read: (data) => data as string,
  },
  {
    name: "Job Manager get workflows response",
    parse: (workflowId: string) =>
      getWorkflowsResponseSchema.safeParse({
        item_count: 1,
        workflows: [{ id: workflowId }],
      }),
    read: (data) =>
      (data as { workflows?: { id?: string }[] }).workflows?.[0]?.id,
  },
];

function expectAcceptedVerbatim(
  parse: WorkflowIDSchema["parse"],
  read: WorkflowIDSchema["read"],
  workflowId: string,
) {
  const result = parse(workflowId);

  expect(result.success).toBe(true);
  if (!result.success) {
    return;
  }
  expect(read(result.data)).toBe(workflowId);
}

describe.each(workflowIDSchemas)("$name workflow ID", ({ parse, read }) => {
  it("accepts one through 256 Unicode scalar values without normalization", () => {
    expectAcceptedVerbatim(parse, read, "x");
    expectAcceptedVerbatim(parse, read, "experiment-42");
    expectAcceptedVerbatim(parse, read, "workflow/%_ e\u0301 多字节🚀");
    expectAcceptedVerbatim(parse, read, "🚀".repeat(256));
  });

  it("rejects empty, overlong, and lone-surrogate values", () => {
    expect(parse("").success).toBe(false);
    expect(parse("🚀".repeat(257)).success).toBe(false);
    expect(parse("\ud800").success).toBe(false);
  });
});

describe("API Gateway generated workflow ID contracts", () => {
  it("requires workflowId in generated TypeScript models", () => {
    expectTypeOf<V1Job>().toMatchTypeOf<{ workflowId: string }>();
    expectTypeOf<V1CreateOrUpdateJobRequest>().toMatchTypeOf<{
      workflowId: string;
    }>();
    expectTypeOf<V1Workflow>().toMatchTypeOf<{ workflowId: string }>();
  });

  it.each([
    ["Job", new URL("./gen/api-gateway/types/V1Job.ts", import.meta.url)],
    [
      "Workflow",
      new URL("./gen/api-gateway/types/V1Workflow.ts", import.meta.url),
    ],
    [
      "CreateOrUpdateJobRequest",
      new URL(
        "./gen/api-gateway/types/V1CreateOrUpdateJobRequest.ts",
        import.meta.url,
      ),
    ],
  ])("requires workflowId in the emitted %s TypeScript source", (_, path) => {
    const source = readFileSync(path, "utf8");

    expect(source).toMatch(/^\s*workflowId: string;$/m);
    expect(source).not.toMatch(/^\s*workflowId\?:/m);
  });

  it.each([
    ["Job", v1JobSchema, { jobId: "job-1" }],
    ["Workflow", v1WorkflowSchema, {}],
    [
      "CreateOrUpdateJobRequest",
      v1CreateOrUpdateJobRequestSchema,
      { jobId: "job-1", attemptIndex: 0 },
    ],
  ])("requires workflowId in the generated %s Zod schema", (_, schema, value) => {
    expect(schema.safeParse(value).success).toBe(false);
    expect(
      schema.safeParse({
        ...value,
        workflowId: "workflow/%_ e\u0301 多字节🚀",
      }).success,
    ).toBe(true);
  });
});
