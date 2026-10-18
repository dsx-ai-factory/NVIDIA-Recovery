// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { toQueryParamString } from "~/app/api/utils";
import type {
  GetJobsJobNameMatchKey,
  GetWorkflowsResponse,
  GetWorkflowsWorkflowIdMatchKey,
  JobExecutionStatuses,
  ListDagDependenciesResponse,
  NodeEvents,
  SlurmJobExecutionStatusOutput,
} from "~/gen/job-manager/types";
import { withBasePath } from "~/lib/base-path";

export type JobKey = {
  jobId?: string;
  attemptIndex?: string;
  jobName?: string;
  jobNameMatch?: GetJobsJobNameMatchKey;
};

export interface AgentAnalysis {
  session_id: string;
  analysis_type: "root_cause" | "job_group_pattern";
  status: string;
}

export type DashboardJobExecutionStatus = SlurmJobExecutionStatusOutput & {
  agent_analysis: AgentAnalysis | null;
};

export type DashboardJobExecutionStatuses = Omit<
  JobExecutionStatuses,
  "statuses"
> & {
  statuses: DashboardJobExecutionStatus[];
  agent_web_url?: string;
};

export function getJobKeysFromId(query: string | undefined = ""): JobKey[] {
  return query
    .split(/[,;]+/)
    .map((id) => ({ jobId: id.trim() }))
    .filter((job) => !!job.jobId); // filter out empty job id
}

export function getJobKeysFromName(query: string | undefined = ""): JobKey[] {
  return query
    .split(/[,;]+/)
    .map((name) => ({ jobName: name.trim() }))
    .filter((job) => !!job.jobName); // filter out empty job name
}

export async function fetchWorkflows(params: {
  workflowId?: string;
  workflowIdMatch?: GetWorkflowsWorkflowIdMatchKey;
  limit?: number;
  offset?: number;
}): Promise<GetWorkflowsResponse> {
  const route = withBasePath(`/api/workflows?${toQueryParamString(params)}`);
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<GetWorkflowsResponse>;
}

export async function fetchJobs(params: {
  completeData?: boolean;
  jobAttemptIndex?: string;
  jobGroupId?: string;
  jobId?: string;
  jobName?: string;
  jobNameMatch?: GetJobsJobNameMatchKey;
  limit?: number;
  offset?: number;
  sortBy?: string;
  state?: string;
  user?: string;
  workflowId?: string;
}): Promise<DashboardJobExecutionStatuses> {
  const route = withBasePath(`/api/jobs?${toQueryParamString(params)}`);
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<DashboardJobExecutionStatuses>;
}

export async function fetchDagDependencies(params: {
  dependentJobId: string;
}): Promise<ListDagDependenciesResponse> {
  const route = withBasePath(
    `/api/dag-dependencies?${toQueryParamString(params)}`,
  );
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<ListDagDependenciesResponse>;
}

export async function getJobErrorAttributions(params: {
  jobId: string;
  jobAttemptIndex?: string;
  anomalyid?: string | null;
}): Promise<NodeEvents> {
  const route = withBasePath(
    `/api/job-attributions?${toQueryParamString(params)}`,
  );
  const response = await fetch(route, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    console.error(
      `[ERROR] GETTING ${route} - ${response.status}: ${response.statusText}`,
    );
    throw new Error(response.statusText);
  }
  return response.json() as Promise<NodeEvents>;
}
