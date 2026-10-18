// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { getAgentApiUrl } from "~/lib/baseUrls";
import { logger } from "~/pino";

const AGENT_LOOKUP_TIMEOUT_MS = 10_000;

type AgentAnalysisType = "root_cause" | "job_group_pattern";

interface AgentAnalysis {
  session_id: string;
  analysis_type: AgentAnalysisType;
  status: string;
}

interface AgentLookupResult {
  job_id: string;
  job_attempt_index: number;
  job_group_id?: string | null;
  analysis: AgentAnalysis | null;
}

interface AgentLookupResponse {
  results: AgentLookupResult[];
}

function isAgentLookupResponse(
  value: unknown,
  jobs: SlurmJobExecutionStatusOutput[],
  analysisType: AgentAnalysisType,
): value is AgentLookupResponse {
  if (!value || typeof value !== "object") return false;
  const results = (value as { results?: unknown }).results;
  if (!Array.isArray(results) || results.length !== jobs.length) return false;

  return results.every((result, index) => {
    if (!result || typeof result !== "object") return false;
    const typed = result as Partial<AgentLookupResult>;
    const job = jobs[index];
    if (!job) return false;
    if (
      typed.job_id !== job.job_id ||
      typed.job_attempt_index !== job.job_attempt_index ||
      (typed.job_group_id ?? null) !== (job.job_group_id || null)
    ) {
      return false;
    }
    if (typed.analysis === null) return true;
    return (
      !!typed.analysis &&
      typeof typed.analysis.session_id === "string" &&
      typed.analysis.session_id.length > 0 &&
      typed.analysis.analysis_type === analysisType &&
      typeof typed.analysis.status === "string"
    );
  });
}

async function lookupAnalysisType(
  agentApiUrl: string,
  jobs: SlurmJobExecutionStatusOutput[],
  analysisType: AgentAnalysisType,
): Promise<(AgentAnalysis | null)[]> {
  try {
    const response = await fetch(
      `${agentApiUrl.replace(/\/+$/, "")}/analyses/lookup`,
      {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          analysis_type: analysisType,
          attempts: jobs.map((job) => ({
            job_id: job.job_id,
            job_attempt_index: job.job_attempt_index,
            ...(analysisType === "job_group_pattern"
              ? { job_group_id: job.job_group_id }
              : {}),
          })),
        }),
        signal: AbortSignal.timeout(AGENT_LOOKUP_TIMEOUT_MS),
      },
    );
    if (!response.ok) {
      throw new Error(`Agent lookup returned HTTP ${response.status}`);
    }

    const payload: unknown = await response.json();
    if (!isAgentLookupResponse(payload, jobs, analysisType)) {
      throw new Error("Agent lookup returned an invalid or unordered response");
    }
    return payload.results.map((result) => result.analysis);
  } catch (error) {
    logger.warn(
      `Agent ${analysisType} lookup failed; suppressing affected links: ${error instanceof Error ? error.message : String(error)}`,
    );
    return jobs.map(() => null);
  }
}

export async function lookupAgentAnalyses(
  jobs: SlurmJobExecutionStatusOutput[],
): Promise<(AgentAnalysis | null)[]> {
  const agentApiUrl = getAgentApiUrl();
  const analyses = jobs.map<AgentAnalysis | null>(() => null);
  if (!agentApiUrl || jobs.length === 0) return analyses;

  const buckets: Array<{
    analysisType: AgentAnalysisType;
    entries: Array<{ index: number; job: SlurmJobExecutionStatusOutput }>;
  }> = [
    { analysisType: "root_cause", entries: [] },
    { analysisType: "job_group_pattern", entries: [] },
  ];

  jobs.forEach((job, index) => {
    buckets[job.job_group_id ? 1 : 0]?.entries.push({ index, job });
  });

  await Promise.all(
    buckets.map(async ({ analysisType, entries }) => {
      if (entries.length === 0) return;
      const matches = await lookupAnalysisType(
        agentApiUrl,
        entries.map(({ job }) => job),
        analysisType,
      );
      entries.forEach(({ index }, matchIndex) => {
        analyses[index] = matches[matchIndex] ?? null;
      });
    }),
  );
  return analyses;
}
