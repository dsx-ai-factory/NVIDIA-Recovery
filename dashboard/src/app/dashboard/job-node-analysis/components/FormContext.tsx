// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import type {
  NodeAnalysisResponse,
  SlurmJobExecutionStatusOutput,
} from "~/gen/job-manager/types";
import { withBasePath } from "~/lib/base-path";

export type Operator = "union" | "intersection";
export type JobAccordionSection = "jobList" | "comparisonResults";
export type QueryBuilderSection = "guided" | "manual";

interface FormContextValue {
  selectedJobs: SlurmJobExecutionStatusOutput[];
  addSelectedJob: (job: SlurmJobExecutionStatusOutput) => void;
  removeSelectedJob: (job: SlurmJobExecutionStatusOutput) => void;
  searchText: string;
  setSearchText: (text: string) => void;
  operator: Operator | undefined;
  setOperator: (operator: Operator | undefined) => void;
  jobAccordion: JobAccordionSection[];
  setJobAccordion: (sections: JobAccordionSection[]) => void;
  isLoadingComparison: boolean;
  setIsLoadingComparison: (isLoading: boolean) => void;
  nodeAnalysis: NodeAnalysisResponse | { error: string } | undefined;
  setNodeAnalysis: (
    analysis: NodeAnalysisResponse | { error: string } | undefined,
  ) => void;
  customExpression: string | undefined;
  setCustomExpression: (value: string | undefined) => void;
  queryBuilderExpanded: QueryBuilderSection;
  setQueryBuilderExpanded: (value: QueryBuilderSection) => void;
  compareJobs: (expression: string, groupBy?: "rack" | "none") => Promise<void>;
  reset: () => void;
}

const FormContext = createContext<FormContextValue | undefined>(undefined);

interface FormProviderProps {
  children: React.ReactNode;
}

export function FormProvider({ children }: FormProviderProps) {
  const [selectedJobs, setSelectedJobs] = useState<
    SlurmJobExecutionStatusOutput[]
  >([]);
  const [searchText, setSearchText] = useState("");
  const [operator, setOperator] = useState<Operator | undefined>(undefined);
  const [jobAccordion, setJobAccordion] = useState<JobAccordionSection[]>([
    "jobList",
  ]);
  const [isLoadingComparison, setIsLoadingComparison] = useState(false);
  const [nodeAnalysis, setNodeAnalysis] = useState<
    NodeAnalysisResponse | { error: string } | undefined
  >(undefined);
  const [customExpression, setCustomExpression] = useState<string | undefined>(
    undefined,
  );
  const [queryBuilderExpanded, setQueryBuilderExpanded] =
    useState<QueryBuilderSection>("guided");

  const addSelectedJob = useCallback((job: SlurmJobExecutionStatusOutput) => {
    setSelectedJobs((prev) => [...prev, job]);
  }, []);

  const removeSelectedJob = useCallback(
    (job: SlurmJobExecutionStatusOutput) => {
      setSelectedJobs((prev) =>
        prev.filter((item) => {
          if (
            item.job_id === job.job_id &&
            item.job_attempt_index === job.job_attempt_index
          ) {
            return false;
          }
          return true;
        }),
      );
    },
    [],
  );

  const compareJobs = useCallback(
    async (
      expression: string,
      groupBy: "rack" | "none" = "rack",
    ): Promise<void> => {
      setIsLoadingComparison(true);
      setNodeAnalysis(undefined);
      setJobAccordion(["comparisonResults"]);
      // wait for accordion to collapse
      await new Promise((resolve) => setTimeout(resolve, 500));
      try {
        const params = new URLSearchParams({ expression, groupBy });
        const response = await fetch(
          withBasePath(`/api/job-node-analysis?${params.toString()}`),
        );
        const data = (await response.json()) as NodeAnalysisResponse;
        if (!response.ok) {
          throw new Error(
            "detail" in data && typeof data.detail === "string"
              ? data.detail
              : "unknown error",
          );
        }
        setNodeAnalysis(data);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        setNodeAnalysis({ error: `Failed to compare jobs: ${message}` });
      } finally {
        setIsLoadingComparison(false);
      }
    },
    [],
  );

  const reset = useCallback(() => {
    setSelectedJobs([]);
    setSearchText("");
    setOperator(undefined);
    setJobAccordion(["jobList"]);
    setNodeAnalysis(undefined);
    setCustomExpression(undefined);
  }, []);

  const value = useMemo(
    () => ({
      selectedJobs,
      addSelectedJob,
      removeSelectedJob,
      searchText,
      setSearchText,
      operator,
      setOperator,
      jobAccordion,
      setJobAccordion,
      isLoadingComparison,
      setIsLoadingComparison,
      nodeAnalysis,
      setNodeAnalysis,
      customExpression,
      setCustomExpression,
      queryBuilderExpanded,
      setQueryBuilderExpanded,
      compareJobs,
      reset,
    }),
    [
      selectedJobs,
      addSelectedJob,
      removeSelectedJob,
      searchText,
      operator,
      jobAccordion,
      isLoadingComparison,
      nodeAnalysis,
      customExpression,
      queryBuilderExpanded,
      compareJobs,
      reset,
    ],
  );

  return <FormContext.Provider value={value}>{children}</FormContext.Provider>;
}

export function useFormContext() {
  const context = useContext(FormContext);
  if (!context) {
    throw new Error("useFormContext must be used within a FormProvider.");
  }
  return context;
}
