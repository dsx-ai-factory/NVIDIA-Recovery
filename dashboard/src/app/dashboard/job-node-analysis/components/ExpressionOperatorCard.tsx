// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Button,
  Card,
  Divider,
  Flex,
  Tag,
  Text,
  TextArea,
  Tooltip,
} from "@nvidia/foundations-react-core";
import {
  ChartTree,
  ChevronDown,
  ChevronUp,
  Close,
} from "@nvidia/react-gui-icons/icons";
import { useFormContext } from "./FormContext";

const REQUIRED_JOBS_COUNT = 2;

export function ExpressionOperatorCard() {
  const {
    selectedJobs,
    operator,
    setOperator,
    searchText,
    removeSelectedJob,
    compareJobs,
    queryBuilderExpanded,
    setQueryBuilderExpanded,
    reset,
  } = useFormContext();

  const comparisonOperator =
    operator === "union" ? "U" : operator === "intersection" ? "&" : undefined;
  const comparisonQuery =
    operator && selectedJobs.length > 0
      ? selectedJobs
          .map((job) => `${job.job_id}/${job.job_attempt_index}`)
          .join(` ${comparisonOperator} `)
      : "";

  return (
    <Card density="compact" className="border-base">
      <Flex direction="col" gap="4">
        <Flex align="center" gap="1">
          <ChartTree className="w-[16px] h-[16px]" />
          <Text kind="body/bold/xl">Jobs + Operators</Text>
          {queryBuilderExpanded === "guided" ? (
            <Button
              kind="tertiary"
              className="ml-auto cursor-pointer"
              size="small"
              onClick={() => setQueryBuilderExpanded("manual")}
            >
              <ChevronUp />
            </Button>
          ) : (
            <Button
              kind="tertiary"
              className="ml-auto cursor-pointer"
              size="small"
              onClick={() => setQueryBuilderExpanded("guided")}
            >
              <ChevronDown className="ml-auto cursor-pointer" />
            </Button>
          )}
        </Flex>
        {queryBuilderExpanded === "guided" && (
          <>
            <Text kind="label/light/sm">
              Select two or more jobs and select and operator to make a
              comparison.
            </Text>

            <Divider />

            <Text kind="label/bold/md">1. Job Selection</Text>

            {selectedJobs.length > 0 ? (
              <Text>
                <Flex gap="1" wrap="wrap">
                  {selectedJobs.map((job) => (
                    <Tooltip
                      key={`${job.job_id}/${job.job_attempt_index}`}
                      slotContent={job.job_name}
                      disabled={!job.job_name}
                    >
                      <Tag
                        kind="outline"
                        color="green"
                        className="cursor-default"
                      >
                        {job.job_id}/{job.job_attempt_index}
                        <Close
                          className="cursor-pointer"
                          onClick={() => removeSelectedJob(job)}
                        />
                      </Tag>
                    </Tooltip>
                  ))}
                </Flex>
              </Text>
            ) : (
              <Text kind="label/light/sm" style={{ lineHeight: "22px" }}>
                From the job list, select {REQUIRED_JOBS_COUNT} jobs or more to
                compare
              </Text>
            )}

            <Divider />

            <Text kind="label/bold/md">2. Operator Selection</Text>
            <Text kind="label/light/sm">
              Select an operator to compare the jobs selected.
            </Text>
            <Flex gap="3" direction="col">
              <Button
                className="w-full rounded-2xl"
                kind={operator === "union" ? "primary" : "secondary"}
                color="neutral"
                size="small"
                type="button"
                onClick={() => setOperator("union")}
              >
                Union (OR)
              </Button>
              <Button
                className="w-full rounded-2xl"
                kind={operator === "intersection" ? "primary" : "secondary"}
                color="neutral"
                size="small"
                type="button"
                onClick={() => setOperator("intersection")}
              >
                Intersection (AND)
              </Button>
            </Flex>
            <Flex direction="col" gap="2">
              <Text>Comparison Expression</Text>
              <TextArea disabled value={comparisonQuery} />
            </Flex>
            <Flex gap="2">
              <Button
                kind="secondary"
                color="neutral"
                disabled={!selectedJobs.length && !operator && !searchText}
                onClick={reset}
              >
                Reset
              </Button>
              <Button
                kind="primary"
                color="brand"
                className="flex-1"
                disabled={
                  selectedJobs.length < REQUIRED_JOBS_COUNT || !operator
                }
                onClick={() => compareJobs(comparisonQuery)}
              >
                Compare Jobs
              </Button>
            </Flex>
          </>
        )}
      </Flex>
    </Card>
  );
}
