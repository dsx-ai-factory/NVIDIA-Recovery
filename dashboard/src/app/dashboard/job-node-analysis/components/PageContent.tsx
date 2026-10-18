// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Accordion, Flex, Text } from "@nvidia/foundations-react-core";
import { ChartTree, MirrorVertical } from "@nvidia/react-gui-icons/icons";
import { ExpressionCustomCard } from "./ExpressionCustomCard";
import { ExpressionOperatorCard } from "./ExpressionOperatorCard";
import {
  FormProvider,
  type JobAccordionSection,
  useFormContext,
} from "./FormContext";
import { JobComparison } from "./JobComparison";
import { JobList } from "./JobList";

export function NodeAnalysisPageContent() {
  return (
    <FormProvider>
      <Content />
    </FormProvider>
  );
}

function Content() {
  const { jobAccordion, setJobAccordion } = useFormContext();
  return (
    <div className={`container mx-auto max-w-[1280px] px-5`}>
      <div className="flex w-full items-start gap-6 py-6">
        <div className="flex w-[350px] flex-shrink-0 flex-col gap-4">
          {/* First column content (sidebar) */}
          <Text>
            Identify the rack or node(s) responsible for job performance issues
            by comparing multiple jobs.
          </Text>
          <Text kind="body/regular/sm">
            Select one of the options below to compare jobs:
          </Text>
          <ExpressionOperatorCard />
          <ExpressionCustomCard />
        </div>
        <div className="bg-surface-sunken min-w-0 flex-1">
          {/* Second column content (accordion) */}
          <Accordion
            multiple
            className="[&_.nv-accordion-item]:border-base bg-transparent [&_.nv-accordion-item]:border [&_.nv-accordion-item:not(:first-child)]:mt-4"
            items={[
              {
                slotTrigger: (
                  <Flex align="center" gap="1">
                    <ChartTree style={{ width: 24, height: 24 }} />
                    <Text kind="body/bold/xl">Job List</Text>
                  </Flex>
                ),
                slotContent: <JobList />,
                value: "jobList",
              },
              {
                slotTrigger: (
                  <Flex align="center" gap="1">
                    <MirrorVertical style={{ width: 24, height: 24 }} />
                    <Text kind="body/bold/xl">Job Node Comparison Results</Text>
                  </Flex>
                ),
                slotContent: <JobComparison />,
                value: "comparisonResults",
              },
            ]}
            value={jobAccordion}
            onValueChange={(val) => {
              setJobAccordion(val as JobAccordionSection[]);
            }}
          />
        </div>
      </div>
    </div>
  );
}
