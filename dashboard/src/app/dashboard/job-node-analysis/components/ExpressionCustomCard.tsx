// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Button,
  Card,
  Divider,
  Flex,
  Text,
  TextArea,
} from "@nvidia/foundations-react-core";
import { ChevronDown, ChevronUp, Code } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import { useFormContext } from "./FormContext";

const DESCRIPTIONS = [
  "Union (OR) A U B → nodes in A or B",
  "Intersection (AND) A & B → nodes in A and B",
  "Difference (subtract) A - B → nodes in A but not B",
  "Grouping (A U B) - C",
];

export function ExpressionCustomCard() {
  const [resetCount, setResetCount] = useState<number>(0);
  const {
    searchText,
    customExpression,
    setCustomExpression,
    queryBuilderExpanded,
    setQueryBuilderExpanded,
    reset,
    compareJobs,
  } = useFormContext();

  return (
    <Card density="compact" className="border-base">
      <Flex direction="col" gap="4">
        <Flex align="center" gap="1">
          <Code className="w-[16px] h-[16px]" />
          <Text kind="body/bold/xl">Query Builder</Text>
          {queryBuilderExpanded === "manual" ? (
            <Button
              kind="tertiary"
              className="ml-auto cursor-pointer"
              size="small"
              onClick={() => setQueryBuilderExpanded("guided")}
            >
              <ChevronUp />
            </Button>
          ) : (
            <Button
              kind="tertiary"
              className="ml-auto cursor-pointer"
              size="small"
              onClick={() => setQueryBuilderExpanded("manual")}
            >
              <ChevronDown className="ml-auto cursor-pointer" />
            </Button>
          )}
        </Flex>

        {queryBuilderExpanded === "manual" && (
          <>
            <Text kind="label/light/sm">
              Create your own expressions, using jobs and operators
            </Text>

            <Divider />

            <Text kind="label/bold/md">1. Expression</Text>
            <Text kind="label/light/sm">Enter your jobs and operators</Text>

            <Divider />

            <Flex direction="col" gap="2">
              <Text>Comparison Expression</Text>
              <TextArea
                key={resetCount}
                value={customExpression}
                onValueChange={setCustomExpression}
              />
            </Flex>

            <Divider />

            <Flex direction="col" gap="2">
              <Text kind="label/light/sm">Example Expression:</Text>
              <ul style={{ listStyleType: "disc", paddingLeft: 20 }}>
                {DESCRIPTIONS.map((desc) => (
                  <li key={desc}>
                    <Text kind="label/light/sm">{desc}</Text>
                  </li>
                ))}
              </ul>
            </Flex>

            <Flex gap="2">
              <Button
                kind="secondary"
                color="neutral"
                disabled={!customExpression?.length && !searchText}
                onClick={() => {
                  reset();
                  setResetCount((prev) => prev + 1);
                }}
              >
                Reset
              </Button>
              <Button
                kind="primary"
                color="brand"
                className="flex-1"
                disabled={!customExpression || customExpression.length === 0}
                onClick={() => compareJobs(customExpression!)}
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
