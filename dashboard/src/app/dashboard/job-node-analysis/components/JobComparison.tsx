// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Button,
  Flex,
  Spinner,
  Tag,
  Text,
  TextInput,
  Tooltip,
} from "@nvidia/foundations-react-core";
import {
  Cancel,
  Check,
  CloseCircle,
  CopyGeneric,
} from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import { useFormContext } from "./FormContext";

const RACK_ALL_NODES = "all";

export function JobComparison() {
  const { nodeAnalysis, isLoadingComparison } = useFormContext();
  const [isCopied, setIsCopied] = useState(false);

  if (isLoadingComparison) {
    return (
      <Flex align="center" justify="center" className="h-full">
        <Spinner size="medium" aria-label="Loading" />
      </Flex>
    );
  }

  if (!nodeAnalysis) {
    return (
      <Flex direction="col" gap="2" align="center" justify="center">
        <CloseCircle className="w-[36px] h-[36px]" />
        <Text kind="body/regular/lg">No analysis detected</Text>
      </Flex>
    );
  }

  if ("error" in nodeAnalysis) {
    return (
      <Flex direction="col" gap="2" align="center" justify="center">
        <CloseCircle className="w-[36px] h-[36px]" />
        <Text kind="body/regular/lg" className="max-w-[80%] text-center">
          {nodeAnalysis.error}
        </Text>
      </Flex>
    );
  }

  if (
    (!nodeAnalysis.groupedByRack ||
      Object.keys(nodeAnalysis.groupedByRack).length === 0) &&
    nodeAnalysis.result.count === 0
  ) {
    return (
      <Flex direction="col" gap="2" align="center" justify="center">
        <Cancel className="w-[36px] h-[36px]" />
        <Text kind="body/regular/lg">No nodes found for this query</Text>
      </Flex>
    );
  }

  const { groupedByRack, expression, result } = nodeAnalysis;
  const hasRackGroups = groupedByRack && Object.keys(groupedByRack).length > 0;
  const rackEntries = hasRackGroups
    ? Object.entries(groupedByRack as Record<string, string[]>)
    : [[RACK_ALL_NODES, result.nodes] as [string, string[]]];

  const copyRackResults = () => {
    setIsCopied(true);
    const rackResultsString = rackEntries
      .map(([rack, nodes]) => {
        const label = rack === RACK_ALL_NODES ? "All Nodes" : `Rack(${rack})`;
        return `${label}: ${nodes.join(", ")}`;
      })
      .join("\n");
    void navigator.clipboard.writeText(rackResultsString);
    setTimeout(() => {
      setIsCopied(false);
    }, 2000);
  };
  return (
    <Flex direction="col" gap="3">
      {expression && (
        <Flex direction="col" gap="1">
          <Flex direction="row" gap="2" justify="between" align="center">
            <Text kind="body/semibold/lg" asChild>
              <div>Comparison Query</div>
            </Text>
            <Tooltip slotContent="Copy all rack results">
              <Button
                size="small"
                kind="tertiary"
                color="neutral"
                onClick={copyRackResults}
              >
                {isCopied ? <Check /> : <CopyGeneric />}
              </Button>
            </Tooltip>
          </Flex>
          <TextInput value={expression} disabled />
        </Flex>
      )}

      {rackEntries.map(([rack, nodes]) => (
        <RackResult key={rack} rack={rack} nodes={nodes} />
      ))}
    </Flex>
  );
}

function RackResult({ rack, nodes }: { rack: string; nodes: string[] }) {
  const displayName = rack === RACK_ALL_NODES ? "All Nodes" : `Rack ${rack}`;
  const [isCopied, setIsCopied] = useState(false);
  const copyRackResults = () => {
    setIsCopied(true);
    const label = rack === RACK_ALL_NODES ? "All Nodes" : `Rack(${rack})`;
    void navigator.clipboard.writeText(`${label}: ${nodes.join(", ")}`);
    setTimeout(() => {
      setIsCopied(false);
    }, 2000);
  };
  return (
    <Flex direction="col" gap="2">
      <Flex direction="row" gap="2" justify="between" align="center">
        <Text kind="body/semibold/lg">{displayName}</Text>
        <Tooltip slotContent={`Copy results for ${displayName}`}>
          <Button
            size="small"
            kind="tertiary"
            color="neutral"
            onClick={copyRackResults}
          >
            {isCopied ? <Check /> : <CopyGeneric />}
          </Button>
        </Tooltip>
      </Flex>
      <Flex direction="row" wrap="wrap" gap="2">
        {nodes.map((node) => (
          <Tag
            key={node}
            kind="outline"
            color="gray"
            className="cursor-default"
          >
            {node}
          </Tag>
        ))}
      </Flex>
    </Flex>
  );
}
