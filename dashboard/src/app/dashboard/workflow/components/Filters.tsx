// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Flex,
  RadioGroup,
  Text,
  TextInput,
} from "@nvidia/foundations-react-core";
import { useState } from "react";

export function TextFilterSubmenu({
  label,
  value,
  onApply,
}: {
  label: string;
  value: string | undefined;
  onApply: (next: string | undefined) => void;
}) {
  const [draft, setDraft] = useState<string>(value ?? "");

  function apply() {
    const trimmed = draft.trim();
    onApply(trimmed === "" ? undefined : trimmed);
  }

  return (
    <div className="w-[280px]">
      <Flex direction="col" gap="2">
        <Text fontWeight="bold">Filter by {label}</Text>
        <TextInput
          value={draft}
          onValueChange={(next) => setDraft(next ?? "")}
          placeholder={`Enter ${label.toLowerCase()}...`}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === "NumpadEnter") {
              e.preventDefault();
              apply();
            }
          }}
        />
        <Flex gap="2" justify="end">
          <Button
            kind="tertiary"
            size="small"
            onClick={() => {
              setDraft("");
              onApply(undefined);
            }}
          >
            Clear
          </Button>
          <Button kind="secondary" size="small" onClick={apply}>
            Apply
          </Button>
        </Flex>
      </Flex>
    </div>
  );
}

export function StateFilterSubmenu({
  value,
  options,
  onApply,
}: {
  value: string | undefined;
  options: { value: string }[];
  onApply: (next: string | undefined) => void;
}) {
  return (
    <div className="w-[240px]">
      <Flex direction="col" gap="2">
        <Flex justify="between" align="center">
          <Text fontWeight="bold">Filter by State</Text>
          <Button
            kind="tertiary"
            size="small"
            onClick={() => onApply(undefined)}
          >
            Clear
          </Button>
        </Flex>
        <RadioGroup
          name="filter-state"
          orientation="vertical"
          items={options.map((opt) => ({
            children: opt.value,
            value: opt.value,
          }))}
          value={value ?? ""}
          onValueChange={(next) => onApply(next === "" ? undefined : next)}
        />
      </Flex>
    </div>
  );
}
