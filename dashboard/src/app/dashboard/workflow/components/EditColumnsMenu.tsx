// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Checkbox,
  Flex,
  PopoverContent,
  PopoverRoot,
  PopoverTrigger,
  Text,
} from "@nvidia/foundations-react-core";
import { ChevronDown, ChevronUp, Sliders } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import type { WorkflowColumn } from "./Columns";

export function EditColumnsMenu({
  columns,
  isVisible,
  onSetVisible,
}: {
  columns: WorkflowColumn[];
  isVisible: (id: string) => boolean;
  onSetVisible: (id: string, visible: boolean) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <PopoverRoot open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button kind="tertiary">
          <Sliders />
          View
          {open ? <ChevronUp /> : <ChevronDown />}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="p-2">
        <div className="w-[220px]">
          <Flex direction="col" gap="2">
            <Text fontWeight="bold" fontSize="12">
              Toggle Columns
            </Text>
            <Flex direction="col" gap="1">
              {columns.map((col) => {
                const visible = isVisible(col.id);
                return (
                  <div
                    key={col.id}
                    className="flex cursor-pointer items-center gap-2 py-1 text-left"
                    onClick={() => onSetVisible(col.id, !visible)}
                  >
                    <Checkbox checked={visible} />
                    <Text>{col.header}</Text>
                  </div>
                );
              })}
            </Flex>
          </Flex>
        </div>
      </PopoverContent>
    </PopoverRoot>
  );
}
