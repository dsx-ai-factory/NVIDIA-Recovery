// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button } from "@nvidia/foundations-react-core";
import { useId, useState } from "react";

const MAX_VISIBLE_PATHS = 5;

/** Displays log paths in order, with longer lists collapsed to five entries. */
export function ExpandableLogFileList({
  paths,
}: {
  /** All log file paths for the selected job attempt. */
  paths: string[];
}) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();

  if (paths.length === 0) return <>-</>;

  return (
    <div className="text-14 flex min-w-0 flex-col gap-1">
      <ul id={listId} className="flex flex-col gap-1">
        {(expanded ? paths : paths.slice(0, MAX_VISIBLE_PATHS)).map(
          (path, index) => (
            <li
              key={`${index}-${path}`}
              className="break-all whitespace-pre-wrap"
            >
              {path}
            </li>
          ),
        )}
      </ul>
      {paths.length > MAX_VISIBLE_PATHS && (
        <Button
          kind="tertiary"
          size="tiny"
          className="self-start"
          aria-expanded={expanded}
          aria-controls={listId}
          onClick={() => setExpanded((value) => !value)}
        >
          {expanded
            ? "Show fewer paths"
            : `Show ${paths.length - MAX_VISIBLE_PATHS} more paths`}
        </Button>
      )}
    </div>
  );
}
