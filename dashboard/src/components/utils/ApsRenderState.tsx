// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Badge, type BadgeProps } from "@nvidia/foundations-react-core";
import type { ReactNode } from "react";
import { SLURM_JOB_STATE } from "~/const/slurm_job";

export function ApsRenderState({
  state,
  children,
}: {
  state: string | null | undefined;
  children?: ReactNode;
}) {
  const colorMap: Record<string, BadgeProps["color"]> = {
    [SLURM_JOB_STATE.COMPLETED]: "blue",
    [SLURM_JOB_STATE.CANCELLED]: "yellow",
    [SLURM_JOB_STATE.PENDING]: "purple",
    [SLURM_JOB_STATE.RUNNING]: "green",
    [SLURM_JOB_STATE.FAILED]: "red",
    [SLURM_JOB_STATE.COMPLETED_STALE]: "red",
    [SLURM_JOB_STATE.REQUEUED]: "gray",
  };
  return (
    <Badge
      key={state}
      className="w-fit text-clip overflow-hidden whitespace-nowrap"
      color={state ? colorMap[state] : "gray"}
      kind="solid"
      title={state ?? undefined}
    >
      <span className="text-clip overflow-hidden whitespace-nowrap">
        {children ?? state}
      </span>
    </Badge>
  );
}
