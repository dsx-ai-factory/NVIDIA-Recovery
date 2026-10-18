// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Tag } from "@nvidia/foundations-react-core";
import { IconError } from "@nvidia/react-gui-icons/icons";
import type React from "react";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { useGetJob } from "../../../hooks";

function LoadingCell() {
  return (
    <div
      className="h-3 animate-pulse rounded-lg bg-gray-100"
      style={{ width: `${Math.floor(Math.random() * 40 + 60)}px` }}
    />
  );
}

export function JobCellProvider({
  jobCompoundKey,
  children,
}: {
  jobCompoundKey: string;
  children: (data: SlurmJobExecutionStatusOutput) => React.ReactNode;
}) {
  const [jobId, attemptIndex] = jobCompoundKey.split("_");
  if (jobId === undefined || attemptIndex === undefined) {
    console.error(`Invalid jobCompoundKey - ${jobCompoundKey}`);
  }
  const { data, isLoading, isError } = useGetJob({
    jobId: jobId!,
    attemptIndex: attemptIndex!,
  });

  if (isLoading) {
    return <LoadingCell />;
  }

  if (isError || !data) {
    return <IconError className="text-feedback-danger" />;
  }

  return children(data);
}

export function IntersectionNodesListCell({
  commonNodes,
  job,
}: {
  commonNodes: string[] | undefined;
  job: SlurmJobExecutionStatusOutput;
}) {
  return (
    <div className="flex flex-wrap gap-1">
      {job.allocated_node_list
        ?.filter((node) => !commonNodes?.includes(node))
        ?.map((node) => (
          <Tag
            key={job.job_id + job.job_attempt_index + node}
            color="red"
            kind="outline"
          >
            {node}
          </Tag>
        ))}
      {commonNodes?.map((int) => (
        <Tag key={int} color="blue" kind="solid">
          {int}
        </Tag>
      ))}
    </div>
  );
}
