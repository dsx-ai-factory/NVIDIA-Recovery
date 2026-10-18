// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import Link from "next/link";
import type React from "react";
import type { DashboardJobExecutionStatus } from "~/app/dashboard/workflow/api";
import { getJobLink } from "~/app/dashboard/workflow/utils/links";
import { ApsDuration } from "~/components/utils/ApsDuration";
import { ApsRenderState } from "~/components/utils/ApsRenderState";
import {
  ApsRenderAlertBadges,
  ApsRenderCheckpoints,
  ApsRenderTimestamp,
} from "~/components/utils/apsJobRender";
import { SLURM_JOB_STATE } from "~/const/slurm_job";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { CalTimeDiffInSeconds } from "~/utils/time";

export type JobRow = DashboardJobExecutionStatus & { id: string };

export type ColumnFilterSpec =
  | { type: "text" }
  | { type: "single-select"; options: { value: string }[] };

export interface WorkflowColumn {
  id: string;
  header: string;
  sortable?: boolean;
  filter?: ColumnFilterSpec;
  render: (row: JobRow) => React.ReactNode;
}

function getDuration(job: SlurmJobExecutionStatusOutput): number | null {
  if (!job.start_time) {
    return null;
  }
  const endTime = job.end_time ?? new Date().toLocaleString();
  return CalTimeDiffInSeconds(job.start_time, endTime);
}

export function makeColumns(
  jobGroupsEnabled: boolean,
  agentWebUrl?: string,
): WorkflowColumn[] {
  const columns: WorkflowColumn[] = [
    {
      id: "job_id",
      header: "Job ID",
      sortable: true,
      filter: { type: "text" },
      render: (row) => {
        const link = getJobLink(row);
        return (
          <Link className="underline" href={link.href}>
            {link.label}
          </Link>
        );
      },
    },
    ...(jobGroupsEnabled
      ? [
          {
            id: "job_group_id",
            header: "Job Group ID",
            sortable: true,
            filter: { type: "text" } as ColumnFilterSpec,
            render: (row: JobRow) =>
              row.job_group_id ? (
                <Link
                  className="underline"
                  href={`/dashboard/job-groups?job_group_id=${row.job_group_id}`}
                >
                  {row.job_group_id}
                </Link>
              ) : (
                "--"
              ),
          },
        ]
      : []),
    {
      id: "job_name",
      header: "Job Name",
      sortable: true,
      render: (row) => (
        <Link
          className="underline"
          href={`/dashboard/workflow?query=${row.job_name}`}
        >
          {row.job_name}
        </Link>
      ),
    },
    {
      id: "user",
      header: "User",
      sortable: true,
      filter: { type: "text" },
      render: (row) => row.user ?? "",
    },
    {
      id: "node_count",
      header: "Node Count",
      sortable: true,
      render: (row) => row.node_count ?? "",
    },
    {
      id: "total_gpu_count",
      header: "GPU Count",
      sortable: true,
      render: (row) => row.total_gpu_count ?? "",
    },
    {
      id: "state",
      header: "State",
      sortable: true,
      filter: {
        type: "single-select",
        options: Object.values(SLURM_JOB_STATE).map((s) => ({ value: s })),
      },
      render: (row) => (
        <div className="flex gap-3">
          <ApsRenderState state={row.state as string} />
          <ApsRenderAlertBadges
            anomalies={row.anomalies}
            reason={row.state === "PENDING" ? row.state_reason : undefined}
          />
        </div>
      ),
    },
    {
      id: "agent_analysis",
      header: "Troubleshooting",
      render: (row) =>
        row.agent_analysis && agentWebUrl ? (
          <a
            className="underline"
            href={`${agentWebUrl.replace(/\/+$/, "")}/agent/${encodeURIComponent(row.agent_analysis.session_id)}#final-rca`}
            target="_blank"
            rel="noopener noreferrer"
          >
            Agent Analysis
          </a>
        ) : (
          "--"
        ),
    },
  ];

  columns.push(
    {
      id: "duration",
      header: "Duration",
      sortable: false,
      render: (row) => {
        // touch getDuration so the helper stays referenced for parity with v1
        void getDuration(row);
        return <ApsDuration job={row} inSeconds={false} />;
      },
    },
    {
      id: "checkpoints",
      header: "Checkpoints",
      render: (row) => <ApsRenderCheckpoints job={row} />,
    },
    {
      id: "start_time",
      header: "Start Time",
      sortable: true,
      render: (row) => <ApsRenderTimestamp timestamp={row.start_time} />,
    },
    {
      id: "end_time",
      header: "End Time",
      sortable: true,
      render: (row) => <ApsRenderTimestamp timestamp={row.end_time} />,
    },
    {
      id: "submit_time",
      header: "Submit Time",
      sortable: true,
      render: (row) => <ApsRenderTimestamp timestamp={row.submit_time} />,
    },
    {
      id: "last_updated_at",
      header: "Last Update",
      sortable: true,
      render: (row) => <ApsRenderTimestamp timestamp={row.last_updated_at} />,
    },
  );

  return columns;
}
