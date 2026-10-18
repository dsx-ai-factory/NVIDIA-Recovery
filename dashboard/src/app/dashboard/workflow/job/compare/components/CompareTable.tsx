// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Combobox,
  TableBody,
  TableDataCell,
  TableHeaderCell,
  TableRoot,
  TableRow,
} from "@nvidia/foundations-react-core";
import React from "react";
import { ApsRenderState } from "~/components/utils/ApsRenderState";
import {
  ApsRenderAnomaly,
  ApsRenderCheckpoints,
  ApsRenderDuration,
  ApsRenderJobID,
  ApsRenderJobName,
  ApsRenderStateReason,
  ApsRenderTimestamp,
} from "~/components/utils/apsJobRender";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { useGetJobs } from "../../../hooks";
import { IntersectionNodesListCell, JobCellProvider } from "./cells";

const HEADERS: {
  label: string;
  render: (
    data: SlurmJobExecutionStatusOutput,
    ctx: { commonNodes: string[] | undefined },
  ) => React.ReactNode;
}[] = [
  {
    label: "ID",
    render: (job) => <ApsRenderJobID job={job} />,
  },
  {
    label: "Job Name",
    render: (job) => <ApsRenderJobName job={job} />,
  },
  {
    label: "State",
    render: (job) => <ApsRenderState state={job.state} />,
  },
  {
    label: "State Reason",
    render: (job) => <ApsRenderStateReason reason={job.state_reason} />,
  },
  {
    label: "Anomalies",
    render: (job) => (
      <div className="grid grid-cols-3 gap-1">
        {job.anomalies?.map((anomaly) => (
          <ApsRenderAnomaly key={anomaly.uuid} anomaly={anomaly} />
        ))}
      </div>
    ),
  },
  {
    label: "Node Count",
    render: (job) => job.node_count,
  },
  {
    label: "GPU Count",
    render: (job) => job.total_gpu_count,
  },
  {
    label: "Duration",
    render: (job) => <ApsRenderDuration job={job} inSeconds={true} />,
  },
  {
    label: "Nodes List",
    render: (job, { commonNodes }) => (
      <IntersectionNodesListCell job={job} commonNodes={commonNodes} />
    ),
  },
  {
    label: "Checkpoints",
    render: (job) => <ApsRenderCheckpoints job={job} />,
  },
  {
    label: "Last Updated",
    render: (job) => <ApsRenderTimestamp timestamp={job?.last_updated_at} />,
  },
];

export function CompareTable({
  jobCompoundKeys,
}: {
  jobCompoundKeys: string[];
}) {
  const [desiredFields, setDesiredFields] = React.useState(
    HEADERS.map((row) => row.label),
  );
  const { data } = useGetJobs({
    jobKeys: jobCompoundKeys.map((x) => {
      const [jobId, attemptIndex] = x.split("_");
      if (jobId === undefined || attemptIndex === undefined) {
        console.error("Invalid jobCompoundKey: ", x, { jobId, attemptIndex });
      }
      return { jobId: jobId!, attemptIndex: attemptIndex! };
    }),
  });

  const jobs = data?.statuses;

  const commonNodes = React.useMemo(() => {
    const nodeLists = jobs
      ?.map((job) => job.allocated_node_list!)
      .filter((nodes) => nodes && nodes.length > 0);
    if (!nodeLists || nodeLists.length === 0) {
      return [];
    }
    return nodeLists.reduce((acc, curr) =>
      acc.filter((node) => curr.includes(node)),
    );
  }, [jobs]);

  return (
    <div className="flex w-full flex-col gap-4">
      <Combobox
        items={HEADERS.map((row) => ({
          children: row.label,
          filterValue: row.label,
          value: row.label,
        }))}
        multiple
        value={desiredFields}
        onValueChange={(newValue: string[]) =>
          setDesiredFields(newValue as string[])
        }
      />
      <TableRoot className="w-full">
        <TableBody>
          {HEADERS.map(
            (header) =>
              desiredFields.includes(header.label) && (
                <TableRow key={header.label}>
                  <TableHeaderCell>{header.label}</TableHeaderCell>
                  {jobCompoundKeys.map((jobCompoundKey) => (
                    <TableDataCell key={`${header.label}-${jobCompoundKey}`}>
                      <JobCellProvider jobCompoundKey={jobCompoundKey}>
                        {(data) => header.render(data, { commonNodes })}
                      </JobCellProvider>
                    </TableDataCell>
                  ))}
                </TableRow>
              ),
          )}
        </TableBody>
      </TableRoot>
    </div>
  );
}
