// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Tooltip } from "@nvidia/foundations-react-core";
import { InfoCircle, Warning } from "@nvidia/react-gui-icons/icons";
import { SLURM_JOB_ANOMALY } from "~/const/slurm_job";
import type { JobAnomaly } from "~/gen/job-manager/types";

const anomalyTypeDisplayName: Record<string, string> = {
  [SLURM_JOB_ANOMALY.CHECKPOINTLOAD]: "Checkpoint Load",
  [SLURM_JOB_ANOMALY.CHECKPOINTSAVE]: "Checkpoint Save",
  [SLURM_JOB_ANOMALY.CRASH]: "Crash",
  [SLURM_JOB_ANOMALY.CRASHAPPEXCEPTION]: "Crash",
  [SLURM_JOB_ANOMALY.CRASHOOM]: "Crash",
  [SLURM_JOB_ANOMALY.CRASHSEGV]: "Crash",
  [SLURM_JOB_ANOMALY.FLTINJECTION]: "Flt Injection",
  [SLURM_JOB_ANOMALY.FLTINJECTIONINFRA]: "Flt Injection",
  [SLURM_JOB_ANOMALY.HANG]: "Hang",
  [SLURM_JOB_ANOMALY.HANGAPP]: "Hang",
  [SLURM_JOB_ANOMALY.HANGCOMM]: "Hang",
  [SLURM_JOB_ANOMALY.HANGPUSHRECV]: "Hang",
  [SLURM_JOB_ANOMALY.HANGNOOUTPUT]: "No Output",
  [SLURM_JOB_ANOMALY.LOWTHROUGHPUT]: "LowThroughput",
  [SLURM_JOB_ANOMALY.LOWTHROUGHPUTEWNET]: "LowThroughput",
  [SLURM_JOB_ANOMALY.LOWTHROUGHPUTGPU]: "LowThroughput",
  [SLURM_JOB_ANOMALY.LOWTHROUGHPUTNSNET]: "LowThroughput",
  [SLURM_JOB_ANOMALY.LOWTHROUGHPUTSTORAGE]: "LowThroughput",
  [SLURM_JOB_ANOMALY.STRAGGLER]: "Straggler",
  [SLURM_JOB_ANOMALY.END]: "End",
  [SLURM_JOB_ANOMALY.UNKNOWN]: "Unknown",
};

function getTooltip(
  items: (string | undefined)[] | undefined,
  limit: number,
  unit: string,
) {
  if (!items?.length) return "";
  const list = items.slice(0, limit).join(", ");
  return items.length > limit
    ? `${list}, and ${items.length - limit} more ${unit}`
    : list;
}

function getAnomalyTypeDisplayName(anomaly: JobAnomaly | undefined) {
  if (!anomaly?.anomaly_type) return "";
  const anomalyType = anomaly.anomaly_type;
  return anomalyType in anomalyTypeDisplayName
    ? anomalyTypeDisplayName[anomalyType]
    : anomalyType;
}

function ApsRenderAlertBadge({
  className,
  count,
  slotIcon,
  tips,
}: {
  className?: string;
  count: number;
  slotIcon: React.ReactNode;
  tips: string;
}) {
  return (
    <>
      {count > 0 && (
        <Tooltip slotContent={tips}>
          <div className={`flex items-center gap-1 ${className}`}>
            {slotIcon}
            <span style={{ textDecoration: "none" }}>{count}</span>
          </div>
        </Tooltip>
      )}
    </>
  );
}

/** Displays the job reason or anomaly count without attribution enrichment. */
export function ApsRenderAlertBadges({
  anomalies,
  reason,
}: {
  anomalies: JobAnomaly[] | undefined;
  reason: string | undefined;
}) {
  if (reason && reason !== "None") {
    return (
      <ApsRenderAlertBadge
        className="text-purple-500"
        count={1}
        slotIcon={<InfoCircle />}
        tips={reason ?? ""}
      />
    );
  }

  const numAnomalies = anomalies?.length ?? 0;
  const tipLimit = 3; // maximum number of anomalies shown in tooltip
  const anomalyTypes =
    anomalies?.map((anomaly: JobAnomaly) =>
      getAnomalyTypeDisplayName(anomaly),
    ) ?? [];
  const anomalyTip = getTooltip(
    anomalyTypes,
    tipLimit,
    numAnomalies === 1 ? "anomaly" : "anomalies",
  );

  return (
    <ApsRenderAlertBadge
      className="text-yellow-500"
      count={numAnomalies}
      slotIcon={<Warning />}
      tips={anomalyTip}
    />
  );
}
