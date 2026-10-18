// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Flex, Tag, Text, Tooltip } from "@nvidia/foundations-react-core";
import { InfoCircle, Warning } from "@nvidia/react-gui-icons/icons";
import { format } from "date-fns";
import Link from "next/link";
import React, { type CSSProperties, type ReactNode, useState } from "react";
import { SLURM_JOB_ANOMALY, SLURM_JOB_STATE_REASON } from "~/const/slurm_job";
import type {
  JobAnomaly,
  NodeEvent,
  SlurmJobExecutionStatusOutput,
} from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { CalTimeDiffInDays, CalTimeDiffInSeconds } from "~/utils/time";
import { Datetime } from "./Datetime";

export function ApsRenderStateReason({
  reason,
}: {
  reason: string | null | undefined;
}) {
  switch (reason) {
    case SLURM_JOB_STATE_REASON.JobHeldAdmin:
    case SLURM_JOB_STATE_REASON.JobHeldUser:
      return (
        <Tag key={reason} color="yellow">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.Dependency:
      return (
        <Tag key={reason} color="green">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.Priority:
      return (
        <Tag key={reason} color="blue">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.Resources:
      return (
        <Tag key={reason} color="yellow">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.MaxNodePerAccount:
      return (
        <Tag key={reason} color="red">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.Prolog:
      return (
        <Tag key={reason} color="red">
          {reason}
        </Tag>
      );
    case SLURM_JOB_STATE_REASON.ReqNodeNotAvail:
      return (
        <Tag key={reason} color="red">
          {reason}
        </Tag>
      );
    default:
      return (
        <Tag key={reason} color="gray">
          {reason}
        </Tag>
      );
  }
}

function ApsRenderAnomalyTag({
  anomaly,
  children,
}: {
  anomaly: JobAnomaly;
  children: React.ReactNode;
}) {
  return (
    <Tooltip
      slotContent={
        <div className="flex max-w-[300px] flex-col gap-3">
          <div>Anomaly Type: {anomaly.anomaly_type}</div>
          <div>
            Timestamp: {format(new Date(anomaly.timestamp), "MMM dd, yyyy")}
          </div>
          <div>UUID: {anomaly.uuid ?? "N/A"}</div>
          <div>Handled By: {anomaly.handled_by ?? "N/A"}</div>
        </div>
      }
    >
      <Tag
        className="foobar1"
        key={anomaly.anomaly_type}
        color="yellow"
        kind="solid"
      >
        {children}
      </Tag>
    </Tooltip>
  );
}

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

function getAnomalyTypeDisplayName(anomaly: JobAnomaly | undefined) {
  if (!anomaly?.anomaly_type) return "";
  const anomalyType = anomaly.anomaly_type;
  return anomalyType in anomalyTypeDisplayName
    ? anomalyTypeDisplayName[anomalyType]
    : anomalyType;
}

export function ApsRenderAnomaly({
  anomaly,
}: {
  anomaly: JobAnomaly | undefined;
}) {
  if (!anomaly?.anomaly_type) return <></>;
  return (
    <ApsRenderAnomalyTag anomaly={anomaly}>
      {getAnomalyTypeDisplayName(anomaly)}
    </ApsRenderAnomalyTag>
  );
}

export function ApsRenderAttribution({
  errorAttribution,
  timestamp,
}: {
  errorAttribution: NodeEvent["error_attribution"] | undefined;
  timestamp: string | undefined;
}) {
  if (!errorAttribution) return <></>;
  return (
    <Tooltip slotContent={`${errorAttribution}@${timestamp}`}>
      <Tag key={errorAttribution} color="red" kind="solid">
        {errorAttribution}
      </Tag>
    </Tooltip>
  );
}

export function ApsRenderOriginalExclusion({
  originalExclusion,
}: {
  originalExclusion: NodeEvent["original_exclusion_type"] | undefined;
}) {
  if (!originalExclusion) return <></>;
  return (
    <Tooltip slotContent={`${originalExclusion}`}>
      <Tag key={originalExclusion} color="red" kind="solid">
        {originalExclusion}
      </Tag>
    </Tooltip>
  );
}

export function ApsRenderTimestamp({
  timestamp = undefined,
}: {
  timestamp?: string | null | undefined;
}) {
  if (!timestamp) return <ApsRenderText />;
  return (
    <ApsRenderText>
      <Datetime utcIso={timestamp} />
    </ApsRenderText>
  );
}

export function ApsRenderDuration({
  job,
  inSeconds = false,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
  inSeconds?: boolean;
}) {
  const [showInSeconds, setShowInSeconds] = useState<boolean>(inSeconds);
  if (!job?.start_time) return <ApsRenderText>N/A</ApsRenderText>;
  const start_time = job.start_time;
  const end_time = job.end_time ? job.end_time : new Date().toLocaleString();
  if (start_time === end_time) return <ApsRenderText>0</ApsRenderText>;
  return (
    <ApsRenderText
      className="truncate whitespace-pre"
      onClick={() => {
        setShowInSeconds(!showInSeconds);
      }}
      tips={`${start_time} - ${end_time}`}
    >
      {showInSeconds
        ? CalTimeDiffInSeconds(start_time, end_time)
        : CalTimeDiffInDays(start_time, end_time)}
    </ApsRenderText>
  );
}

export function ApsRenderCheckpoints({
  job,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
}) {
  if (!job?.checkpoints) return <ApsRenderText>0</ApsRenderText>;
  return (
    <ApsRenderText
      tips={
        <>
          Last Checkpoint:{" "}
          <Datetime
            utcIso={job.checkpoints[job.checkpoints.length - 1]?.timestamp}
          />
        </>
      }
    >
      {job.checkpoints?.length}
    </ApsRenderText>
  );
}

export function ApsRenderJobID({
  job,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
}) {
  if (!job) return <ApsRenderText />;
  return (
    <ApsRenderLink
      href={`/dashboard/workflow/job/${job.job_id}/${job.job_attempt_index}`}
    >
      {`${job.job_id} (${job.job_attempt_index})`}
    </ApsRenderLink>
  );
}

export function ApsRenderJobName({
  job,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
}) {
  if (!job) return <ApsRenderText />;
  return (
    <ApsRenderLink href={`/dashboard/workflow?query=${job.job_name}`}>
      {job.job_name}
    </ApsRenderLink>
  );
}

function ApsRenderLink({
  href,
  style = undefined,
  children = undefined,
}: {
  href: string;
  style?: CSSProperties | undefined;
  children?: ReactNode;
}) {
  return (
    <Flex
      style={{
        justifyContent: "left",
      }}
    >
      <Link href={href} key={href} style={style}>
        <Text
          style={{
            textDecoration: "underline",
            textDecorationColor: "darkblue",
            ...style,
          }}
        >
          {children}
        </Text>
      </Link>
    </Flex>
  );
}

function ApsRenderText({
  className,
  tips = undefined,
  children = undefined,
  onClick = undefined,
}: {
  className?: string;
  tips?: ReactNode;
  children?: ReactNode;
  onClick?: () => void;
}) {
  const { theme } = useUserPreferences();
  const darkMode = theme === "dark";
  const color = darkMode ? "#90de32" : "#699730";
  if (!children) {
    return <Text className={className}>N/A</Text>;
  }

  if (!tips)
    return (
      <Flex
        style={{
          justifyContent: "left",
        }}
      >
        <Text
          className={className}
          onClick={onClick}
          style={{
            color: color,
          }}
        >
          {children}
        </Text>
      </Flex>
    );

  return (
    <Flex
      style={{
        justifyContent: "left",
      }}
    >
      <Tooltip slotContent={tips}>
        <Text
          className={className}
          onClick={onClick}
          style={{
            color: color,
          }}
        >
          {children}
        </Text>
      </Tooltip>
    </Flex>
  );
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
            {count}
          </div>
        </Tooltip>
      )}
    </>
  );
}

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

/** Displays the job reason or anomaly count without attribution enrichment. */
export function ApsRenderAlertBadges({
  anomalies,
  reason,
}: {
  anomalies?: JobAnomaly[] | null | undefined;
  reason: string | null | undefined;
}) {
  if (reason) {
    return reason !== "None" ? (
      <ApsRenderAlertBadge
        className="text-purple-500"
        count={1}
        slotIcon={<InfoCircle />}
        tips={reason}
      />
    ) : (
      <></>
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

export function ApsRenderAttributionExclusionType({
  exclusionType,
}: {
  exclusionType: string;
}) {
  const getExclusionTypeDescription = (exclusionType: string): string => {
    if (exclusionType === "PERMANENT") {
      return "This node will be excluded from all future job runs.";
    } else if (exclusionType === "TEMPORARY") {
      return "This node will be excluded from the next job run. Afterward, it will either become available again or be excluded permanently, depending on the result of the next job.";
    } else {
      return "The node encountered an error, but it has not been excluded and remains available for job runs.";
    }
  };
  return (
    <Tooltip slotContent={getExclusionTypeDescription(exclusionType)}>
      <Tag key={exclusionType} color="red" kind="solid">
        {exclusionType}
      </Tag>
    </Tooltip>
  );
}

export function ApsRenderAttributionType({
  attributionType,
}: {
  attributionType: string;
}) {
  const getAttributionTypeDescription = (type: string): string => {
    if (type === "FAST") {
      return "Uses immediate signals such as application logs available at the time the anomaly was detected for quick error attribution.";
    } else if (type === "SLOW") {
      return "Performs a more thorough analysis using additional signals that become available after the job ends, such as SLURM epilog data and health checks.";
    } else {
      return "";
    }
  };
  return (
    <Tooltip
      className="max-w-[300px]"
      slotContent={getAttributionTypeDescription(attributionType)}
    >
      <Tag color="blue" kind="solid">
        {attributionType}
      </Tag>
    </Tooltip>
  );
}

export function ApsRenderAttributionSource({
  attributionSource,
}: {
  attributionSource: string;
}) {
  return (
    <Tag color="blue" kind="solid">
      {attributionSource}
    </Tag>
  );
}

export function ApsRenderGlobalEventAttribution({
  errorAttribution,
  timestamp,
}: {
  errorAttribution: string | undefined;
  timestamp: string;
}) {
  if (!errorAttribution) {
    return <></>;
  }
  return (
    <Tooltip slotContent={`${errorAttribution}@${timestamp}`}>
      <Tag key={errorAttribution} color="red" kind="solid">
        {errorAttribution}
      </Tag>
    </Tooltip>
  );
}
