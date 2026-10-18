// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Text, Tooltip } from "@nvidia/foundations-react-core";
import type { ReactNode } from "react";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { CalTimeDiffInDays, CalTimeDiffInSeconds } from "~/utils/time";
import { Datetime } from "./Datetime";

function ApsText({
  className,
  tips = undefined,
  children = undefined,
  onClick = undefined,
  color = undefined,
}: {
  className?: string;
  tips?: ReactNode;
  children?: number | string;
  onClick?: () => void;
  color?: string;
}) {
  return tips ? (
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
  ) : (
    <Text
      className={className}
      onClick={onClick}
      style={{
        color: color,
      }}
    >
      {children}
    </Text>
  );
}

export function getDuration(
  startTime: string | undefined,
  endTime: string,
  inSeconds: boolean,
) {
  if (!startTime) {
    return "N/A";
  } else if (startTime === endTime) {
    return 0;
  }
  return inSeconds
    ? CalTimeDiffInSeconds(startTime, endTime)
    : CalTimeDiffInDays(startTime, endTime);
}

function getTips(
  startTime: string | null | undefined,
  endTime: string | null | undefined,
) {
  if (!startTime || !endTime || startTime === endTime) {
    return undefined;
  }
  return (
    <>
      <Datetime utcIso={startTime} /> - <Datetime utcIso={endTime} />
    </>
  );
}

export function ApsDuration({
  job,
  inSeconds = false,
  onClick = undefined,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
  inSeconds?: boolean;
  onClick?: () => void;
}) {
  const { start_time, end_time } = job ?? {};
  const { theme } = useUserPreferences();
  const color = theme === "dark" ? "#90de32" : "#699730";
  const FALLBACK_DURATION = "N/A";

  let duration: string | number = FALLBACK_DURATION;

  if (start_time && end_time) {
    const durationAsTimestamp = getDuration(start_time, end_time, false);
    const durationAsSeconds = getDuration(start_time, end_time, true);

    duration =
      (durationAsSeconds as number) < 0
        ? FALLBACK_DURATION
        : inSeconds
          ? durationAsSeconds
          : durationAsTimestamp;
  }

  return (
    <ApsText
      className="truncate whitespace-pre"
      color={duration === FALLBACK_DURATION ? undefined : color}
      tips={getTips(start_time, end_time)}
      onClick={onClick}
    >
      {duration}
    </ApsText>
  );
}
