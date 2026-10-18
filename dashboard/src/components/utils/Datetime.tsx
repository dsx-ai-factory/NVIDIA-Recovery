// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Tooltip } from "@nvidia/foundations-react-core";
import { useUserPreferences } from "~/provider/userPreferencesProvider";

const UTC_REGEX =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;

const LOCAL_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-US", {
  year: "2-digit",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
  hour12: true,
  // timeZone defaults to the user's local zone
  // timeZoneName: 'short',
});

function isUtcFormat(utcIso: string | null | undefined): boolean {
  return utcIso ? UTC_REGEX.test(utcIso) : false;
}

function removeFractionalSeconds(
  utcIso: string | undefined,
): string | undefined {
  return utcIso ? utcIso.replace(/\.\d+/, "") : undefined;
}

export function epochMicrosecondsToIso(epochMicroseconds: number): string {
  return new Date(epochMicroseconds / 1000).toISOString();
}

export function utcToLocal(
  utcIso: string | null | undefined,
  timezone: "utc" | "system",
): string | undefined {
  const isUtc = isUtcFormat(utcIso);

  if (utcIso && isUtc && timezone === "system") {
    // Remove fractional seconds
    const utcIsoNonFractional = removeFractionalSeconds(utcIso)!;
    const date = new Date(utcIsoNonFractional);
    const localDatetime = LOCAL_DATE_TIME_FORMATTER.format(date);
    return localDatetime;
  }

  return utcIso ? removeFractionalSeconds(utcIso) : undefined;
}

// Renders a utc iso datetime in the user's local timezone
export function Datetime({
  utcIso,
  tooltipDisabled = false,
}: {
  utcIso?: string;
  tooltipDisabled?: boolean;
}) {
  const { timezone } = useUserPreferences();
  const DISPLAY_TIMEZONE = {
    utc: "UTC",
    system: "local time",
  };

  if (!utcIso) {
    return null;
  }

  return (
    <Tooltip
      slotContent={`Date displayed as ${DISPLAY_TIMEZONE[timezone]}. To change this update your preferences in the main menu.`}
      className="max-w-sm"
      disabled={tooltipDisabled}
    >
      <span>{utcToLocal(utcIso, timezone)}</span>
    </Tooltip>
  );
}
