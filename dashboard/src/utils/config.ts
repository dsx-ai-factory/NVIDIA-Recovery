// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { env } from "~/env";

export const isApiKeysEnabled = () => {
  return env.API_KEYS_ENABLED === "true";
};

export const isJobNodeAnalysisEnabled = () => {
  return env.JOB_NODE_ANALYSIS_ENABLED === "true";
};

export const isJobGroupsEnabled = () => {
  return env.JOB_GROUPS_ENABLED === "true";
};

export const isGrafanaTraceUrlEnabled = () => {
  return env.GRAFANA_TRACE_URL_ENABLED === "true";
};

export const getGrafanaDataSourceId = () => {
  return env.GRAFANA_DATA_SOURCE_ID;
};

/** Controls audit UI visibility independently of audit production and destinations. */
export const isAuditEventsEnabled = () => {
  return (env.AUDIT_EVENTS_ENABLED ?? "true") === "true";
};
