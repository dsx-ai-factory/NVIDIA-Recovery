// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { env } from "~/env";

export const getApiGatewayUrl = () => {
  return env.API_GATEWAY_URL;
};

export const getJobManagerHost = () => {
  return env.JOB_MANAGER_HOST;
};

export const getAgentApiUrl = () => {
  return env.AGENT_API_URL;
};

export const getAgentWebUrl = () => {
  return env.AGENT_WEB_URL;
};

export const getNotificationsHost = () => {
  return env.NOTIFICATIONS_HOST;
};
