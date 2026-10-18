// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import "server-only";

import { env } from "~/env";

export const areMetricsEnabled = (): boolean => {
  return env.METRICS_ENABLED?.toLowerCase() === "true";
};
