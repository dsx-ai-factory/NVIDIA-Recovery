// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { env } from "~/env";
import { resolveAuthConfiguration } from "~/server/auth-config";

export function getAuthConfiguration() {
  return resolveAuthConfiguration(env);
}

export function isAuthAvailable(): boolean {
  return getAuthConfiguration().available;
}
