// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";
import { env } from "~/env";
import { isAuthAvailable } from "~/utils/auth";

export async function isAuthAvailableAsync() {
  return isAuthAvailable();
}

export async function isDatadogEnabledAsync() {
  return env.DATADOG_ENABLED.toUpperCase() === "TRUE";
}

export async function isFACTEnabledAsync() {
  return env.FACT_ENABLED.toUpperCase() === "TRUE";
}

export async function getFactUrl() {
  return env.FACT_URL;
}

export async function getCockpitNameAsync() {
  return env.COCKPIT_NAME ?? "NVIDIA Recovery";
}
