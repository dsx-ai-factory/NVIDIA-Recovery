// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";
import { useAutoRefresh } from "~/hooks/useAutoRefresh";

export function AutoRefresh({
  intervalSecondsOverride,
}: {
  intervalSecondsOverride?: number;
}) {
  useAutoRefresh(intervalSecondsOverride);
  return null;
}
