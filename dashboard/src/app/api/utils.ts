// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export function toQueryParamString(
  params: Record<string, string | number | boolean | undefined | null>,
): string {
  return Object.entries(params)
    .filter(
      ([, value]) => value !== undefined && value !== null && value !== "",
    )
    .reduce((params, [key, value]) => {
      params.append(key, String(value));
      return params;
    }, new URLSearchParams())
    .toString();
}
