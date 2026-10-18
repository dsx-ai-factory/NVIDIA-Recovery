// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export function GenRandomColors(size: number): string[] {
  return [...Array(size).keys()].map(
    () =>
      "#" + (0x1000000 + Math.random() * 0xffffff).toString(16).substring(1, 7),
  );
}
