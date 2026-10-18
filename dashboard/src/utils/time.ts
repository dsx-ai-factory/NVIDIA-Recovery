// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export function CalTimeDiffInSeconds(
  startTime: string,
  endTime: string,
): number {
  return (new Date(endTime).valueOf() - new Date(startTime).valueOf()) / 1000;
}

export function CalTimeDiffInDays(startTime: string, endTime: string): string {
  const seconds = CalTimeDiffInSeconds(startTime, endTime);
  const d = Math.floor(seconds / (3600 * 24));
  const m = Math.floor((seconds % 3600) / 60);
  const h = Math.floor((seconds % (3600 * 24)) / 3600);
  const s = Math.floor(seconds % 60);

  const dDisplay = d > 0 ? d + "D" : "";
  const hDisplay = h >= 10 ? h : `0${h}`;
  const mDisplay = m >= 10 ? m : `0${m}`;
  const sDisplay = s >= 10 ? s : `0${s}`;
  return `${dDisplay} ${hDisplay}:${mDisplay}:${sDisplay}`;
}
