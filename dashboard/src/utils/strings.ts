// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export const areAllStringsValid = (inputs: (string | undefined)[]): boolean => {
  return inputs.every(
    (value) => typeof value === "string" && value.trim().length > 0,
  );
};

export function pluralize(count: number, word: string): string {
  return count === 1 ? `${count} ${word}` : `${count} ${word}s`;
}
