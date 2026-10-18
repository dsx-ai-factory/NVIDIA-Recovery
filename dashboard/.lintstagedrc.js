// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import path from "node:path";
import { fileURLToPath } from "node:url";

const dashboardDir = path.dirname(fileURLToPath(import.meta.url));

/**
 * lint-staged passes paths relative to the git repository root. When the repo root is the
 * monorepo (e.g. nvidia-recovery/), strip the `dashboard/` prefix so arguments are valid for
 * `pnpm --dir <dashboard>` + Biome.
 */
const toDashboardRelative = (file) => {
  const normalized = file.replace(/\\/g, "/");
  const prefix = "dashboard/";
  return normalized.startsWith(prefix)
    ? normalized.slice(prefix.length)
    : normalized;
};

export default {
  "*": (filenames) => {
    if (filenames.length === 0) {
      return [];
    }
    const relFiles = filenames.map(toDashboardRelative);
    const args = relFiles.map((f) => `"${f.replace(/"/g, '\\"')}"`).join(" ");
    return `pnpm --dir "${dashboardDir}" exec biome check --write --files-ignore-unknown=true --no-errors-on-unmatched ${args}`;
  },
};
