// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { logger } from "~/pino";

export async function register() {
  logger.info("Registering instrumentation");

  if (process.env.NEXT_RUNTIME === "nodejs") {
    await import("~/server/prometheus");
  }
}
