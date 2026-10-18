// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import pino from "pino";

export const logger =
  process.env.NODE_ENV === "production"
    ? // JSON in production
      pino({ level: process.env.LOGGING_LEVEL ?? "info" })
    : // Pretty print in development
      pino({
        transport: {
          target: "pino-pretty",
          options: {
            colorize: true,
          },
        },
        level: "debug",
      });
