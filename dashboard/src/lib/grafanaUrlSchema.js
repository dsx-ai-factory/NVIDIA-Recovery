// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { z } from "zod";

/** Validate Grafana links before exposing operator configuration to the browser. */
export const grafanaUrlSchema = z
  .string()
  .url()
  .refine(
    (value) => {
      try {
        const url = new URL(value);
        return (
          ["http:", "https:"].includes(url.protocol) &&
          !url.username &&
          !url.password
        );
      } catch {
        return false;
      }
    },
    {
      message:
        "Grafana URL must use HTTP or HTTPS without embedded credentials",
    },
  );
