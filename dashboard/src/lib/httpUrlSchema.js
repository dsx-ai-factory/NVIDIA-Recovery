// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { z } from "zod";

/** URL schema for server-to-server API calls and operator-facing links. */
export const httpUrlSchema = z
  .string()
  .url()
  .refine((value) => /^https?:\/\//i.test(value), {
    message: "URL must use HTTP or HTTPS",
  })
  .refine(
    (value) => {
      try {
        const url = new URL(value);
        return !url.search && !url.hash;
      } catch {
        return false;
      }
    },
    { message: "URL must not include a query string or fragment" },
  );
