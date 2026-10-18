// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import { httpUrlSchema } from "./httpUrlSchema.js";

describe("httpUrlSchema", () => {
  it.each([
    "http://agent:8111",
    "https://agent.example.test/base",
  ])("accepts %s", (value) => {
    expect(httpUrlSchema.safeParse(value).success).toBe(true);
  });

  it.each([
    "ftp://agent.example.test",
    "data:text/plain,agent",
    "javascript:alert(1)",
    "not a url",
    "",
    "https://agent.example.test/base?tenant=av",
    "https://agent.example.test/base#sessions",
  ])("rejects %s", (value) => {
    expect(() => httpUrlSchema.safeParse(value)).not.toThrow();
    expect(httpUrlSchema.safeParse(value).success).toBe(false);
  });
});
