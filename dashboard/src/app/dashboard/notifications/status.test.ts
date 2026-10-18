// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { notificationType } from "../../../gen/notifications/types";
import { notificationStatusFromType } from "./status";

describe("notificationStatusFromType", () => {
  it("maps warning and performance_alert to error", () => {
    expect(notificationStatusFromType("warning")).toBe("error");
    expect(notificationStatusFromType("performance_alert")).toBe("error");
  });

  it("maps info and optional to info", () => {
    expect(notificationStatusFromType("info")).toBe("info");
    expect(notificationStatusFromType("optional")).toBe("info");
  });

  // Guards against schema drift: every NotificationType must map to a valid CardStatus.
  it.each(
    Object.values(notificationType),
  )("returns a valid CardStatus for %s", (type) => {
    expect(["error", "info"]).toContain(notificationStatusFromType(type));
  });
});
