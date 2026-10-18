// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { NotificationTypeKey } from "~/gen/notifications/types";

export type CardStatus = "error" | "info";

/** Map a notification's severity to the card's status icon. */
export function notificationStatusFromType(
  type: NotificationTypeKey,
): CardStatus {
  switch (type) {
    case "warning":
    case "performance_alert":
      return "error";
    case "info":
    case "optional":
      return "info";
    default: {
      // Exhaustiveness guard: adding a NotificationType member without handling
      // it here becomes a compile-time error instead of a silent fall-through.
      const _exhaustive: never = type;
      return _exhaustive;
    }
  }
}
