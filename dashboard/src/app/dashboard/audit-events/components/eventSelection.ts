// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { AuditEvent } from "~/gen/notifications/types";

const AUDIT_EVENT_ROW_SELECTOR = "[data-audit-event-row]";

/** Selects a new audit event, or clears the selection when it is clicked again. */
export function getNextSelectedAuditEvent(
  selectedEvent: AuditEvent | null,
  clickedEvent: AuditEvent,
): AuditEvent | null {
  return selectedEvent === clickedEvent ? null : clickedEvent;
}

/** Reports whether an outside-panel interaction belongs to an event row. */
export function isAuditEventRowInteraction(
  target: EventTarget | null,
): boolean {
  return (
    target instanceof Element &&
    target.closest(AUDIT_EVENT_ROW_SELECTOR) !== null
  );
}
