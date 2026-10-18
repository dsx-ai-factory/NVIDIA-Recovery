// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/** Timestamp ordering choices exposed by the audit event UI. */
export const AUDIT_EVENT_SORT = {
  ascending: "event_produced_at:asc",
  descending: "event_produced_at:desc",
} as const;

/** One of the UI's supported timestamp ordering choices. */
export type AuditEventSort =
  (typeof AUDIT_EVENT_SORT)[keyof typeof AUDIT_EVENT_SORT];

/**
 * Sort events in a reverse chronological order unless the query param explicitly
 * requests chronological order.
 */
export function parseAuditEventSort(value?: string | null): AuditEventSort {
  return value === AUDIT_EVENT_SORT.ascending
    ? value
    : AUDIT_EVENT_SORT.descending;
}
