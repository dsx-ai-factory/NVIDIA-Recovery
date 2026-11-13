// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const placeholders = /\{(trace_id|span_id|recovery_sequence_id)\}/g;

/** Validate an operator-owned destination; event data may only fill URL path/query/fragment values.
 * @param {unknown} value
 * @returns {value is string}
 */
export function isTraceViewerTemplate(value) {
  if (typeof value !== "string" || !value.includes("{trace_id}")) return false;
  if (/[\s\\"'<>]/.test(value)) return false;
  const substituted = value.replace(placeholders, "0123456789abcdef");
  if (/[{}]/.test(substituted)) return false;
  try {
    const url = new URL(substituted);
    const authority = value.match(/^https?:\/\/([^/?#]+)/i)?.[1];
    return Boolean(
      authority &&
        !/[{}]/.test(authority) &&
        !url.username &&
        !url.password &&
        ["http:", "https:"].includes(url.protocol),
    );
  } catch {
    return false;
  }
}

/** Build navigation only from explicit, valid historical correlation; legacy correlation_uuid is unrelated.
 * @param {unknown} template
 * @param {unknown} payload
 * @returns {string | undefined}
 */
export function auditTraceUrl(template, payload) {
  if (
    !isTraceViewerTemplate(template) ||
    !payload ||
    typeof payload !== "object"
  )
    return undefined;
  const { trace_id, span_id, recovery_sequence_id } =
    /** @type {Record<string, unknown>} */ (payload);
  if (
    typeof trace_id !== "string" ||
    !/^[0-9a-f]{32}$/i.test(trace_id) ||
    /^0+$/.test(trace_id)
  )
    return undefined;
  /** @type {Record<string, string | undefined>} */
  const values = {
    trace_id,
    span_id:
      typeof span_id === "string" &&
      /^[0-9a-f]{16}$/i.test(span_id) &&
      !/^0+$/.test(span_id)
        ? span_id
        : undefined,
    recovery_sequence_id:
      typeof recovery_sequence_id === "string" &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        recovery_sequence_id,
      )
        ? recovery_sequence_id
        : undefined,
  };
  let missing = false;
  const url = template.replace(placeholders, (_, name) => {
    if (!values[name]) missing = true;
    return encodeURIComponent(values[name] ?? "");
  });
  return missing ? undefined : url;
}
