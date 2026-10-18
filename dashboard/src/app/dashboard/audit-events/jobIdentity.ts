// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/** A scheduler job filter, optionally narrowed to one execution attempt. */
export interface JobAttemptIdentity {
  jobId: string;
  attemptIndex?: number;
}

const URL_NAMESPACE = Uint8Array.from([
  0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f,
  0xd4, 0x30, 0xc8,
]);

export const JOB_ATTEMPT_FORMAT_ERROR =
  "Enter a job ID, optionally followed by / and a non-negative attempt index.";

/** Parse a non-negative decimal attempt index without JavaScript number coercion. */
export function parseAttemptIndex(value: unknown): number | undefined {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    return undefined;
  }

  const attemptIndex = Number(value);
  return Number.isSafeInteger(attemptIndex) ? attemptIndex : undefined;
}

/** Parse a job ID or the user-facing `<job_id>/<attempt_index>` notation. */
export function parseJobAttempt(value: string): JobAttemptIdentity | undefined {
  if (value.trim() === "") {
    return undefined;
  }
  const separator = value.lastIndexOf("/");
  if (separator === -1) {
    return { jobId: value.trim() };
  }
  if (separator === 0) {
    return undefined;
  }

  const jobId = value.slice(0, separator).trim();
  const attemptText = value.slice(separator + 1).trim();
  if (jobId === "") {
    return undefined;
  }

  const attemptIndex = parseAttemptIndex(attemptText);
  if (attemptIndex === undefined) {
    return undefined;
  }
  return { jobId, attemptIndex };
}

/** Return a user-facing error when a non-empty scheduler job identity is malformed. */
export function jobAttemptValidationError(value: string): string | undefined {
  if (value.trim() === "" || parseJobAttempt(value)) {
    return undefined;
  }
  return JOB_ATTEMPT_FORMAT_ERROR;
}

/** Format scheduler job identity using the audit filter notation. */
export function formatJobAttempt(
  jobId: string | null | undefined,
  attemptIndex: number | null | undefined,
): string {
  if (!jobId) {
    return "";
  }
  if (attemptIndex === null || attemptIndex === undefined) {
    return jobId;
  }
  return `${jobId}/${attemptIndex}`;
}

/** Generate the canonical UUIDv5 Heimdall identifier for a scheduler job attempt. */
export async function generateHeimdallJobId(
  jobId: string,
  attemptIndex: number,
): Promise<string> {
  const name = new TextEncoder().encode(`${jobId}-${attemptIndex}`);
  const input = new Uint8Array(URL_NAMESPACE.length + name.length);
  input.set(URL_NAMESPACE);
  input.set(name, URL_NAMESPACE.length);

  const digest = new Uint8Array(
    await globalThis.crypto.subtle.digest("SHA-1", input),
  );
  const bytes = digest.slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;

  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
  return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10).join("")}`;
}
