// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { createContext, type ReactNode, useContext } from "react";
import { auditTraceUrl } from "../../../../lib/trace-viewer.js";

const TraceViewerContext = createContext<string | undefined>(undefined);

/** Supply the public operator-selected destination at runtime to both audit views. */
export function TraceViewerProvider({
  template,
  children,
}: {
  template?: string;
  children: ReactNode;
}) {
  return (
    <TraceViewerContext.Provider value={template}>
      {children}
    </TraceViewerContext.Provider>
  );
}

/** Navigate to a historical trace without querying mutable Advice or checking trace retention. */
export function AuditTraceLink({ payload }: { payload: unknown }) {
  const url = auditTraceUrl(useContext(TraceViewerContext), payload);
  if (!url) return null;
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="ml-2 text-green-700 underline"
      title="Open trace; it may be unavailable if export failed or retention expired."
      onClick={(event) => event.stopPropagation()}
    >
      View trace
    </a>
  );
}
