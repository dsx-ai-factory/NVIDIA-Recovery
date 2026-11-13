// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  auditTraceUrl,
  isTraceViewerTemplate,
} from "../../../../lib/trace-viewer.js";
import { AuditTraceLink, TraceViewerProvider } from "./AuditTraceLink";

const trace_id = "0123456789abcdef0123456789abcdef";
const span_id = "0123456789abcdef";
const recovery_sequence_id = "81d63765-7518-472e-bc9d-ff7f9f18e425";
const template =
  "https://traces.example.test/view/{trace_id}?span={span_id}&sequence={recovery_sequence_id}";

function renderEvent(
  payload: unknown,
  destination: string | undefined = template,
) {
  return renderToStaticMarkup(
    <TraceViewerProvider template={destination}>
      <article>
        Recovery event
        <AuditTraceLink payload={payload} />
      </article>
    </TraceViewerProvider>,
  );
}

describe("audit trace navigation", () => {
  it("links explicit historical identifiers to the operator destination", () => {
    const html = renderEvent({ trace_id, span_id, recovery_sequence_id });
    expect(html).toContain(
      `/view/${trace_id}?span=${span_id}&amp;sequence=${recovery_sequence_id}`,
    );
    expect(html).toContain('rel="noopener noreferrer"');
    expect(html).toContain('target="_blank"');
  });

  it.each([
    null,
    {},
    { correlation_uuid: trace_id },
    { trace_id: "0".repeat(32) },
    { trace_id: "javascript:alert(1)" },
    { trace_id },
    { trace_id, span_id: "0".repeat(16), recovery_sequence_id },
  ])("keeps events visible when required correlation is absent or invalid: %j", (payload) => {
    expect(renderEvent(payload)).toBe("<article>Recovery event</article>");
  });

  it("needs neither a sequence nor a span when linking only a trace", () => {
    expect(
      auditTraceUrl("https://traces.example.test/{trace_id}", { trace_id }),
    ).toBe(`https://traces.example.test/${trace_id}`);
  });

  it("does not require a destination for audit visibility", () => {
    expect(renderEvent({ trace_id }, "")).toBe(
      "<article>Recovery event</article>",
    );
  });

  it.each([
    "javascript:{trace_id}",
    "//traces.test/{trace_id}",
    "https://{trace_id}.test/view",
    "https://user:secret@traces.test/{trace_id}",
    "https://traces.test/{unknown}/{trace_id}",
    "https://traces.test/{trace_id}\nOTHER=value",
    "https://traces.test/trace",
    "https://traces.test\\@evil.test/{trace_id}",
  ])("rejects unsafe or ambiguous destinations: %s", (destination) => {
    expect(isTraceViewerTemplate(destination)).toBe(false);
    expect(
      renderEvent({ trace_id, span_id, recovery_sequence_id }, destination),
    ).toBe("<article>Recovery event</article>");
  });
});
