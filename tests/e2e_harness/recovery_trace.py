# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exact-attempt recovery evidence from Advice, Tempo, and workload audit APIs."""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from e2e_harness.audit import SAC_PRODUCER, ObservedAuditEvent, heimdall_job_id


@dataclass(frozen=True, slots=True)
class RecoveryTraceContext:
    """Canonical recovery identity returned by the production Advice API."""

    sequence_id: str
    trace_id: str
    parent_span_id: str


@dataclass(frozen=True, slots=True)
class RecoveryTraceEvidence:
    """Whether the exact recovery has correlated stage and audit evidence."""

    observed: bool
    summary: str
    trace_id: str = ""


def recovery_context_from_advice(
    response_text: str, *, job_id: str, attempt: int, operation: str
) -> RecoveryTraceContext:
    """Read exact-attempt context, rejecting a different operation or invalid W3C parent.

    Raises ValueError when the public response cannot identify this recovery.
    Missing context remains an evidence failure, never a request to create it.
    """

    payload = json.loads(response_text)
    if not isinstance(payload, dict) or payload.get("jobId") != job_id or payload.get("restartCount") != attempt:
        raise ValueError("Advice does not identify the requested job attempt")
    context = payload.get("recoveryContext")
    if not isinstance(context, dict) or context.get("operation") != operation:
        raise ValueError("Advice has no matching recovery context")
    sequence_id = context.get("sequenceId")
    parent = context.get("traceparent")
    if not isinstance(sequence_id, str) or not sequence_id:
        raise ValueError("Advice recovery sequence ID is missing")
    if not isinstance(parent, str) or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", parent):
        raise ValueError("Advice recovery traceparent is invalid")
    _, trace_id, span_id, _ = parent.split("-")
    if int(trace_id, 16) == 0 or int(span_id, 16) == 0:
        raise ValueError("Advice recovery traceparent contains a zero ID")
    return RecoveryTraceContext(sequence_id, trace_id, span_id)


def _otel_id(value: object, size: int) -> str:
    """Decode Tempo's protobuf-JSON bytes or OTLP-JSON hexadecimal identifiers."""

    if not isinstance(value, str):
        return ""
    if re.fullmatch(rf"[0-9a-fA-F]{{{size * 2}}}", value):
        return value.lower() if int(value, 16) else ""
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        return ""
    return decoded.hex() if len(decoded) == size and any(decoded) else ""


def _attributes(span: Mapping[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for attribute in span.get("attributes", []):
        value = attribute["value"]
        if "stringValue" in value:
            result[attribute["key"]] = value["stringValue"]
        elif "intValue" in value:
            result[attribute["key"]] = int(value["intValue"])
        elif "boolValue" in value:
            result[attribute["key"]] = value["boolValue"]
    return result


def evaluate_recovery_trace(
    response_text: str,
    *,
    context: RecoveryTraceContext,
    job_id: str,
    attempt: int,
    operation: str,
    require_attribution: bool,
    audit_events: Sequence[ObservedAuditEvent],
) -> RecoveryTraceEvidence:
    """Verify real stages and release/audit time agreement in a Tempo v2 response.

    Only spans for the Advice's canonical sequence, trace, operation, and source
    attempt qualify. A successful release must be a child of a real reconcile
    span and match its audit event's exact span ID and completion microsecond.
    Later convergence spans or unrelated attempts cannot provide completion.
    """

    def missing(detail: str) -> RecoveryTraceEvidence:
        return RecoveryTraceEvidence(False, detail, context.trace_id)

    try:
        payload = json.loads(response_text)
        spans = [
            span
            for resource in payload["trace"]["resourceSpans"]
            for scope in resource.get("scopeSpans", [])
            for span in scope.get("spans", [])
        ]
        matching = []
        for span in spans:
            attributes = _attributes(span)
            if (
                _otel_id(span.get("traceId"), 16) == context.trace_id
                and attributes.get("recovery.sequence.id") == context.sequence_id
                and attributes.get("job.id") == job_id
                and attributes.get("job.attempt.source") == attempt
                and attributes.get("recovery.operation") == operation
            ):
                start, end = int(span["startTimeUnixNano"]), int(span["endTimeUnixNano"])
                if start <= 0 or end < start:
                    return missing("recovery span has invalid operation timestamps")
                if not isinstance(span.get("name"), str) or not _otel_id(span.get("spanId"), 8):
                    return missing("recovery span has an invalid name or ID")
                matching.append((span, attributes, start, end))
    except (KeyError, TypeError, ValueError, AttributeError):
        return missing("Tempo response has malformed trace data")

    required = {"job.recovery.trigger", "job.recovery.advice", "job.recovery.reconcile"}
    if require_attribution:
        required.add("job.recovery.attribution")
    missing_stages = required - {span.get("name") for span, _, _, _ in matching}
    if missing_stages:
        return missing(f"recovery trace lacks stages: {sorted(missing_stages)}")

    roots = [
        start
        for span, _, start, _ in matching
        if span.get("name") == "job.recovery.trigger" and _otel_id(span.get("spanId"), 8) == context.parent_span_id
    ]
    if len(roots) != 1:
        return missing("trace does not contain the canonical backend trigger span")
    reconciles = {
        _otel_id(span.get("spanId"), 8)
        for span, _, _, _ in matching
        if span.get("name") == "job.recovery.reconcile"
        and _otel_id(span.get("parentSpanId"), 8) == context.parent_span_id
    }
    for span, attributes, start, end in matching:
        if span.get("name") != "job.recovery.scheduler_action":
            continue
        if attributes.get("scheduler.action") != "slurm.release":
            continue
        if attributes.get("scheduler.action.outcome") != "success":
            continue
        if attributes.get("recovery.completes_sequence") is not True:
            continue
        if attributes.get("recovery.slo.eligible") is not (operation == "restart"):
            return missing("release span has the wrong restart SLO eligibility")
        if start < roots[0] or _otel_id(span.get("parentSpanId"), 8) not in reconciles:
            return missing("release span is not a child of canonical recovery reconciliation")
        span_id = _otel_id(span.get("spanId"), 8)
        if not span_id:
            return missing("release span ID is invalid")
        for event in audit_events:
            if (
                event.heimdall_job_id == heimdall_job_id(job_id, attempt)
                and event.event_producer == SAC_PRODUCER
                and event.event_type == "advice_scheduler_action_completed"
                and event.payload.get("job_id") == job_id
                and event.payload.get("attempt_index") == attempt
                and event.payload.get("action") == "slurm.release"
                and event.payload.get("success") is True
                and event.payload.get("recovery_sequence_id") == context.sequence_id
                and event.payload.get("trace_id") == context.trace_id
                and event.payload.get("span_id") == span_id
                and event.event_produced_at == end // 1_000
            ):
                return RecoveryTraceEvidence(
                    True,
                    f"sequence={context.sequence_id} has correlated stages and release/audit completion",
                    context.trace_id,
                )
    return missing("no successful release span matches its audit identity and completion timestamp")
