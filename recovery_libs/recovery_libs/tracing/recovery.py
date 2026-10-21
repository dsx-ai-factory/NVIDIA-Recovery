# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Optional recovery identity shared by local observations and message propagation."""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import context
from opentelemetry.context import Context
from opentelemetry.trace import Span
from pydantic import BaseModel, Field

_SEQUENCE_KEY = "heimdall.recovery_sequence_id"
_OPERATION_KEY = "heimdall.recovery_operation"


class RecoveryCorrelation(BaseModel):
    """Explicit optional audit correlation; absence does not invalidate an event."""

    recovery_sequence_id: str | None = Field(default=None, description="Backend-owned recovery sequence UUID.")
    trace_id: str | None = Field(default=None, description="Validated W3C trace identifier.")
    span_id: str | None = Field(default=None, description="The actual observed operation span identifier.")


def with_recovery_identity(parent: Context, sequence_id: str, operation: str) -> Context:
    """Attach validated recovery identity to an existing canonical trace context."""
    uuid.UUID(sequence_id)
    if operation not in {"restart", "dag_release", "terminate"}:
        raise ValueError("Invalid recovery operation")
    parent = context.set_value(_SEQUENCE_KEY, sequence_id, parent)
    return context.set_value(_OPERATION_KEY, operation, parent)


def recovery_headers(parent: Context | None) -> dict[str, str]:
    """Serialize identity alongside W3C headers for a backend attribution message."""
    sequence_id = context.get_value(_SEQUENCE_KEY, parent)
    operation = context.get_value(_OPERATION_KEY, parent)
    if isinstance(sequence_id, str) and isinstance(operation, str):
        return {"recovery-sequence-id": sequence_id, "recovery-operation": operation}
    return {}


def recovery_context_from_headers(parent: Context | None, headers: dict | None) -> Context | None:
    """Restore optional identity only when the producer also supplied valid W3C context."""
    if parent is None or not headers:
        return parent
    try:
        return with_recovery_identity(parent, headers["recovery-sequence-id"], headers["recovery-operation"])
    except (KeyError, ValueError, TypeError, AttributeError):
        return parent


def recovery_span_attributes() -> dict[str, str]:
    """Return the current canonical sequence identity for a real operation span."""
    headers = recovery_headers(None)
    if not headers:
        return {}
    return {
        "recovery.sequence.id": headers["recovery-sequence-id"],
        "recovery.operation": headers["recovery-operation"],
    }


def operation_correlation(span: Span | None, sequence_id: str | None = None) -> dict[str, str | None]:
    """Read identifiers from the actual operation span, never an ambient transport span.

    Trace and span IDs are returned only for a sampled span. An unsampled span
    still has valid IDs, but nothing is exported under them, so audit events
    carrying them would link to a trace that never exists.

    Args:
        span: The span of the operation the audit event describes, if any.
        sequence_id: The recovery sequence the operation belongs to, if any.

    Returns:
        The recovery sequence, trace, and span IDs to record; each is None when
        unknown or, for trace and span IDs, when the span is not sampled.
    """
    result: dict[str, str | None] = {"recovery_sequence_id": sequence_id, "trace_id": None, "span_id": None}
    if span is not None:
        span_context = span.get_span_context()
        if span_context.is_valid and span_context.trace_flags.sampled:
            result["trace_id"] = format(span_context.trace_id, "032x")
            result["span_id"] = format(span_context.span_id, "016x")
    return result


@contextmanager
def recovery_scope(parent: Context | None) -> Iterator[None]:
    """Make propagated identity available to nested operations and restore caller context."""
    if parent is None:
        yield
        return
    token = context.attach(parent)
    try:
        yield
    finally:
        context.detach(token)
