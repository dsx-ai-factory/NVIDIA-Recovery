# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from recovery_libs.tracing.recovery import operation_correlation

TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
SPAN_ID = 0x00F067AA0BA902B7
SEQUENCE_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"


def _span(flags: TraceFlags) -> NonRecordingSpan:
    return NonRecordingSpan(SpanContext(TRACE_ID, SPAN_ID, is_remote=False, trace_flags=flags))


def test_sampled_operation_links_audit_to_its_span() -> None:
    assert operation_correlation(_span(TraceFlags(TraceFlags.SAMPLED)), SEQUENCE_ID) == {
        "recovery_sequence_id": SEQUENCE_ID,
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "00f067aa0ba902b7",
    }


@pytest.mark.parametrize("span", [_span(TraceFlags(TraceFlags.DEFAULT)), None], ids=["sampled out", "no span"])
def test_audit_never_links_to_a_trace_that_is_not_exported(span: NonRecordingSpan | None) -> None:
    """A sampled-out recovery keeps its sequence ID but must not name a trace the backend never received."""

    assert operation_correlation(span, SEQUENCE_ID) == {
        "recovery_sequence_id": SEQUENCE_ID,
        "trace_id": None,
        "span_id": None,
    }
