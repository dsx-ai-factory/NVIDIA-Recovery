# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

import pytest
from anomaly_detection_isolator.models.audit import (
    ADIAttributionRequestForwardedAuditEvent,
    ADIAttributionResponseReceivedAuditEvent,
    AttributionRequestForwardedPayload,
    AttributionResponseReceivedPayload,
)
from pydantic import ValidationError
from recovery_libs.audit import AuditEventLevel
from recovery_libs.schema.attribution_interface.enums import AttributionPhase
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse


def _request() -> WorkloadAttributionRequest:
    observed_at = datetime(2026, 8, 23, tzinfo=UTC)
    return WorkloadAttributionRequest(
        schema_version="1.0",
        workload_id="job-42",
        user_id="alice",
        scope=WorkloadScope(nodes=["node-1"], start_time=observed_at, end_time=observed_at),
        failure_context=FailureContext(detected_at=observed_at, detection_signal="CRASH"),
        timeout_ms=90_000,
        workload_context=WorkloadContext(workflow_id="workflow-1", attempt_index=2),
        phase=AttributionPhase.FAST,
    )


def _response() -> WorkloadAttributionResponse:
    return WorkloadAttributionResponse(
        schema_version="1.0",
        service_identifier="attribution-adapter",
        service_version="test",
        request_id="job-42",
        attribution_session_id="session-1",
    )


def test_attribution_request_forwarded_event_serializes_for_delivery() -> None:
    event = ADIAttributionRequestForwardedAuditEvent(
        heimdall_job_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
        event_level=AuditEventLevel.OPERATIONAL,
        payload=AttributionRequestForwardedPayload(
            advice_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
            job_id="job-42",
            attempt_index=2,
            request_payload=_request(),
            request_uuid="019c8cab-e1cc-7a79-95ac-5de8d3e41000",
        ),
        event_produced_at=1_755_900_000,
        idempotency_key="1b3a957b-3018-4c02-a9e2-b09720408178",
    )

    serialized = event.model_dump(mode="json")

    assert serialized["event_producer"] == "anomaly-detection-isolator"
    assert serialized["event_type"] == "attribution_request_forwarded"
    assert serialized["payload"]["request_payload"]["workload_id"] == "job-42"
    assert "correlation_uuid" not in serialized["payload"]


def test_attribution_response_received_event_serializes_for_delivery() -> None:
    event = ADIAttributionResponseReceivedAuditEvent(
        heimdall_job_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
        event_level=AuditEventLevel.OPERATIONAL,
        payload=AttributionResponseReceivedPayload(
            advice_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
            job_id="job-42",
            attempt_index=2,
            request_uuid="019c8cab-e1cc-7a79-95ac-5de8d3e41000",
            response_payload=_response(),
        ),
        event_produced_at=1_755_900_001,
        idempotency_key="5bf08e1c-b328-4450-a47d-646c37bcba2a",
    )

    serialized = event.model_dump(mode="json")

    assert serialized["event_producer"] == "anomaly-detection-isolator"
    assert serialized["event_type"] == "attribution_response_received"
    assert serialized["payload"]["response_payload"]["request_id"] == "job-42"
    assert "correlation_uuid" not in serialized["payload"]


def test_attribution_audit_payload_rejects_correlation_uuid_for_now() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AttributionRequestForwardedPayload(
            advice_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
            job_id="job-42",
            attempt_index=2,
            request_payload=_request(),
            request_uuid="019c8cab-e1cc-7a79-95ac-5de8d3e41000",
            correlation_uuid="01a02d05-a02b-7f0f-8744-c972731d0e0c",
        )
