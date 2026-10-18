# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import (
    AttributionErrorType,
    ErrorCategory,
    ResourceKind,
)
from recovery_libs.schema.attribution_interface.response import (
    Attribution,
    AttributionError,
    ResourceSet,
    WorkloadAttributionResponse,
)

OBSERVED = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)


def _make_node_attribution() -> Attribution:
    """Build a fully populated NODE-targeted Attribution."""
    return Attribution(
        resource_set=ResourceSet(kind=ResourceKind.NODE, value="node-1"),
        error_category=ErrorCategory.HARDWARE,
        error_detail="XID 79 fell off the bus",
        confidence=None,
        symptom="gpu_fell_off_bus",
        evidence_excerpt="NVRM: GPU has fallen off the bus",
        observed_at=OBSERVED,
        recommended_action="DRAIN",
        evidence_references=["source:syslog", "source:node_metrics"],
    )


def test_resource_set_defaults():
    """value defaults to None (used for AllJobResources)."""
    rs = ResourceSet(kind=ResourceKind.ALL_JOB_RESOURCES)
    assert rs.value is None
    assert rs.kind == ResourceKind.ALL_JOB_RESOURCES


def test_attribution_defaults():
    """String detail fields default to '', confidence/recommended_action to None, refs to []."""
    attr = Attribution(
        resource_set=ResourceSet(kind=ResourceKind.ALL_JOB_RESOURCES),
        error_category=ErrorCategory.AMBIGUOUS,
        observed_at=OBSERVED,
    )
    assert attr.error_detail == ""
    assert attr.confidence is None
    assert attr.symptom == ""
    assert attr.evidence_excerpt == ""
    assert attr.recommended_action is None
    assert attr.evidence_references == []


def test_attribution_error_defaults():
    """message defaults to ''."""
    err = AttributionError(type=AttributionErrorType.SERVICE_ERROR)
    assert err.message == ""
    assert err.type == AttributionErrorType.SERVICE_ERROR


def test_response_defaults():
    """attribution_session_id='', attributions/unattributed_targets=[], error=None."""
    resp = WorkloadAttributionResponse(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        service_identifier="attribution-adapter",
        service_version="0.1.0",
        request_id="req-1",
    )
    assert resp.attribution_session_id == ""
    assert resp.attributions == []
    assert resp.unattributed_targets == []
    assert resp.error is None


def test_response_json_round_trip():
    """model_dump_json -> model_validate_json reproduces an equal object."""
    resp = WorkloadAttributionResponse(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        service_identifier="attribution-adapter",
        service_version="0.1.0",
        request_id="req-2",
        attribution_session_id="fact-token-abc",
        attributions=[_make_node_attribution()],
        unattributed_targets=[ResourceSet(kind=ResourceKind.NODE, value="node-2")],
        error=AttributionError(type=AttributionErrorType.TIMEOUT, message="deadline exceeded"),
    )
    restored = WorkloadAttributionResponse.model_validate_json(resp.model_dump_json())
    assert restored == resp


def test_attribution_datetime_iso_serialization():
    """observed_at serializes as ISO-8601 with a Z suffix for UTC."""
    payload = _make_node_attribution().model_dump(mode="json")
    assert payload["observed_at"] == "2026-06-02T11:30:00Z"
    assert payload["resource_set"] == {"kind": "Node", "value": "node-1"}
    assert payload["error_category"] == "hardware"
