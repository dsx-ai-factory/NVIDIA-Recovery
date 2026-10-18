# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from anomaly_detection_isolator.result_builder import (
    ERROR_CATEGORY_TO_ATTRIBUTION,
    EVIDENCE_TAG_TO_SOURCE,
    _map_evidence_references,
    build_attribution_result,
)
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.attribution_interface.enums import ErrorCategory, ResourceKind
from recovery_libs.schema.attribution_interface.response import (
    Attribution,
    ResourceSet,
    WorkloadAttributionResponse,
)
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
)

# --------------------------------------------------------------------------- helpers


def _job_status():
    return SlurmJobExecutionStatus(
        job_id="12345",
        workflow_id="test-workflow",
        job_attempt_index=0,
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 1, 1, tzinfo=UTC),
        allocated_node_list=["node1"],
        cluster="test-cluster",
    )


def _settings():
    return SimpleNamespace(
        fact_isolated_errors=[],
        fact_isolated_errors_env=[],
        fact_ambiguous_errors=[],
        fact_ambiguous_errors_env=[],
        fact_non_isolated_errors=[],
        fact_non_isolated_errors_env=[],
    )


def _response(attributions):
    return WorkloadAttributionResponse(
        schema_version="0.1.0",
        service_identifier="attribution-adapter",
        service_version="0.0.0",
        request_id="req-1",
        attribution_session_id="sess-1",
        attributions=attributions,
    )


# --------------------------------------------------------------------------- maps


@pytest.mark.parametrize(
    "category, expected",
    [
        (ErrorCategory.HARDWARE, ErrorAttribution.HARDWARE),
        (ErrorCategory.SYSTEM_SOFTWARE, ErrorAttribution.SYSTEM),
        (ErrorCategory.APPLICATION, ErrorAttribution.APPLICATION),
        (ErrorCategory.AMBIGUOUS, ErrorAttribution.UNKNOWN),
        (ErrorCategory.UNATTRIBUTED, ErrorAttribution.UNDEFINED),
        (ErrorCategory.INSUFFICIENT_TELEMETRY, ErrorAttribution.UNDEFINED),
    ],
)
def test_error_category_to_attribution(category, expected):
    assert ERROR_CATEGORY_TO_ATTRIBUTION[category] == expected


def test_evidence_tag_to_source_map():
    assert EVIDENCE_TAG_TO_SOURCE["source:syslog"] == ErrorAttributionSource.SYSLOGS
    assert EVIDENCE_TAG_TO_SOURCE["source:network_syslog"] == ErrorAttributionSource.SYSLOGS
    assert EVIDENCE_TAG_TO_SOURCE["source:applog"] == ErrorAttributionSource.APPLICATION_LOGS
    assert EVIDENCE_TAG_TO_SOURCE["source:ib_metrics"] == ErrorAttributionSource.NETWORK_METRICS
    assert EVIDENCE_TAG_TO_SOURCE["source:eth_metrics"] == ErrorAttributionSource.NETWORK_METRICS
    assert EVIDENCE_TAG_TO_SOURCE["source:nvlink_metrics"] == ErrorAttributionSource.NETWORK_METRICS
    assert EVIDENCE_TAG_TO_SOURCE["source:node_metrics"] == ErrorAttributionSource.NODE_METRICS


def test_map_evidence_references_sorted_dedup_unknown_to_none():
    refs = ["source:nvlink_metrics", "source:syslog", "source:applog", "source:syslog", "obs:1234"]
    result = _map_evidence_references(refs)
    # sorted by IntEnum value, deduped; unknown tag -> NONE
    assert result == sorted(set(result), key=lambda s: s.value)
    assert ErrorAttributionSource.NETWORK_METRICS in result
    assert ErrorAttributionSource.SYSLOGS in result
    assert ErrorAttributionSource.APPLICATION_LOGS in result
    assert ErrorAttributionSource.NONE in result


def test_map_evidence_references_empty():
    assert _map_evidence_references([]) == []


def test_map_evidence_references_health_check_is_none_sentinel():
    """C4: an unrecognized source tag (health_check) maps to the NONE sentinel."""
    assert _map_evidence_references(["source:health_check"]) == [ErrorAttributionSource.NONE]
    # mixed: applog + health_check -> {APPLICATION_LOGS, NONE} sorted by int value.
    assert _map_evidence_references(["source:applog", "source:health_check"]) == [
        ErrorAttributionSource.NONE,
        ErrorAttributionSource.APPLICATION_LOGS,
    ]


# --------------------------------------------------------------------------- build: node


def test_build_node_event_defaults_and_coercion():
    obs = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.HARDWARE,
                error_detail="Test error",
                symptom="Symptom 1",
                evidence_excerpt='{"123": "123"}',
                observed_at=obs,
                recommended_action="DRAIN",
                evidence_references=["source:applog"],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    assert isinstance(result, AnomalyAttributionResult)
    serialized = json.loads(result.to_json())
    node_events = next(iter(serialized["node_attributions"].values()))
    ev = node_events[0]
    # defaults left untouched by result_builder
    assert ev["cluster"] == ""
    assert ev["job_start_time"] is None
    assert ev["job_end_time"] is None
    assert ev["original_exclusion_type"] == ExclusionType.UNDEFINED.value
    assert ev["node"] == "node1"
    assert ev["error_attribution"] == ErrorAttribution.HARDWARE.value
    assert ev["error_attribution_origin"] == ErrorAttributionOrigin.FACT.value
    assert ev["error_attribution_type"] == ErrorAttributionType.FAST.value
    assert ev["exclusion_type"] == ExclusionType.CONFIRMED.value
    assert ev["fact_status"] == FactStatus.OK.value
    assert ev["error_attribution_sources"] == [ErrorAttributionSource.APPLICATION_LOGS.value]
    # JobExecutionError key carries message/description/signature
    key = json.loads(next(iter(serialized["node_attributions"].keys())))
    assert key == {"code": 0, "message": '{"123": "123"}', "description": "Test error", "signature": "Symptom 1"}


def test_build_node_event_exclusion_via_derive_exclusion():
    """Non-DRAIN node with a symptom in the isolated list -> CONFIRMED via derive_exclusion."""
    settings = SimpleNamespace(
        fact_isolated_errors=["XID 79"],
        fact_isolated_errors_env=[],
        fact_ambiguous_errors=[],
        fact_ambiguous_errors_env=[],
        fact_non_isolated_errors=[],
        fact_non_isolated_errors_env=[],
    )
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.HARDWARE,
                symptom="XID 79 on bus",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                recommended_action=None,
                evidence_references=[],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, settings)
    ev = next(iter(json.loads(result.to_json())["node_attributions"].values()))[0]
    assert ev["exclusion_type"] == ExclusionType.CONFIRMED.value


def test_build_empty_response_returns_empty_result_not_none():
    result = build_attribution_result(
        _response([]), _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings()
    )
    assert result is not None
    assert result.node_attributions == {}
    assert result.global_attributions == {}
    assert result.attribution_type == ErrorAttributionType.FAST
    assert result.anomaly_type == EventType.CRASHNAN


# --------------------------------------------------------------------------- build: global


def test_build_global_event_no_exclusion_and_no_node_fields():
    obs = datetime(2026, 2, 2, tzinfo=UTC)
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.ALL_JOB_RESOURCES),
                error_category=ErrorCategory.SYSTEM_SOFTWARE,
                error_detail="global err",
                symptom="global symptom",
                evidence_excerpt="line",
                observed_at=obs,
                recommended_action=None,
                evidence_references=["source:node_metrics"],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.SLOW, _settings())
    serialized = json.loads(result.to_json())
    assert serialized["node_attributions"] == {}
    ev = next(iter(serialized["global_attributions"].values()))[0]
    # GlobalEvent has no node/exclusion/cluster/timestamp-pair fields
    assert "node" not in ev
    assert "exclusion_type" not in ev
    assert "cluster" not in ev
    assert ev["error_attribution"] == ErrorAttribution.SYSTEM.value
    assert ev["error_attribution_type"] == ErrorAttributionType.SLOW.value
    assert ev["error_attribution_sources"] == [ErrorAttributionSource.NODE_METRICS.value]


def test_build_node_and_global_in_one_response():
    obs = datetime(2026, 1, 1, tzinfo=UTC)
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.HARDWARE,
                symptom="node symptom",
                observed_at=obs,
                recommended_action="drain",
                evidence_references=["source:syslog"],
            ),
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.ALL_JOB_RESOURCES),
                error_category=ErrorCategory.APPLICATION,
                symptom="global symptom",
                observed_at=obs,
                recommended_action=None,
                evidence_references=["source:applog"],
            ),
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    serialized = json.loads(result.to_json())
    assert len(serialized["node_attributions"]) == 1
    assert len(serialized["global_attributions"]) == 1


# --------------------------------------------------------------------------- build: edge cases


def test_observed_at_naive_normalized_to_utc():
    naive = datetime(2026, 3, 3, 4, 5)  # no tzinfo
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.AMBIGUOUS,
                observed_at=naive,
                evidence_references=[],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    serialized = json.loads(result.to_json())
    ts = next(iter(serialized["node_attributions"].values()))[0]["timestamp"]
    assert ts == naive.replace(tzinfo=UTC).isoformat()


def test_none_to_empty_string_on_job_execution_error_fields():
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.UNATTRIBUTED,
                error_detail="",
                symptom="",
                evidence_excerpt="",
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                evidence_references=[],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    serialized = json.loads(result.to_json())
    key = json.loads(next(iter(serialized["node_attributions"].keys())))
    assert key == {"code": 0, "message": "", "description": "", "signature": ""}
    ev = next(iter(serialized["node_attributions"].values()))[0]
    assert ev["error_attribution"] == ErrorAttribution.UNDEFINED.value  # UNATTRIBUTED -> UNDEFINED


@pytest.mark.parametrize(
    "category, expected_value",
    [
        (ErrorCategory.UNATTRIBUTED, ErrorAttribution.UNDEFINED.value),  # NO_ERROR round-trip
        (ErrorCategory.AMBIGUOUS, ErrorAttribution.UNKNOWN.value),  # EMPTY/UNDETERMINED round-trip
    ],
)
def test_build_node_error_category_mapping(category, expected_value):
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=category,
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                evidence_references=[],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    ev = next(iter(json.loads(result.to_json())["node_attributions"].values()))[0]
    assert ev["error_attribution"] == expected_value


def test_health_check_source_yields_none_present():
    """C4: a health_check-tagged evidence ref keeps ErrorAttributionSource.NONE in the serialized list."""
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.HARDWARE,
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
                evidence_references=["source:health_check"],
            )
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    ev = next(iter(json.loads(result.to_json())["node_attributions"].values()))[0]
    assert ev["error_attribution_sources"] == [ErrorAttributionSource.NONE.value]


def test_multi_key_node_attributions_grouped():
    """C11: two attributions on one node with different symptom/description produce two JobExecutionError keys."""
    obs = datetime(2026, 1, 1, tzinfo=UTC)
    resp = _response(
        [
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.HARDWARE,
                error_detail="error A",
                symptom="symptom A",
                observed_at=obs,
                recommended_action="drain",
                evidence_references=["source:syslog"],
            ),
            Attribution(
                resource_set=ResourceSet(kind=ResourceKind.NODE, value="node1"),
                error_category=ErrorCategory.SYSTEM_SOFTWARE,
                error_detail="error B",
                symptom="symptom B",
                observed_at=obs,
                recommended_action=None,
                evidence_references=["source:applog"],
            ),
        ]
    )
    result = build_attribution_result(resp, _job_status(), "CRASHNAN", "", ErrorAttributionType.FAST, _settings())
    serialized = json.loads(result.to_json())
    keys = {json.loads(k)["signature"] for k in serialized["node_attributions"]}
    assert keys == {"symptom A", "symptom B"}
    # Each key groups exactly one node_event.
    for events in serialized["node_attributions"].values():
        assert len(events) == 1
