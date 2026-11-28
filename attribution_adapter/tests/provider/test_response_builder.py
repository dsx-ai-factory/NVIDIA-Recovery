# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Tests for attribution_adapter.response_builder.build_response (FACT v2)."""

from datetime import UTC, datetime

import pytest
from attribution_adapter.response_builder import build_response
from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from recovery_libs.schema.attribution_interface.enums import AttributionPhase, ErrorCategory, ResourceKind
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)

NOW = datetime(2026, 1, 1, 1, tzinfo=UTC)


def _request(detection_signal="CRASH"):
    return WorkloadAttributionRequest(
        schema_version="0.1.0",
        workload_id="123",
        user_id="alice",
        scope=WorkloadScope(nodes=["nodeA"], ranks=[], start_time=NOW, end_time=NOW),
        failure_context=FailureContext(detected_at=NOW, detection_signal=detection_signal),
        timeout_ms=90000,
        workload_context=WorkloadContext(
            workflow_id="workflow-test",
            attempt_index=0,
            command="",
            cluster="c1",
            workload_name="j",
            state="FAILED",
            node_count=1,
            total_gpu_count=8,
            job_end_time=NOW,
            anomaly_uuid="",
        ),
        continue_session=None,
        phase=AttributionPhase.FAST,
    )


def _v2(symptoms, outcome="Fail InfraHW", detail="Known HW Cause - Node", collectors=None):
    return AttributionResponseV2.from_dict(
        {
            "collectors": collectors,
            "model_type": "AttributionResponseV2",
            "verdict": {
                "attribution_outcome": outcome,
                "terminal_states": ["Pass" if outcome == "Pass" else "Fail"],
                "detail": detail,
                "root_symptom": {
                    "symptom": "x",
                    "source": {
                        "observation_id": 1,
                        "source": "syslog",
                        "pos": None,
                        "line": "l",
                        "rule": "r",
                        "time": None,
                    },
                },
            },
            "symptoms": symptoms,
            "observation_context": [1],
        }
    )


def test_node_site_maps_to_node_resource():
    v2 = _v2(
        {
            "XID-79": [
                {
                    "type": "NodeSource",
                    "status": "bad",
                    "node": "nodeA",
                    "source": {
                        "observation_id": 1,
                        "source": "syslog",
                        "pos": None,
                        "line": "xid 79 fell off bus",
                        "rule": "r",
                        "time": "2026-01-01T00:30:00Z",
                    },
                }
            ],
        }
    )
    resp = build_response(v2, _request(), attribution_session_id="att-1")
    assert len(resp.attributions) == 1
    a = resp.attributions[0]
    assert a.resource_set.kind == ResourceKind.NODE and a.resource_set.value == "nodeA"
    assert a.symptom == "XID-79"
    assert a.error_category == ErrorCategory.HARDWARE
    assert a.recommended_action is None  # symptom-driven exclusion downstream
    assert a.evidence_excerpt == "xid 79 fell off bus"
    assert a.observed_at == datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
    assert resp.attribution_session_id == "att-1"
    assert a.evidence_references == ["source:syslog"]


def test_resourceless_site_maps_to_all_job_resources_and_time_fallback():
    v2 = _v2(
        {
            "the process completes": [
                {
                    "type": "ResourcelessSource",
                    "status": "bad",
                    "source": {
                        "observation_id": 1,
                        "source": "slurm_info",
                        "pos": None,
                        "line": "COMPLETED",
                        "rule": "r",
                        "time": None,
                    },
                }
            ],
        },
        outcome="Pass",
        detail="Not Applicable",
    )
    resp = build_response(v2, _request(), attribution_session_id="att-1")
    a = resp.attributions[0]
    assert a.resource_set.kind == ResourceKind.ALL_JOB_RESOURCES and a.resource_set.value is None
    assert a.error_category == ErrorCategory.UNATTRIBUTED
    assert a.observed_at == NOW  # null time -> job_end_time fallback


def test_two_symptom_fanout_yields_two_attributions():
    """One NodeSource site + one ResourcelessSource site -> two Attribution entries."""
    v2 = _v2(
        {
            "XID-79": [
                {
                    "type": "NodeSource",
                    "status": "bad",
                    "node": "nodeA",
                    "source": {
                        "observation_id": 1,
                        "source": "syslog",
                        "pos": None,
                        "line": "xid 79",
                        "rule": "r",
                        "time": "2026-01-01T00:30:00Z",
                    },
                }
            ],
            "process_exit": [
                {
                    "type": "ResourcelessSource",
                    "status": "bad",
                    "source": {
                        "observation_id": 2,
                        "source": "slurm_info",
                        "pos": None,
                        "line": "EXIT",
                        "rule": "r",
                        "time": None,
                    },
                }
            ],
        }
    )
    resp = build_response(v2, _request(), attribution_session_id="att-2")
    assert len(resp.attributions) == 2
    kinds = {a.resource_set.kind for a in resp.attributions}
    assert kinds == {ResourceKind.NODE, ResourceKind.ALL_JOB_RESOURCES}


def _straggler_source(line, time):
    return {"observation_id": 1, "source": "applog", "pos": 1, "line": line, "rule": "r", "time": time}


def _running_job_with_straggler_digest(collector_key, pattern):
    """A running job whose only node-level finding is a straggler report on nodeA."""
    report = {
        "pattern": pattern,
        "rank_scores": [{"rank": 5, "score": 213.46}, {"rank": 5, "score": 213.55}],
        "sources": [
            _straggler_source("first report: rank 5 is slow", "2026-01-01T00:30:00Z"),
            _straggler_source("second report: rank 5 is slow", "2026-01-01T00:40:00Z"),
        ],
    }
    running = {
        "running without error": [
            {
                "type": "ResourcelessSource",
                "status": "bad",
                "source": {
                    "observation_id": 1,
                    "source": "slurm_info",
                    "pos": None,
                    "line": "",
                    "rule": "r",
                    "time": None,
                },
            }
        ]
    }
    return _v2(running, outcome="Pass", detail="Not Applicable", collectors={collector_key: {"nodeA": report}})


def _node_attributions(resp):
    return [a for a in resp.attributions if a.resource_set.kind == ResourceKind.NODE]


@pytest.mark.parametrize(
    "collector_key, pattern",
    [("megatron_straggler", "Megatron Pattern"), ("nvrx_straggler", "NVRX Pattern")],
)
def test_straggler_report_maps_to_node_attribution_for_straggler_signal(collector_key, pattern):
    """A straggler anomaly names the node that FACT reports in its straggler collector digest."""
    v2 = _running_job_with_straggler_digest(collector_key, pattern)

    resp = build_response(v2, _request("STRAGGLER"), attribution_session_id="att-1")

    nodes = _node_attributions(resp)
    assert len(nodes) == 1
    a = nodes[0]
    assert a.resource_set.value == "nodeA"
    assert a.symptom == f"Straggler ({pattern})"
    assert a.recommended_action is None  # symptom-driven exclusion downstream
    assert a.evidence_excerpt == "second report: rank 5 is slow"
    assert a.observed_at == datetime(2026, 1, 1, 0, 40, tzinfo=UTC)
    assert a.evidence_references == ["source:applog"]


@pytest.mark.parametrize("detection_signal", ["CRASH", "HANGNOOUTPUT"])
def test_straggler_report_is_ignored_for_other_signals(detection_signal):
    """A crash or hang does not name a node only because it was slow earlier in the window."""
    v2 = _running_job_with_straggler_digest("megatron_straggler", "Megatron Pattern")

    resp = build_response(v2, _request(detection_signal), attribution_session_id="att-1")

    assert _node_attributions(resp) == []


@pytest.mark.parametrize(
    "collectors",
    [
        None,
        {"megatron_straggler": {}},
        {"megatron_straggler": ["not", "a", "map"]},
        {"megatron_straggler": {"nodeA": "not a report"}},
        {"megatron_straggler": {"nodeA": {"pattern": "Megatron Pattern", "rank_scores": [], "sources": []}}},
        {"nvrx_straggler": {"nodeA": {"pattern": "NVRX Pattern", "rank_scores": [{"score": 1.0}]}}},
        {"megatron_straggler": {"nodeA": {"pattern": "Megatron Pattern", "rank_scores": [{"rank": 5}], "sources": 42}}},
    ],
)
def test_empty_or_malformed_straggler_digest_adds_no_attribution(collectors):
    """The untyped collectors digest cannot fail the response or name a node without a flagged rank."""
    v2 = _running_job_with_straggler_digest("megatron_straggler", "Megatron Pattern")
    v2.collectors = collectors

    resp = build_response(v2, _request("STRAGGLER"), attribution_session_id="att-1")

    assert _node_attributions(resp) == []
    assert len(resp.attributions) == 1  # the job-level symptom is still reported


def test_straggler_report_with_unusable_evidence_fields_still_names_the_node():
    """A source without a usable line or source name cannot fail the response or supply evidence."""
    v2 = _running_job_with_straggler_digest("megatron_straggler", "Megatron Pattern")
    v2.collectors["megatron_straggler"]["nodeA"]["sources"] = [{"line": 123, "source": None, "time": 5}, "not a source"]

    resp = build_response(v2, _request("STRAGGLER"), attribution_session_id="att-1")

    nodes = _node_attributions(resp)
    assert len(nodes) == 1
    assert nodes[0].resource_set.value == "nodeA"
    assert nodes[0].evidence_excerpt == ""
    assert nodes[0].evidence_references == []
    assert nodes[0].observed_at == NOW  # no usable source time -> job_end_time fallback
