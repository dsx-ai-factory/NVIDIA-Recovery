# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Tests for attribution_adapter.response_builder.build_response (FACT v2)."""

from datetime import UTC, datetime

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


def _request():
    return WorkloadAttributionRequest(
        schema_version="0.1.0",
        workload_id="123",
        user_id="alice",
        scope=WorkloadScope(nodes=["nodeA"], ranks=[], start_time=NOW, end_time=NOW),
        failure_context=FailureContext(detected_at=NOW, detection_signal="CRASH"),
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


def _v2(symptoms, outcome="Fail InfraHW", detail="Known HW Cause - Node"):
    return AttributionResponseV2.from_dict(
        {
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
