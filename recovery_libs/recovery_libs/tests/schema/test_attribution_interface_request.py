# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionPhase
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)

T0 = datetime(2026, 6, 2, 10, 0, 0, tzinfo=UTC)
T1 = datetime(2026, 6, 2, 11, 0, 0, tzinfo=UTC)


def _make_request(**overrides) -> WorkloadAttributionRequest:
    """Build a fully populated WorkloadAttributionRequest, allowing field overrides."""
    kwargs = {
        "schema_version": ATTRIBUTION_SCHEMA_VERSION,
        "workload_id": "job-123",
        "scope": WorkloadScope(nodes=["node-1", "node-2"], start_time=T0, end_time=T1),
        "failure_context": FailureContext(detected_at=T0, detection_signal="CRASH"),
        "timeout_ms": 90000,
        "workload_context": WorkloadContext(attempt_index=1, workflow_id="experiment-42"),
        "phase": AttributionPhase.FAST,
    }
    kwargs.update(overrides)
    return WorkloadAttributionRequest(**kwargs)


def test_workload_scope_defaults():
    """ranks defaults to an empty list."""
    scope = WorkloadScope(nodes=["n1"], start_time=T0, end_time=T1)
    assert scope.ranks == []
    assert scope.nodes == ["n1"]


def test_workload_context_defaults():
    """All string fields default to '', counts/times to None."""
    ctx = WorkloadContext(attempt_index=2, workflow_id="experiment-42")
    assert ctx.workflow_id == "experiment-42"
    assert ctx.command == ""
    assert ctx.cluster == ""
    assert ctx.workload_name == ""
    assert ctx.state == ""
    assert ctx.node_count is None
    assert ctx.total_gpu_count is None
    assert ctx.job_end_time is None
    assert ctx.anomaly_uuid == ""


def test_workload_context_requires_workflow_id():
    """The canonical workflow identifier is required at the attribution boundary."""
    with pytest.raises(ValidationError, match="workflow_id"):
        WorkloadContext(attempt_index=2)


def test_workload_context_workflow_id_json_round_trip():
    """The exact canonical workflow identifier survives JSON serialization."""
    ctx = WorkloadContext(attempt_index=2, workflow_id="experiment-42", workload_name="train")

    restored = WorkloadContext.model_validate_json(ctx.model_dump_json())

    assert restored.workflow_id == "experiment-42"
    assert restored.workload_name == "train"


@pytest.mark.parametrize(
    "workflow_id",
    ("", "🚀" * 257, "\ud800", b"experiment-42", bytearray(b"experiment-42")),
)
def test_workload_context_rejects_invalid_workflow_id(workflow_id):
    """Attribution uses the same strict canonical workflow-ID contract as job status."""
    with pytest.raises(ValidationError, match="workflow_id"):
        WorkloadContext(attempt_index=2, workflow_id=workflow_id)


def test_workload_context_accepts_256_codepoint_workflow_id():
    """The inclusive canonical limit is 256 Unicode code points, not bytes."""
    workflow_id = "🚀" * 256

    context = WorkloadContext(attempt_index=2, workflow_id=workflow_id)

    assert context.workflow_id == workflow_id


def test_request_defaults():
    """user_id defaults to '' and continue_session defaults to None."""
    req = _make_request()
    assert req.user_id == ""
    assert req.continue_session is None
    assert req.phase == AttributionPhase.FAST


def test_request_json_round_trip():
    """model_dump_json -> model_validate_json reproduces an equal object."""
    req = _make_request(
        user_id="alice",
        continue_session="fact-token-abc",
        phase=AttributionPhase.SLOW,
        workload_context=WorkloadContext(
            attempt_index=3,
            workflow_id="experiment-42",
            command="srun python train.py",
            cluster="cluster-a",
            workload_name="llm-pretrain",
            state="FAILED",
            node_count=4,
            total_gpu_count=32,
            job_end_time=T1,
            anomaly_uuid="uuid-xyz",
        ),
    )
    restored = WorkloadAttributionRequest.model_validate_json(req.model_dump_json())
    assert restored == req


def test_request_datetime_iso_serialization():
    """Datetimes serialize as ISO-8601 with a Z suffix for UTC."""
    req = _make_request()
    payload = req.model_dump(mode="json")
    assert payload["scope"]["start_time"] == "2026-06-02T10:00:00Z"
    assert payload["scope"]["end_time"] == "2026-06-02T11:00:00Z"
    assert payload["failure_context"]["detected_at"] == "2026-06-02T10:00:00Z"
    assert payload["workload_context"]["job_end_time"] is None
    assert payload["phase"] == "fast"


def test_workload_scope_none_start_end_times():
    """C12: start_time and end_time accept None when SlurmJobExecutionStatus times are absent."""
    scope = WorkloadScope(nodes=["n1"])
    assert scope.start_time is None
    assert scope.end_time is None

    # round-trip preserves None
    restored = WorkloadScope.model_validate_json(scope.model_dump_json())
    assert restored.start_time is None
    assert restored.end_time is None

    payload = scope.model_dump(mode="json")
    assert payload["start_time"] is None
    assert payload["end_time"] is None
