# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the production-interface DAG E2E oracle."""

from __future__ import annotations

import json

import pytest
from e2e_harness.dag import (
    ADVICE_STATE_TERMINATED,
    advice_state,
    dag_dependency_matches,
    dependency_by_id,
    dependency_by_name,
    parse_dag_dependencies_response,
    parse_job_status_response,
)


def test_dependency_builders_emit_supported_policy() -> None:
    assert dependency_by_id("123") == {
        "target": {"type": "id", "id": "123"},
        "trigger": "completed",
    }
    assert dependency_by_name("prepare") == {
        "target": {"type": "name", "name": "prepare"},
        "trigger": "completed",
    }


def test_job_status_parser_requires_one_exact_attempt() -> None:
    payload = {
        "statuses": [
            {"job_id": "123", "job_attempt_index": 0, "state": "PENDING"},
            {"job_id": "123", "job_attempt_index": 1, "state": "RUNNING"},
        ]
    }

    assert parse_job_status_response(json.dumps(payload), job_id="123", attempt=1)["state"] == "RUNNING"
    with pytest.raises(ValueError, match="expected one status"):
        parse_job_status_response(json.dumps(payload), job_id="missing", attempt=0)


def test_dag_dependency_parser_rejects_non_object_rows() -> None:
    assert parse_dag_dependencies_response('{"dag_dependencies":[{"action_status":"terminated"}]}') == [
        {"action_status": "terminated"}
    ]
    with pytest.raises(ValueError, match="valid DAG dependency list"):
        parse_dag_dependencies_response('{"dag_dependencies":["invalid"]}')


def test_advice_and_dependency_reason_helpers_extract_new_observability_contract() -> None:
    assert advice_state({"advice": {"state": ADVICE_STATE_TERMINATED}}) == ADVICE_STATE_TERMINATED
    dependency = {
        "action_status": "aborted",
        "reason_code": "PARENT_TERMINATED",
        "reason_details": {
            "parent_job_id": "123",
            "parent_terminal_advice_reason": "user_canceled",
            "workflow_id": "workflow-a",
        },
    }
    assert dag_dependency_matches(
        dependency,
        action_status="aborted",
        reason_code="PARENT_TERMINATED",
        reason_details={"parent_job_id": "123", "parent_terminal_advice_reason": "user_canceled"},
    )
    assert not dag_dependency_matches(
        dependency,
        action_status="aborted",
        reason_code="PARENT_TERMINATED",
        reason_details={"parent_job_id": "wrong"},
    )
