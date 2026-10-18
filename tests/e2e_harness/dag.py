# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Production-interface DAG evidence for native E2E scenarios."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

ADVICE_STATE_RELEASED = "Released"
ADVICE_STATE_TERMINATED = "Terminated"


def dependency_by_id(job_id: str) -> dict[str, object]:
    return {"target": {"type": "id", "id": job_id}, "trigger": "completed"}


def dependency_by_name(job_name: str) -> dict[str, object]:
    return {"target": {"type": "name", "name": job_name}, "trigger": "completed"}


def parse_job_status_response(response_text: str, *, job_id: str, attempt: int) -> dict[str, Any]:
    payload = json.loads(response_text)
    statuses = payload.get("statuses") if isinstance(payload, dict) else None
    if not isinstance(statuses, list):
        raise ValueError("Job Manager response has no statuses list")  # noqa: TRY004
    matches = [
        status
        for status in statuses
        if isinstance(status, dict) and status.get("job_id") == job_id and status.get("job_attempt_index") == attempt
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one status for {job_id}/{attempt}, observed {len(matches)}")
    return matches[0]


def parse_dag_dependencies_response(response_text: str) -> list[dict[str, Any]]:
    payload = json.loads(response_text)
    dependencies = payload.get("dag_dependencies") if isinstance(payload, dict) else None
    if not isinstance(dependencies, list) or not all(isinstance(item, dict) for item in dependencies):
        raise ValueError("Job Manager response has no valid DAG dependency list")
    return dependencies


def advice_state(payload: Mapping[str, object] | None) -> str | None:
    if payload is None:
        return None
    recommendation = payload.get("advice")
    return recommendation.get("state") if isinstance(recommendation, dict) else None


def dag_dependency_matches(
    dependency: Mapping[str, object],
    *,
    action_status: str,
    reason_code: str | None = None,
    reason_details: Mapping[str, object] | None = None,
) -> bool:
    """Match a dependency gate and an optional subset of its evaluation reason."""

    if dependency.get("action_status") != action_status:
        return False
    if reason_code is not None and dependency.get("reason_code") != reason_code:
        return False
    if reason_details is None:
        return True
    observed_details = dependency.get("reason_details")
    return isinstance(observed_details, dict) and all(
        observed_details.get(key) == value for key, value in reason_details.items()
    )
