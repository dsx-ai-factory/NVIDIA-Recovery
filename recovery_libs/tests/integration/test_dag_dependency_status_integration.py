# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for guarded DAG dependency status updates against MySQL."""

import uuid
from datetime import UTC, datetime

from recovery_libs.storage.dataclass import DagDependency, DagDependencyActionStatus
from recovery_libs.storage.mysql_storage import MySQLStorage


def _create_dependency(storage: MySQLStorage) -> tuple[int, str]:
    dependent_job_id = f"child-{uuid.uuid4().hex}"
    dependency, created = storage.upsert_dag_dependency(
        DagDependency(
            dependent_job_id=dependent_job_id,
            dependent_job_action="release",
            dependency_job_identifier="parent-100",
            dependency_job_identifier_type="id",
            dependency_trigger_condition="completed",
        )
    )
    assert created is True
    assert dependency.id is not None
    return dependency.id, dependent_job_id


def _read_dependency(storage: MySQLStorage, dependent_job_id: str) -> DagDependency:
    dependencies, total = storage.read_dag_dependencies(dependent_job_id=dependent_job_id)
    assert total == 1
    return dependencies[0]


def test_status_update_keeps_action_time_when_none_is_given(storage):
    dependency_id, dependent_job_id = _create_dependency(storage)
    action_time = datetime(2026, 4, 16, 12, 30, tzinfo=UTC)

    assert storage.update_dag_dependency_status(
        id=dependency_id, action_status=DagDependencyActionStatus.IN_PROGRESS.value, action_time=action_time
    )
    assert storage.update_dag_dependency_status(
        id=dependency_id, action_status=DagDependencyActionStatus.SUCCESSFUL.value
    )

    stored = _read_dependency(storage, dependent_job_id)
    assert stored.action_status == DagDependencyActionStatus.SUCCESSFUL.value
    assert stored.action_time == action_time


def test_aborted_dependency_cannot_be_reopened(storage):
    dependency_id, dependent_job_id = _create_dependency(storage)
    assert storage.update_dag_dependency_status(id=dependency_id, action_status=DagDependencyActionStatus.ABORTED.value)

    reopened = storage.update_dag_dependency_status(
        id=dependency_id, action_status=DagDependencyActionStatus.IN_PROGRESS.value
    )

    assert reopened is False
    assert _read_dependency(storage, dependent_job_id).action_status == DagDependencyActionStatus.ABORTED.value


def test_status_update_of_missing_dependency_reports_no_change(storage):
    updated = storage.update_dag_dependency_status(
        id=2**31 - 1, action_status=DagDependencyActionStatus.IN_PROGRESS.value
    )

    assert updated is False
