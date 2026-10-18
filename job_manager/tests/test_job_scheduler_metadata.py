# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from job_manager.job_scheduler_metadata import (
    INVALID_SLURM_JOB_METADATA_DETAIL,
    SlurmJobMetadataValidationError,
    project_job_scheduler_fields,
)
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id


def _fields(metadata, **overrides):
    values = {
        "scheduler_metadata": metadata,
        "cluster": None,
        "partition": None,
        "user": None,
        "command": None,
        "state_reason": None,
        "node_count": None,
        "total_gpu_count": None,
        "total_cpu_count": None,
        "total_memory_mb": None,
    }
    values.update(overrides)
    return project_job_scheduler_fields(**values)


def test_project_job_scheduler_fields_maps_slurm_metadata():
    projected = _fields(
        {
            "scheduler": "slurm",
            "slurm_cluster": "cluster-a",
            "slurm_partition": "batch",
            "slurm_user": "alice",
            "slurm_command": "/workspace/train.sh",
            "slurm_reason": "Resources",
            "slurm_node_count": 2.0,
            "slurm_total_gpu_count": 8.0,
            "slurm_total_cpu_count": 16.0,
            "slurm_total_memory_mb": 64000.0,
        }
    )

    assert projected.cluster == "cluster-a"
    assert projected.partition == "batch"
    assert projected.user == "alice"
    assert projected.command == "/workspace/train.sh"
    assert projected.state_reason == "Resources"
    assert projected.node_count == 2
    assert projected.total_gpu_count == 8
    assert projected.total_cpu_count == 16
    assert projected.total_memory_mb == 64000


def test_project_job_scheduler_fields_preserves_absent_slurm_values():
    projected = _fields(
        {"scheduler": "slurm", "slurm_cluster": "cluster-a"},
        command="/workspace/existing.sh",
    )

    assert projected.cluster == "cluster-a"
    assert projected.command == "/workspace/existing.sh"


def test_project_job_scheduler_fields_ignores_non_slurm_metadata():
    projected = _fields(
        {"scheduler": "kubernetes", "slurm_cluster": "wrong-cluster"},
        cluster="kubernetes-cluster",
    )

    assert projected.cluster == "kubernetes-cluster"


@pytest.mark.parametrize(
    "key,value",
    [
        ("slurm_cluster", 42),
        ("slurm_reason", 42),
        ("slurm_user", ""),
        ("slurm_node_count", True),
        ("slurm_total_gpu_count", 1.5),
        ("slurm_total_memory_mb", -1),
    ],
)
def test_project_job_scheduler_fields_rejects_malformed_values(key, value):
    with pytest.raises(SlurmJobMetadataValidationError):
        _fields({"scheduler": "slurm", key: value})


def _return_upserted_record(record, *, populate_heimdall_id_log_path=False):
    record.heimdall_job_id = generate_heimdall_job_id(record.job_id, record.job_attempt_index)
    return record, True, None


def test_post_jobs_projects_slurm_metadata_into_persisted_record():
    payload = {
        "job_id": "12345",
        "job_attempt_index": 0,
        "workflow_id": "workflow-a",
        "state": "RUNNING",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_cluster": "cluster-a",
            "slurm_partition": "batch",
            "slurm_user": "alice",
            "slurm_command": "/workspace/train.sh",
            "slurm_reason": "None",
            "slurm_node_count": 2,
            "slurm_total_gpu_count": 8,
            "slurm_total_cpu_count": 16,
            "slurm_total_memory_mb": 64000,
        },
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert record.cluster == "cluster-a"
        assert record.partition == "batch"
        assert record.user == "alice"
        assert record.command == "/workspace/train.sh"
        assert record.state_reason == "None"
        assert record.node_count == 2
        assert record.total_gpu_count == 8
        assert record.total_cpu_count == 16
        assert record.total_memory_mb == 64000
        assert record.scheduler_metadata == payload["scheduler_metadata"]


def test_post_jobs_leaves_missing_slurm_command_unset_for_last_known_good_update():
    payload = {
        "job_id": "12345",
        "job_attempt_index": 0,
        "workflow_id": "workflow-a",
        "state": "FAILED",
        "scheduler_metadata": {"scheduler": "slurm", "slurm_cluster": "cluster-a"},
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert record.cluster == "cluster-a"
        assert record.command is None


def test_post_jobs_preserves_stored_state_reason_when_slurm_reason_is_omitted():
    payload = {
        "job_id": "12345",
        "job_attempt_index": 0,
        "workflow_id": "workflow-a",
        "state": "FAILED",
        "scheduler_metadata": {"scheduler": "slurm", "slurm_cluster": "cluster-a"},
    }
    stored_state_reason = "Dependency"

    def preserve_existing_state_reason(record, *, populate_heimdall_id_log_path=False):
        assert record.state_reason is None
        record.state_reason = stored_state_reason
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = preserve_existing_state_reason

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        assert response.json()["state_reason"] == stored_state_reason
        record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert "slurm_reason" not in record.scheduler_metadata


def test_post_jobs_rejects_malformed_slurm_job_metadata():
    payload = {
        "job_id": "12345",
        "job_attempt_index": 0,
        "workflow_id": "workflow-a",
        "state": "RUNNING",
        "scheduler_metadata": {"scheduler": "slurm", "slurm_node_count": 1.5},
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 400
        assert response.json() == {"detail": INVALID_SLURM_JOB_METADATA_DETAIL}
        job_manager_mock.upsert_job_record.assert_not_called()
