# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from job_manager.job_lifecycle_times import (
    INVALID_SLURM_LIFECYCLE_TIMES_DETAIL,
    SlurmLifecycleTimesValidationError,
    normalize_slurm_lifecycle_time,
    project_job_lifecycle_times,
)
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage.mysql_storage import MySQLStorage


def test_normalize_slurm_lifecycle_time_parses_timezone_aware_iso_8601():
    assert normalize_slurm_lifecycle_time(
        "2026-07-31T19:21:06Z",
        metadata_key="slurm_end_time",
    ) == datetime(2026, 7, 31, 19, 21, 6, tzinfo=UTC)


@pytest.mark.parametrize("raw_time", [None, "", 1, True, [], {}, "not-a-time", "2026-07-31T19:21:06"])
def test_normalize_slurm_lifecycle_time_rejects_malformed_or_naive_values(raw_time):
    with pytest.raises(SlurmLifecycleTimesValidationError):
        normalize_slurm_lifecycle_time(raw_time, metadata_key="slurm_end_time")


@pytest.mark.parametrize("raw_time", ["1969-12-31T23:59:59Z", "2106-02-07T06:28:14Z"])
def test_normalize_slurm_lifecycle_time_rejects_values_outside_storage_range(raw_time):
    with pytest.raises(SlurmLifecycleTimesValidationError, match="outside the supported MySQL TIMESTAMP range"):
        normalize_slurm_lifecycle_time(raw_time, metadata_key="slurm_start_time")


def test_project_job_lifecycle_times_projects_present_slurm_metadata():
    existing_end_time = datetime(2026, 7, 31, 20, 0, tzinfo=UTC)

    projected = project_job_lifecycle_times(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_submit_time": "2026-07-31T19:18:00Z",
            "slurm_start_time": "2026-07-31T19:19:38Z",
            "slurm_end_time": "2026-07-31T19:21:06Z",
        },
        state="FAILED",
        submit_time=None,
        start_time=None,
        end_time=existing_end_time,
    )

    assert projected.submit_time == datetime(2026, 7, 31, 19, 18, tzinfo=UTC)
    assert projected.start_time == datetime(2026, 7, 31, 19, 19, 38, tzinfo=UTC)
    assert projected.end_time == datetime(2026, 7, 31, 19, 21, 6, tzinfo=UTC)


def test_project_job_lifecycle_times_preserves_fields_when_keys_are_absent():
    submit_time = datetime(2026, 7, 31, 19, 18, tzinfo=UTC)
    start_time = datetime(2026, 7, 31, 19, 19, 38, tzinfo=UTC)
    end_time = datetime(2026, 7, 31, 19, 21, 6, tzinfo=UTC)

    assert project_job_lifecycle_times(
        scheduler_metadata={"scheduler": "slurm"},
        state="FAILED",
        submit_time=submit_time,
        start_time=start_time,
        end_time=end_time,
    ) == (submit_time, start_time, end_time)


def test_project_job_lifecycle_times_ignores_non_slurm_metadata():
    start_time = datetime(2026, 7, 31, 19, 19, 38, tzinfo=UTC)

    assert project_job_lifecycle_times(
        scheduler_metadata={
            "scheduler": "kubernetes",
            "slurm_start_time": "malformed-but-not-owned",
        },
        state="RUNNING",
        submit_time=None,
        start_time=start_time,
        end_time=None,
    ) == (None, start_time, None)


def test_project_job_lifecycle_times_does_not_treat_active_deadline_as_end_time():
    projected = project_job_lifecycle_times(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_submit_time": "2026-07-31T19:55:44Z",
            "slurm_start_time": "2026-07-31T19:55:44Z",
            "slurm_end_time": "2026-07-31T20:07:44Z",
        },
        state="RUNNING",
        submit_time=None,
        start_time=None,
        end_time=None,
    )

    assert projected.submit_time == datetime(2026, 7, 31, 19, 55, 44, tzinfo=UTC)
    assert projected.start_time == datetime(2026, 7, 31, 19, 55, 44, tzinfo=UTC)
    assert projected.end_time is None


def _return_upserted_record(record, *, populate_heimdall_id_log_path=False):
    if record.heimdall_job_id is None:
        record.heimdall_job_id = generate_heimdall_job_id(record.job_id, record.job_attempt_index)
    return record, True, None


def test_post_jobs_projects_slurm_lifecycle_times_before_persistence():
    payload = {
        "job_id": "30",
        "job_attempt_index": 0,
        "workflow_id": "megatron-crash-segfault",
        "state": "FAILED",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_submit_time": "2026-07-31T19:18:00Z",
            "slurm_start_time": "2026-07-31T19:19:38Z",
            "slurm_end_time": "2026-07-31T19:21:06Z",
        },
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.submit_time == datetime(2026, 7, 31, 19, 18, tzinfo=UTC)
        assert upserted_record.start_time == datetime(2026, 7, 31, 19, 19, 38, tzinfo=UTC)
        assert upserted_record.end_time == datetime(2026, 7, 31, 19, 21, 6, tzinfo=UTC)


def test_post_jobs_does_not_persist_active_slurm_deadline_as_end_time():
    payload = {
        "job_id": "32",
        "job_attempt_index": 0,
        "workflow_id": "heimdall-mr178-proof",
        "state": "RUNNING",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_submit_time": "2026-07-31T19:55:44Z",
            "slurm_start_time": "2026-07-31T19:55:44Z",
            "slurm_end_time": "2026-07-31T20:07:44Z",
        },
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.submit_time == datetime(2026, 7, 31, 19, 55, 44, tzinfo=UTC)
        assert upserted_record.start_time == datetime(2026, 7, 31, 19, 55, 44, tzinfo=UTC)
        assert upserted_record.end_time is None


def test_post_jobs_preserves_missing_start_time():
    payload = {
        "job_id": "33",
        "job_attempt_index": 0,
        "workflow_id": "pending-without-start",
        "state": "PENDING",
        "last_updated_at": "2026-07-31T19:55:44Z",
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.start_time is None


def test_post_jobs_rejects_malformed_slurm_lifecycle_time_metadata():
    payload = {
        "job_id": "30",
        "job_attempt_index": 0,
        "workflow_id": "megatron-crash-segfault",
        "state": "FAILED",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_end_time": "not-a-time",
        },
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 400
        assert response.json() == {"detail": INVALID_SLURM_LIFECYCLE_TIMES_DETAIL}
        job_manager_mock.upsert_job_record.assert_not_called()


def test_post_jobs_rejects_out_of_range_slurm_lifecycle_time_metadata():
    payload = {
        "job_id": "30",
        "job_attempt_index": 6,
        "workflow_id": "requeued-without-start",
        "state": "FAILED",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_state": "REQUEUED",
            "slurm_start_time": "2106-02-07T06:28:14Z",
        },
    }

    with patch("job_manager.main.job_manager") as job_manager_mock:
        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 400
        assert response.json() == {"detail": INVALID_SLURM_LIFECYCLE_TIMES_DETAIL}
        job_manager_mock.upsert_job_record.assert_not_called()


@pytest.mark.parametrize("field_name", ["submit_time", "start_time", "end_time"])
@pytest.mark.parametrize("raw_time", ["1969-12-31T23:59:59Z", "2106-02-07T06:28:14Z"])
def test_post_jobs_rejects_out_of_range_top_level_lifecycle_time_before_database(field_name, raw_time):
    payload = {
        "job_id": "34",
        "job_attempt_index": 0,
        "workflow_id": "invalid-top-level-lifecycle-time",
        "state": "RUNNING",
        field_name: raw_time,
    }
    connection = MagicMock()

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = MySQLStorage(connection).upsert_job_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 400
        assert response.json() == {"detail": f"{field_name} is outside the supported MySQL TIMESTAMP range"}
        connection.session.assert_not_called()


@pytest.mark.parametrize("state", ["PENDING", "CONFIGURING", ""])
def test_post_jobs_does_not_promote_unstarted_slurm_start(state):
    """Queue estimates must not become typed execution starts at HTTP ingress."""
    payload = {
        "job_id": "16",
        "job_attempt_index": 1,
        "workflow_id": "retry-runtime",
        "state": state,
        "start_time": "2026-10-02T19:25:08Z",
        "scheduler_metadata": {
            "scheduler": "slurm",
            "slurm_start_time": "2026-10-02T19:25:08Z",
        },
    }
    with patch("job_manager.main.job_manager") as manager:
        manager.upsert_job_record.side_effect = _return_upserted_record
        response = TestClient(app).post("/jobs", json=payload)
        assert response.status_code == 200
        record = manager.upsert_job_record.call_args.args[0]
        assert record.start_time is None
        assert record.scheduler_metadata["slurm_start_time"] == "2026-10-02T19:25:08Z"


@pytest.mark.parametrize("state", ["RUNNING", "COMPLETING", "RESIZING", "SUSPENDED", "COMPLETED", "FAILED", "REQUEUED"])
def test_project_job_lifecycle_times_accepts_execution_start(state):
    """Execution observations can supply starts even when running was missed."""
    projected = project_job_lifecycle_times(
        scheduler_metadata={"scheduler": "slurm", "slurm_start_time": "2026-10-02T19:33:20Z"},
        state=state,
        submit_time=None,
        start_time=None,
        end_time=None,
    )
    assert projected.start_time == datetime(2026, 10, 2, 19, 33, 20, tzinfo=UTC)
