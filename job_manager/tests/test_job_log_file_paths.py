# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from job_manager.job_log_file_paths import (
    INVALID_SLURM_LOG_FILE_PATHS_DETAIL,
    MAX_SLURM_LOG_FILE_PATH_BYTES,
    MAX_SLURM_LOG_FILE_PATHS,
    SlurmLogFilePathsValidationError,
    needs_heimdall_job_id_placeholder,
    normalize_slurm_log_file_paths,
    project_job_log_file_paths,
)
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id


class StringSubclass(str):
    pass


@pytest.mark.parametrize("raw_paths", [None, "/logs/job.out", (), {}, 1])
def test_normalize_slurm_log_file_paths_requires_list(raw_paths):
    with pytest.raises(SlurmLogFilePathsValidationError):
        normalize_slurm_log_file_paths(raw_paths)


@pytest.mark.parametrize(
    "element",
    [None, 1, True, b"/logs/job.out", ["/logs/job.out"], StringSubclass("/logs/job.out")],
)
def test_normalize_slurm_log_file_paths_requires_exact_strings(element):
    with pytest.raises(SlurmLogFilePathsValidationError):
        normalize_slurm_log_file_paths([element])


def test_normalize_slurm_log_file_paths_rejects_more_than_raw_limit_before_deduplication():
    raw_paths = ["/logs/job.out"] * (MAX_SLURM_LOG_FILE_PATHS + 1)

    with pytest.raises(SlurmLogFilePathsValidationError):
        normalize_slurm_log_file_paths(raw_paths)


def test_normalize_slurm_log_file_paths_accepts_raw_limit_before_deduplication():
    raw_paths = ["/logs/job.out"] * MAX_SLURM_LOG_FILE_PATHS

    assert normalize_slurm_log_file_paths(raw_paths) == ["/logs/job.out"]


@pytest.mark.parametrize("path", ["", "\x00", "/logs/job\x00.out", "\ud800"])
def test_normalize_slurm_log_file_paths_rejects_invalid_strings(path):
    with pytest.raises(SlurmLogFilePathsValidationError):
        normalize_slurm_log_file_paths([path])


@pytest.mark.parametrize(
    "path",
    [
        "a" * (MAX_SLURM_LOG_FILE_PATH_BYTES + 1),
        ("é" * (MAX_SLURM_LOG_FILE_PATH_BYTES // 2)) + "a",
    ],
)
def test_normalize_slurm_log_file_paths_rejects_paths_over_utf8_byte_limit(path):
    with pytest.raises(SlurmLogFilePathsValidationError):
        normalize_slurm_log_file_paths([path])


@pytest.mark.parametrize(
    "path",
    [
        "a" * MAX_SLURM_LOG_FILE_PATH_BYTES,
        "é" * (MAX_SLURM_LOG_FILE_PATH_BYTES // 2),
        " ",
    ],
)
def test_normalize_slurm_log_file_paths_accepts_boundary_valid_strings(path):
    assert normalize_slurm_log_file_paths([path]) == [path]


def test_normalize_slurm_log_file_paths_deduplicates_and_sorts_exact_strings():
    assert normalize_slurm_log_file_paths(["/logs/z.out", "/logs/a.out", "/logs/z.out", "/logs/ä.out"]) == [
        "/logs/a.out",
        "/logs/z.out",
        "/logs/ä.out",
    ]


def test_normalize_slurm_log_file_paths_projects_empty_list_to_none():
    assert normalize_slurm_log_file_paths([]) is None


def test_project_job_log_file_paths_projects_present_slurm_metadata():
    assert project_job_log_file_paths(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_log_file_paths": ["/logs/z.out", "/logs/a.out", "/logs/z.out"],
        },
        log_file_paths=["legacy-placeholder"],
    ) == ["/logs/a.out", "/logs/z.out"]


def test_project_job_log_file_paths_leaves_legacy_field_unchanged_when_slurm_key_is_absent():
    legacy_paths = ["legacy-placeholder"]

    projected = project_job_log_file_paths(
        scheduler_metadata={"scheduler": "slurm"},
        log_file_paths=legacy_paths,
    )

    assert projected is legacy_paths


def test_project_job_log_file_paths_preserves_explicit_non_slurm_legacy_paths():
    legacy_paths = ["/logs/from-legacy-client.out"]

    projected = project_job_log_file_paths(
        scheduler_metadata={"scheduler": "kubernetes"},
        log_file_paths=legacy_paths,
    )

    assert projected is legacy_paths


@pytest.mark.parametrize(
    "scheduler_metadata",
    [
        {"scheduler": "slurm"},
        {},
        {"scheduler": "kubernetes"},
    ],
)
def test_project_job_log_file_paths_preserves_explicit_empty_legacy_paths(scheduler_metadata):
    legacy_paths = []

    projected = project_job_log_file_paths(
        scheduler_metadata=scheduler_metadata,
        log_file_paths=legacy_paths,
    )

    assert projected is legacy_paths


def test_project_job_log_file_paths_without_scheduler_or_direct_paths_remains_none():
    assert (
        project_job_log_file_paths(
            scheduler_metadata={},
            log_file_paths=None,
        )
        is None
    )


@pytest.mark.parametrize(
    ("scheduler_metadata", "log_file_paths", "expected"),
    [
        ({}, None, True),
        ({"scheduler": "kubernetes"}, None, True),
        ({"scheduler": "slurm"}, None, False),
        ({}, [], False),
        ({"scheduler": "kubernetes"}, ["/logs/job.out"], False),
    ],
)
def test_needs_heimdall_job_id_placeholder(scheduler_metadata, log_file_paths, expected):
    assert (
        needs_heimdall_job_id_placeholder(
            scheduler_metadata=scheduler_metadata,
            log_file_paths=log_file_paths,
        )
        is expected
    )


def _job_payload(
    *,
    scheduler_metadata,
    log_file_paths=None,
    heimdall_job_id=None,
    job_attempt_index=0,
    state="RUNNING",
    aps_spec=None,
):
    payload = {
        "job_id": "job-log-paths",
        "job_attempt_index": job_attempt_index,
        "workflow_id": "workflow-test",
        "state": state,
        "scheduler_metadata": scheduler_metadata,
    }
    if aps_spec is not None:
        payload["aps_spec"] = aps_spec
    if log_file_paths is not None:
        payload["log_file_paths"] = log_file_paths
    if heimdall_job_id is not None:
        payload["heimdall_job_id"] = str(heimdall_job_id)
    return payload


def _return_upserted_record(record, *, populate_heimdall_id_log_path=False):
    if record.heimdall_job_id is None:
        record.heimdall_job_id = generate_heimdall_job_id(
            record.job_id,
            record.job_attempt_index,
        )
    if populate_heimdall_id_log_path:
        assert record.log_file_paths is None
        record.log_file_paths = [str(record.heimdall_job_id)]
    return record, True, None


def test_post_jobs_projects_deduplicated_sorted_slurm_paths():
    payload = _job_payload(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_log_file_paths": ["/logs/z.out", "/logs/a.out", "/logs/z.out"],
        },
        log_file_paths=["legacy-placeholder"],
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == ["/logs/a.out", "/logs/z.out"]
        assert response.json()["log_file_paths"] == ["/logs/a.out", "/logs/z.out"]
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": False}
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_preserves_retry_paths_reused_from_previous_attempt():
    reused_path = "/logs/slurm-123.out"
    payload = _job_payload(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_log_file_paths": [reused_path],
        },
        job_attempt_index=1,
        state="COMPLETED",
        aps_spec={"log_file_paths_shared_across_attempts": True},
    )
    previous_record = SimpleNamespace(log_file_paths=[reused_path])

    def read_attempt(*, job_id, job_attempt_index):
        assert job_id == "job-log-paths"
        if job_attempt_index == 0:
            return previous_record
        return None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.storage.read_job_record.side_effect = read_attempt
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == [reused_path]
        assert response.json()["log_file_paths"] == [reused_path]
        job_manager_mock.storage.read_job_record.assert_called_once_with(
            job_id="job-log-paths",
            job_attempt_index=1,
        )


def test_post_jobs_drops_reused_retry_paths_by_default():
    reused_path = "/logs/slurm-123.out"
    payload = _job_payload(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_log_file_paths": [reused_path],
        },
        job_attempt_index=1,
        state="COMPLETED",
    )
    previous_record = SimpleNamespace(log_file_paths=[reused_path])

    def read_attempt(*, job_id, job_attempt_index):
        assert job_id == "job-log-paths"
        if job_attempt_index == 0:
            return previous_record
        return None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.storage.read_job_record.side_effect = read_attempt
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths is None
        assert response.json()["log_file_paths"] is None
        assert job_manager_mock.storage.read_job_record.call_count == 2


def test_post_jobs_does_not_sanitize_non_slurm_retry_paths():
    payload = _job_payload(
        scheduler_metadata={"scheduler": "kubernetes"},
        log_file_paths=["/logs/from-kubernetes-client.out"],
        job_attempt_index=1,
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == ["/logs/from-kubernetes-client.out"]
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_slurm_key_absence_passes_none_to_preserve_last_known_good():
    payload = _job_payload(scheduler_metadata={"scheduler": "slurm"})
    received_log_file_paths = []

    def preserve_existing_paths(record, *, populate_heimdall_id_log_path=False):
        assert populate_heimdall_id_log_path is False
        received_log_file_paths.append(record.log_file_paths)
        record.log_file_paths = ["/logs/last-known-good.out"]
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = preserve_existing_paths

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        assert received_log_file_paths == [None]
        assert response.json()["log_file_paths"] == ["/logs/last-known-good.out"]
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_slurm_key_absence_preserves_explicit_direct_legacy_paths():
    payload = _job_payload(
        scheduler_metadata={"scheduler": "slurm"},
        log_file_paths=["/logs/from-legacy-client.out"],
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == ["/logs/from-legacy-client.out"]
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": False}
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_empty_slurm_metadata_projects_none_for_last_known_good_update():
    payload = _job_payload(
        scheduler_metadata={"scheduler": "slurm", "slurm_log_file_paths": []},
        log_file_paths=["legacy-placeholder"],
    )
    received_log_file_paths = []

    def preserve_existing_paths(record, *, populate_heimdall_id_log_path=False):
        assert populate_heimdall_id_log_path is False
        received_log_file_paths.append(record.log_file_paths)
        record.log_file_paths = ["/logs/last-known-good.out"]
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = preserve_existing_paths

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        assert received_log_file_paths == [None]
        assert response.json()["log_file_paths"] == ["/logs/last-known-good.out"]
        job_manager_mock.storage.read_job_record.assert_not_called()


@pytest.mark.parametrize(
    "scheduler_metadata",
    [
        {},
        {"scheduler": "kubernetes"},
    ],
)
def test_post_jobs_adds_heimdall_id_placeholder_for_non_slurm(scheduler_metadata):
    heimdall_job_id = uuid.uuid4()
    payload = _job_payload(
        scheduler_metadata=scheduler_metadata,
        heimdall_job_id=heimdall_job_id,
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == [str(heimdall_job_id)]
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": True}
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_omitted_id_create_generates_canonical_placeholder():
    payload = _job_payload(scheduler_metadata={})
    canonical_heimdall_job_id = generate_heimdall_job_id("job-log-paths", 0)

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.heimdall_job_id == canonical_heimdall_job_id
        assert upserted_record.log_file_paths == [str(canonical_heimdall_job_id)]
        assert response.json()["heimdall_job_id"] == str(canonical_heimdall_job_id)
        assert response.json()["log_file_paths"] == [str(canonical_heimdall_job_id)]
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": True}
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_omitted_id_update_uses_existing_id_placeholder():
    payload = _job_payload(scheduler_metadata={"scheduler": "kubernetes"})
    existing_heimdall_job_id = uuid.uuid4()
    existing_record = SimpleNamespace(heimdall_job_id=existing_heimdall_job_id)

    def retain_existing_id(record, *, populate_heimdall_id_log_path=False):
        assert populate_heimdall_id_log_path is True
        assert record.log_file_paths is None
        record.heimdall_job_id = existing_record.heimdall_job_id
        record.log_file_paths = [str(existing_record.heimdall_job_id)]
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = retain_existing_id

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.heimdall_job_id == existing_heimdall_job_id
        assert upserted_record.log_file_paths == [str(existing_heimdall_job_id)]
        assert response.json()["heimdall_job_id"] == str(existing_heimdall_job_id)
        assert response.json()["log_file_paths"] == [str(existing_heimdall_job_id)]
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_omitted_paths_update_preserves_existing_nonempty_paths():
    payload = _job_payload(scheduler_metadata={"scheduler": "kubernetes"})
    existing_heimdall_job_id = uuid.uuid4()
    existing_log_file_paths = ["/logs/real.out", "/logs/direct.out"]
    existing_record = SimpleNamespace(
        heimdall_job_id=existing_heimdall_job_id,
        log_file_paths=existing_log_file_paths,
    )

    def preserve_existing_paths(record, *, populate_heimdall_id_log_path=False):
        assert populate_heimdall_id_log_path is True
        assert record.log_file_paths is None
        record.heimdall_job_id = existing_record.heimdall_job_id
        record.log_file_paths = existing_record.log_file_paths
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = preserve_existing_paths

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.heimdall_job_id == existing_heimdall_job_id
        assert upserted_record.log_file_paths == existing_log_file_paths
        assert response.json()["log_file_paths"] == existing_log_file_paths
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_omitted_id_update_fills_null_existing_id_with_canonical_placeholder():
    payload = _job_payload(scheduler_metadata={"scheduler": "kubernetes"})
    canonical_heimdall_job_id = generate_heimdall_job_id("job-log-paths", 0)
    existing_record = SimpleNamespace(heimdall_job_id=None)

    def fill_null_existing_id(record, *, populate_heimdall_id_log_path=False):
        assert populate_heimdall_id_log_path is True
        assert record.log_file_paths is None
        if existing_record.heimdall_job_id is None:
            existing_record.heimdall_job_id = generate_heimdall_job_id(
                record.job_id,
                record.job_attempt_index,
            )
        record.heimdall_job_id = existing_record.heimdall_job_id
        record.log_file_paths = [str(existing_record.heimdall_job_id)]
        return record, False, None

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = fill_null_existing_id

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.heimdall_job_id == canonical_heimdall_job_id
        assert upserted_record.log_file_paths == [str(canonical_heimdall_job_id)]
        assert response.json()["heimdall_job_id"] == str(canonical_heimdall_job_id)
        assert response.json()["log_file_paths"] == [str(canonical_heimdall_job_id)]
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_preserves_explicit_non_slurm_log_file_paths():
    heimdall_job_id = uuid.uuid4()
    payload = _job_payload(
        scheduler_metadata={"scheduler": "kubernetes"},
        log_file_paths=["/logs/from-legacy-client.out"],
        heimdall_job_id=heimdall_job_id,
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == ["/logs/from-legacy-client.out"]
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": False}
        job_manager_mock.storage.read_job_record.assert_not_called()


def test_post_jobs_preserves_explicit_empty_non_slurm_log_file_paths():
    payload = _job_payload(
        scheduler_metadata={"scheduler": "kubernetes"},
        log_file_paths=[],
        heimdall_job_id=uuid.uuid4(),
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        job_manager_mock.upsert_job_record.side_effect = _return_upserted_record

        response = TestClient(app).post("/jobs", json=payload)

        assert response.status_code == 200
        upserted_record = job_manager_mock.upsert_job_record.call_args.args[0]
        assert upserted_record.log_file_paths == []
        assert job_manager_mock.upsert_job_record.call_args.kwargs == {"populate_heimdall_id_log_path": False}
        job_manager_mock.storage.read_job_record.assert_not_called()


@pytest.mark.parametrize(
    "raw_paths",
    [
        "/logs/not-a-list.out",
        [1],
        [""],
        ["/logs/job.out"] * (MAX_SLURM_LOG_FILE_PATHS + 1),
        ["\ud800"],
        ["a" * (MAX_SLURM_LOG_FILE_PATH_BYTES + 1)],
        ["/logs/job\x00.out"],
    ],
    ids=[
        "not-list",
        "not-string",
        "empty",
        "raw-count",
        "invalid-utf8",
        "utf8-byte-limit",
        "nul",
    ],
)
def test_post_jobs_rejects_malformed_slurm_metadata_with_redacted_400(raw_paths):
    payload = _job_payload(
        scheduler_metadata={
            "scheduler": "slurm",
            "slurm_log_file_paths": raw_paths,
        }
    )

    with patch("job_manager.main.job_manager") as job_manager_mock:
        response = TestClient(app).post(
            "/jobs",
            content=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": INVALID_SLURM_LOG_FILE_PATHS_DETAIL}
        job_manager_mock.upsert_job_record.assert_not_called()
