# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify publication identity and observations across HTTP and real storage."""

from unittest.mock import patch

from fastapi.testclient import TestClient
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.testing.storage.mysql import check_mysql_available, mysql_container, storage

__all__ = ["check_mysql_available", "mysql_container", "storage"]


def _workload_metadata(workload_uid, name="training", **extra):
    """Return the scheduler metadata discovery publishes for one workload object."""
    return {
        "scheduler": "kubernetes",
        "namespace": "training",
        "job_name": name,
        "job_kind": "JobSet",
        "job_api_version": "jobset.x-k8s.io/v1alpha2",
        "workload_uid": workload_uid,
        **extra,
    }


def test_kubernetes_publication_round_trips_without_merging_same_name(storage):
    client = TestClient(app)
    original_id = "7e913d4d-98fa-4468-937d-15a7a925155e"
    replacement_id = "4c77dded-38bd-4380-ae79-f739f84bd765"

    def publish(job_id, workload_uid, nodes, state="RUNNING"):
        return client.post(
            "/jobs",
            json={
                "job_id": job_id,
                "workflow_id": job_id,
                "job_attempt_index": 0,
                "job_name": "training",
                "state": state,
                "allocated_node_list": nodes,
                "scheduler_metadata": _workload_metadata(workload_uid),
            },
        )

    with patch("job_manager.main.job_manager") as manager:
        manager.storage = storage
        manager.upsert_job_record.side_effect = storage.upsert_job_record
        for nodes, state in ((["node-a"], "RUNNING"), ([], "DELETED")):
            response = publish(original_id, "uid-a", nodes, state)
            assert response.status_code == 200, response.text
        collision = publish(original_id, "uid-b", ["node-b"])
        assert collision.status_code == 400, collision.text
        replacement = publish(replacement_id, "uid-b", ["node-b"])
        assert replacement.status_code == 200, replacement.text

        response = client.get("/jobs", params={"job_id": original_id, "completed_jobs": "true"})
        assert response.status_code == 200, response.text
        [original] = response.json()["statuses"]
        # Deletion keeps the last nodes recorded while the attempt was open.
        assert original["state"] == "DELETED"
        assert original["allocated_node_list"] == ["node-a"]
        assert original["schedulerMetadata"]["workload_uid"] == "uid-a"
        assert original["aps_spec"] is None
        assert original["log_file_paths"] is None
        assert original["heimdall_job_id"] == str(generate_heimdall_job_id(original_id, 0))
        assert replacement.json()["heimdall_job_id"] == str(generate_heimdall_job_id(replacement_id, 0))


def test_kubernetes_final_outcome_survives_cleanup_and_late_observations(storage):
    client = TestClient(app)
    job_id = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a01"
    terminal_first_id = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a02"

    def publish(state, nodes, job_id=job_id):
        response = client.post(
            "/jobs",
            json={
                "job_id": job_id,
                "workflow_id": job_id,
                "job_attempt_index": 0,
                "job_name": "training",
                "state": state,
                "allocated_node_list": nodes,
                "scheduler_metadata": _workload_metadata("uid-a"),
            },
        )
        assert response.status_code == 200, response.text
        return response.json()

    with patch("job_manager.main.job_manager") as manager:
        manager.storage = storage
        manager.upsert_job_record.side_effect = storage.upsert_job_record
        # A node removal while open is applied; worker cleanup at the end is not.
        publish("RUNNING", ["node-a", "node-b"])
        publish("RUNNING", ["node-a"])
        # A partial terminal snapshot can race with cleanup on other nodes.
        publish("FAILED", ["node-c"])
        # A retried observation that commits late must not reopen the attempt.
        publish("RUNNING", ["node-d"])

        response = client.get("/jobs", params={"job_id": job_id, "completed_jobs": "true"})
        assert response.status_code == 200, response.text
        [stored] = response.json()["statuses"]
        assert stored["state"] == "FAILED"
        assert stored["allocated_node_list"] == ["node-a", "node-c"]
        assert stored["end_time"] is None

        # Closure can insert the terminal row before an open write whose reply
        # timed out commits. The late write must not reopen it, and its
        # response reports the stored outcome that discovery acknowledges.
        publish("DELETED", ["node-a"], job_id=terminal_first_id)
        late = publish("RUNNING", ["node-b"], job_id=terminal_first_id)
        assert late["state"] == "DELETED"
        assert late["schedulerMetadata"]["workload_uid"] == "uid-a"
        assert late["allocated_node_list"] == ["node-a"]
        assert late["end_time"] is None


def test_open_inventory_pages_include_all_kubernetes_attempts(storage):
    client = TestClient(app)
    jobs = ["d173ba71-add6-4458-8dfa-7d52c42bb33a", "a4400ec3-cbd8-4df5-b514-7ef0e5dcb50e"]
    expected = {(jobs[0], 0), (jobs[0], 1), (jobs[1], 0)}

    with patch("job_manager.main.job_manager") as manager:
        manager.storage = storage
        manager.upsert_job_record.side_effect = storage.upsert_job_record
        for job_id, attempt in [(jobs[0], 1), (jobs[1], 0), (jobs[0], 0)]:
            response = client.post(
                "/jobs",
                json={
                    "job_id": job_id,
                    "workflow_id": job_id,
                    "job_attempt_index": attempt,
                    "state": "RUNNING",
                    "scheduler_metadata": _workload_metadata(job_id),
                },
            )
            assert response.status_code == 200, response.text

        found = set()
        for offset in range(len(expected)):
            response = client.get("/jobs", params={"completed_jobs": "false", "limit": 1, "offset": offset})
            assert response.status_code == 200, response.text
            [row] = response.json()["statuses"]
            found.add((row["job_id"], row["job_attempt_index"]))
        assert found == expected


def test_kubernetes_lookup_identity_is_fixed_for_a_workload_uid(storage):
    """Restart inventory and Advice job references resolve the workload from its stored lookup keys."""
    client = TestClient(app)
    job_id = "5d0c3f43-90a4-4b8e-9a51-2a3d0c6f7e11"

    def publish(metadata, state="RUNNING"):
        return client.post(
            "/jobs",
            json={
                "job_id": job_id,
                "workflow_id": job_id,
                "job_attempt_index": 0,
                "state": state,
                "allocated_node_list": ["node-a"],
                "scheduler_metadata": metadata,
            },
        )

    def stored_metadata():
        response = client.get("/jobs", params={"job_id": job_id})
        assert response.status_code == 200, response.text
        [row] = response.json()["statuses"]
        return row["schedulerMetadata"]

    with patch("job_manager.main.job_manager") as manager:
        manager.storage = storage
        manager.upsert_job_record.side_effect = storage.upsert_job_record
        incomplete = _workload_metadata("uid-a")
        del incomplete["job_api_version"]
        response = publish(incomplete)
        assert response.status_code == 400, response.text
        assert storage.read_job_record(job_id, 0) is None

        assert publish(_workload_metadata("uid-a")).status_code == 200
        renamed = publish(_workload_metadata("uid-a", name="renamed"))
        assert renamed.status_code == 400, renamed.text
        assert stored_metadata()["job_name"] == "training"

        response = publish({"scheduler": "kubernetes", "workload_uid": "uid-a"})
        assert response.status_code == 200, response.text
        assert stored_metadata() == _workload_metadata("uid-a")


def test_kubernetes_deletion_replaces_an_unrecognized_state(storage):
    """Only recognized final states are sticky, so discovery's verified closure can still be recorded."""
    client = TestClient(app)
    job_id = "5d0c3f43-90a4-4b8e-9a51-2a3d0c6f7e12"

    def publish(state, nodes):
        response = client.post(
            "/jobs",
            json={
                "job_id": job_id,
                "workflow_id": job_id,
                "job_attempt_index": 0,
                "state": state,
                "allocated_node_list": nodes,
                "scheduler_metadata": _workload_metadata("uid-a"),
            },
        )
        assert response.status_code == 200, response.text
        return response.json()

    with patch("job_manager.main.job_manager") as manager:
        manager.storage = storage
        manager.upsert_job_record.side_effect = storage.upsert_job_record
        publish("RUNNING", ["node-a"])
        assert publish("UNKNOWN", ["node-a"])["state"] == "UNKNOWN"
        closed = publish("DELETED", ["node-b"])
        assert closed["state"] == "DELETED"
        assert closed["allocated_node_list"] == ["node-a", "node-b"]
        assert publish("RUNNING", ["node-c"])["state"] == "DELETED"
