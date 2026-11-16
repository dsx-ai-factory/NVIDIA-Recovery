# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task identity and group queries through HTTP and real MySQL storage."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from job_manager.main import app
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from recovery_libs.testing.storage.mysql import DB_NAME, ROOT_PASSWORD, mysql_container

__all__ = ["mysql_container"]


@pytest.fixture
def array_storage(mysql_container):
    connection = MySQLConnection(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        username="root",
        password=ROOT_PASSWORD,
        database_name=DB_NAME,
    )
    storage = MySQLStorage(connection=connection)
    assert storage.bootstrap()
    yield storage
    connection.write_engine.dispose()
    connection.engine.dispose()


def test_array_identity_survives_http_storage_round_trips(array_storage):
    client = TestClient(app)
    with patch("job_manager.main.job_manager") as manager:
        manager.storage = array_storage
        manager.upsert_job_record.side_effect = array_storage.upsert_job_record
        for job_id, attempt, raw in [("4200_0", 2, "4200"), ("4200_1", 0, "4201"), ("4300", 0, "4300")]:
            metadata = {"scheduler": "slurm", "slurm_job_id": job_id, "slurm_raw_job_id": raw}
            if "_" in job_id:
                metadata.update(slurm_array_job_id="4200", slurm_array_task_id=job_id.split("_")[1])
            response = client.post(
                "/jobs",
                json={
                    "job_id": job_id,
                    "job_attempt_index": attempt,
                    "workflow_id": "array-identity",
                    "state": "RUNNING",
                    "scheduler_metadata": metadata,
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["schedulerMetadata"] == metadata
        partial = {
            "job_id": "4200_0",
            "job_attempt_index": 2,
            "workflow_id": "array-identity",
            "state": "RUNNING",
            "scheduler_metadata": {"scheduler": "slurm", "slurm_state": "RUNNING"},
        }
        response = client.post("/jobs", json=partial)
        assert response.status_code == 200, response.text
        assert response.json()["job_group_id"] == "4200"
        assert response.json()["schedulerMetadata"]["slurm_array_task_id"] == "0"
        assert response.json()["schedulerMetadata"]["slurm_raw_job_id"] == "4200"

        response = client.get("/jobs", params={"job_group_id": "4200"})
        assert response.status_code == 200, response.text
        records = response.json()["statuses"]
        assert {(record["job_id"], record["job_attempt_index"]) for record in records} == {("4200_0", 2), ("4200_1", 0)}
        for record in records:
            assert record["heimdall_job_id"] == str(
                generate_heimdall_job_id(record["job_id"], record["job_attempt_index"])
            )
        response = client.get("/job_groups", params={"job_group_id": "4200"})
        assert response.status_code == 200, response.text
        assert response.json()["job_groups"][0]["unique_job_count"] == 2

        for change in [
            {"job_group_id": "4300"},
            {"scheduler_metadata": {"scheduler": "slurm", "slurm_raw_job_id": "4202"}},
            {"scheduler_metadata": {"scheduler": "slurm", "slurm_array_task_id": "1"}},
        ]:
            response = client.post("/jobs", json={**partial, **change})
            assert response.status_code == 400, response.text
        stored = array_storage.read_job_record(job_id="4200_0", job_attempt_index=2)
        assert stored.job_group_id == "4200"
        assert stored.scheduler_metadata["slurm_raw_job_id"] == "4200"
