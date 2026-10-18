# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime, timedelta
from typing import Any

from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.job_manager.models import JobAnomaly, JobCheckpointInfo
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord


def create_example_slurm_job_record(
    submit_time: datetime,
    last_updated_at: datetime,
    anomaly_time: datetime | None = None,
    checkpoint_time: datetime | None = None,
    job_id: str = "12345",
    attempt_index: int = 0,
    job_group_id: str | None = None,
    job_name: str = "Test Job",
    state: str = "Pending",
    retry_of: str | None = "12000",
    allocated_node_list_length: int = 5,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    log_file_paths: list[str] | None = None,
) -> SlurmJobExecutionRecord:
    return SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=attempt_index,
        workflow_id=f"workflow-{job_id}",
        job_group_id=job_group_id,
        job_name=job_name,
        state=state,
        state_reason="Dependency",
        submit_time=submit_time,
        start_time=start_time,
        end_time=end_time,
        cluster="test_cluster",
        partition="recovery",
        reservation="recovery_prod",
        namespace="test-k8s-namespace",
        user="test_user",
        command="sbatch test.sh",
        comment="{'APS': {}}",
        dependency="10000",
        total_task_count=50 * 8,
        node_count=allocated_node_list_length,
        total_cpu_count=800,
        total_gpu_count=50 * 8,
        total_memory_mb=50 * 100 * 1000,
        allocated_node_list=[f"node_{i}" for i in range(allocated_node_list_length)],
        inclusion_node_list=[f"node_{i}" for i in range(allocated_node_list_length)],
        anomalies=(
            [JobAnomaly(anomaly_type="Hang", timestamp=anomaly_time, original_watcher_anomaly_event="EVENT")]
            if anomaly_time
            else None
        ),
        checkpoints=[JobCheckpointInfo(timestamp=checkpoint_time, uuid=str(uuid.uuid4()))] if checkpoint_time else None,
        aps_spec=ApsSlurmJobSpec(workflow_name="test_workflow"),
        last_state_transition_time=submit_time + timedelta(minutes=5),
        last_updated_at=last_updated_at,
        retry_of=retry_of,
        log_file_paths=log_file_paths,
        scheduler_metadata=None,
    )


def create_example_squeue_json_output(
    resource_request_str: str = "cpu=800,mem=50000M,gres/gpu=8",
    resource_allocation_str: str = "",
    is_time_dict: bool = False,
    is_state_list: bool = False,
    is_in_job_group: bool = False,
    is_synthetic_array_entry: bool = False,
) -> dict[str, Any]:
    aps_spec = ApsSlurmJobSpec(workflow_name="test_workflow")

    return {
        "job_id": 12345,
        "name": "Test Job",
        "array_job_id": {
            "set": True,
            "infinite": False,
            "number": 12345 if (is_in_job_group or is_synthetic_array_entry) else 0,
        },
        "array_task_id": {"set": not is_synthetic_array_entry and is_in_job_group, "infinite": False, "number": 0},
        "user_name": "test_user",
        "user_id": "1000",
        # this intentionally different than "user_name" as it should only be used when "user_name" is empty
        "mail_user": "mail_user",
        "restart_cnt": 0,
        "job_state": ["PENDING"] if is_state_list else "PENDING",
        "state_reason": "Dependency",
        "submit_time": {"set": True, "infinite": False, "number": 1722495600} if is_time_dict else 1722495600,
        "start_time": {"set": False, "infinite": False, "number": -1} if is_time_dict else -1,
        "end_time": {"set": False, "infinite": False, "number": -1} if is_time_dict else -1,
        "cluster": "test_cluster",
        "partition": "recovery",
        "resv_name": "recovery_prod",
        "nodes": "node[1-5]",
        "excluded_nodes": "node[6-8],node10",
        "required_nodes": "node[1-3]",
        "command": "sbatch test.sh",
        "dependency": "10000",
        "comment": f'{{"APS": {aps_spec.model_dump_json()}}}',
        "tres_req_str": resource_request_str,
        "tres_alloc_str": resource_allocation_str,
        "node_count": {"number": 5},
    }


def create_example_sacct_json_output(
    is_state_list: bool = False,
    job_state: str = "COMPLETED",
    comment: str = '{"APS": {}}',
    is_in_job_group: bool = False,
    is_synthetic_array_entry: bool = False,
) -> dict[str, Any]:
    return {
        "allocation_nodes": 1,
        "job_id": 12345,
        "array": {
            "job_id": 12345 if (is_in_job_group or is_synthetic_array_entry) else 0,
            "limits": {"max": {"running": {"tasks": 0}}},
            "task_id": {"set": not is_synthetic_array_entry and is_in_job_group, "infinite": False, "number": 0},
            "task": "",
        },
        "name": "Test Job",
        "state": {
            "current": ["PENDING", job_state] if is_state_list else job_state,
            "reason": "BeginTime",
        },
        "cluster": "test_cluster",
        "partition": "recovery",
        "comment": {"job": comment},
        "reservation": {
            "name": "recovery_prod",
        },
        "user": "test_user",
        "submit_line": "sbatch test.sh",
        "tres": {
            "requested": {
                "total": [
                    {"type": "cpu", "name": "", "id": 1, "count": 20},
                    {"type": "mem", "name": "", "id": 2, "count": 5910528},
                    {"type": "energy", "name": "", "id": 3, "count": 0},
                    {"type": "fs", "name": "disk", "id": 6, "count": 75049},
                    {"type": "vmem", "name": "", "id": 7, "count": 5918720},
                    {"type": "pages", "name": "", "id": 8, "count": 0},
                ]
            },
            "allocated": [
                {"type": "cpu", "name": "", "id": 1, "count": 128},
                {"type": "mem", "name": "", "id": 2, "count": 2063000},
                {"type": "node", "name": "", "id": 4, "count": 1},
                {"type": "gres", "name": "gpu", "id": 1001, "count": 8},
            ],
        },
        "nodes": "node[1-5]",
        "failed_node": "node3" if job_state == "FAILED" else None,
        "time": {
            "start": 1730751854,
            "end": 1730751905,
            "submission": 1730750582,
        },
    }
