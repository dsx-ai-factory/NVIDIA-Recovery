# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import unittest
import uuid
from datetime import UTC, datetime, timedelta
from inspect import Parameter, signature
from unittest.mock import patch

import pytest
from recovery_libs.schema.aps_spec.spec import (
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
)
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    JobCheckpointInfo,
    SlurmJobExecutionStatus,
)
from recovery_libs.storage.dataclass import (
    JobConfigValidationFailureTypes,
    SlurmJobExecutionRecord,
    parse_job_group_id_from_sacct_json,
    parse_job_group_id_from_squeue_json,
    parse_job_id_from_sacct_json,
    parse_job_id_from_squeue_json,
    parse_job_state,
    parse_resource_value_sacct,
)
from test_utils import (
    create_example_sacct_json_output,
    create_example_slurm_job_record,
    create_example_squeue_json_output,
)


class TestSlurmJobExecutionRecord(unittest.TestCase):
    def test_workflow_id_is_a_required_binary_collated_field(self):
        parameters = signature(SlurmJobExecutionRecord).parameters
        self.assertIn("workflow_id", parameters)
        self.assertIs(parameters["workflow_id"].default, Parameter.empty)

        column = SlurmJobExecutionRecord.__table__.c.workflow_id
        self.assertFalse(column.nullable)
        self.assertEqual(column.type.length, 256)
        self.assertEqual(column.type.charset, "utf8mb4")
        self.assertEqual(column.type.collation, "utf8mb4_0900_bin")

    def test_initialization(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        anomaly_time = submit_time + timedelta(minutes=5)

        record = create_example_slurm_job_record(submit_time, now, anomaly_time)

        self.assertEqual(record.job_id, "12345")
        self.assertEqual(record.job_attempt_index, 0)
        self.assertEqual(record.state, "Pending")
        self.assertEqual(record.state_reason, "Dependency")
        self.assertEqual(record.submit_time, submit_time)

        self.assertEqual(record.cluster, "test_cluster")
        self.assertEqual(record.partition, "recovery")
        self.assertEqual(record.reservation, "recovery_prod")
        self.assertEqual(record.namespace, "test-k8s-namespace")
        self.assertEqual(record.user, "test_user")
        self.assertEqual(record.command, "sbatch test.sh")
        self.assertEqual(record.dependency, "10000")

        self.assertEqual(record.inclusion_node_list, [f"node_{i}" for i in range(5)])
        self.assertEqual(record.allocated_node_list, [f"node_{i}" for i in range(5)])
        self.assertEqual(record.node_count, 5)
        self.assertEqual(record.total_cpu_count, 800)
        self.assertEqual(record.total_gpu_count, 400)
        self.assertEqual(record.total_memory_mb, 50 * 100 * 1000)
        self.assertEqual(record.total_task_count, 400)
        self.assertEqual(len(record.anomalies), 1)
        self.assertEqual(record.anomalies[0].anomaly_type, "Hang")
        self.assertEqual(record.anomalies[0].timestamp, anomaly_time)

        self.assertEqual(record.last_updated_at, now)
        self.assertEqual(record.retry_of, "12000")

    def test_serialization_deserialization(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(now - timedelta(minutes=10), now)

        # Serialization + deserialization should produce the same object.
        self.assertEqual(record, SlurmJobExecutionRecord.from_json(json.loads(record.to_json())))

    def test_attribution_request_emitted_round_trips(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(now - timedelta(minutes=10), now)
        record.attribution_request_emitted = True

        deserialized = SlurmJobExecutionRecord.from_json(json.loads(record.to_json()))
        self.assertTrue(deserialized.attribution_request_emitted)

    def test_attribution_request_emitted_defaults_false_for_legacy_json(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(now - timedelta(minutes=10), now)
        record_json_dict = json.loads(record.to_json())
        # Legacy record persisted before the field existed.
        record_json_dict.pop("attribution_request_emitted", None)

        deserialized = SlurmJobExecutionRecord.from_json(record_json_dict)
        self.assertFalse(deserialized.attribution_request_emitted)

    def test_json_deserializer_backward_compatibility(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=30)
        checkpoint_time = now - timedelta(minutes=10)
        anomaly_time = submit_time + timedelta(minutes=5)

        record = create_example_slurm_job_record(submit_time, now, anomaly_time, checkpoint_time)
        record_json_dict = json.loads(record.to_json())

        # Remove a few keys from the JSON dict to simulate the incompatible package version scenario.
        record_json_dict.pop("checkpoints")
        record_json_dict.pop("aps_spec")

        deserialized = SlurmJobExecutionRecord.from_json(record_json_dict)
        self.assertEqual(deserialized.job_id, record.job_id)

    def test_from_squeue_json_handles_dict_time(self):
        squeue_json = create_example_squeue_json_output(is_time_dict=True)

        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")

        self.assertEqual(record.job_id, str(squeue_json["job_id"]))
        self.assertEqual(record.workflow_id, "workflow-squeue")
        self.assertEqual(record.job_attempt_index, squeue_json["restart_cnt"])
        self.assertEqual(record.state, squeue_json["job_state"])
        self.assertEqual(record.state_reason, squeue_json["state_reason"])
        self.assertEqual(record.cluster, squeue_json["cluster"])
        self.assertEqual(record.partition, squeue_json["partition"])
        self.assertEqual(record.reservation, squeue_json["resv_name"])
        self.assertEqual(record.user, squeue_json["user_name"])
        self.assertEqual(record.command, squeue_json["command"])
        self.assertEqual(record.dependency, squeue_json["dependency"])
        self.assertEqual(record.node_count, squeue_json["node_count"]["number"])
        self.assertEqual(record.total_cpu_count, 800)
        self.assertEqual(record.total_gpu_count, 8)
        self.assertEqual(record.total_memory_mb, 50000)
        self.assertEqual(record.exclusion_node_list, ["node6", "node7", "node8", "node10"])
        self.assertEqual(record.inclusion_node_list, ["node1", "node2", "node3"])

        self.assertEqual(
            record.aps_spec,
            ApsSlurmJobSpec(workflow_name="test_workflow"),
        )

    def test_from_squeue_json(self):
        squeue_json = create_example_squeue_json_output()

        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")

        self.assertEqual(record.job_id, str(squeue_json["job_id"]))
        self.assertEqual(record.job_attempt_index, squeue_json["restart_cnt"])
        self.assertEqual(record.state, squeue_json["job_state"])
        self.assertEqual(record.state_reason, squeue_json["state_reason"])
        self.assertEqual(record.cluster, squeue_json["cluster"])
        self.assertEqual(record.partition, squeue_json["partition"])
        self.assertEqual(record.reservation, squeue_json["resv_name"])
        self.assertEqual(record.user, squeue_json["user_name"])
        self.assertEqual(record.command, squeue_json["command"])
        self.assertEqual(record.dependency, squeue_json["dependency"])
        self.assertEqual(record.node_count, squeue_json["node_count"]["number"])
        self.assertEqual(record.total_cpu_count, 800)
        self.assertEqual(record.total_gpu_count, 8)
        self.assertEqual(record.total_memory_mb, 50000)
        self.assertEqual(record.exclusion_node_list, ["node6", "node7", "node8", "node10"])
        self.assertEqual(record.inclusion_node_list, ["node1", "node2", "node3"])

        self.assertEqual(
            record.aps_spec,
            ApsSlurmJobSpec(workflow_name="test_workflow"),
        )

    def test_from_squeue_json_with_state_list(self):
        squeue_json = create_example_squeue_json_output(is_state_list=True)

        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")

        self.assertEqual(record.state, "PENDING")

    def test_from_squeue_json_resolves_final_state_while_completing(self):
        for job_state in (["TIMEOUT", "COMPLETING"], ["COMPLETING", "TIMEOUT"]):
            with self.subTest(job_state=job_state):
                squeue_json = create_example_squeue_json_output()
                squeue_json["job_state"] = job_state

                record = SlurmJobExecutionRecord.from_squeue_json(
                    squeue_json, datetime.now(UTC), workflow_id="workflow-squeue"
                )

                self.assertEqual(record.state, "TIMEOUT")

    def test_from_squeue_json_preserves_bare_completing_state(self):
        squeue_json = create_example_squeue_json_output()
        squeue_json["job_state"] = "COMPLETING"
        squeue_json["state_reason"] = "TimeLimit"

        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")

        self.assertEqual(record.state, "COMPLETING")
        self.assertEqual(record.state_reason, "TimeLimit")

    def test_from_squeue_json_with_allocation_str(self):
        squeue_json = create_example_squeue_json_output(
            resource_request_str="cpu=8,mem=50000M,gres/gpu=8", resource_allocation_str="cpu=80,mem=80000M,gres/gpu=8"
        )

        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")

        # Only verify the resource fields as other fields are covered in the previous test.
        self.assertEqual(record.total_cpu_count, 80)
        self.assertEqual(record.total_memory_mb, 80000)
        self.assertEqual(record.total_gpu_count, 8)

    def test_from_squeue_json_with_empty_user_name(self):
        """Test that if the user name is empty, than the mail user is used instead"""
        squeue_json = create_example_squeue_json_output()
        squeue_json["user_name"] = ""
        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")
        self.assertEqual(record.user, squeue_json["mail_user"])

    def test_from_squeue_json_with_only_user_id(self):
        """Test that if both user name and mail user are empty, then the user is seet to with the user id"""
        squeue_json = create_example_squeue_json_output()
        squeue_json["user_name"] = ""
        squeue_json["mail_user"] = ""
        record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")
        self.assertEqual(record.user, "user_" + squeue_json["user_id"])

    def test_from_sacct_json(self):
        sacct_json = create_example_sacct_json_output(job_state="FAILED")

        record = SlurmJobExecutionRecord.from_sacct_json(sacct_json, datetime.now(UTC), 0, workflow_id="workflow-sacct")

        self.assertEqual(record.job_id, str(sacct_json["job_id"]))
        self.assertEqual(record.workflow_id, "workflow-sacct")
        self.assertEqual(record.job_attempt_index, 0)
        self.assertEqual(record.job_name, sacct_json["name"])
        self.assertEqual(record.state, sacct_json["state"]["current"])
        self.assertEqual(record.cluster, sacct_json["cluster"])
        self.assertEqual(record.partition, sacct_json["partition"])
        self.assertEqual(record.reservation, sacct_json["reservation"]["name"])
        self.assertEqual(record.user, sacct_json["user"])
        self.assertEqual(record.total_cpu_count, 128)
        self.assertEqual(record.total_memory_mb, 2063000)
        self.assertEqual(record.total_gpu_count, 8)
        self.assertEqual(record.allocated_node_list, ["node1", "node2", "node3", "node4", "node5"])
        self.assertEqual(record.failed_node_list, ["node3"])

    def test_from_sacct_json_with_requeue(self):
        sacct_json = create_example_sacct_json_output(comment='{"APS": {"auto_resume_mode": "requeue"}}')
        record = SlurmJobExecutionRecord.from_sacct_json(sacct_json, datetime.now(UTC), 0, workflow_id="workflow-sacct")

        self.assertEqual(record.aps_spec.auto_resume_mode, "requeue")
        self.assertIsNone(record.state_reason)
        self.assertIsNone(record.failed_node_list)

    def test_from_sacct_json_with_state_list(self):
        sacct_json = create_example_sacct_json_output(is_state_list=True)

        record = SlurmJobExecutionRecord.from_sacct_json(sacct_json, datetime.now(UTC), 0, workflow_id="workflow-sacct")

        self.assertEqual(record.state, "COMPLETED")

    def test_parse_job_state(self):
        # state is a list with no final state included, no matter if choose_final_state is True or False, the first
        # element of the list should be returned.
        state = ["PENDING", "RUNNING"]
        self.assertEqual(parse_job_state(state=state), "PENDING")
        self.assertEqual(parse_job_state(state=state, choose_final_state=True), "PENDING")

        # state is a list with a final state included, when choose_final_state is True, the final state should be
        # returned.
        state = ["PENDING", "REQUEUED"]
        self.assertEqual(parse_job_state(state=state, choose_final_state=True), "REQUEUED")

        # state is a plain string, itself should be returned.
        state = "PENDING"
        self.assertEqual(parse_job_state(state=state), "PENDING")

    def test_from_sacct_json_empty_user_name(self):
        """Test that if the json dict has an empty user, then the record user is None and not an empty string"""
        sacct_json = create_example_sacct_json_output()
        sacct_json["user"] = ""

        record = SlurmJobExecutionRecord.from_sacct_json(sacct_json, datetime.now(UTC), 0, workflow_id="workflow-sacct")
        assert record.user is None  # this ensures the empty string does not override value in db

    def test_from_sacct_json_with_real_output(self):
        data = {}
        current_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(current_dir, "sacct_output.json"), "r", encoding="utf-8") as f:
            data = json.load(f)

        record = SlurmJobExecutionRecord.from_sacct_json(data, datetime.now(UTC), 0, workflow_id="workflow-sacct")

        self.assertIsNotNone(record.aps_spec)
        self.assertEqual(record.aps_spec.auto_resume_mode, "requeue")


def test_to_dict_with_all_fields():
    """
    Test to_dict method returns a dictionary with all fields populated.
    """
    now = datetime.now(UTC)
    submit_time = now - timedelta(minutes=10)
    checkpoint_time = submit_time + timedelta(minutes=5)
    anomaly_time = submit_time + timedelta(minutes=3)

    record = create_example_slurm_job_record(submit_time, now, anomaly_time, checkpoint_time)
    result_dict = record.to_dict()

    assert isinstance(result_dict, dict)
    assert result_dict["job_id"] == "12345"
    assert result_dict["job_attempt_index"] == 0
    assert result_dict["job_name"] == record.job_name
    assert result_dict["state"] == "Pending"
    assert result_dict["state_reason"] == "Dependency"
    assert result_dict["submit_time"] == submit_time
    assert result_dict["cluster"] == "test_cluster"
    assert result_dict["partition"] == "recovery"
    assert result_dict["reservation"] == "recovery_prod"
    assert result_dict["namespace"] == "test-k8s-namespace"
    assert result_dict["user"] == "test_user"
    assert result_dict["command"] == "sbatch test.sh"
    assert result_dict["dependency"] == "10000"
    assert result_dict["total_task_count"] == 400
    assert result_dict["node_count"] == 5
    assert result_dict["allocated_node_list"] == [f"node_{i}" for i in range(5)]
    assert result_dict["inclusion_node_list"] == [f"node_{i}" for i in range(5)]
    assert result_dict["exclusion_node_list"] == record.exclusion_node_list
    assert result_dict["total_cpu_count"] == 800
    assert result_dict["total_memory_mb"] == 50 * 100 * 1000
    assert result_dict["total_gpu_count"] == 400
    assert len(result_dict["anomalies"]) == 1
    assert len(result_dict["checkpoints"]) == 1
    assert result_dict["last_updated_at"] == now
    assert result_dict["retry_of"] == "12000"
    assert result_dict["aps_spec"] == record.aps_spec

    # The default factory for events is an empty list.
    assert result_dict["events"] == []


@patch("recovery_libs.storage.dataclass.inspect")
def test_to_dict_with_unloaded_events(mock_inspect):
    """
    Test that to_dict properly handles the events relationship field properly when it's unloaded.
    """
    mock_inspect.return_value.unloaded = {"events"}

    now = datetime.now(UTC)
    submit_time = now - timedelta(minutes=10)
    checkpoint_time = submit_time + timedelta(minutes=5)
    anomaly_time = submit_time + timedelta(minutes=3)

    record = create_example_slurm_job_record(submit_time, now, anomaly_time, checkpoint_time)
    result_dict = record.to_dict()

    # When events field is unloaded, it's set to an empty list.
    assert "events" in result_dict
    assert result_dict["events"] == []
    assert mock_inspect.call_count == 1


@pytest.mark.parametrize(
    "is_in_job_group, is_synthetic_array_entry, expected_job_id, expected_job_group_id",
    [
        (True, False, "12345_0", "12345"),
        (False, False, "12345", None),
        (False, True, None, "12345"),
    ],
)
def test_parse_job_and_job_group_id_squeue_json_parser(
    is_in_job_group, is_synthetic_array_entry, expected_job_id, expected_job_group_id
):
    squeue_json = create_example_squeue_json_output(
        is_in_job_group=is_in_job_group, is_synthetic_array_entry=is_synthetic_array_entry
    )
    job_id = parse_job_id_from_squeue_json(squeue_json)
    job_group_id = parse_job_group_id_from_squeue_json(squeue_json)
    assert job_id == expected_job_id
    assert job_group_id == expected_job_group_id


@pytest.mark.parametrize(
    "is_in_job_group, is_synthetic_array_entry, expected_job_id, expected_job_group_id",
    [
        (True, False, "12345_0", "12345"),
        (False, False, "12345", None),
        (False, True, None, "12345"),
    ],
)
def test_parse_job_and_job_group_id_sacct_json_parser(
    is_in_job_group, is_synthetic_array_entry, expected_job_id, expected_job_group_id
):
    sacct_json = create_example_sacct_json_output(
        is_in_job_group=is_in_job_group, is_synthetic_array_entry=is_synthetic_array_entry
    )
    job_id = parse_job_id_from_sacct_json(sacct_json)
    job_group_id = parse_job_group_id_from_sacct_json(sacct_json)
    assert job_id == expected_job_id
    assert job_group_id == expected_job_group_id


def test_from_squeue_json_returns_none_for_synthetic_array_entry():
    """Test that from_squeue_json returns None for synthetic job array entries that are not eligible for allocation."""
    squeue_json = create_example_squeue_json_output(is_synthetic_array_entry=True)
    record = SlurmJobExecutionRecord.from_squeue_json(squeue_json, datetime.now(UTC), workflow_id="workflow-squeue")
    assert record is None


def test_from_sacct_json_returns_none_for_synthetic_array_entry():
    """Test that from_sacct_json returns None for synthetic job array entries that are not eligible for allocation."""
    sacct_json = create_example_sacct_json_output(is_synthetic_array_entry=True)
    record = SlurmJobExecutionRecord.from_sacct_json(sacct_json, datetime.now(UTC), 0, workflow_id="workflow-sacct")
    assert record is None


@pytest.mark.regression
def test_parse_resource_value_sacct_reads_requested_total_without_allocated():
    """Resource lists without an "allocated" section used to raise TypeError instead of reading "requested"."""
    tres = {
        "requested": {
            "max": [{"type": "cpu", "name": "", "count": 2}],
            "total": [
                {"type": "cpu", "name": "", "count": 16},
                {"type": "gres", "name": "gpu", "count": 8},
            ],
        }
    }

    assert parse_resource_value_sacct(typekey="cpu", vals=tres) == 16
    assert parse_resource_value_sacct(typekey="gres", namekey="gpu", vals=tres) == 8


def test_to_dict_covers_all_external_model_fields():
    """
    Test that to_dict() method includes all fields from SlurmJobExecutionStatus (external model).
    This ensures data model consistency between internal and external representations.
    """
    now = datetime.now(UTC)
    submit_time = now - timedelta(minutes=10)
    checkpoint_time = submit_time + timedelta(minutes=5)
    anomaly_time = submit_time + timedelta(minutes=3)

    # Create a record with all fields populated
    record = SlurmJobExecutionRecord(
        job_id="12345",
        heimdall_job_id=uuid.uuid4(),
        job_attempt_index=0,
        workflow_id="workflow-complete",
        job_group_id="test_group",
        job_name="Test Job",
        state="RUNNING",
        state_reason="None",
        submit_time=submit_time,
        start_time=submit_time + timedelta(minutes=1),
        end_time=None,
        cluster="test_cluster",
        partition="test_partition",
        reservation="test_reservation",
        namespace="test_namespace",
        worker_cluster_name="test_worker_cluster",
        user="test_user",
        command="sbatch test.sh",
        dependency="after:1234",
        node_count=4,
        total_gpu_count=8,
        total_cpu_count=64,
        total_memory_mb=128000,
        allocated_node_list=["node1", "node2", "node3", "node4"],
        inclusion_node_list=["node1", "node2"],
        exclusion_node_list=["node5", "node6"],
        sticky_exclusion_node_list=["node7", "node8"],
        failed_node_list=["node9"],
        scheduler_metadata={"key": "value"},
        anomalies=[JobAnomaly(anomaly_type="CRASH", timestamp=anomaly_time, original_watcher_anomaly_event="EVENT")],
        checkpoints=[JobCheckpointInfo(timestamp=checkpoint_time, uuid=str(uuid.uuid4()))],
        aps_spec=ApsSlurmJobSpec(workflow_name="test_workflow"),
        last_updated_at=now,
        log_file_paths=["/path/to/log1.txt", "/path/to/log2.txt"],
    )

    # Convert to dictionary
    result_dict = record.to_dict()

    # Get all field names from SlurmJobExecutionStatus external model
    external_model_fields = set(SlurmJobExecutionStatus.model_fields.keys())

    # Check that all external model fields are present in to_dict() output
    missing_fields = external_model_fields - set(result_dict.keys())

    assert len(missing_fields) == 0, (
        f"The following fields are in SlurmJobExecutionStatus (external model) "
        f"but missing from to_dict() output: {missing_fields}"
    )

    # Verify specific field values to ensure they're correctly mapped
    assert result_dict["job_id"] == "12345"
    assert result_dict["heimdall_job_id"] == record.heimdall_job_id
    assert result_dict["job_attempt_index"] == 0
    assert result_dict["job_name"] == "Test Job"
    assert result_dict["state"] == "RUNNING"
    assert result_dict["sticky_exclusion_node_list"] == ["node7", "node8"]
    assert result_dict["exclusion_node_list"] == ["node5", "node6"]
    assert result_dict["inclusion_node_list"] == ["node1", "node2"]
    assert result_dict["scheduler_metadata"] == {"key": "value"}


def test_to_json_from_json_preserves_all_fields():
    """
    Test that to_json() and from_json() preserve all fields during serialization/deserialization.
    This ensures no data is lost during the round-trip conversion.
    """
    # Create timestamps
    now = datetime.now(UTC)
    submit_time = now - timedelta(hours=2)
    start_time = now - timedelta(hours=1)
    end_time = now - timedelta(minutes=30)
    last_state_transition = now - timedelta(minutes=45)
    anomaly_time = now - timedelta(minutes=20)
    checkpoint_time = now - timedelta(minutes=10)

    # Create a record with ALL possible fields populated (except non_init_fields and relationship fields)
    original_record = SlurmJobExecutionRecord(
        job_id="test-job-12345",
        job_attempt_index=3,
        workflow_id="workflow-complete",
        job_group_id="group-456",
        job_name="Comprehensive Test Job",
        state="FAILED",
        state_reason="NodeFailure",
        submit_time=submit_time,
        start_time=start_time,
        end_time=end_time,
        cluster="test-cluster-001",
        partition="gpu-partition",
        reservation="special-reservation",
        namespace="test-k8s-namespace",
        worker_cluster_name="worker-cluster-01",
        user="test_user_123",
        comment="Test comment for validation",
        command="sbatch --nodes=8 --gres=gpu:8 train.sh",
        dependency="afterok:98765",
        total_task_count=800,
        node_count=8,
        allocated_node_list=[
            "node-001",
            "node-002",
            "node-003",
            "node-004",
            "node-005",
            "node-006",
            "node-007",
            "node-008",
        ],
        inclusion_node_list=["node-001", "node-002"],
        exclusion_node_list=["node-099", "node-100"],
        sticky_exclusion_node_list=["node-098", "node-097"],
        failed_node_list=["node-005"],
        total_cpu_count=256,
        total_memory_mb=1024000,
        total_gpu_count=64,
        scheduler_metadata={"crd_kind": "PyTorchJob", "labels": {"team": "ml", "priority": "high"}},
        anomalies=[
            JobAnomaly(
                anomaly_type="HANG",
                timestamp=anomaly_time,
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="HANG_EVENT_DATA",
            )
        ],
        checkpoints=[
            JobCheckpointInfo(
                timestamp=checkpoint_time,
                uuid=str(uuid.uuid4()),
            )
        ],
        aps_spec=ApsSlurmJobSpec(
            workflow_name="training_workflow",
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
            max_requeue_times=5,
        ),
        config_validation_failure_types=[
            JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND,
        ],
        last_state_transition_time=last_state_transition,
        last_updated_at=now,
        retry_of="previous-job-12344",
        log_file_paths=["/mnt/logs/job-12345-attempt-3.log", "/mnt/logs/job-12345-step-1.log"],
    )

    # Serialize to JSON
    json_str = original_record.to_json()
    assert isinstance(json_str, str), "to_json() should return a string"
    assert len(json_str) > 0, "to_json() should return non-empty string"

    # Deserialize from JSON
    json_dict = json.loads(json_str)
    restored_record = SlurmJobExecutionRecord.from_json(json_dict)

    # Verify all fields are preserved (excluding id, heimdall_job_id, and events which are not serialized)
    # Job metadata
    assert restored_record.job_id == original_record.job_id
    assert restored_record.job_attempt_index == original_record.job_attempt_index
    assert restored_record.job_group_id == original_record.job_group_id
    assert restored_record.job_name == original_record.job_name
    assert restored_record.state == original_record.state
    assert restored_record.state_reason == original_record.state_reason

    # Timestamps
    assert restored_record.submit_time == original_record.submit_time
    assert restored_record.start_time == original_record.start_time
    assert restored_record.end_time == original_record.end_time
    assert restored_record.last_state_transition_time == original_record.last_state_transition_time
    assert restored_record.last_updated_at == original_record.last_updated_at

    # Cluster information
    assert restored_record.cluster == original_record.cluster
    assert restored_record.partition == original_record.partition
    assert restored_record.reservation == original_record.reservation
    assert restored_record.namespace == original_record.namespace
    assert restored_record.worker_cluster_name == original_record.worker_cluster_name

    # User and job configuration
    assert restored_record.user == original_record.user
    assert restored_record.comment == original_record.comment
    assert restored_record.command == original_record.command
    assert restored_record.dependency == original_record.dependency
    assert restored_record.total_task_count == original_record.total_task_count

    # Resource allocation
    assert restored_record.node_count == original_record.node_count
    assert restored_record.allocated_node_list == original_record.allocated_node_list
    assert restored_record.inclusion_node_list == original_record.inclusion_node_list
    assert restored_record.exclusion_node_list == original_record.exclusion_node_list
    assert restored_record.sticky_exclusion_node_list == original_record.sticky_exclusion_node_list
    assert restored_record.failed_node_list == original_record.failed_node_list
    assert restored_record.total_cpu_count == original_record.total_cpu_count
    assert restored_record.total_memory_mb == original_record.total_memory_mb
    assert restored_record.total_gpu_count == original_record.total_gpu_count

    # Scheduler metadata
    assert restored_record.scheduler_metadata == original_record.scheduler_metadata

    # Anomalies
    assert restored_record.anomalies is not None
    assert len(restored_record.anomalies) == len(original_record.anomalies)
    assert restored_record.anomalies[0].anomaly_type == original_record.anomalies[0].anomaly_type
    assert restored_record.anomalies[0].timestamp == original_record.anomalies[0].timestamp
    assert restored_record.anomalies[0].uuid == original_record.anomalies[0].uuid

    # Checkpoints
    assert restored_record.checkpoints is not None
    assert len(restored_record.checkpoints) == len(original_record.checkpoints)
    assert restored_record.checkpoints[0].timestamp == original_record.checkpoints[0].timestamp
    assert restored_record.checkpoints[0].uuid == original_record.checkpoints[0].uuid

    # APS spec
    assert restored_record.aps_spec is not None
    assert restored_record.aps_spec.workflow_name == original_record.aps_spec.workflow_name
    assert restored_record.aps_spec.auto_resume_mode == original_record.aps_spec.auto_resume_mode
    assert restored_record.aps_spec.max_requeue_times == original_record.aps_spec.max_requeue_times

    # Config validation failures
    assert restored_record.config_validation_failure_types is not None
    assert len(restored_record.config_validation_failure_types) == len(original_record.config_validation_failure_types)
    assert set(restored_record.config_validation_failure_types) == set(original_record.config_validation_failure_types)

    # Internal tracking fields
    assert restored_record.retry_of == original_record.retry_of
    assert restored_record.log_file_paths == original_record.log_file_paths


def test_to_json_from_json_handles_null_fields():
    """
    Test that to_json() and from_json() correctly handle records with minimal/null fields.
    """
    # Create a minimal record with only required fields
    minimal_record = SlurmJobExecutionRecord(
        job_id="minimal-job",
        job_attempt_index=0,
        workflow_id="workflow-minimal",
        last_updated_at=datetime.now(UTC),
    )

    # Serialize and deserialize
    json_str = minimal_record.to_json()
    json_dict = json.loads(json_str)
    restored_record = SlurmJobExecutionRecord.from_json(json_dict)

    # Verify required fields
    assert restored_record.job_id == minimal_record.job_id
    assert restored_record.job_attempt_index == minimal_record.job_attempt_index
    assert restored_record.last_updated_at == minimal_record.last_updated_at

    # Verify optional fields are None
    assert restored_record.job_name is None
    assert restored_record.state is None
    assert restored_record.scheduler_metadata is None
    assert restored_record.sticky_exclusion_node_list is None
    assert restored_record.anomalies is None
    assert restored_record.checkpoints is None


if __name__ == "__main__":
    unittest.main()
