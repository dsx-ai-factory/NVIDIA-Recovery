# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    JobTimeFilter,
    JobTimeFilterTarget,
    SlurmJobExecutionStatus,
    Workflow,
    filter_anomalies,
)


class TestJobManagerSchemas(unittest.TestCase):
    def test_slurm_job_execution_status(self):
        now = datetime.now()
        anomaly_uuid = uuid.uuid4()
        job = SlurmJobExecutionStatus(
            job_id="12345",
            job_attempt_index=0,
            workflow_id="workflow-test-job",
            job_name="test_job",
            state="Running",
            submit_time=now - timedelta(minutes=10),
            cluster="test_cluster",
            partition="default",
            user="test_user",
            command="sbatch test.sh",
            node_count=4,
            allocated_node_list=[f"node_{i}" for i in range(4)],
            anomalies=[
                JobAnomaly(
                    anomaly_type="HANG",
                    timestamp=now - timedelta(minutes=5),
                    uuid=str(anomaly_uuid),
                    processed=False,
                    original_watcher_anomaly_event="EVENT",
                )
            ],
        )

        self.assertEqual(job.job_id, "12345")
        self.assertEqual(job.job_attempt_index, 0)
        self.assertEqual(job.workflow_id, "workflow-test-job")
        self.assertEqual(job.job_name, "test_job")
        self.assertEqual(job.state, "Running")
        self.assertEqual(job.submit_time, now - timedelta(minutes=10))
        self.assertEqual(job.cluster, "test_cluster")
        self.assertEqual(job.partition, "default")
        self.assertEqual(job.user, "test_user")
        self.assertEqual(job.command, "sbatch test.sh")
        self.assertEqual(job.node_count, 4)
        self.assertEqual(job.allocated_node_list, [f"node_{i}" for i in range(4)])
        self.assertEqual(
            job.anomalies[0],
            JobAnomaly(
                anomaly_type="HANG",
                timestamp=now - timedelta(minutes=5),
                uuid=str(anomaly_uuid),
                processed=False,
                original_watcher_anomaly_event="EVENT",
            ),
        )

    def test_workflow_id_is_required(self):
        with pytest.raises(ValidationError):
            SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0)

    def test_workflow_id_rejects_invalid_values(self):
        for workflow_id in ("", "🚀" * 257, "\ud800"):
            with self.subTest(workflow_id=workflow_id), pytest.raises(ValidationError):
                SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0, workflow_id=workflow_id)

    def test_workflow_id_rejects_bytes(self):
        with pytest.raises(ValidationError):
            SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0, workflow_id=b"workflow-id")

    def test_workflow_id_rejects_bytearray(self):
        with pytest.raises(ValidationError):
            SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0, workflow_id=bytearray(b"workflow-id"))

    def test_workflow_id_accepts_256_multibyte_scalars(self):
        workflow_id = "🚀" * 256

        job = SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0, workflow_id=workflow_id)

        self.assertEqual(job.workflow_id, workflow_id)

    def test_workflow_id_is_preserved_verbatim(self):
        for workflow_id in (
            "  Workflow ID  ",
            "Workflow",
            "workflow",
            "é",
            "é",
        ):
            with self.subTest(workflow_id=workflow_id):
                job = SlurmJobExecutionStatus(job_id="12345", job_attempt_index=0, workflow_id=workflow_id)

                self.assertEqual(job.workflow_id, workflow_id)

    def test_workflow_summary_uses_canonical_workflow_id_contract(self):
        self.assertEqual(Workflow(id=" 🚀 workflow ").id, " 🚀 workflow ")

        for workflow_id in ("", "🚀" * 257, "\ud800"):
            with self.subTest(workflow_id=workflow_id), pytest.raises(ValidationError):
                Workflow(id=workflow_id)

    def test_time_filter_validation(self):
        """
        Test validation for JobTimeFilter
        """
        end_time = datetime.now(tz=UTC)
        start_time = end_time - timedelta(hours=1)

        with pytest.raises(ValidationError):
            # Neither start nor end date
            JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME)

        with pytest.raises(ValidationError):
            # Start date after end date
            JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME, from_time=end_time, to_time=start_time)

        with pytest.raises(ValidationError):
            # Start and end date are the same
            JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME, from_time=end_time, to_time=start_time)

        with pytest.raises(ValidationError):
            # Missing timezone
            JobTimeFilter(job_time_target=JobTimeFilterTarget.START_TIME, from_time=datetime.now())

        with pytest.raises(ValidationError):
            # Wrong timezone
            JobTimeFilter(
                job_time_target=JobTimeFilterTarget.START_TIME,
                from_time=datetime.now(tz=timezone(timedelta(hours=-5), "US/Pacific")),
            )

    def test_filter_anomalies_empty_list(self):
        anomalies = []
        self.assertEqual(anomalies, filter_anomalies(anomalies))

    def test_filter_anomalies_non_empty(self):
        anomalies = [
            JobAnomaly(
                anomaly_type=EventType.CRASH.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.CRASHAPPEXCEPTION.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.CRASHNAN.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.CRASHOOM.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.CRASHSEGV.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.END.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANG.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANGAPP.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANGCOMM.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANGNOOUTPUT.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANGPUSHRECV.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.STRAGGLER.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.LOWTHROUGHPUT.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.LOWTHROUGHPUTEWNET.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.LOWTHROUGHPUTGPU.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.LOWTHROUGHPUTNSNET.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.LOWTHROUGHPUTSTORAGE.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.UNKNOWN.value,
                timestamp=datetime.now(tz=UTC),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
        ]

        filtered_list = filter_anomalies(anomalies=anomalies)

        # The first 11 elements in the list are actionable ones (STRAGGLER is excluded).
        self.assertEqual(len(filtered_list), 11)

        # Check that the filtered list contains the first 11 anomalies (excluding STRAGGLER)
        for i in range(len(filtered_list)):
            self.assertEqual(anomalies[i], filtered_list[i])

    def test_filter_anomalies_with_timestamp(self):
        now = datetime.now(tz=UTC)
        anomalies = [
            JobAnomaly(
                anomaly_type=EventType.CRASH.value,
                # Anomaly happened 2 hours ago
                timestamp=now - timedelta(minutes=120),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
            JobAnomaly(
                anomaly_type=EventType.HANGNOOUTPUT.value,
                # Anomaly happened 10 minutes ago
                timestamp=now - timedelta(minutes=10),
                uuid=str(uuid.uuid4()),
                original_watcher_anomaly_event="EVENT",
            ),
        ]

        # Filter out anomalies that are older than 1 hour
        filtered_list = filter_anomalies(anomalies=anomalies, later_than=now - timedelta(minutes=60))

        assert len(filtered_list) == 1
        assert filtered_list[0].anomaly_type == EventType.HANGNOOUTPUT.value
        assert filtered_list[0].timestamp == anomalies[1].timestamp
