# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import uuid6
import uvicorn
from fastapi.testclient import TestClient
from job_manager.audit.events import (
    AdviceUnpublishedEvent,
    JobAttemptFirstSeenEvent,
    JobAttemptUpdatedEvent,
)
from job_manager.constants import WATCHER, WORKFLOW_CONTROLLER
from job_manager.main import app, start_http_server
from job_manager.service import JobManager
from recovery_libs.audit import AuditEventLevel
from recovery_libs.schema.aps_spec.spec import AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    GetRecoverySequenceResponse,
    JobAnomaly,
    JobCheckpointInfo,
    JobExecutionStatuses,
    RecordRecoveryMilestoneResponse,
    RecoveryContext,
    RecoveryOperation,
    SlurmJobExecutionStatus,
    SystemRequeueConfig,
)
from recovery_libs.schema.watcher.model import Event
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import (
    AdviceReconciliationStatusRecord,
    AdviceRecord,
    JobConfigValidationFailureTypes,
)
from recovery_libs.storage.job_execution_errors import (
    JobExecutionError as JobExecutionErrorInternal,
)
from recovery_libs.storage.mysql_storage import SoftDeleteAdviceResult
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
)
from recovery_libs.storage.node_events import NodeEvent as NodeEventInternal
from test_utils import (
    create_basic_service_config,
    create_example_advice_record,
    create_example_slurm_job_record,
    create_example_workflow_record,
    expected_job_audit_snapshot,
)


class TestHttpService(unittest.TestCase):
    def setUp(self):
        self._config = create_basic_service_config()
        self._client = TestClient(app)

    @staticmethod
    def _use_real_job_upsert_boundary(job_manager_mock):
        job_manager_mock.upsert_job_record.side_effect = lambda *args, **kwargs: JobManager.upsert_job_record(
            job_manager_mock, *args, **kwargs
        )

    def test_start_http_server(self):
        with patch.object(uvicorn, attribute="run") as http_server_mock:
            start_http_server(self._config["service"])
            http_server_mock.assert_called_once()
            self.assertEqual(http_server_mock.call_args.args[0], app)
            self.assertEqual(
                http_server_mock.call_args.kwargs["host"],
                self._config["service"]["http"]["host"],
            )
            self.assertEqual(
                http_server_mock.call_args.kwargs["port"],
                self._config["service"]["http"]["port"],
            )

    def test_get_status_endpoint_not_ready(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.ready.return_value = False
            response = self._client.get("/status")
            self.assertEqual(response.status_code, 503)

    def test_get_status_endpoint_ready(self):
        with patch("job_manager.main.job_manager", autospec=True) as job_manager_mock:
            job_manager_mock.storage.ready.return_value = True
            job_manager_mock.event_bus_consumer_threads = {
                WATCHER: MagicMock(),
                WORKFLOW_CONTROLLER: MagicMock(),
            }
            for thread in job_manager_mock.event_bus_consumer_threads.values():
                thread.is_alive.return_value = True
            response = self._client.get("/status")
            self.assertEqual(response.status_code, 200)

    def test_get_recovery_sequence_endpoint(self):
        triggered_at = datetime.now(UTC)
        response_model = GetRecoverySequenceResponse(
            recoveryContext=RecoveryContext(
                sequenceId=str(uuid.uuid4()),
                traceparent="00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                triggeredAt=triggered_at,
                operation=RecoveryOperation.RESTART,
            )
        )
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.recovery_service.get.return_value = response_model
            response = self._client.get(
                "/recovery/sequences",
                params={
                    "scheduler_system": "slurm",
                    "scheduler_scope": "cluster-a",
                    "job_id": "42",
                    "source_attempt_index": 3,
                    "operation": "restart",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["recoveryContext"]["sequenceId"], response_model.recovery_context.sequence_id)

    def test_missing_recovery_sequence_returns_not_found(self):
        with patch("job_manager.main.job_manager") as manager:
            manager.recovery_service.get.return_value = None
            response = self._client.get(
                "/recovery/sequences",
                params={
                    "scheduler_system": "slurm",
                    "scheduler_scope": "cluster-a",
                    "job_id": "42",
                    "source_attempt_index": 3,
                    "operation": "restart",
                },
            )
        self.assertEqual(response.status_code, 404)
        manager.recovery_service.begin_or_get.assert_not_called()

    def test_record_recovery_milestone_endpoint(self):
        sequence_id = str(uuid.uuid4())
        completed_at = datetime.now(UTC)
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.recovery_service.record_milestone.return_value = RecordRecoveryMilestoneResponse(
                duplicate=False,
                sequenceCompleted=True,
                endToEndDurationSeconds=4.5,
            )
            response = self._client.post(
                "/recovery/milestones",
                json={
                    "sequenceId": sequence_id,
                    "idempotencyKey": "release-1",
                    "operation": "restart",
                    "action": "slurm.release",
                    "outcome": "success",
                    "completesSequence": True,
                    "completedAt": completed_at.isoformat(),
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["endToEndDurationSeconds"], 4.5)

    def test_record_recovery_milestone_requires_completion_intent(self):
        response = self._client.post(
            "/recovery/milestones",
            json={
                "sequenceId": str(uuid.uuid4()),
                "idempotencyKey": "release-1",
                "operation": "restart",
                "action": "slurm.release",
                "outcome": "success",
                "completedAt": datetime.now(UTC).isoformat(),
            },
        )

        self.assertEqual(response.status_code, 422)

    def test_post_jobs_applies_defaults_without_backfilling_start_time(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-defaults",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
        }

        persisted_record = create_example_slurm_job_record(
            job_id="job-defaults",
            attempt_index=0,
            state="RUNNING",
            submit_time=now - timedelta(minutes=1),
            last_updated_at=now,
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        )
        persisted_record.heimdall_job_id = uuid.uuid4()
        persisted_record.user = "unknown"
        persisted_record.cluster = "unknown"
        persisted_record.start_time = now

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            upserted_record = job_manager_mock.storage.upsert_job_record.call_args.args[0]
            self.assertEqual(upserted_record.user, "unknown")
            self.assertEqual(upserted_record.cluster, "unknown")
            self.assertIsNotNone(upserted_record.aps_spec)
            self.assertEqual(upserted_record.aps_spec.auto_resume_mode, AutoResumeModeEnum.REQUEUE)
            self.assertIsNone(upserted_record.start_time)
            job_manager_mock.register_dag_dependencies_for_new_job.assert_called_once_with(persisted_record)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertIsInstance(audit_event, JobAttemptFirstSeenEvent)
            self.assertEqual(audit_event.event_producer, "job-manager")
            self.assertEqual(audit_event.heimdall_job_id, str(persisted_record.heimdall_job_id))
            self.assertIs(audit_event.event_level, AuditEventLevel.OPERATIONAL)
            self.assertEqual(audit_event.payload.job_id, "job-defaults")
            self.assertEqual(audit_event.payload.attempt_index, 0)
            self.assertEqual(
                audit_event.payload.job,
                expected_job_audit_snapshot(persisted_record),
            )
            self.assertNotIn("id", audit_event.payload.job)
            self.assertNotIn("events", audit_event.payload.job)
            self.assertNotIn("filename", audit_event.payload.job)
            self.assertNotIn("user", audit_event.payload.job)
            self.assertNotIn("command", audit_event.payload.job)
            self.assertIsInstance(audit_event.event_produced_at, int)
            uuid.UUID(audit_event.idempotency_key)

    def test_post_jobs_emits_audit_event_with_changed_job_fields(self):
        now = datetime.now(UTC)
        heimdall_job_id = uuid.uuid4()
        payload = {
            "job_id": "job-state-change",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
        }
        old_record = create_example_slurm_job_record(
            job_id="job-state-change",
            attempt_index=0,
            state="PENDING",
            last_updated_at=now - timedelta(seconds=30),
        )
        persisted_record = create_example_slurm_job_record(
            job_id="job-state-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now,
        )
        old_record.heimdall_job_id = heimdall_job_id
        persisted_record.heimdall_job_id = heimdall_job_id

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertIsInstance(audit_event, JobAttemptUpdatedEvent)
            self.assertEqual(audit_event.event_producer, "job-manager")
            self.assertEqual(audit_event.heimdall_job_id, str(heimdall_job_id))
            self.assertIs(audit_event.event_level, AuditEventLevel.OPERATIONAL)
            self.assertEqual(audit_event.payload.job_id, "job-state-change")
            self.assertEqual(audit_event.payload.attempt_index, 0)
            self.assertNotIn("record_updated_at", audit_event.payload.model_dump())
            self.assertEqual(audit_event.payload.old_job, expected_job_audit_snapshot(old_record))
            self.assertEqual(audit_event.payload.new_job, expected_job_audit_snapshot(persisted_record))
            self.assertIsInstance(audit_event.event_produced_at, int)
            uuid.UUID(audit_event.idempotency_key)

    def test_post_jobs_suppresses_audit_event_when_only_update_timestamp_changes(self):
        now = datetime.now(UTC)
        old_record = create_example_slurm_job_record(
            job_id="job-timestamp-only-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        persisted_record = deepcopy(old_record)
        persisted_record.last_updated_at = now
        payload = {
            "job_id": old_record.job_id,
            "job_attempt_index": old_record.job_attempt_index,
            "workflow_id": old_record.workflow_id,
            "state": old_record.state,
        }

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.audit_buffer.add.assert_not_called()

    def test_post_jobs_audits_job_resource_fields(self):
        now = datetime.now(UTC)
        heimdall_job_id = uuid.uuid4()
        payload = {
            "job_id": "job-resource-change",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
        }
        old_record = create_example_slurm_job_record(
            job_id="job-resource-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        persisted_record = deepcopy(old_record)
        old_record.heimdall_job_id = heimdall_job_id
        persisted_record.heimdall_job_id = heimdall_job_id
        old_record.allocated_node_list = ["node-b", "node-a"]
        persisted_record.allocated_node_list = ["node-c", "node-a"]
        old_record.inclusion_node_list = ["node-b"]
        persisted_record.inclusion_node_list = ["node-a", "node-b"]
        old_record.exclusion_node_list = ["node-x"]
        persisted_record.exclusion_node_list = ["node-y"]
        persisted_record.total_gpu_count = 450
        persisted_record.last_updated_at = now

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertEqual(audit_event.payload.old_job, expected_job_audit_snapshot(old_record))
            self.assertEqual(audit_event.payload.new_job, expected_job_audit_snapshot(persisted_record))
            self.assertEqual(audit_event.payload.old_job["allocated_node_list"], ["node-b", "node-a"])
            self.assertEqual(audit_event.payload.new_job["allocated_node_list"], ["node-c", "node-a"])
            self.assertEqual(audit_event.payload.new_job["total_gpu_count"], 450)

    def test_post_jobs_audits_aps_spec(self):
        now = datetime.now(UTC)
        heimdall_job_id = uuid.uuid4()
        payload = {
            "job_id": "job-recovery-config-change",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
        }
        old_record = create_example_slurm_job_record(
            job_id="job-recovery-config-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        )
        persisted_record = deepcopy(old_record)
        old_record.heimdall_job_id = heimdall_job_id
        persisted_record.heimdall_job_id = heimdall_job_id
        persisted_record.aps_spec.auto_resume_mode = AutoResumeModeEnum.SINGLETON_DEPENDENCY
        persisted_record.last_updated_at = now

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertEqual(audit_event.payload.old_job, expected_job_audit_snapshot(old_record))
            self.assertEqual(audit_event.payload.new_job, expected_job_audit_snapshot(persisted_record))
            self.assertEqual(audit_event.payload.old_job["aps_spec"]["auto_resume_mode"], AutoResumeModeEnum.REQUEUE)
            self.assertEqual(
                audit_event.payload.new_job["aps_spec"]["auto_resume_mode"],
                AutoResumeModeEnum.SINGLETON_DEPENDENCY,
            )
            self.assertEqual(
                audit_event.payload.new_job["aps_spec"],
                persisted_record.aps_spec.model_dump(mode="json"),
            )

    def test_post_jobs_suppresses_audit_when_only_scheduler_metadata_changes(self):
        now = datetime.now(UTC)
        heimdall_job_id = uuid.uuid4()
        payload = {
            "job_id": "job-raw-state-change",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
            "scheduler_metadata": {
                "scheduler": "slurm",
                "slurm_state": "COMPLETING",
                "slurm_reason": "NonZeroExitCode",
            },
        }
        old_record = create_example_slurm_job_record(
            job_id="job-raw-state-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        persisted_record = deepcopy(old_record)
        old_record.heimdall_job_id = heimdall_job_id
        persisted_record.heimdall_job_id = heimdall_job_id
        old_record.scheduler_metadata = {
            "scheduler": "slurm",
            "slurm_state": "RUNNING",
            "slurm_reason": "None",
            "slurm_log_file_paths": ["/sensitive/old.log"],
        }
        persisted_record.scheduler_metadata = {
            "scheduler": "slurm",
            "slurm_state": "COMPLETING",
            "slurm_reason": "NonZeroExitCode",
            "slurm_log_file_paths": ["/sensitive/new.log"],
        }
        persisted_record.last_updated_at = now

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.audit_buffer.add.assert_not_called()

    def test_post_jobs_audits_only_approved_job_snapshot_fields(self):
        now = datetime.now(UTC)
        heimdall_job_id = uuid.uuid4()
        payload = {
            "job_id": "job-audit-noise",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
        }
        old_record = create_example_slurm_job_record(
            job_id="job-audit-noise",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
            log_file_paths=["/sensitive/old.log"],
        )
        persisted_record = deepcopy(old_record)
        old_record.heimdall_job_id = heimdall_job_id
        persisted_record.heimdall_job_id = heimdall_job_id
        persisted_record.last_updated_at = now
        old_record.allocated_node_list = ["node-b", "node-a"]
        persisted_record.allocated_node_list = ["node-a", "node-b"]
        persisted_record.user = "different-user"
        persisted_record.command = "different command --token secret"
        persisted_record.comment = "sensitive comment"
        persisted_record.state_reason = "different internal reason"
        persisted_record.partition = "different-partition"
        persisted_record.failed_node_list = ["failed-node"]
        persisted_record.log_file_paths = ["/sensitive/new.log"]
        persisted_record.anomalies = [
            JobAnomaly(
                anomaly_type="CRASH",
                timestamp=now,
                original_watcher_anomaly_event='{"body":"sensitive log line"}',
            )
        ]
        persisted_record.checkpoints = [JobCheckpointInfo(timestamp=now, uuid=str(uuid.uuid4()))]
        persisted_record.scheduler_metadata = {"slurm_log_file_paths": ["/sensitive/scheduler.log"]}
        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, old_record)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertEqual(audit_event.payload.old_job, expected_job_audit_snapshot(old_record))
            self.assertEqual(audit_event.payload.new_job, expected_job_audit_snapshot(persisted_record))
            self.assertEqual(
                set(audit_event.payload.new_job),
                set(expected_job_audit_snapshot(persisted_record)),
            )
            for excluded_field in (
                "anomalies",
                "checkpoints",
                "command",
                "comment",
                "events",
                "filename",
                "last_updated_at",
                "log_file_paths",
                "scheduler_metadata",
                "user",
            ):
                self.assertNotIn(excluded_field, audit_event.payload.new_job)
            self.assertEqual(audit_event.payload.new_job["failed_node_list"], ["failed-node"])
            serialized_job = audit_event.model_dump(mode="json")["payload"]["new_job"]
            self.assertEqual(serialized_job["heimdall_job_id"], str(heimdall_job_id))
            self.assertEqual(serialized_job["aps_spec"]["nvrx"], "disabled")

    def test_post_jobs_does_not_reregister_dag_dependencies_for_existing_job(self):
        """A repeated scheduler poll must not reset a released dependency gate."""
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-existing-dag-child",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "PENDING",
            "last_updated_at": now.isoformat(),
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-existing-dag-child",
            attempt_index=0,
            state="PENDING",
            last_updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, None)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.register_dag_dependencies_for_new_job.assert_not_called()

    def test_post_jobs_preserves_explicit_auto_resume_mode(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-explicit-mode",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
            "aps_spec": {"auto_resume_mode": "singleton_dependency"},
        }

        persisted_record = create_example_slurm_job_record(
            job_id="job-explicit-mode",
            attempt_index=0,
            state="RUNNING",
            submit_time=now - timedelta(minutes=1),
            last_updated_at=now,
            auto_resume_mode=AutoResumeModeEnum.SINGLETON_DEPENDENCY,
        )
        persisted_record.user = "unknown"
        persisted_record.cluster = "unknown"
        persisted_record.start_time = now

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            upserted_record = job_manager_mock.storage.upsert_job_record.call_args.args[0]
            self.assertEqual(upserted_record.aps_spec.auto_resume_mode, AutoResumeModeEnum.SINGLETON_DEPENDENCY)

    def test_post_jobs_generates_crash_during_legacy_slurm_completing(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-state-crash",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
            "scheduler_metadata": {
                "scheduler": "slurm",
                "slurm_state": "COMPLETING",
                "slurm_reason": "NonZeroExitCode",
                "slurm_end_time": now.isoformat(),
            },
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-state-crash",
            attempt_index=0,
            state="RUNNING",
            end_time=now,
            last_updated_at=now,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            job_manager_mock.should_send_crash_anomaly.return_value = True
            generated_event = MagicMock(type=EventType.CRASH)
            generated_event.attributes.uuid = str(uuid.uuid4())
            job_manager_mock._generate_crash_event.return_value = generated_event
            persisted_record.anomalies = [
                JobAnomaly(anomaly_type=EventType.CRASH, timestamp=now, uuid=generated_event.attributes.uuid)
            ]
            job_manager_mock._is_advice_management_enabled.return_value = True

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            generated_record = job_manager_mock._generate_crash_event.call_args.kwargs["record"]
            self.assertEqual(generated_record.job_id, "job-state-crash")
            self.assertFalse(generated_record.aps_spec.anomaly_detection_config.log_based_crash_enabled)
            job_manager_mock.should_send_crash_anomaly.assert_called_once_with(record=generated_record, state="FAILED")
            job_manager_mock._maybe_emit_attribution_request.assert_called_once_with(persisted_record)
            persist_mock.assert_called_once()

    def test_post_jobs_state_crash_advice_requires_committed_anomaly(self):
        """Only a committed CRASH publishes its original cause before attribution dispatch."""
        original_time = datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=UTC)
        known_record = create_example_slurm_job_record(
            job_id="job-state-crash",
            attempt_index=2,
            state="RUNNING",
            submit_time=original_time - timedelta(minutes=10),
        )
        known_record.heimdall_job_id = generate_heimdall_job_id(known_record.job_id, 2)
        known_record.aps_spec.anomaly_detection_config.log_based_crash_enabled = False
        payload = {
            "job_id": known_record.job_id,
            "job_attempt_index": 2,
            "workflow_id": "workflow-test",
            "state": "FAILED",
            "submit_time": known_record.submit_time.isoformat(),
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }

        for storage_fails in (False, True):
            with (
                self.subTest(storage_fails=storage_fails),
                patch("job_manager.main.job_manager") as manager,
                patch("job_manager.service.time_module.time_ns", return_value=1_700_000_000_123_456_000),
            ):
                self._use_real_job_upsert_boundary(manager)
                for method in (
                    "should_send_crash_anomaly",
                    "_generate_anomaly_event",
                    "_generate_crash_event",
                    "reconcile_anomaly_advice",
                ):
                    getattr(manager, method).side_effect = getattr(JobManager, method).__get__(manager, JobManager)
                manager._is_advice_enabled.return_value = True
                manager._is_advice_management_enabled.return_value = False
                manager._is_dag_support_enabled.return_value = False
                manager.storage.read_job_record.return_value = known_record
                manager.storage.read_advice.return_value = None
                committed_steps = []

                def persist_job(record, *, _fails=storage_fails, _steps=committed_steps, **_kwargs):
                    if _fails:
                        raise RecoveryStorageException("storage unavailable")
                    _steps.append("anomaly")
                    persisted_record = deepcopy(record)
                    persisted_record.heimdall_job_id = known_record.heimdall_job_id
                    return persisted_record, False, known_record

                def persist_advice(record, _steps=committed_steps, **_):
                    _steps.append("advice")
                    return record, True, None

                manager.storage.upsert_job_record.side_effect = persist_job
                manager.storage.upsert_advice.side_effect = persist_advice
                manager._maybe_emit_attribution_request.side_effect = lambda _record, _steps=committed_steps: (
                    _steps.append("attribution")
                )

                response = self._client.post("/jobs", json=payload)

                manager.storage.upsert_job_record.assert_called_once()
                if storage_fails:
                    self.assertEqual(response.status_code, 500)
                    self.assertEqual(committed_steps, [])
                    manager.storage.upsert_advice.assert_not_called()
                    manager.audit_buffer.add.assert_not_called()
                    manager._maybe_emit_attribution_request.assert_not_called()
                    continue

                self.assertEqual(response.status_code, 200)
                self.assertEqual(committed_steps, ["anomaly", "advice", "attribution"])
                advice = manager.storage.upsert_advice.call_args.args[0]
                self.assertEqual(advice.heimdall_id, known_record.heimdall_job_id)
                self.assertEqual(advice.attempt_index, 2)
                self.assertEqual(advice.issues[0].timestamp, original_time)
                self.assertEqual(advice.issues[0].detected_by, "Heimdall Job Manager")
                dispatched_record = manager._maybe_emit_attribution_request.call_args.args[0]
                self.assertEqual(dispatched_record.anomalies[0].timestamp, original_time)
                advice_events = [
                    call.args[0]
                    for call in manager.audit_buffer.add.call_args_list
                    if call.args[0].event_type == "advice_created"
                ]
                self.assertEqual(len(advice_events), 1)
                event = advice_events[0]
                self.assertEqual(event.heimdall_job_id, str(known_record.heimdall_job_id))
                self.assertEqual(event.payload.attempt_index, 2)
                self.assertEqual(event.payload.creation_reasons, ["CRASH"])
                self.assertEqual(datetime.fromisoformat(event.payload.advice["issues"][0]["timestamp"]), original_time)

    def test_post_jobs_repairs_failed_advice_write_from_committed_crash(self):
        """A repeated scheduler poll repairs the original cause without generating another anomaly."""
        original_time = datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=UTC)
        known_record = create_example_slurm_job_record(
            job_id="job-advice-retry",
            attempt_index=2,
            state="RUNNING",
            submit_time=original_time - timedelta(minutes=10),
        )
        known_record.heimdall_job_id = generate_heimdall_job_id(known_record.job_id, 2)
        known_record.aps_spec.anomaly_detection_config.log_based_crash_enabled = False
        payload = {
            "job_id": known_record.job_id,
            "job_attempt_index": 2,
            "workflow_id": "workflow-test",
            "state": "FAILED",
            "submit_time": known_record.submit_time.isoformat(),
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }

        with (
            patch("job_manager.main.job_manager") as manager,
            patch("job_manager.service.time_module.time_ns", return_value=1_700_000_000_123_456_000) as clock,
        ):
            self._use_real_job_upsert_boundary(manager)
            for method in (
                "should_send_crash_anomaly",
                "_generate_anomaly_event",
                "_generate_crash_event",
                "reconcile_anomaly_advice",
            ):
                getattr(manager, method).side_effect = getattr(JobManager, method).__get__(manager, JobManager)
            manager._is_advice_enabled.return_value = True
            manager._is_advice_management_enabled.return_value = False
            manager._is_dag_support_enabled.return_value = False
            manager.storage.read_job_record.return_value = known_record
            manager.storage.read_advice.return_value = None

            def persist_job(record, **_kwargs):
                previous_record = manager.storage.read_job_record.return_value
                committed = deepcopy(record)
                committed.heimdall_job_id = known_record.heimdall_job_id
                if committed.anomalies is None:
                    committed.anomalies = deepcopy(previous_record.anomalies)
                manager.storage.read_job_record.return_value = committed
                return committed, False, previous_record

            manager.storage.upsert_job_record.side_effect = persist_job
            manager.storage.upsert_advice.side_effect = RecoveryStorageException("Advice storage unavailable")

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 500)
            manager._maybe_emit_attribution_request.assert_not_called()
            committed_anomaly = manager.storage.read_job_record.return_value.anomalies[0]
            original_json = committed_anomaly.original_watcher_anomaly_event
            original_event = Event.model_validate_json(original_json)
            self.assertEqual(original_event.timestamp, "1700000000123456000")
            self.assertEqual(original_event.attributes.uuid, committed_anomaly.uuid)
            self.assertEqual(original_event.body, "State-based CRASH event for failed job")
            self.assertFalse(
                any(call.args[0].event_type.startswith("advice_") for call in manager.audit_buffer.add.call_args_list)
            )

            clock.return_value = 1_700_000_060_123_456_000
            manager.storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            manager._generate_crash_event.assert_called_once()
            manager._maybe_emit_attribution_request.assert_called_once()
            repaired_record = manager._maybe_emit_attribution_request.call_args.args[0]
            self.assertEqual(len(repaired_record.anomalies), 1)
            self.assertEqual(repaired_record.anomalies[0].uuid, original_event.attributes.uuid)
            self.assertEqual(repaired_record.anomalies[0].original_watcher_anomaly_event, original_json)
            advice = manager.storage.upsert_advice.call_args.args[0]
            self.assertEqual(advice.heimdall_id, known_record.heimdall_job_id)
            self.assertEqual(advice.attempt_index, 2)
            self.assertEqual(len(advice.issues), 1)
            self.assertEqual(advice.issues[0].timestamp, original_time)
            self.assertEqual(advice.issues[0].relevant_log_lines, [original_event.body])
            self.assertEqual(advice.issues[0].detected_by, "Heimdall Job Manager")
            advice_events = [
                call.args[0]
                for call in manager.audit_buffer.add.call_args_list
                if call.args[0].event_type == "advice_created"
            ]
            self.assertEqual(len(advice_events), 1)
            self.assertEqual(advice_events[0].payload.creation_reasons, ["CRASH"])

    def test_post_jobs_does_not_duplicate_state_crash_for_known_failed_attempt(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-state-crash",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "FAILED",
            "last_updated_at": now.isoformat(),
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-state-crash",
            attempt_index=0,
            state="FAILED",
            last_updated_at=now,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = persisted_record
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, None)
            job_manager_mock._is_advice_management_enabled.return_value = True

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.should_send_crash_anomaly.assert_not_called()
            job_manager_mock._generate_crash_event.assert_not_called()
            job_manager_mock._maybe_emit_attribution_request.assert_not_called()
            persist_mock.assert_called_once()

    def test_post_jobs_does_not_duplicate_anomaly_after_completing(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-crash-completing",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "FAILED",
            "last_updated_at": now.isoformat(),
            "scheduler_metadata": {
                "scheduler": "slurm",
                "slurm_state": "FAILED",
                "slurm_reason": "NonZeroExitCode",
            },
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }
        known_record = create_example_slurm_job_record(
            job_id="job-crash-completing",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        known_record.scheduler_metadata = {
            "scheduler": "slurm",
            "slurm_state": "COMPLETING",
            "slurm_reason": "NonZeroExitCode",
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-crash-completing",
            attempt_index=0,
            state="FAILED",
            last_updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = known_record
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, None)

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.should_send_crash_anomaly.assert_not_called()
            job_manager_mock.should_send_end_event.assert_not_called()
            job_manager_mock._generate_crash_event.assert_not_called()
            job_manager_mock._generate_end_event.assert_not_called()
            job_manager_mock._maybe_emit_attribution_request.assert_not_called()

    def test_post_jobs_does_not_generate_recovery_evidence_for_completion(self):
        # A healthy COMPLETED job and a scheduler TIMEOUT are level-based
        # scheduler classifications. The Advisor path must not emit an END or
        # CRASH anomaly, terminal Advice, or attribution for them; otherwise
        # healthy completions and timeouts become false-positive recoveries
        # (mirrors the healthy and scheduler-timeout e2e negative controls).
        now = datetime.now(UTC)
        for slurm_state, status_state in (("COMPLETED", "COMPLETED"), ("TIMEOUT", "FAILED")):
            with self.subTest(slurm_state=slurm_state):
                job_id = f"job-{slurm_state.lower()}"
                payload = {
                    "job_id": job_id,
                    "job_attempt_index": 0,
                    "workflow_id": "workflow-test",
                    "state": status_state,
                    "last_updated_at": now.isoformat(),
                    "scheduler_metadata": {
                        "scheduler": "slurm",
                        "slurm_state": slurm_state,
                        "slurm_reason": "None",
                        "slurm_end_time": now.isoformat(),
                    },
                    "aps_spec": {"auto_resume_mode": "requeue"},
                }
                persisted_record = create_example_slurm_job_record(
                    job_id=job_id,
                    attempt_index=0,
                    state=status_state,
                    end_time=now,
                    last_updated_at=now,
                )

                with (
                    patch("job_manager.main.job_manager") as job_manager_mock,
                    patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
                ):
                    self._use_real_job_upsert_boundary(job_manager_mock)
                    job_manager_mock.storage.read_job_record.return_value = None
                    job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
                    job_manager_mock._is_advice_management_enabled.return_value = True

                    response = self._client.post("/jobs", json=payload)

                    self.assertEqual(response.status_code, 200)
                    job_manager_mock.should_send_crash_anomaly.assert_not_called()
                    job_manager_mock._generate_crash_event.assert_not_called()
                    job_manager_mock._generate_end_event.assert_not_called()
                    job_manager_mock._maybe_emit_attribution_request.assert_not_called()
                    persist_mock.assert_not_called()

    def test_post_jobs_ignores_healthy_completing_epilog(self):
        # A job still in its COMPLETING epilog with a benign reason has no
        # resolved terminal outcome, so the Advisor path must not emit an
        # anomaly, reconcile terminal Advice, or request attribution while the
        # job is still cleaning up.
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-healthy-completing",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "RUNNING",
            "last_updated_at": now.isoformat(),
            "scheduler_metadata": {
                "scheduler": "slurm",
                "slurm_state": "COMPLETING",
                "slurm_reason": "None",
            },
            "aps_spec": {
                "auto_resume_mode": "requeue",
                "anomaly_detection_config": {"log_based_crash_enabled": False},
            },
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-healthy-completing",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            job_manager_mock._is_advice_management_enabled.return_value = True
            job_manager_mock._is_dag_support_enabled.return_value = False

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.should_send_crash_anomaly.assert_not_called()
            job_manager_mock._generate_crash_event.assert_not_called()
            job_manager_mock._generate_end_event.assert_not_called()
            job_manager_mock._maybe_emit_attribution_request.assert_not_called()
            persist_mock.assert_not_called()

    def test_post_jobs_persists_terminal_advice_on_first_cancelled_transition(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-cancelled",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "CANCELLED",
            "last_updated_at": now.isoformat(),
            "aps_spec": {"auto_resume_mode": "requeue", "max_requeue_times": 1},
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-cancelled",
            attempt_index=0,
            state="CANCELLED",
            last_updated_at=now,
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
            max_requeue_times=1,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            job_manager_mock._is_advice_management_enabled.return_value = True
            job_manager_mock._is_dag_support_enabled.return_value = False

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            persist_mock.assert_called_once_with(
                job_manager_mock.storage,
                persisted_record,
                audit_buffer=job_manager_mock.audit_buffer,
                system_requeue_config=job_manager_mock._build_system_requeue_config.return_value,
                get_jobs=job_manager_mock._get_jobs_for_requeue_check,
                on_persisted=job_manager_mock._maybe_send_recovery_notification,
                on_advice_persisted=job_manager_mock.propagate_terminated_advice_for_dag_dependents,
                allow_explicit_dag_abort=False,
                on_advice_ready=job_manager_mock._begin_sequence_for_advice,
            )

    def test_post_jobs_reconciles_terminal_advice_for_known_cancelled_attempt(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-cancelled",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "CANCELLED",
            "last_updated_at": now.isoformat(),
            "aps_spec": {"auto_resume_mode": "requeue", "max_requeue_times": 1},
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-cancelled",
            attempt_index=0,
            state="CANCELLED",
            last_updated_at=now,
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
            max_requeue_times=1,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = persisted_record
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, False, None)
            job_manager_mock._is_advice_management_enabled.return_value = True
            job_manager_mock._is_dag_support_enabled.return_value = False

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            persist_mock.assert_called_once_with(
                job_manager_mock.storage,
                persisted_record,
                audit_buffer=job_manager_mock.audit_buffer,
                system_requeue_config=job_manager_mock._build_system_requeue_config.return_value,
                get_jobs=job_manager_mock._get_jobs_for_requeue_check,
                on_persisted=job_manager_mock._maybe_send_recovery_notification,
                on_advice_persisted=job_manager_mock.propagate_terminated_advice_for_dag_dependents,
                allow_explicit_dag_abort=False,
                on_advice_ready=job_manager_mock._begin_sequence_for_advice,
            )

    def test_post_jobs_reconciles_stopped_parent_as_terminal_advice(self):
        now = datetime.now(UTC)
        payload = {
            "job_id": "job-stopped",
            "job_attempt_index": 0,
            "workflow_id": "workflow-test",
            "state": "STOPPED",
            "last_updated_at": now.isoformat(),
            "aps_spec": {"auto_resume_mode": "singleton_dependency"},
        }
        persisted_record = create_example_slurm_job_record(
            job_id="job-stopped",
            attempt_index=0,
            state="STOPPED",
            last_updated_at=now,
            auto_resume_mode=AutoResumeModeEnum.SINGLETON_DEPENDENCY,
        )

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.main.maybe_persist_requeue_terminal_advice") as persist_mock,
        ):
            self._use_real_job_upsert_boundary(job_manager_mock)
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.storage.upsert_job_record.return_value = (persisted_record, True, None)
            job_manager_mock._is_advice_management_enabled.return_value = True
            job_manager_mock._is_dag_support_enabled.return_value = True

            response = self._client.post("/jobs", json=payload)

            self.assertEqual(response.status_code, 200)
            persist_mock.assert_called_once()
            self.assertIs(
                persist_mock.call_args.kwargs["on_advice_persisted"],
                job_manager_mock.propagate_terminated_advice_for_dag_dependents,
            )
            self.assertTrue(persist_mock.call_args.kwargs["allow_explicit_dag_abort"])

    def test_get_job_status_endpoint_200_status_without_anomaly_attribution(self):
        example_job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
            attempt_index=0,
        )
        example_job_record.heimdall_job_id = uuid.uuid4()

        example_job_record.config_validation_failure_types = [JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND]

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = example_job_record
            job_manager_mock.config = self._config

            response = self._client.get("/job_status/12345")
            job_manager_mock.storage.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=None)
            job_manager_mock.attribution_storage.read_attributions.assert_not_called()

            job_status = SlurmJobExecutionStatus.model_validate_json(response.content)
            self.assertEqual(job_status.config_validation_failure_types, ["LOG_FILE_PATH_NOT_FOUND"])
            self.assertEqual(job_status.filename, str(example_job_record.heimdall_job_id))
            self.assertEqual(response.status_code, 200)

    def test_job_reads_do_not_query_embedded_attributions(self):
        """Job reads remain independent of attribution storage, even with stale config."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(state="FAILED", attempt_index=0)
        record.anomalies = [
            JobAnomaly(anomaly_type="CRASH", timestamp=now, uuid=str(uuid6.uuid7())),
            JobAnomaly(anomaly_type="HANG", timestamp=now, uuid=str(uuid6.uuid7())),
        ]
        # Existing installations may still supply this retired setting.
        self._config["service"]["populate_attribution_for_anomalies"] = True

        for path in ("/job_status/12345", "/jobs", "/job_anomalies/12345"):
            with self.subTest(path=path), patch("job_manager.main.job_manager") as manager:
                manager.config = self._config
                manager.storage.read_job_record.return_value = record
                manager.storage.read_job_records.return_value = ([record], 1)
                manager.attribution_storage.read_attributions.side_effect = AssertionError(
                    "Unexpected attribution query"
                )
                manager.attribution_storage.read_global_attributions.side_effect = AssertionError(
                    "Unexpected attribution query"
                )

                response = self._client.get(path)

                self.assertEqual(response.status_code, 200)
                body = response.json()
                if path == "/jobs":
                    anomalies = body["statuses"][0]["anomalies"]
                elif path.startswith("/job_status/"):
                    anomalies = body["anomalies"]
                else:
                    anomalies = body
                self.assertEqual([anomaly["uuid"] for anomaly in anomalies], [a.uuid for a in record.anomalies])
                for anomaly in anomalies:
                    self.assertNotIn("node_events", anomaly)
                    self.assertNotIn("global_events", anomaly)
                self.assertEqual(manager.attribution_storage.mock_calls, [])

    def test_get_job_status_endpoint_200_status_with_attempt_index(self):
        example_job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
            attempt_index=1,
        )
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = example_job_record
            response = self._client.get(url="/job_status/12345", params={"job_attempt_index": 1})
            job_manager_mock.storage.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=1)
            self.assertEqual(response.status_code, 200)

    def test_get_job_status_endpoint_404_status(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            response = self._client.get("/job_status/test_job_id")
            self.assertEqual(response.status_code, 404)
            job_manager_mock.storage.read_job_record.assert_called_once_with(
                job_id="test_job_id", job_attempt_index=None
            )

    def test_get_job_status_endpoint_500_status(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.side_effect = RecoveryStorageException("Test exception")
            response = self._client.get("/job_status/test_job_id")
            self.assertEqual(response.status_code, 500)
            job_manager_mock.storage.read_job_record.assert_called_once_with(
                job_id="test_job_id", job_attempt_index=None
            )

    def test_get_jobs_endpoint_400_status_unsupported_parameter_combination(self):
        # Scnario where only job_attempt_index is present.
        response = self._client.get("/jobs", params={"job_attempt_index": 0})
        self.assertEqual(response.status_code, 400)

    def test_get_jobs_endpoint_200_status_non_numeric_job_id(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get("/jobs", params={"job_id": "qa-amcharg-xid149pri-0402-142414"})

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["job_id"], "qa-amcharg-xid149pri-0402-142414"
            )

    def test_get_jobs_endpoint_400_status_invalid_job_attempt_index(self):
        response = self._client.get("/jobs", params={"job_id": "123", "job_attempt_index": "0details"})
        self.assertEqual(response.status_code, 400)

    def test_get_jobs_endpoint_400_status_invalid_sort_by(self):
        # 'aaa" is not a valid sort order.
        response = self._client.get("/jobs", params={"job_name": "test_job", "sort_by": "job_id:aaa"})
        self.assertEqual(response.status_code, 400)

    def test_get_jobs_endpoint_500_status(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.side_effect = RecoveryStorageException("Test exception")
            response = self._client.get("/jobs", params={"job_id": "1234", "state": "Pending"})
            self.assertEqual(response.status_code, 500)

    def test_get_jobs_endpoint_200_status(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)

        job_list = [
            create_example_slurm_job_record(
                submit_time=submit_time + timedelta(minutes=5),
                last_updated_at=now,
                attempt_index=0,
                job_id="100",
            ),
            create_example_slurm_job_record(
                submit_time=submit_time,
                last_updated_at=now,
                attempt_index=0,
                job_id="101",
            ),
        ]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = (job_list, 2)
            response = self._client.get(
                "/jobs",
                params={
                    "job_name": "Test job",
                    "limit": 5,
                    "offset": 10,
                    "sort_by": "job_id:asc,job_attempt_index:asc",
                },
            )

            self.assertEqual(response.status_code, 200)
            job_statuses = JobExecutionStatuses.model_validate_json(response.content)

            self.assertEqual(job_statuses.item_count, 2)
            self.assertEqual(len(job_statuses.statuses), 2)
            self.assertEqual(job_statuses.statuses[0].job_id, "100")
            self.assertEqual(job_statuses.statuses[1].job_id, "101")

            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["job_name"],
                "Test job",
            )
            self.assertEqual(job_manager_mock.storage.read_job_records.call_args.kwargs["limit"], 5)
            self.assertEqual(job_manager_mock.storage.read_job_records.call_args.kwargs["offset"], 10)

    def test_get_jobs_endpoint_reconciliation_filter_includes_only_scheduler_relevant_states(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get("/jobs", params={"reconciliation_eligible_jobs": "true"})

            self.assertEqual(response.status_code, 200)
            states = job_manager_mock.storage.read_job_records.call_args.kwargs["states"]
            self.assertIn("RUNNING", states)
            self.assertIn("COMPLETING", states)
            self.assertIn("FAILED", states)
            self.assertNotIn("NODE_FAIL", states)
            self.assertIn("OUT_OF_MEMORY", states)
            self.assertIn("TIMEOUT", states)
            self.assertIn("BOOT_FAIL", states)
            self.assertIn("DEADLINE", states)
            self.assertNotIn("PREEMPTED", states)
            self.assertIn("REVOKED", states)
            self.assertIn("SPECIAL_EXIT", states)
            self.assertNotIn("COMPLETED", states)
            self.assertNotIn("CANCELLED", states)
            self.assertNotIn("STOPPED", states)
            self.assertNotIn("SUSPENDED", states)
            self.assertNotIn("REQUEUED", states)

    def test_get_jobs_endpoint_non_final_filter_excludes_finished_history(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get("/jobs", params={"completed_jobs": "false", "limit": 5, "offset": 10})

            self.assertEqual(response.status_code, 200)
            query = job_manager_mock.storage.read_job_records.call_args.kwargs
            # Include in-flight transitions, not just RUNNING; exclude all
            # finished failure history before storage counts or paginates.
            self.assertEqual(
                set(query["states"]),
                {
                    "COMPLETING",
                    "CONFIGURING",
                    "PENDING",
                    "POWER_UP_NODE",
                    "RESIZING",
                    "RESV_DEL_HOLD",
                    "RUNNING",
                    "SIGNALING",
                    "STAGE_OUT",
                    "UPDATE_DB",
                },
            )
            self.assertEqual(query["limit"], 5)
            self.assertEqual(query["offset"], 10)

    def test_get_jobs_endpoint_rejects_conflicting_reconciliation_filter(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            response = self._client.get(
                "/jobs",
                params={"reconciliation_eligible_jobs": "true", "state": "RUNNING"},
            )

            self.assertEqual(response.status_code, 400)
            job_manager_mock.storage.read_job_records.assert_not_called()

    def test_get_jobs_endpoint_recent_failure_cutoff_uses_end_time(self):
        cutoff = datetime(2026, 10, 6, 12, 30, tzinfo=UTC)
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get(
                "/jobs",
                params={
                    "reconciliation_eligible_jobs": "true",
                    "time_filter_target": "END_TIME",
                    "time_filter_from": cutoff.isoformat(),
                    "limit": 5,
                    "offset": 10,
                },
            )

            self.assertEqual(response.status_code, 200)
            query = job_manager_mock.storage.read_job_records.call_args.kwargs
            self.assertIn("FAILED", query["states"])
            self.assertNotIn("COMPLETED", query["states"])
            self.assertEqual(query["time_filter"].job_time_target.value, "END_TIME")
            self.assertEqual(query["time_filter"].from_time, cutoff)
            self.assertEqual(query["limit"], 5)
            self.assertEqual(query["offset"], 10)

    def test_get_jobs_endpoint_forwards_workflow_id_verbatim_and_independently_of_job_name(self):
        workflow_ids = [
            "x",
            "  Wørk/%_\\路  ",
            "路" * 256,
        ]

        for workflow_id in workflow_ids:
            with self.subTest(workflow_id=workflow_id), patch("job_manager.main.job_manager") as job_manager_mock:
                job_manager_mock.storage.read_job_records.return_value = ([], 0)

                response = self._client.get(
                    "/jobs",
                    params={
                        "workflow_id": workflow_id,
                        "job_name": "shared-metadata",
                        "job_name_match": "exact",
                    },
                )

                self.assertEqual(response.status_code, 200)
                storage_kwargs = job_manager_mock.storage.read_job_records.call_args.kwargs
                self.assertEqual(storage_kwargs["workflow_id"], workflow_id)
                self.assertEqual(storage_kwargs["job_name"], "shared-metadata")
                self.assertEqual(storage_kwargs["job_name_match"], "exact")

    def test_get_jobs_endpoint_rejects_workflow_id_outside_length_bounds(self):
        for workflow_id in ["", "x" * 257]:
            with self.subTest(length=len(workflow_id)), patch("job_manager.main.job_manager") as job_manager_mock:
                job_manager_mock.storage.read_job_records.return_value = ([], 0)

                response = self._client.get("/jobs", params={"workflow_id": workflow_id})

                self.assertEqual(response.status_code, 422)
                job_manager_mock.storage.read_job_records.assert_not_called()

    def test_get_jobs_endpoint_200_with_job_group_id_filter(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get("/jobs", params={"job_group_id": "99999"})

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["job_group_id"],
                "99999",
            )
            # include_job_group_attempts should default to True
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["include_job_group_attempts"],
                True,
            )

    def test_get_jobs_endpoint_200_with_include_job_group_attempts_false(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get("/jobs", params={"include_job_group_attempts": False})

            self.assertEqual(response.status_code, 200)
            self.assertIsNone(
                job_manager_mock.storage.read_job_records.call_args.kwargs["job_group_id"],
            )
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["include_job_group_attempts"],
                False,
            )

    def test_get_jobs_endpoint_200_with_job_group_id_and_include_flag_false(self):
        """When both job_group_id and include_job_group_attempts=False are provided, both are passed through."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_records.return_value = ([], 0)

            response = self._client.get(
                "/jobs",
                params={"job_group_id": "99999", "include_job_group_attempts": False},
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["job_group_id"],
                "99999",
            )
            self.assertEqual(
                job_manager_mock.storage.read_job_records.call_args.kwargs["include_job_group_attempts"],
                False,
            )

    def test_get_workflows_endpoint_200_status(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_workflows.return_value = (
                [
                    create_example_workflow_record(
                        "test_workflow_id",
                        submit_time=datetime.now(UTC) - timedelta(minutes=10),
                        end_time=datetime.now(UTC),
                    )
                ],
                1,
            )
            response = self._client.get("/workflows")
            self.assertEqual(response.status_code, 200)

            response_json = response.json()

            self.assertEqual(response_json["item_count"], 1)
            self.assertEqual(len(response_json["workflows"]), 1)
            self.assertEqual(response_json["workflows"][0]["id"], "test_workflow_id")

    def _test_get_job_groups_200(self, params, expected_kwargs, expected_response=None):
        """Helper to test GET /job_groups returns 200 and verifies complete storage call kwargs."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            return_value = expected_response or ([], 0)
            job_manager_mock.storage.read_job_groups.return_value = return_value

            response = self._client.get("/job_groups", params=params)

            self.assertEqual(response.status_code, 200)
            kwargs = job_manager_mock.storage.read_job_groups.call_args.kwargs
            self.assertEqual(kwargs, expected_kwargs)
            return response

    def test_get_job_groups_endpoint_200_default_params(self):
        """Verify GET /job_groups returns 200 with correct response and default storage kwargs."""
        mock_data = [
            {"job_group_id": "99999", "unique_job_count": 3, "total_job_attempt_count": 7},
            {"job_group_id": "88888", "unique_job_count": 2, "total_job_attempt_count": 4},
        ]
        response = self._test_get_job_groups_200(
            params={"limit": 10, "offset": 0},
            expected_kwargs={
                "job_group_id": None,
                "user": None,
                "sort_by": None,
                "limit": 10,
                "offset": 0,
            },
            expected_response=(mock_data, 2),
        )
        response_json = response.json()
        self.assertEqual(response_json["item_count"], 2)
        self.assertEqual(len(response_json["job_groups"]), 2)
        self.assertEqual(response_json["job_groups"][0]["job_group_id"], "99999")
        self.assertEqual(response_json["job_groups"][0]["unique_job_count"], 3)
        self.assertEqual(response_json["job_groups"][0]["total_job_attempt_count"], 7)

    def test_get_job_groups_endpoint_200_with_filters(self):
        """Verify GET /job_groups forwards job_group_id and user filters to storage."""
        self._test_get_job_groups_200(
            params={"job_group_id": "99999", "user": "alice"},
            expected_kwargs={
                "job_group_id": "99999",
                "user": "alice",
                "sort_by": None,
                "limit": 50,
                "offset": 0,
            },
        )

    def test_get_job_groups_endpoint_200_with_sort_by(self):
        """Verify GET /job_groups parses sort_by into list-of-tuples for storage."""
        self._test_get_job_groups_200(
            params={"sort_by": "total_job_attempt_count:desc"},
            expected_kwargs={
                "job_group_id": None,
                "user": None,
                "sort_by": [("total_job_attempt_count", "desc")],
                "limit": 50,
                "offset": 0,
            },
        )

    def test_get_job_groups_endpoint_400_invalid_sort_by(self):
        """Verify GET /job_groups returns 400 when sort_by contains an invalid sort direction."""
        response = self._client.get("/job_groups", params={"sort_by": "job_group_id:invalid"})
        self.assertEqual(response.status_code, 400)

    def test_get_job_groups_endpoint_400_malformed_sort_by(self):
        """Verify GET /job_groups returns 400 when sort_by is missing the colon separator."""
        response = self._client.get("/job_groups", params={"sort_by": "job_group_id"})
        self.assertEqual(response.status_code, 400)

    def test_get_job_groups_endpoint_500_status(self):
        """Verify GET /job_groups returns 500 when storage raises RecoveryStorageException."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_groups.side_effect = RecoveryStorageException("Test exception")
            response = self._client.get("/job_groups")
            self.assertEqual(response.status_code, 500)

    def test_get_workflows_endpoint_with_workflow_id_filter(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_workflows.return_value = ([], 0)

            response = self._client.get(
                "/workflows",
                params={"workflow-id": "test_workflow_id", "workflow_id_match": "exact"},
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                job_manager_mock.storage.read_workflows.call_args.kwargs["workflow_id"],
                "test_workflow_id",
            )
            self.assertEqual(
                job_manager_mock.storage.read_workflows.call_args.kwargs["workflow_id_match"],
                "exact",
            )

    def test_get_workflows_endpoint_rejects_invalid_workflow_id_before_storage(self):
        for workflow_id in ["", "x" * 257]:
            with self.subTest(length=len(workflow_id)), patch("job_manager.main.job_manager") as job_manager_mock:
                response = self._client.get(
                    "/workflows",
                    params={"workflow-id": workflow_id, "workflow_id_match": "exact"},
                )

                self.assertEqual(response.status_code, 422)
                job_manager_mock.storage.read_workflows.assert_not_called()

    def test_get_users_endpoint_200_status(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_users.return_value = ["user1", "user2"]
            response = self._client.get("/admin/users")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), ["user1", "user2"])

    def test_get_system_max_requeue_times(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.get_system_max_requeue_times.return_value = 50
            job_manager_mock.get_checkpoint_based_requeue_stop_threshold.return_value = 5
            job_manager_mock.get_requeue_on_completion_config.return_value = False
            response = self._client.get("/config/requeue")
            self.assertEqual(response.status_code, 200)

            requeue_config = SystemRequeueConfig.model_validate(response.json())
            self.assertEqual(requeue_config.max_requeue_times, 50)
            self.assertEqual(requeue_config.checkpoint_based_requeue_stop_threshold, 5)
            self.assertFalse(requeue_config.requeue_on_completion)

    def test_get_job_error_attributions_success(self):
        """Test successful retrieval of job error attributions."""
        # Setup test data
        job_id = "12345"
        job_attempt_index = 1
        node_events = []
        node_id_start = 0
        anomaly_uuid = uuid.uuid3(uuid.NAMESPACE_DNS, "nvidia.com")
        num_events = 10
        for i in range(num_events):
            error = JobExecutionErrorInternal.create(
                code=i + 1,
                message=f"error message {i}",
                description=f"error description {i}",
                signature=f"error signature {i}",
            )

            event = NodeEventInternal.create(
                node=f"node_{node_id_start + i}",
                timestamp=datetime.now(),
                job_id=job_id,
                job_attempt_index=0,
                job_start_time=datetime.now(),
                job_end_time=datetime.now(),
                job_anomaly_id=anomaly_uuid,
                cluster="test cluster",
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.FACT,
                error_attribution_sources=[
                    ErrorAttributionSource.SYSLOGS,
                    ErrorAttributionSource.APPLICATION_LOGS,
                ],
                exclusion_type=ExclusionType.TEMPORARY,
                fact_status=FactStatus.OK,
            )
            event.job_execution_error = error
            node_events.append(event)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.attribution_storage.read_node_attributions_for_job.return_value = node_events
            job_manager_mock.attribution_storage.read_global_attributions_for_job.return_value = []
            job_manager_mock.config = self._config
            # Make request
            response = self._client.get(
                f"/job_error_attributions?job_id={job_id}&job_attempt_index={job_attempt_index}&anomaly_uuid={anomaly_uuid}"
            )

        # Verify response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["item_count"], num_events)
        self.assertEqual(len(data["events"]), num_events)
        self.assertEqual(
            data["attribution_summary"],
            self._config["service"]["attribution_summary_config"]["infra"]["summary"],
        )
        self.assertEqual(
            data["attribution_insight"],
            self._config["service"]["attribution_insight_config"]["temporary"]["insight"],
        )

        # Verify node storage was called correctly
        job_manager_mock.attribution_storage.read_node_attributions_for_job.assert_called_once_with(
            job_id=job_id, job_attempt_index=job_attempt_index, anomaly_uuid=anomaly_uuid
        )

    def test_get_job_error_attributions_no_attribution_storage(self):
        """Test error when node storage is not initialized."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.attribution_storage = None
            # Make request
            response = self._client.get("/job_error_attributions?job_id=12345")

        # Verify response
        self.assertEqual(response.status_code, 500)
        data = response.json()
        self.assertEqual(data["detail"], "Attribution storage is not initialized.")

    def test_get_job_error_attributions_missing_job_id(self):
        """Test error when job_id is not provided."""
        with patch("job_manager.main.job_manager"):
            # Make request without job_id
            response = self._client.get("/job_error_attributions")

        # Verify response
        self.assertEqual(response.status_code, 422)  # Validation error
        data = response.json()
        self.assertIn("detail", data)

    def test_get_job_error_attributions_storage_error(self):
        """Test error handling when storage raises an exception."""
        # Setup test data
        job_id = "12345"

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.attribution_storage.read_node_attributions_for_job.side_effect = RecoveryStorageException(
                "Storage error"
            )
            # Make request
            response = self._client.get(f"/job_error_attributions?job_id={job_id}")

        # Verify response
        self.assertEqual(response.status_code, 500)
        data = response.json()
        self.assertEqual(data["detail"], "Storage error")

        # Verify node storage was called
        job_manager_mock.attribution_storage.read_node_attributions_for_job.assert_called_once_with(
            job_id=job_id, job_attempt_index=None, anomaly_uuid=None
        )

    def test_get_job_error_attributions_value_error(self):
        """Test error handling when storage raises a ValueError."""
        # Setup test data
        job_id = "12345"

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.attribution_storage.read_node_attributions_for_job.side_effect = ValueError(
                "Invalid value"
            )
            # Make request
            response = self._client.get(f"/job_error_attributions?job_id={job_id}")

        # Verify response
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data["detail"], "Invalid value")

        # Verify node storage was called
        job_manager_mock.attribution_storage.read_node_attributions_for_job.assert_called_once_with(
            job_id=job_id, job_attempt_index=None, anomaly_uuid=None
        )

    def test_get_job_error_attributions_running_job(self):
        """Test that running jobs return Unknown for summary and insight."""
        # Setup test data
        job_id = "12345"
        job_attempt_index = 1

        # Create a job record with RUNNING state
        job_record = create_example_slurm_job_record(
            job_id=job_id,
            attempt_index=job_attempt_index,
            state="RUNNING",
        )

        # Create node events
        node_events = []
        error = JobExecutionErrorInternal.create(
            code=1,
            message="error message",
            description="error description",
            signature="error signature",
        )

        event = NodeEventInternal.create(
            node="node_1",
            timestamp=datetime.now(),
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            cluster="test cluster",
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )
        event.job_execution_error = error
        node_events.append(event)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.attribution_storage.read_node_attributions_for_job.return_value = node_events
            job_manager_mock.attribution_storage.read_global_attributions_for_job.return_value = []
            job_manager_mock.config = self._config

            # Make request
            response = self._client.get(
                f"/job_error_attributions?job_id={job_id}&job_attempt_index={job_attempt_index}"
            )

        # Verify response
        self.assertEqual(response.status_code, 200)
        data = response.json()

        # Should return Unknown for both summary and insight when job is running
        self.assertEqual(data["attribution_summary"], "Unknown")
        self.assertEqual(data["attribution_insight"], "Unknown")

        # Events should still be populated
        self.assertEqual(len(data["events"]), 1)
        self.assertEqual(data["item_count"], 1)

        # Verify storage was called correctly
        job_manager_mock.storage.read_job_record.assert_called_once_with(
            job_id=job_id, job_attempt_index=job_attempt_index
        )

    def test_get_job_error_attributions_completed_job(self):
        """Test that completed jobs return computed summary and insight."""
        # Setup test data
        job_id = "12345"
        job_attempt_index = 1

        # Create a job record with COMPLETED state
        job_record = create_example_slurm_job_record(
            job_id=job_id,
            attempt_index=job_attempt_index,
            state="COMPLETED",
        )

        # Create node events
        node_events = []
        error = JobExecutionErrorInternal.create(
            code=1,
            message="error message",
            description="error description",
            signature="error signature",
        )

        event = NodeEventInternal.create(
            node="node_1",
            timestamp=datetime.now(),
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            cluster="test cluster",
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )
        event.job_execution_error = error
        node_events.append(event)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.attribution_storage.read_node_attributions_for_job.return_value = node_events
            job_manager_mock.attribution_storage.read_global_attributions_for_job.return_value = []
            job_manager_mock.config = self._config

            # Make request
            response = self._client.get(
                f"/job_error_attributions?job_id={job_id}&job_attempt_index={job_attempt_index}"
            )

        # Verify response
        self.assertEqual(response.status_code, 200)
        data = response.json()

        # Should return computed summary and insight when job is completed
        self.assertEqual(
            data["attribution_summary"],
            self._config["service"]["attribution_summary_config"]["infra"]["summary"],
        )
        self.assertEqual(
            data["attribution_insight"],
            self._config["service"]["attribution_insight_config"]["confirmed"]["insight"],
        )

        # Events should be populated
        self.assertEqual(len(data["events"]), 1)
        self.assertEqual(data["item_count"], 1)

    def test_get_job_error_attributions_no_job_record(self):
        """Test that missing job record allows normal summary/insight computation."""
        # Setup test data
        job_id = "12345"
        job_attempt_index = 1

        # Create node events
        node_events = []
        error = JobExecutionErrorInternal.create(
            code=1,
            message="error message",
            description="error description",
            signature="error signature",
        )

        event = NodeEventInternal.create(
            node="node_1",
            timestamp=datetime.now(),
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            cluster="test cluster",
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status=FactStatus.OK,
        )
        event.job_execution_error = error
        node_events.append(event)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            # Return None for job record (job not found)
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.attribution_storage.read_node_attributions_for_job.return_value = node_events
            job_manager_mock.attribution_storage.read_global_attributions_for_job.return_value = []
            job_manager_mock.config = self._config

            # Make request
            response = self._client.get(
                f"/job_error_attributions?job_id={job_id}&job_attempt_index={job_attempt_index}"
            )

        # Verify response
        self.assertEqual(response.status_code, 200)
        data = response.json()

        # Should return computed summary and insight when job record is not found
        self.assertEqual(
            data["attribution_summary"],
            self._config["service"]["attribution_summary_config"]["infra"]["summary"],
        )
        self.assertEqual(
            data["attribution_insight"],
            self._config["service"]["attribution_insight_config"]["confirmed"]["insight"],
        )

    # ============================================================================
    # Advice Endpoint Tests
    # ============================================================================

    def test_get_advice_endpoint_200_status(self):
        """Test successful retrieval of advice list."""
        advice_records = [create_example_advice_record(job_id=f"job-{i}") for i in range(3)]

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.return_value = (advice_records, 3)

            response = self._client.get("/advice", params={"page_size": 10})

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["item_count"], 3)
            self.assertEqual(len(data["advice"]), 3)
            self.assertIsNone(data.get("nextPageToken"))

            job_manager_mock.storage.read_advice_list.assert_called_once_with(
                job_id=None,
                limit=10,
                offset=0,
                include_hidden=False,
                created_after=None,
                only_active=False,
            )

    def test_get_advice_endpoint_with_pagination(self):
        """Test advice list with pagination."""
        advice_records = [create_example_advice_record(job_id=f"job-{i}") for i in range(5)]

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.return_value = (advice_records, 10)

            response = self._client.get("/advice", params={"page_size": 5})

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["item_count"], 10)
            self.assertEqual(len(data["advice"]), 5)
            self.assertIsNotNone(data.get("nextPageToken"))

    def test_get_advice_only_active_leaves_age_classification_to_controller(self):
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.return_value = ([], 0)
            requested_created_after = datetime(2026, 9, 1, tzinfo=UTC)

            response = self._client.get(
                "/advice",
                params={
                    "only_active": True,
                    "created_after": requested_created_after.isoformat(),
                },
            )

            self.assertEqual(response.status_code, 200)
            job_manager_mock.storage.read_advice_list.assert_called_once_with(
                job_id=None,
                limit=50,
                offset=0,
                include_hidden=False,
                created_after=requested_created_after,
                only_active=True,
            )

    def test_get_advice_only_active_page_token_retains_created_after(self):
        advice_record = create_example_advice_record(job_id="active-job")
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.return_value = ([advice_record], 2)
            requested_created_after = datetime(2026, 9, 1, tzinfo=UTC)

            first = self._client.get(
                "/advice",
                params={
                    "page_size": 1,
                    "only_active": True,
                    "created_after": requested_created_after.isoformat(),
                },
            )
            self.assertEqual(first.status_code, 200)
            token = first.json()["nextPageToken"]
            first_boundary = job_manager_mock.storage.read_advice_list.call_args.kwargs["created_after"]

            second = self._client.get(
                "/advice",
                params={
                    "page_size": 1,
                    "page_token": token,
                    "only_active": True,
                },
            )
            self.assertEqual(second.status_code, 200)
            second_boundary = job_manager_mock.storage.read_advice_list.call_args.kwargs["created_after"]
            self.assertEqual(second_boundary, first_boundary)

    def test_put_advice_reconciliation_status(self):
        advice_id = uuid.uuid4()
        observed_at = datetime.now(UTC)
        stored = AdviceReconciliationStatusRecord(
            advice_id=advice_id,
            observed_generation=2,
            disposition=AdviceReconciliationDisposition.CONVERGED.value,
            reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
            observed_at=observed_at,
            transitioned_at=observed_at,
        )
        payload = {
            "observedGeneration": 2,
            "disposition": "CONVERGED",
            "reason": "DESIRED_STATE_OBSERVED",
            "observedAt": observed_at.isoformat(),
            "transitionedAt": observed_at.isoformat(),
        }

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.upsert_advice_reconciliation_status.return_value = stored

            response = self._client.put(
                f"/advice/{advice_id}/reconciliation-status",
                json=payload,
            )

        self.assertEqual(response.status_code, 200)
        persisted = job_manager_mock.storage.upsert_advice_reconciliation_status.call_args.args[0]
        self.assertEqual(persisted.advice_id, advice_id)
        self.assertEqual(persisted.observed_generation, 2)

    def test_get_advice_endpoint_with_job_id_filter(self):
        """Test advice list filtered by job_id."""
        advice_record = create_example_advice_record(job_id="specific-job")

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.return_value = ([advice_record], 1)

            response = self._client.get("/advice", params={"job_id": "specific-job"})

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["item_count"], 1)
            self.assertEqual(data["advice"][0]["jobId"], "specific-job")

            job_manager_mock.storage.read_advice_list.assert_called_once_with(
                job_id="specific-job",
                limit=50,
                offset=0,
                include_hidden=False,
                created_after=None,
                only_active=False,
            )

    def test_get_advice_endpoint_500_status(self):
        """Test error handling when storage raises exception."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_list.side_effect = RecoveryStorageException("Storage error")

            response = self._client.get("/advice")

            self.assertEqual(response.status_code, 500)

    def test_get_advice_by_id_endpoint_200_status(self):
        """Test successful retrieval of advice by ID."""
        advice_id = uuid.uuid4()
        advice_record = create_example_advice_record(heimdall_id=advice_id)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice.return_value = advice_record

            response = self._client.get(f"/advice/{advice_id!s}")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["id"], str(advice_id))
            self.assertEqual(data["jobId"], advice_record.job_id)

            job_manager_mock.storage.read_advice.assert_called_once_with(heimdall_id=advice_id)

    def test_get_advice_by_id_endpoint_404_status(self):
        """Test 404 when advice not found."""
        advice_id = uuid.uuid4()

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice.return_value = None

            response = self._client.get(f"/advice/{advice_id!s}")

            self.assertEqual(response.status_code, 404)

    def test_get_advice_by_id_endpoint_400_invalid_uuid(self):
        """Test 400 for invalid UUID format."""
        response = self._client.get("/advice/not-a-valid-uuid")
        self.assertEqual(response.status_code, 400)

    def test_post_advice_endpoint_200_create(self):
        """Test successful creation of advice record."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)

        # Use Python field names for input (serialization_alias only affects output)
        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 1,
            "scheduler_metadata": {"namespace": "default"},
            "advice": {
                "state": "Restarted",
                "node_constraints": {"exclusions": ["bad-node-1"]},
            },
            "issues": [
                {
                    "issue_type": "CRASH",
                    "timestamp": now.isoformat(),
                },
                {
                    "issue_type": "HANG",
                    "timestamp": now.isoformat(),
                },
            ],
        }

        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata={"namespace": "default"},
            attempt_index=1,
            advice_state="Restarted",
            node_exclusions=["bad-node-1"],
            node_inclusions=None,
            issues=[
                {"issue_type": "CRASH", "timestamp": now.isoformat()},
                {"issue_type": "HANG", "timestamp": now.isoformat()},
            ],
            created_at=now,
            updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.upsert_advice_record.return_value = (advice_record, True, None)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["id"], advice_id)
            self.assertEqual(data["jobId"], "test-job-123")

            job_manager_mock.upsert_advice_record.assert_called_once()

    def test_post_advice_ignores_caller_recovery_context_when_job_lookup_misses(self):
        record = create_example_advice_record()
        with patch("job_manager.main.job_manager") as manager:
            manager.storage.read_job_record.return_value = None
            manager.upsert_advice_record.return_value = (record, True, None)
            response = self._client.post(
                "/advice",
                json={
                    "id": str(record.heimdall_id),
                    "job_id": record.job_id,
                    "restart_count": 0,
                    "advice": {"state": "Restarted"},
                    "recovery_context": {
                        "sequence_id": str(uuid.uuid4()),
                        "operation": "restart",
                        "traceparent": "00-11111111111111111111111111111111-1111111111111111-01",
                        "triggered_at": datetime.now(UTC).isoformat(),
                    },
                },
            )
        self.assertEqual(response.status_code, 200)
        passed_record = manager.upsert_advice_record.call_args.args[0]
        self.assertIsNone(passed_record.recovery_sequence_id)
        self.assertIsNone(passed_record.recovery_traceparent)

    def test_post_advice_endpoint_200_update(self):
        """Test successful update of existing advice record."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)

        # Use Python field names for input (serialization_alias only affects output)
        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 2,
            "scheduler_metadata": {},
            "advice": {
                "state": "Terminated",
            },
        }

        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata={},
            attempt_index=2,
            advice_state="Terminated",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )
        old_record = deepcopy(advice_record)
        old_record.advice_state = "Restarted"

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.upsert_advice_record.return_value = (advice_record, False, old_record)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["advice"]["state"], "Terminated")
            job_manager_mock.propagate_terminated_advice_for_dag_dependents.assert_called_once_with(advice_record)

            job_manager_mock.upsert_advice_record.assert_called_once()

    def test_post_advice_backfills_scheduler_metadata_from_job_record(self):
        """Backfill scheduler metadata from job record when advice payload omits it."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 2,
            "scheduler_metadata": {},
            "advice": {"state": "Terminated"},
        }
        expected_scheduler_metadata = {"namespace": "runai-training", "job_kind": "PyTorchJob"}
        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata=expected_scheduler_metadata,
            attempt_index=2,
            advice_state="Terminated",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )
        job_record = create_example_slurm_job_record(job_id="test-job-123", attempt_index=2)
        job_record.scheduler_metadata = expected_scheduler_metadata

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.upsert_advice_record.return_value = (advice_record, False, deepcopy(advice_record))

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            upserted_advice = job_manager_mock.upsert_advice_record.call_args.args[0]
            self.assertEqual(upserted_advice.scheduler_metadata, expected_scheduler_metadata)
            job_manager_mock.storage.read_job_record.assert_called_once_with(
                job_id="test-job-123",
                job_attempt_index=2,
            )

    def test_post_advice_keeps_provided_scheduler_metadata(self):
        """Do not overwrite scheduler metadata when already present in advice payload."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 2,
            "scheduler_metadata": {"namespace": "override"},
            "advice": {"state": "Terminated"},
        }
        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata={"namespace": "override"},
            attempt_index=2,
            advice_state="Terminated",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )
        job_record = create_example_slurm_job_record(job_id="test-job-123", attempt_index=2)
        job_record.heimdall_job_id = uuid.UUID(advice_id)
        job_record.scheduler_metadata = {"namespace": "from-job-record"}

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.upsert_advice_record.return_value = (advice_record, False, deepcopy(advice_record))

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            upserted_advice = job_manager_mock.upsert_advice_record.call_args.args[0]
            self.assertEqual(upserted_advice.scheduler_metadata, {"namespace": "override"})

    def test_post_advice_empty_scheduler_metadata_without_job_record(self):
        """Keep empty scheduler metadata when there is no matching job record."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        advice_data = {
            "id": advice_id,
            "job_id": "missing-job",
            "restart_count": 0,
            "scheduler_metadata": {},
            "advice": {"state": "Terminated"},
        }
        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="missing-job",
            scheduler_metadata={},
            attempt_index=0,
            advice_state="Terminated",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.upsert_advice_record.return_value = (advice_record, True, None)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            upserted_advice = job_manager_mock.upsert_advice_record.call_args.args[0]
            self.assertEqual(upserted_advice.scheduler_metadata, {})
            job_manager_mock.storage.read_job_record.assert_called_once_with(
                job_id="missing-job",
                job_attempt_index=0,
            )

    def test_post_advice_endpoint_500_status(self):
        """Test error handling when storage raises exception."""
        # Use Python field names for input (serialization_alias only affects output)
        advice_data = {
            "id": str(uuid.uuid4()),
            "job_id": "test-job-123",
            "restart_count": 0,
            "scheduler_metadata": {},
            "advice": {},
        }

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.upsert_advice_record.side_effect = RecoveryStorageException("Storage error")

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 500)

    def test_post_advice_succeeds_with_matching_heimdall_id(self):
        """Test that POST /advice succeeds when the advice ID matches the job record's heimdall_job_id."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)

        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 1,
            "scheduler_metadata": {},
            "advice": {"state": "Restarted"},
        }

        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata={},
            attempt_index=1,
            advice_state="Restarted",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )

        job_record = create_example_slurm_job_record(job_id="test-job-123", attempt_index=1)
        job_record.heimdall_job_id = uuid.UUID(advice_id)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.upsert_advice_record.return_value = (advice_record, True, None)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["id"], advice_id)
            job_manager_mock.storage.read_job_record.assert_called_once_with(job_id="test-job-123", job_attempt_index=1)

    def test_post_advice_rejects_mismatched_heimdall_id(self):
        """Test that POST /advice returns 409 when the advice ID does not match the job record's heimdall_job_id."""
        advice_id = str(uuid.uuid4())
        canonical_id = uuid.uuid4()  # Different from advice_id

        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 1,
            "scheduler_metadata": {},
            "advice": {"state": "Restarted"},
        }

        job_record = create_example_slurm_job_record(job_id="test-job-123", attempt_index=1)
        job_record.heimdall_job_id = canonical_id

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 409)
            data = response.json()
            self.assertIn("Advice ID mismatch", data["detail"])
            self.assertIn(advice_id, data["detail"])
            self.assertIn(str(canonical_id), data["detail"])
            job_manager_mock.upsert_advice_record.assert_not_called()

    def test_post_advice_succeeds_when_no_job_record(self):
        """Test that POST /advice proceeds normally when no job record exists for the given job_id."""
        advice_id = str(uuid.uuid4())
        now = datetime.now(UTC)

        advice_data = {
            "id": advice_id,
            "job_id": "test-job-123",
            "restart_count": 0,
            "scheduler_metadata": {},
            "advice": {"state": "Restarted"},
        }

        advice_record = AdviceRecord(
            heimdall_id=uuid.UUID(advice_id),
            job_id="test-job-123",
            scheduler_metadata={},
            attempt_index=0,
            advice_state="Restarted",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = None
            job_manager_mock.upsert_advice_record.return_value = (advice_record, True, None)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["id"], advice_id)

    def test_post_advice_succeeds_when_job_has_no_heimdall_job_id(self):
        """Test that POST /advice backfills a missing job record heimdall_job_id."""
        canonical_id = generate_heimdall_job_id("test-job-123", 0)
        now = datetime.now(UTC)

        advice_data = {
            "id": str(uuid.uuid4()),
            "job_id": "test-job-123",
            "restart_count": 0,
            "scheduler_metadata": {},
            "advice": {"state": "Restarted"},
        }

        advice_record = AdviceRecord(
            heimdall_id=canonical_id,
            job_id="test-job-123",
            scheduler_metadata={},
            attempt_index=0,
            advice_state="Restarted",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )

        job_record = create_example_slurm_job_record(job_id="test-job-123", attempt_index=0)
        job_record.heimdall_job_id = None

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_job_record.return_value = job_record
            job_manager_mock.upsert_advice_record.return_value = (advice_record, True, None)

            response = self._client.post("/advice", json=advice_data)

            self.assertEqual(response.status_code, 200)
            job_manager_mock.update_job_record.assert_called_once_with(
                job_id="test-job-123",
                job_attempt_index=0,
                heimdall_job_id=canonical_id,
            )
            self.assertEqual(job_manager_mock.upsert_advice_record.call_args.args[0].heimdall_id, canonical_id)

    def test_get_advice_by_job_returns_advice(self):
        """Test that GET /advice/by-job/{job_id}/{attempt_index} returns the correct advice."""
        advice_id = uuid.uuid4()
        now = datetime.now(UTC)

        advice_record = AdviceRecord(
            heimdall_id=advice_id,
            job_id="test-job-123",
            scheduler_metadata={},
            attempt_index=2,
            advice_state="Restarted",
            node_exclusions=None,
            node_inclusions=None,
            issues=None,
            created_at=now,
            updated_at=now,
        )

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_by_job.return_value = advice_record

            response = self._client.get("/advice/by-job/test-job-123/2")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["id"], str(advice_id))
            self.assertEqual(data["jobId"], "test-job-123")
            self.assertEqual(data["restartCount"], 2)
            job_manager_mock.storage.read_advice_by_job.assert_called_once_with(job_id="test-job-123", attempt_index=2)

    def test_get_advice_by_job_not_found(self):
        """Test that GET /advice/by-job returns 404 when no advice exists."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_advice_by_job.return_value = None

            response = self._client.get("/advice/by-job/nonexistent-job/0")

            self.assertEqual(response.status_code, 404)
            data = response.json()
            self.assertIn("No advice found", data["detail"])

    def test_delete_advice_endpoint_204_status(self):
        """Test successful deletion of advice record."""
        advice_id = uuid.uuid4()
        recovery_sequence_id = uuid.uuid4()
        advice_record = create_example_advice_record(heimdall_id=advice_id, visible=False)
        advice_record.recovery_sequence_id = recovery_sequence_id

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.delete_advice.return_value = SoftDeleteAdviceResult(
                advice_record=advice_record,
                was_deleted=True,
            )

            response = self._client.delete(f"/advice/{advice_id!s}")

            self.assertEqual(response.status_code, 204)
            job_manager_mock.storage.delete_advice.assert_called_once_with(heimdall_id=advice_id)
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertIsInstance(audit_event, AdviceUnpublishedEvent)
            payload = audit_event.model_dump(mode="json")["payload"]
            self.assertEqual(payload["job_id"], advice_record.job_id)
            self.assertEqual(payload["attempt_index"], advice_record.attempt_index)
            self.assertEqual(audit_event.event_producer, "job-manager")
            self.assertEqual(audit_event.heimdall_job_id, str(advice_id))
            self.assertIs(audit_event.event_level, AuditEventLevel.RECOVERY)
            self.assertEqual(audit_event.payload.advice_id, str(advice_id))
            self.assertIsNone(audit_event.payload.reason)
            self.assertEqual(audit_event.payload.correlation_uuid, str(recovery_sequence_id))
            self.assertIsInstance(audit_event.event_produced_at, int)
            uuid.UUID(audit_event.idempotency_key)

    def test_delete_advice_preserves_204_on_audit_build_error(self):
        """An unexpected unpublish-event error cannot turn a committed deletion into HTTP 500."""
        advice_record = create_example_advice_record(visible=False)

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch(
                "job_manager.audit.advice_changes.build_advice_unpublished_event",
                side_effect=TypeError("event construction failed"),
            ) as build_event,
            patch("job_manager.audit.advice_changes.logger.exception") as log_exception,
        ):
            job_manager_mock.storage.delete_advice.return_value = SoftDeleteAdviceResult(
                advice_record=advice_record, was_deleted=True
            )

            response = self._client.delete(f"/advice/{advice_record.heimdall_id}")

        self.assertEqual(response.status_code, 204)
        job_manager_mock.storage.delete_advice.assert_called_once_with(heimdall_id=advice_record.heimdall_id)
        build_event.assert_called_once()
        job_manager_mock.audit_buffer.add.assert_not_called()
        log_exception.assert_called_once()

    def test_delete_advice_preserves_204_on_audit_enqueue_error(self):
        """An unexpected buffer error cannot turn a committed deletion into HTTP 500."""
        advice_record = create_example_advice_record(visible=False)

        with (
            patch("job_manager.main.job_manager") as job_manager_mock,
            patch("job_manager.audit.advice_changes.logger.exception") as log_exception,
        ):
            job_manager_mock.storage.delete_advice.return_value = SoftDeleteAdviceResult(
                advice_record=advice_record, was_deleted=True
            )
            job_manager_mock.audit_buffer.add.side_effect = RuntimeError("buffer unavailable")

            response = self._client.delete(f"/advice/{advice_record.heimdall_id}")

        self.assertEqual(response.status_code, 204)
        job_manager_mock.storage.delete_advice.assert_called_once_with(heimdall_id=advice_record.heimdall_id)
        job_manager_mock.audit_buffer.add.assert_called_once()
        log_exception.assert_called_once()

    def test_delete_advice_does_not_repeat_event_for_already_hidden_advice(self):
        advice_id = uuid.uuid4()
        advice_record = create_example_advice_record(heimdall_id=advice_id, visible=False)

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.delete_advice.side_effect = [
                SoftDeleteAdviceResult(advice_record=advice_record, was_deleted=True),
                SoftDeleteAdviceResult(advice_record=advice_record, was_deleted=False),
            ]

            first_response = self._client.delete(f"/advice/{advice_id!s}")
            second_response = self._client.delete(f"/advice/{advice_id!s}")

            self.assertEqual(first_response.status_code, 204)
            self.assertEqual(second_response.status_code, 204)
            job_manager_mock.audit_buffer.add.assert_called_once()
            audit_event = job_manager_mock.audit_buffer.add.call_args.args[0]
            self.assertEqual(audit_event.payload.correlation_uuid, str(advice_id))

    def test_delete_advice_endpoint_404_status(self):
        """Test 404 when deleting non-existent advice."""
        advice_id = uuid.uuid4()

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.delete_advice.return_value = SoftDeleteAdviceResult(
                advice_record=None,
                was_deleted=False,
            )

            response = self._client.delete(f"/advice/{advice_id!s}")

            self.assertEqual(response.status_code, 404)

    def test_delete_advice_endpoint_400_invalid_uuid(self):
        """Test 400 for invalid UUID format."""
        response = self._client.delete("/advice/not-a-valid-uuid")
        self.assertEqual(response.status_code, 400)

    def test_delete_advice_endpoint_500_status(self):
        """Test error handling when storage raises exception."""
        advice_id = uuid.uuid4()

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.delete_advice.side_effect = RecoveryStorageException("Storage error")

            response = self._client.delete(f"/advice/{advice_id!s}")

            self.assertEqual(response.status_code, 500)

    def test_get_node_analysis_success_union(self):
        """Test successful node analysis with union operation."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            # Mock storage to return job node lists
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": {"node1", "node2", "node3"},
                "12346/0": {"node2", "node3", "node4"},
            }

            response = self._client.get("/jobs/node-analysis?expr=12345/0 U 12346/0&group_by=none")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["expression"], "12345/0 U 12346/0")
            self.assertEqual(len(data["result"]["nodes"]), 4)  # Union of 4 unique nodes
            self.assertIn("node1", data["result"]["nodes"])
            self.assertIn("node4", data["result"]["nodes"])
            self.assertEqual(data["result"]["count"], 4)
            self.assertIsNone(data["groupedByRack"])
            self.assertEqual(sorted(data["jobsQueried"]), ["12345/0", "12346/0"])

    def test_get_node_analysis_success_intersection(self):
        """Test successful node analysis with intersection operation."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": {"node1", "node2", "node3"},
                "12346/0": {"node2", "node3", "node4"},
            }

            # URL encode the & operator as %26
            response = self._client.get("/jobs/node-analysis?expr=12345/0 %26 12346/0&group_by=none")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(len(data["result"]["nodes"]), 2)  # Common nodes
            self.assertIn("node2", data["result"]["nodes"])
            self.assertIn("node3", data["result"]["nodes"])

    def test_get_node_analysis_success_difference(self):
        """Test successful node analysis with difference operation."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": {"node1", "node2", "node3"},
                "12346/0": {"node2", "node3", "node4"},
            }

            response = self._client.get("/jobs/node-analysis?expr=12345/0 - 12346/0&group_by=none")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(len(data["result"]["nodes"]), 1)  # Only in first set
            self.assertIn("node1", data["result"]["nodes"])

    def test_get_node_analysis_success_with_rack_grouping(self):
        """Test successful node analysis with rack grouping."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": {"nvl72013-T01", "nvl72013-T02", "nvl72014-T01"},
                "12346/0": {"nvl72013-T02", "nvl72014-T01", "nvl72014-T02"},
            }

            response = self._client.get("/jobs/node-analysis?expr=12345/0 U 12346/0&group_by=rack")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(len(data["result"]["nodes"]), 4)
            self.assertIsNotNone(data["groupedByRack"])
            self.assertIn("nvl72013", data["groupedByRack"])
            self.assertIn("nvl72014", data["groupedByRack"])
            self.assertEqual(len(data["groupedByRack"]["nvl72013"]), 2)
            self.assertEqual(len(data["groupedByRack"]["nvl72014"]), 2)

    def test_get_node_analysis_400_invalid_expression(self):
        """Test 400 for invalid expression syntax."""
        response = self._client.get("/jobs/node-analysis?expr=12345/0 U&group_by=none")

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid expression", response.json()["detail"])

    def test_get_node_analysis_400_invalid_group_by(self):
        """Test 400 for invalid group_by parameter."""
        response = self._client.get("/jobs/node-analysis?expr=12345/0&group_by=invalid")

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid group_by value", response.json()["detail"])

    def test_get_node_analysis_400_invalid_job_id_format(self):
        """Test 400 for invalid job ID format - caught by parser as invalid character."""
        with patch("job_manager.main.job_manager"):
            response = self._client.get("/jobs/node-analysis?expr=invalid-job-id U 12346/0&group_by=none")

            self.assertEqual(response.status_code, 400)
            # Parser catches this as invalid character before job ID validation
            self.assertIn("Invalid expression", response.json()["detail"])

    def test_get_node_analysis_404_job_not_found(self):
        """Test 404 when job is not found in storage."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            # Return empty sets for missing jobs
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": set(),
                "12346/0": {"node1", "node2"},
            }

            response = self._client.get("/jobs/node-analysis?expr=12345/0 U 12346/0&group_by=none")

            self.assertEqual(response.status_code, 404)
            self.assertIn("not found", response.json()["detail"])

    def test_get_node_analysis_500_storage_exception(self):
        """Test 500 when storage raises exception."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.get_allocated_nodes_batch.side_effect = RecoveryStorageException("Storage error")

            response = self._client.get("/jobs/node-analysis?expr=12345/0 U 12346/0&group_by=none")

            self.assertEqual(response.status_code, 500)
            # Storage exceptions are caught and return specific error
            detail = response.json()["detail"]
            self.assertTrue("Failed to fetch job records" in detail or "Storage error" in detail)

    def test_get_node_analysis_complex_expression(self):
        """Test complex expression with multiple operations."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.get_allocated_nodes_batch.return_value = {
                "12345/0": {"node1", "node2", "node3"},
                "12346/0": {"node2", "node3", "node4"},
                "12347/0": {"node3", "node4", "node5"},
            }

            # (A U B) - C
            response = self._client.get("/jobs/node-analysis?expr=(12345/0 U 12346/0) - 12347/0&group_by=none")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            # Union of A and B gives {1,2,3,4}, minus C {3,4,5} gives {1,2}
            self.assertEqual(len(data["result"]["nodes"]), 2)
            self.assertIn("node1", data["result"]["nodes"])
            self.assertIn("node2", data["result"]["nodes"])

    # ==========================================
    # DAG Dependencies Endpoint Tests
    # ==========================================

    def _create_test_dag_dependency(self, **overrides):
        from recovery_libs.storage.dataclass import DagDependency, DagDependencyActionStatus

        record_id = overrides.pop("id", 1)
        defaults = {
            "dependent_job_id": "12345",
            "dependent_job_action": "release",
            "dependency_job_identifier": "preprocess_job",
            "dependency_job_identifier_type": "name",
            "dependency_trigger_condition": "completed",
            "action_status": DagDependencyActionStatus.NOT_ELIGIBLE.value,
            "action_time": None,
        }
        defaults.update(overrides)
        dep = DagDependency(**defaults)
        dep.id = record_id
        return dep

    def test_get_dag_dependencies_200_no_filters(self):
        """Test basic retrieval with no filters returns all records."""
        records = [
            self._create_test_dag_dependency(id=1, dependent_job_id="100"),
            self._create_test_dag_dependency(id=2, dependent_job_id="200"),
        ]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 2)

            response = self._client.get("/dag_dependencies")

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["item_count"], 2)
            self.assertEqual(len(data["dag_dependencies"]), 2)
            self.assertEqual(data["dag_dependencies"][0]["dependent_job_id"], "100")
            self.assertEqual(data["dag_dependencies"][1]["dependent_job_id"], "200")
            self.assertIsNone(data["dag_dependencies"][0]["reason_code"])
            self.assertIsNone(data["dag_dependencies"][0]["reason_message"])
            self.assertIsNone(data["dag_dependencies"][0]["reason_details"])
            self.assertIsNone(data["dag_dependencies"][0]["last_evaluated_at"])

    def test_get_dag_dependencies_exposes_terminal_action_status(self):
        """The listing API preserves the absorbing aborted gate value."""
        records = [self._create_test_dag_dependency(action_status="aborted")]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 1)

            response = self._client.get("/dag_dependencies")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["dag_dependencies"][0]["action_status"], "aborted")

    def test_get_dag_dependencies_exposes_structured_evaluation_reason(self):
        evaluated_at = datetime(2026, 8, 18, tzinfo=UTC)
        records = [
            self._create_test_dag_dependency(
                reason_code="WAITING_FOR_PARENT_COMPLETION",
                reason_message="Waiting for parent job 100 to complete",
                reason_details={"parent_job_id": "100", "parent_state": "RUNNING"},
                last_evaluated_at=evaluated_at,
            )
        ]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 1)

            response = self._client.get("/dag_dependencies")

        self.assertEqual(response.status_code, 200)
        dependency = response.json()["dag_dependencies"][0]
        self.assertEqual(dependency["reason_code"], "WAITING_FOR_PARENT_COMPLETION")
        self.assertEqual(dependency["reason_message"], "Waiting for parent job 100 to complete")
        self.assertEqual(dependency["reason_details"], {"parent_job_id": "100", "parent_state": "RUNNING"})
        self.assertEqual(dependency["last_evaluated_at"], "2026-08-18T00:00:00Z")

    def test_get_dag_dependencies_200_filter_by_dependent_job_id(self):
        """Test filtering by dependent_job_id passes through to storage."""
        records = [self._create_test_dag_dependency(id=1, dependent_job_id="12345")]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 1)

            response = self._client.get("/dag_dependencies", params={"dependent_job_id": "12345"})

            self.assertEqual(response.status_code, 200)
            call_kwargs = job_manager_mock.storage.read_dag_dependencies.call_args.kwargs
            self.assertEqual(call_kwargs["dependent_job_id"], "12345")

    def test_get_dag_dependencies_200_filter_by_parent_identifier(self):
        """Test filtering by dependency_job_identifier and type."""
        records = [self._create_test_dag_dependency(id=1)]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 1)

            response = self._client.get(
                "/dag_dependencies",
                params={
                    "dependency_job_identifier": "preprocess_job",
                    "dependency_job_identifier_type": "name",
                },
            )

            self.assertEqual(response.status_code, 200)
            call_kwargs = job_manager_mock.storage.read_dag_dependencies.call_args.kwargs
            self.assertEqual(call_kwargs["dependency_job_identifier"], "preprocess_job")
            self.assertEqual(call_kwargs["dependency_job_identifier_type"], "name")

    def test_get_dag_dependencies_200_filter_by_action_status(self):
        """Test filtering by action_status list."""
        records = [self._create_test_dag_dependency(id=1, action_status="in_progress")]
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = (records, 1)

            response = self._client.get(
                "/dag_dependencies",
                params={"action_status": ["in_progress", "failed"]},
            )

            self.assertEqual(response.status_code, 200)
            call_kwargs = job_manager_mock.storage.read_dag_dependencies.call_args.kwargs
            self.assertEqual(call_kwargs["action_statuses"], ["in_progress", "failed"])

    def test_get_dag_dependencies_200_pagination(self):
        """Test pagination parameters are passed through to storage."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = ([], 10)

            response = self._client.get("/dag_dependencies", params={"limit": 5, "offset": 10})

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["item_count"], 10)
            self.assertEqual(len(data["dag_dependencies"]), 0)
            call_kwargs = job_manager_mock.storage.read_dag_dependencies.call_args.kwargs
            self.assertEqual(call_kwargs["limit"], 5)
            self.assertEqual(call_kwargs["offset"], 10)

    def test_get_dag_dependencies_200_sort_by(self):
        """Test sort_by parameter is parsed and passed through to storage."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.return_value = ([], 0)

            response = self._client.get(
                "/dag_dependencies",
                params={"sort_by": "id:asc,dependent_job_id:desc"},
            )

            self.assertEqual(response.status_code, 200)
            call_kwargs = job_manager_mock.storage.read_dag_dependencies.call_args.kwargs
            self.assertEqual(call_kwargs["sort_by"], [("id", "asc"), ("dependent_job_id", "desc")])

    def test_get_dag_dependencies_400_invalid_sort_by(self):
        """Test invalid sort_by returns 400."""
        response = self._client.get("/dag_dependencies", params={"sort_by": "id:invalid"})
        self.assertEqual(response.status_code, 400)

    def test_get_dag_dependencies_400_validation_error(self):
        """Test storage validation error returns 400."""
        from recovery_libs.storage.base import RecoveryStorageInputValidationError

        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.side_effect = RecoveryStorageInputValidationError(
                "dependency_job_identifier_type is required"
            )

            response = self._client.get(
                "/dag_dependencies",
                params={"dependency_job_identifier": "some_job"},
            )

            self.assertEqual(response.status_code, 400)

    def test_get_dag_dependencies_500_storage_error(self):
        """Test storage exception returns 500."""
        with patch("job_manager.main.job_manager") as job_manager_mock:
            job_manager_mock.storage.read_dag_dependencies.side_effect = RecoveryStorageException("DB error")

            response = self._client.get("/dag_dependencies")

            self.assertEqual(response.status_code, 500)
