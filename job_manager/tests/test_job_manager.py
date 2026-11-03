# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import time
import unittest
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import MagicMock, call, patch

import pika
import uvicorn
from job_manager.advice_mapping import storage_to_http_advice
from job_manager.audit.events import (
    AdviceCreatedEvent,
    AdviceUpdatedEvent,
    ApplicationTerminationRequestedEvent,
    JobAttemptUpdatedEvent,
)
from job_manager.constants import DEFAULT_SYSTEM_MAX_REQUEUE_TIMES, WORKFLOW_CONTROLLER
from job_manager.main import app, start_http_server
from job_manager.metrics import LEADER_STATUS_GAUGE
from job_manager.notification_utils import invalid_job_config_messages
from job_manager.periodic_tasks import reconcile_timed_out_dag_actions
from job_manager.service import (
    MESSAGE_MAX_DELIVERY_TIMES,
    ApplicationIntentionReconciliation,
    JobManager,
    StorageMode,
    resolve_effective_final_state,
)
from opentelemetry.trace import SpanKind, StatusCode
from recovery_libs.audit import AuditEventLevel
from recovery_libs.notifications_client import (
    InvalidJobConfig,
    NotificationCreate,
    NotificationsApi,
    TemplateData,
)
from recovery_libs.schema.aps_spec.spec import (
    AnomalyDetectionConfig,
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
    DagActionEnum,
    DagDependencyStep,
    JobReferenceByName,
)
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    AdviceState,
    ApplicationIntentionDesiredState,
    ApplicationIntentionInput,
    Issue,
    JobAnomaly,
    JobAnomalyHandledBy,
    JobTerminalAdvice,
    SortOrderEnum,
)
from recovery_libs.schema.watcher.model import Attributes
from recovery_libs.schema.watcher.model import Event as EventWatcher
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.base import RecoveryStorageException, UpdateJobResult
from recovery_libs.storage.dataclass import (
    ApplicationIntentionRecord,
    DagDependency,
    DagDependencyActionStatus,
    DagDependencyReasonCode,
    JobConfigValidationFailureTypes,
    SlurmJobNonFinalStates,
)
from recovery_libs.storage.mysql_storage import AdviceIssueMerge, MySQLStorage
from test_utils import (
    create_basic_service_config,
    create_example_advice_record,
    create_example_anoamly_attribution_result,
    create_example_slurm_job_record,
    expected_job_audit_snapshot,
)


def _expected_advice_audit_snapshot(record):
    return storage_to_http_advice(record).model_dump(mode="json", exclude={"updated_at"})


def create_example_anomaly(
    anomaly_type: EventType = EventType.HANG,
    log_file_name: str | None = None,
    **extra_attrs,
) -> EventWatcher:
    return EventWatcher(
        type=anomaly_type,
        timestamp=str(time.time_ns()),
        attributes=Attributes(
            job_id="12345",
            uuid=str(uuid.uuid4()),
            log_file_name=log_file_name,
            **extra_attrs,
        ),
    )


class TestJobManager(unittest.TestCase):
    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.ApiClient")
    @patch("kubernetes.leaderelection.leaderelection.LeaderElection")
    @patch("job_manager.service.RabbitMQPublisher")
    @patch("job_manager.service.RabbitMQConsumer")
    @patch("job_manager.service.MySQLConnection")
    def setUp(
        self, mock_connection, mock_consumer, mock_publisher, mock_leader_election, mock_api_client, mock_load_config
    ):
        self._config = create_basic_service_config()
        self._job_manager = JobManager(config=self._config, shutdown_event=Event())
        self._job_manager.storage = MagicMock(spec=MySQLStorage)
        self._job_manager.storage.claim_application_intention_audit_attempt.return_value = (None, False)
        self._job_manager.attribution_storage = MagicMock(spec=AttributionStorage)
        self._job_manager.notifications_client = MagicMock(spec=NotificationsApi)

    def test_adi_publisher_initializes_without_legacy_exchange(self):
        """Direct attribution needs only its own event-bus exchange."""
        with patch("job_manager.service.RabbitMQPublisher") as publisher:
            result = self._job_manager.initialize_adi_attribution_request_publisher()
        publisher.assert_called_once()
        self.assertIs(result, publisher.return_value)
        self.assertEqual(
            publisher.call_args.kwargs["publisher_config"].exchange_name,
            "attribution_request_exchange",
        )

    def test_attribution_consumer_uses_existing_delayed_retry_route(self):
        """Attribution retries use the same bounded delayed route as anomaly delivery."""
        event_bus = self._config["service"]["event_bus"]
        event_bus["anomaly_queue_delivery_retry_delay_secs"] = 17

        with patch("job_manager.service.RabbitMQConsumer") as consumer:
            self._job_manager.initialize_event_bus_consumers()

        attribution_config = consumer.call_args_list[2].kwargs["consumer_config"]
        self.assertEqual(attribution_config.delay_secs, 17)
        self.assertEqual(attribution_config.dead_letter_routing_key, event_bus["anomaly_dead_letter_routing_key"])
        self.assertEqual(
            attribution_config.dead_letter_redelivery_routing_key,
            event_bus["anomaly_dead_letter_redelivery_routing_key"],
        )

    def _configure_pending_application_cancellation(self, job_id: str, attempt_index: int) -> None:
        """Make the storage mock expose a durable request accepted before observation."""
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id=job_id,
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="application finished",
            audit_attempt_index=attempt_index,
            created_at=datetime.now(UTC),
        )
        self._job_manager.storage.read_advice_by_job.return_value = None

    def test_create_application_intention_accepts_request_before_job_registration(self):
        created_at = datetime.now(UTC)
        stored = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="training converged",
            created_at=created_at,
        )
        stored.id = 7
        self._job_manager.storage.create_or_get_application_intention.return_value = (stored, True)
        self._job_manager.storage.read_job_record.return_value = None
        self._job_manager.audit_buffer = MagicMock()

        result = self._job_manager.create_application_intention(
            ApplicationIntentionInput(
                job_id="job-42",
                desired_state=ApplicationIntentionDesiredState.CANCELLED,
                reason="training converged",
            )
        )

        self.assertEqual(result.id, 7)
        self.assertEqual(result.job_id, "job-42")
        self.assertEqual(result.reason, "training converged")
        self._job_manager.storage.read_job_record.assert_not_called()
        self._job_manager.audit_buffer.add.assert_not_called()

    def test_claimed_application_intention_audit_uses_latest_attempt(self):
        created_at = datetime.now(UTC)
        stored = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="training converged",
            audit_attempt_index=3,
            created_at=created_at,
        )
        stored.id = 7
        self._job_manager.storage.claim_application_intention_audit_attempt.return_value = (stored, True)
        self._job_manager.audit_buffer = MagicMock()
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")

        self._job_manager.audit_application_intention_for_attempt(record)

        self._job_manager.storage.claim_application_intention_audit_attempt.assert_called_once_with("job-42", 3)
        self._job_manager.audit_buffer.add.assert_called_once()
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, ApplicationTerminationRequestedEvent)
        self.assertEqual(audit_event.payload.application_intention_id, 7)
        self.assertEqual(audit_event.payload.job_id, "job-42")
        self.assertEqual(audit_event.payload.attempt_index, 3)
        self.assertEqual(audit_event.payload.desired_state, "CANCELLED")
        self.assertEqual(audit_event.payload.reason, "training converged")
        self.assertEqual(audit_event.payload.requested_at, created_at)

    def test_already_claimed_application_intention_does_not_repeat_request_audit(self):
        stored = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="training converged",
            audit_attempt_index=2,
            created_at=datetime.now(UTC),
        )
        stored.id = 7
        self._job_manager.storage.claim_application_intention_audit_attempt.return_value = (stored, False)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.audit_application_intention_for_attempt(
            create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
        )

        self._job_manager.audit_buffer.add.assert_not_called()

    def test_request_audit_failure_does_not_reject_durable_application_intention(self):
        stored = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason=None,
            audit_attempt_index=3,
            created_at=datetime.now(UTC),
        )
        stored.id = 7
        self._job_manager.storage.claim_application_intention_audit_attempt.return_value = (stored, True)
        self._job_manager.audit_buffer = MagicMock()
        self._job_manager.audit_buffer.add.side_effect = RuntimeError("audit buffer unavailable")

        with patch("job_manager.audit.application_intentions.logger.exception") as log_exception:
            self._job_manager.audit_application_intention_for_attempt(
                create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
            )

        log_exception.assert_called_once()

    def test_reconcile_latest_application_intention_runs_outside_acceptance_path(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
        self._job_manager.storage.read_job_record.return_value = record

        with (
            patch.object(self._job_manager, "audit_application_intention_for_attempt") as audit_request,
            patch.object(self._job_manager, "reconcile_application_intention") as reconcile,
        ):
            self._job_manager.reconcile_latest_application_intention("job-42")

        self._job_manager.storage.read_job_record.assert_called_once_with(job_id="job-42")
        audit_request.assert_called_once_with(record)
        reconcile.assert_called_once_with(record)

    def test_reconcile_latest_application_intention_tolerates_background_failure(self):
        self._job_manager.storage.read_job_record.side_effect = RecoveryStorageException("database unavailable")

        with patch("job_manager.service.logger.exception") as log_exception:
            self._job_manager.reconcile_latest_application_intention("job-42")

        log_exception.assert_called_once()

    def test_reconcile_application_intention_terminates_observed_attempt(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="training converged",
            created_at=datetime.now(UTC),
        )
        self._job_manager.storage.read_advice_by_job.return_value = None

        with patch("job_manager.service.create_terminal_advice_for_job") as persist_terminal:
            persist_terminal.return_value = JobTerminalAdvice.APPLICATION_REQUESTED_CANCELLATION
            result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.TERMINATED)
        persist_terminal.assert_called_once()
        call_kwargs = persist_terminal.call_args.kwargs
        self.assertEqual(call_kwargs["job_id"], "job-42")
        self.assertEqual(call_kwargs["job_attempt_index"], 3)
        self.assertEqual(call_kwargs["terminal_advice"].value, "application_requested_cancellation")
        self.assertEqual(call_kwargs["audit_reason"], "training converged")

    def test_reconcile_application_intention_is_noop_without_request(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=0, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = None

        with patch("job_manager.service.create_terminal_advice_for_job") as persist_terminal:
            result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.NOT_REQUESTED)
        persist_terminal.assert_not_called()

    def test_reconcile_application_intention_respects_advice_management_disabled(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=0, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = False

        result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.NOT_REQUESTED)
        self._job_manager.storage.read_application_intention.assert_not_called()

    def test_reconcile_application_intention_does_not_rewrite_terminated_advice(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            created_at=datetime.now(UTC),
        )
        self._job_manager.storage.read_advice_by_job.return_value = create_example_advice_record(
            job_id="job-42",
            attempt_index=3,
            advice_state=AdviceState.TERMINATED.value,
        )

        with patch("job_manager.service.create_terminal_advice_for_job") as persist_terminal:
            result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.TERMINATED)
        persist_terminal.assert_not_called()

    def test_reconcile_application_intention_reports_failed_advice_write_for_retry(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=3, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            created_at=datetime.now(UTC),
        )
        self._job_manager.storage.read_advice_by_job.return_value = None

        with patch("job_manager.service.create_terminal_advice_for_job", return_value=None):
            result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.RETRY_REQUIRED)
        self.assertTrue(result.blocks_recovery)

    def test_reconcile_application_intention_tolerates_missing_table(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=0, state="RUNNING")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.side_effect = RecoveryStorageException(
            "application_intentions does not exist"
        )

        result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.NOT_REQUESTED)

    def test_reconcile_application_intention_blocks_late_recovery_when_lookup_fails(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=0, state="FAILED")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.side_effect = RecoveryStorageException(
            "temporary application intention lookup failure"
        )

        result = self._job_manager.reconcile_application_intention(
            record,
            fail_closed_on_lookup_error=True,
        )

        self.assertEqual(result, ApplicationIntentionReconciliation.RETRY_REQUIRED)
        self.assertTrue(result.blocks_recovery)

    def test_reconcile_application_intention_ignores_unsupported_stored_state(self):
        record = create_example_slurm_job_record(job_id="job-42", attempt_index=0, state="FAILED")
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id="job-42",
            desired_state="UNSUPPORTED",
            created_at=datetime.now(UTC),
        )

        result = self._job_manager.reconcile_application_intention(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.NOT_REQUESTED)
        self.assertFalse(result.blocks_recovery)

    def test_legacy_relay_flag_preserves_direct_attribution_only(self):
        """Old overrides cannot restore the retired watcher-message route."""
        self._config["service"]["job_anomaly_message_relay_enabled"] = True
        self.test_process_job_anomalies_with_file_exact_match()
        publisher = self._job_manager.adi_attribution_request_publisher
        publisher.publish_message.assert_called_once()
        self.assertEqual(
            publisher.publish_message.call_args.kwargs["routing_key"],
            "job.updates.attribution_request",
        )

    def test_upsert_advice_record_emits_created_event_for_released_advice(self):
        advice_record = create_example_advice_record(
            job_id="released-job",
            attempt_index=0,
            advice_state=AdviceState.RELEASED.value,
            visible=True,
        )
        advice_record.issues = None
        self._job_manager.storage.upsert_advice.return_value = (advice_record, True, None)
        self._job_manager.audit_buffer = MagicMock()

        result = self._job_manager.upsert_advice_record(advice_record)

        self.assertEqual(result, (advice_record, True, None))
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceCreatedEvent)
        self.assertEqual(audit_event.event_producer, "job-manager")
        self.assertIs(audit_event.event_level, AuditEventLevel.RECOVERY)
        self.assertEqual(audit_event.heimdall_job_id, str(advice_record.heimdall_id))
        self.assertEqual(audit_event.payload.advice_id, str(advice_record.heimdall_id))
        self.assertEqual(audit_event.payload.job_id, "released-job")
        self.assertEqual(audit_event.payload.attempt_index, 0)
        self.assertEqual(audit_event.payload.advice, _expected_advice_audit_snapshot(advice_record))
        self.assertEqual(audit_event.payload.advice["advice"]["state"], AdviceState.RELEASED.value)
        self.assertTrue(audit_event.payload.advice["visible"])
        self.assertNotIn("updated_at", audit_event.payload.advice)
        self.assertEqual(audit_event.payload.creation_reasons, [AdviceState.RELEASED.value])
        self.assertEqual(audit_event.payload.correlation_uuid, str(advice_record.heimdall_id))
        self._job_manager.storage.read_job_record.assert_called_once_with(
            job_id=advice_record.job_id, job_attempt_index=advice_record.attempt_index
        )

    def test_upsert_advice_record_uses_issues_as_creation_reasons(self):
        advice_record = create_example_advice_record()
        self._job_manager.storage.upsert_advice.return_value = (advice_record, True, None)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(advice_record)

        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertEqual(audit_event.payload.creation_reasons, [EventType.CRASH.value])

    def test_upsert_advice_record_correlates_created_event_with_recovery_sequence(self):
        recovery_sequence_id = uuid.uuid4()
        advice_record = create_example_advice_record()
        advice_record.recovery_sequence_id = recovery_sequence_id
        advice_record.recovery_operation = "restart"
        self._job_manager.storage.upsert_advice.return_value = (advice_record, True, None)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(advice_record)

        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertEqual(audit_event.payload.correlation_uuid, str(recovery_sequence_id))

    def test_upsert_advice_record_tolerates_audit_validation_error(self):
        advice_record = create_example_advice_record()
        advice_record.generation = 0
        persistence_result = (advice_record, True, None)
        self._job_manager.storage.upsert_advice.return_value = persistence_result
        self._job_manager.audit_buffer = MagicMock()

        with patch("job_manager.audit.advice_changes.logger.exception") as log_exception:
            result = self._job_manager.upsert_advice_record(advice_record)

        self.assertIs(result, persistence_result)
        self._job_manager.audit_buffer.add.assert_not_called()
        log_exception.assert_called_once()

    def test_upsert_advice_record_emits_updated_event(self):
        old_record = create_example_advice_record(advice_state=AdviceState.RESTARTED.value)
        updated_record = deepcopy(old_record)
        updated_record.advice_state = AdviceState.TERMINATED.value
        updated_record.issues = [
            *(updated_record.issues or []),
            Issue(
                issue_type=EventType.HANG.value,
                timestamp=updated_record.updated_at,
                detected_by="Heimdall Watcher",
            ),
        ]
        updated_record.created_at += timedelta(seconds=1)
        updated_record.updated_at += timedelta(seconds=1)
        self._job_manager.storage.upsert_advice.return_value = (updated_record, False, old_record)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(updated_record)

        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceUpdatedEvent)
        self.assertEqual(audit_event.payload.old_advice, _expected_advice_audit_snapshot(old_record))
        self.assertEqual(audit_event.payload.new_advice, _expected_advice_audit_snapshot(updated_record))
        self.assertEqual(audit_event.payload.old_advice["advice"]["state"], AdviceState.RESTARTED.value)
        self.assertEqual(audit_event.payload.new_advice["advice"]["state"], AdviceState.TERMINATED.value)
        self.assertNotEqual(audit_event.payload.old_advice["created_at"], audit_event.payload.new_advice["created_at"])
        self.assertNotIn("updated_at", audit_event.payload.old_advice)
        self.assertNotIn("updated_at", audit_event.payload.new_advice)
        self.assertNotIn("record_updated_at", audit_event.payload.model_dump())
        self.assertEqual(audit_event.payload.change_reasons, [EventType.HANG.value])
        self.assertEqual(audit_event.payload.correlation_uuid, str(updated_record.heimdall_id))
        self._job_manager.storage.read_job_record.assert_called_once_with(
            job_id=updated_record.job_id, job_attempt_index=updated_record.attempt_index
        )

    def test_upsert_advice_record_correlates_updated_event_with_recovery_sequence(self):
        recovery_sequence_id = uuid.uuid4()
        old_record = create_example_advice_record(advice_state=AdviceState.RESTARTED.value)
        old_record.recovery_sequence_id = uuid.uuid4()
        old_record.recovery_operation = "restart"
        updated_record = deepcopy(old_record)
        updated_record.advice_state = AdviceState.TERMINATED.value
        updated_record.recovery_sequence_id = recovery_sequence_id
        updated_record.recovery_operation = "terminate"
        self._job_manager.storage.upsert_advice.return_value = (updated_record, False, old_record)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(updated_record)

        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertEqual(audit_event.payload.correlation_uuid, str(recovery_sequence_id))

    def test_upsert_advice_record_appends_caller_known_change_reason(self):
        old_record = create_example_advice_record(advice_state=AdviceState.RESTARTED.value)
        updated_record = deepcopy(old_record)
        updated_record.advice_state = AdviceState.TERMINATED.value
        self._job_manager.storage.upsert_advice.return_value = (updated_record, False, old_record)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(
            updated_record,
            audit_reason="DAG_PARENT_TERMINATED",
        )

        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertEqual(audit_event.payload.change_reasons, ["DAG_PARENT_TERMINATED"])

    def test_upsert_advice_record_suppresses_checkpoint_only_update(self):
        """Checkpoint-only updates persist without emitting recovery audit noise."""
        for existing_issues in (None, [], create_example_advice_record().issues):
            with self.subTest(existing_issues=existing_issues):
                old_record = create_example_advice_record()
                old_record.issues = existing_issues
                updated_record = deepcopy(old_record)
                updated_record.issues = [
                    *(updated_record.issues or []),
                    Issue(issue_type=EventType.CHECKPOINTLOAD.value, timestamp=updated_record.updated_at),
                    Issue(issue_type=EventType.CHECKPOINTSAVE.value, timestamp=updated_record.updated_at),
                ]
                updated_record.updated_at += timedelta(seconds=1)
                persistence_result = (updated_record, False, old_record)
                self._job_manager.storage.upsert_advice.return_value = persistence_result
                self._job_manager.audit_buffer = MagicMock()

                result = self._job_manager.upsert_advice_record(updated_record)

                self.assertIs(result, persistence_result)
                self._job_manager.storage.upsert_advice.assert_called_with(
                    updated_record, issue_merge=AdviceIssueMerge.REPLACE
                )
                self._job_manager.audit_buffer.add.assert_not_called()

    def test_upsert_advice_record_audits_checkpoint_with_other_changes(self):
        """Checkpoint activity cannot hide a concurrent advice or anomaly change."""
        for change in ("state", "node_constraints", "anomaly"):
            with self.subTest(change=change):
                old_record = create_example_advice_record()
                updated_record = deepcopy(old_record)
                updated_record.issues.append(
                    Issue(issue_type=EventType.CHECKPOINTSAVE.value, timestamp=updated_record.updated_at)
                )
                if change == "state":
                    updated_record.advice_state = AdviceState.TERMINATED.value
                elif change == "node_constraints":
                    updated_record.node_exclusions = ["node-3"]
                else:
                    updated_record.issues.append(
                        Issue(issue_type=EventType.HANG.value, timestamp=updated_record.updated_at)
                    )
                self._job_manager.storage.upsert_advice.return_value = (updated_record, False, old_record)
                self._job_manager.audit_buffer = MagicMock()

                self._job_manager.upsert_advice_record(updated_record)

                self._job_manager.audit_buffer.add.assert_called_once()
                audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
                self.assertIsInstance(audit_event, AdviceUpdatedEvent)
                self.assertEqual(audit_event.payload.old_advice, _expected_advice_audit_snapshot(old_record))
                self.assertEqual(audit_event.payload.new_advice, _expected_advice_audit_snapshot(updated_record))

    def test_upsert_advice_record_suppresses_no_op_update(self):
        old_record = create_example_advice_record()
        updated_record = deepcopy(old_record)
        updated_record.updated_at += timedelta(seconds=1)
        self._job_manager.storage.upsert_advice.return_value = (updated_record, False, old_record)
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.upsert_advice_record(
            updated_record,
            audit_reason="DAG_DEPENDENCIES_SATISFIED",
        )

        self._job_manager.audit_buffer.add.assert_not_called()

    def test_upsert_job_record_tolerates_audit_validation_error(self):
        job_record = create_example_slurm_job_record()
        job_record.job_attempt_index = "invalid"  # type: ignore[assignment]
        persistence_result = (job_record, True, None)
        self._job_manager.storage.upsert_job_record.return_value = persistence_result
        self._job_manager.audit_buffer = MagicMock()

        with patch("job_manager.audit.job_changes.logger.exception") as log_exception:
            result = self._job_manager.upsert_job_record(job_record)

        self.assertIs(result, persistence_result)
        self._job_manager.audit_buffer.add.assert_not_called()
        log_exception.assert_called_once()

    def test_update_job_record_emits_audit_with_approved_snapshots(self):
        now = datetime.now(UTC)
        old_record = create_example_slurm_job_record(
            job_id="job-state-change",
            attempt_index=0,
            state="PENDING",
            last_updated_at=now - timedelta(seconds=30),
        )
        updated_record = deepcopy(old_record)
        updated_record.state = "RUNNING"
        updated_record.last_updated_at = now
        self._job_manager.storage.update_job_record.return_value = UpdateJobResult(
            updated_record=updated_record,
            old_record=old_record,
        )
        self._job_manager.audit_buffer = MagicMock()

        updated = self._job_manager.update_job_record(
            job_id=old_record.job_id,
            job_attempt_index=old_record.job_attempt_index,
            state=updated_record.state,
            last_updated_at=now,
        )

        self.assertTrue(updated)
        self._job_manager.storage.update_job_record.assert_called_once_with(
            job_id=old_record.job_id,
            job_attempt_index=old_record.job_attempt_index,
            state="RUNNING",
            last_updated_at=now,
        )
        self._job_manager.storage.read_job_record.assert_not_called()
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, JobAttemptUpdatedEvent)
        self.assertIs(audit_event.event_level, AuditEventLevel.OPERATIONAL)
        self.assertEqual(audit_event.heimdall_job_id, str(updated_record.heimdall_job_id))
        self.assertEqual(audit_event.payload.job_id, "job-state-change")
        self.assertEqual(audit_event.payload.attempt_index, 0)
        self.assertEqual(audit_event.payload.old_job, expected_job_audit_snapshot(old_record))
        self.assertEqual(audit_event.payload.new_job, expected_job_audit_snapshot(updated_record))

    def test_update_job_record_tolerates_audit_validation_error(self):
        old_record = create_example_slurm_job_record(state="PENDING")
        updated_record = deepcopy(old_record)
        updated_record.state = "RUNNING"
        updated_record.job_attempt_index = "invalid"  # type: ignore[assignment]
        self._job_manager.storage.update_job_record.return_value = UpdateJobResult(
            updated_record=updated_record,
            old_record=old_record,
        )
        self._job_manager.audit_buffer = MagicMock()

        with patch("job_manager.audit.job_changes.logger.exception") as log_exception:
            updated = self._job_manager.update_job_record(
                job_id=old_record.job_id,
                job_attempt_index=old_record.job_attempt_index,
                state="RUNNING",
            )

        self.assertTrue(updated)
        self._job_manager.audit_buffer.add.assert_not_called()
        log_exception.assert_called_once()

    def test_update_job_record_suppresses_internal_field_change(self):
        now = datetime.now(UTC)
        old_record = create_example_slurm_job_record(
            job_id="job-private-change",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        updated_record = deepcopy(old_record)
        updated_record.attribution_request_emitted = True
        updated_record.last_updated_at = now
        self._job_manager.storage.update_job_record.return_value = UpdateJobResult(
            updated_record=updated_record,
            old_record=old_record,
        )
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.update_job_record(
            job_id=old_record.job_id,
            job_attempt_index=old_record.job_attempt_index,
            attribution_request_emitted=True,
            last_updated_at=now,
        )

        self._job_manager.storage.read_job_record.assert_not_called()
        self._job_manager.audit_buffer.add.assert_not_called()

    def test_update_job_record_suppresses_unchanged_record(self):
        now = datetime.now(UTC)
        old_record = create_example_slurm_job_record(
            job_id="job-no-op",
            attempt_index=0,
            state="RUNNING",
            last_updated_at=now - timedelta(seconds=30),
        )
        updated_record = deepcopy(old_record)
        updated_record.last_updated_at = now
        self._job_manager.storage.update_job_record.return_value = UpdateJobResult(
            updated_record=updated_record,
            old_record=old_record,
        )
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager.update_job_record(
            job_id=old_record.job_id,
            job_attempt_index=old_record.job_attempt_index,
            state="RUNNING",
        )

        self._job_manager.audit_buffer.add.assert_not_called()

    def test_start_http_server(self):
        with patch.object(uvicorn, attribute="run") as http_server_mock:
            start_http_server(self._config["service"])
            http_server_mock.assert_called_once()
            self.assertEqual(http_server_mock.call_args.args[0], app)
            self.assertEqual(http_server_mock.call_args.kwargs["host"], self._config["service"]["http"]["host"])
            self.assertEqual(http_server_mock.call_args.kwargs["port"], self._config["service"]["http"]["port"])

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.ApiClient")
    @patch("kubernetes.leaderelection.leaderelection.LeaderElection")
    def test_mysql_storage_initilization_with_env_var(self, mock_leader_election, mock_api_client, mock_load_config):
        with (
            patch("job_manager.service.MySQLConnection") as storage_connection_mock,
            patch("job_manager.service.RabbitMQConsumer"),
            patch("job_manager.service.RabbitMQPublisher"),
            patch.dict(os.environ, {"MYSQL_PASSWORD": "mysql_aws_password"}),
        ):
            self._config["service"]["storage"]["mode"] = StorageMode.MYSQL
            self._config["service"]["storage"]["sql"] = {
                "host": "localhost",
                "port": 3306,
                "username": "aps_test",
                "password": "aps_test",
                "database_name": "aps_workflow_db",
            }

            JobManager(self._config, shutdown_event=Event())
            storage_connection_mock.assert_called_once()
            # Verify the password used for MySQL connection is fetched from the environment variable.
            self.assertEqual(storage_connection_mock.call_args.kwargs["password"], "mysql_aws_password")

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.ApiClient")
    @patch("kubernetes.leaderelection.leaderelection.LeaderElection")
    def test_mysql_storage_initilization_without_env_var(self, mock_leader_election, mock_api_client, mock_load_config):
        with (
            patch("job_manager.service.MySQLConnection") as storage_connection_mock,
            patch("job_manager.service.RabbitMQConsumer"),
            patch("job_manager.service.RabbitMQPublisher"),
        ):
            self._config["service"]["storage"]["mode"] = StorageMode.MYSQL
            self._config["service"]["storage"]["sql"] = {
                "host": "localhost",
                "port": 3306,
                "username": "aps_test",
                "password": "aps_test",
                "database_name": "aps_workflow_db",
            }

            JobManager(self._config, shutdown_event=Event())
            storage_connection_mock.assert_called_once()
            # Verify the password used for MySQL connection is read from the config file.
            self.assertEqual(storage_connection_mock.call_args.kwargs["password"], "aps_test")

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_process_job_updates_record_create(self, mock_sanitize_log_file_paths):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        record = create_example_slurm_job_record(submit_time=submit_time, last_updated_at=now, attempt_index=0)
        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER") as job_update_counter_mock:
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

            self._job_manager.storage.get_job_latest_attempt_index.assert_called_once_with(record.job_id)
            self._job_manager.storage.create_job_record.assert_called_once()
            call_args = self._job_manager.storage.create_job_record.call_args.kwargs
            self.assertEqual(call_args["job_record"].last_state_transition_time, record.last_updated_at)
            self.assertEqual(call_args["job_record"].namespace, record.namespace)
            job_update_counter_mock.inc.assert_called_once()
            mock_sanitize_log_file_paths.assert_called_once()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_rabbitmq_registration_reconciles_preexisting_application_cancellation(
        self,
        mock_sanitize_log_file_paths,
    ):
        """A request accepted before registration terminates the first RabbitMQ-observed attempt."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="RUNNING",
            last_updated_at=now,
            attempt_index=0,
        )
        record.aps_spec.dag_dependencies = [DagDependencyStep(target=JobReferenceByName(name="parent"))]
        self._config["service"]["dag_support"]["enabled"] = True
        self._configure_pending_application_cancellation(record.job_id, record.job_attempt_index)
        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1
        persisted_advice = create_example_advice_record(
            job_id=record.job_id,
            attempt_index=record.job_attempt_index,
            advice_state=AdviceState.TERMINATED.value,
        )
        reconciliation_order: list[str] = []

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"),
            patch.object(self._job_manager, "reconcile_anomaly_advice"),
            patch.object(
                self._job_manager,
                "_register_dag_dependencies",
                side_effect=lambda *_args, **_kwargs: reconciliation_order.append("dag_registered"),
            ),
            patch.object(self._job_manager, "refresh_dag_dependency_observability_for_observed_job"),
            patch("job_manager.service.emit_attribution_request_if_needed") as emit_attribution,
            patch(
                "job_manager.service.create_terminal_advice_for_job",
                side_effect=lambda *_args, **_kwargs: (
                    reconciliation_order.append("termination_persisted") or persisted_advice
                ),
            ) as persist_terminal,
        ):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        self.assertEqual(reconciliation_order, ["dag_registered", "termination_persisted"])
        persist_terminal.assert_called_once()
        self.assertEqual(persist_terminal.call_args.kwargs["job_id"], record.job_id)
        self.assertEqual(persist_terminal.call_args.kwargs["job_attempt_index"], record.job_attempt_index)
        emit_attribution.assert_not_called()
        mock_sanitize_log_file_paths.assert_called_once()

    @patch("job_manager.service.populate_sticky_exclusion_node_list", MagicMock(return_value=None))
    def test_process_job_updates_record_update(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        record = create_example_slurm_job_record(
            submit_time=submit_time, state="RUNNING", last_updated_at=now, attempt_index=0
        )
        record.config_validation_failure_types = [JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND]
        known_record = create_example_slurm_job_record(
            submit_time=submit_time, last_updated_at=now - timedelta(minutes=3), attempt_index=0
        )
        known_record.last_state_transition_time = known_record.last_updated_at

        # Return value of get_job_latest_attempt_index is the same as the actual attmept index
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER") as job_update_counter_mock,
            patch("job_manager.service.JOB_WITH_INVALID_CONFIG_COUNTER") as invalid_job_counter_mock,
        ):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

            self._job_manager.storage.get_job_latest_attempt_index.assert_called_once_with(record.job_id)
            self._job_manager.storage.update_job_record.assert_called_once()
            self._job_manager.notifications_client.notification_create.assert_called_once_with(
                NotificationCreate(
                    template_data=TemplateData(
                        InvalidJobConfig(
                            job_id=record.job_id,
                            job_attempt_index=record.job_attempt_index,
                            job_name=record.job_name,
                            reason=f"{JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND.name}: {invalid_job_config_messages[JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND]}",
                            user=record.user,
                        )
                    ),
                    recipient=record.user,
                    job_id=record.job_id,
                )
            )

            job_update_counter_mock.inc.assert_called_once()
            invalid_job_counter_mock.labels(failure_type="LOG_FILE_PATH_NOT_FOUND").inc.assert_called_once()

            call_args = self._job_manager.storage.update_job_record.call_args.kwargs
            self.assertEqual(call_args["last_state_transition_time"], record.last_updated_at)
            self.assertEqual(call_args["state_reason"], "None")

    @patch("job_manager.service.populate_sticky_exclusion_node_list", MagicMock(return_value=None))
    def test_rabbitmq_update_reconciles_preexisting_application_cancellation(self):
        """An existing attempt update retries a durable request and suppresses attribution."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="RUNNING",
            last_updated_at=now,
            attempt_index=0,
        )
        known_record = create_example_slurm_job_record(
            submit_time=record.submit_time,
            state="PENDING",
            last_updated_at=now - timedelta(minutes=1),
            attempt_index=0,
        )
        known_record.sticky_exclusion_node_list = []
        self._configure_pending_application_cancellation(record.job_id, record.job_attempt_index)
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record
        persisted_advice = create_example_advice_record(
            job_id=record.job_id,
            attempt_index=record.job_attempt_index,
            advice_state=AdviceState.TERMINATED.value,
        )

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"),
            patch.object(self._job_manager, "reconcile_anomaly_advice"),
            patch("job_manager.service.emit_attribution_request_if_needed") as emit_attribution,
            patch(
                "job_manager.service.create_terminal_advice_for_job", return_value=persisted_advice
            ) as persist_terminal,
        ):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        persist_terminal.assert_called_once()
        emit_attribution.assert_not_called()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_process_job_updates_record_not_found_creates_new(self, mock_sanitize_log_file_paths):
        """When update path finds no existing record (gap in attempt indices), it should create a new record."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10), state="REQUEUED", last_updated_at=now, attempt_index=1
        )

        # Latest attempt is 2 (higher), so attempt 1 routes to the update path, but read returns None
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 2
        self._job_manager.storage.read_job_record.return_value = None

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER") as job_update_counter_mock:
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

            self._job_manager.storage.read_job_record.assert_called_once_with(record.job_id, record.job_attempt_index)
            self._job_manager.storage.create_job_record.assert_called_once()
            self._job_manager.storage.update_job_record.assert_not_called()
            mock_sanitize_log_file_paths.assert_called_once()
            job_update_counter_mock.inc.assert_called_once()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_rabbitmq_missing_attempt_reconciles_preexisting_application_cancellation(
        self,
        mock_sanitize_log_file_paths,
    ):
        """The recovery branch for a missing attempt also applies the durable request."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="REQUEUED",
            last_updated_at=now,
            attempt_index=1,
        )
        self._configure_pending_application_cancellation(record.job_id, record.job_attempt_index)
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 2
        self._job_manager.storage.read_job_record.return_value = None
        persisted_advice = create_example_advice_record(
            job_id=record.job_id,
            attempt_index=record.job_attempt_index,
            advice_state=AdviceState.TERMINATED.value,
        )

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"),
            patch(
                "job_manager.service.create_terminal_advice_for_job", return_value=persisted_advice
            ) as persist_terminal,
        ):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        persist_terminal.assert_called_once()
        mock_sanitize_log_file_paths.assert_called_once()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_process_job_updates_with_final_state(self, mock_sanitize_log_file_paths):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        end_time = now - timedelta(minutes=1)
        record = create_example_slurm_job_record(
            submit_time=submit_time,
            end_time=end_time,
            state="COMPLETED",
            last_updated_at=now,
            attempt_index=0,
            log_file_paths=["/var/log/heimdall/prod/job_12345.log"],
        )
        known_record = create_example_slurm_job_record(
            submit_time=submit_time, state="RUNNING", last_updated_at=now - timedelta(minutes=3), attempt_index=0
        )
        known_record.sticky_exclusion_node_list = []

        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record
        self._job_manager.storage.update_job_record.return_value = UpdateJobResult(
            updated_record=record,
            old_record=known_record,
        )
        self._job_manager.audit_buffer = MagicMock()

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER") as job_update_counter_mock,
            patch("job_manager.service.END_ANOMALY_GENERATED_COUNTER") as end_anomaly_generated_counter_mock,
            patch("job_manager.service.tracer") as mock_tracer,
        ):
            mock_detection_span = MagicMock()
            mock_tracer.start_span.return_value = mock_detection_span
            mock_persistence_span = MagicMock()
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_persistence_span

            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

            self._job_manager.storage.get_job_latest_attempt_index.assert_called_once_with(record.job_id)
            self._job_manager.storage.update_job_record.assert_called_once()

            mock_sanitize_log_file_paths.assert_called_once()

            job_update_counter_mock.inc.assert_called_once()
            end_anomaly_generated_counter_mock.inc.assert_called_once()

            job_update_call_args = self._job_manager.storage.update_job_record.call_args.kwargs
            self.assertEqual(len(job_update_call_args["anomalies"]), 1)
            self.assertEqual(job_update_call_args["anomalies"][0].anomaly_type, EventType.END)

            mock_tracer.start_span.assert_called_once_with(
                name="job_anomaly.detection",
                kind=SpanKind.PRODUCER,
                attributes={
                    "job_id": record.job_id,
                    "job_attempt_index": record.job_attempt_index,
                    "job_anomaly.type": EventType.END,
                    "job_anomaly.uuid": job_update_call_args["anomalies"][0].uuid,
                },
                start_time=end_time.timestamp() * 1_000_000_000,
            )
            mock_detection_span.end.assert_called_once()

            mock_tracer.start_as_current_span.assert_called_once_with(
                name="job_anomaly.persistence",
                kind=SpanKind.INTERNAL,
                attributes={
                    "job_id": record.job_id,
                    "job_attempt_index": record.job_attempt_index,
                    "job_anomaly.type": EventType.END,
                    "job_anomaly.uuid": job_update_call_args["anomalies"][0].uuid,
                },
            )
            mock_persistence_span.set_status.assert_called_once_with(StatusCode.OK)
            audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
            self.assertIsInstance(audit_event, JobAttemptUpdatedEvent)
            self.assertEqual(audit_event.payload.old_job["state"], "RUNNING")
            self.assertEqual(audit_event.payload.new_job["state"], "COMPLETED")

    def test_job_end_event_not_generated_for_non_completed_state(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10), state="CANCELLED", last_updated_at=now, attempt_index=0
        )

        self.assertFalse(self._job_manager.should_send_end_event(record, record.state))

    def test_generated_bus_advice_follows_successful_job_persistence(self):
        """Both bus write paths commit CRASH/END causes before Advice and attribution."""
        self._config["service"]["advice_enabled"] = True
        detection_time = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)
        for new_attempt, state, storage_fails in (
            (True, "FAILED", False),
            (True, "FAILED", True),
            (False, "FAILED", False),
            (False, "FAILED", True),
            (True, "COMPLETED", False),
            (True, "COMPLETED", True),
            (False, "COMPLETED", False),
            (False, "COMPLETED", True),
        ):
            with self.subTest(new_attempt=new_attempt, state=state, storage_fails=storage_fails):
                storage = self._job_manager.storage = MagicMock(spec=MySQLStorage)
                self._job_manager.audit_buffer = MagicMock()
                record = create_example_slurm_job_record(
                    state=state,
                    submit_time=detection_time - timedelta(minutes=10),
                    end_time=detection_time - timedelta(seconds=5),
                    last_updated_at=detection_time,
                )
                record.aps_spec.anomaly_detection_config.log_based_crash_enabled = False
                known_record = deepcopy(record)
                known_record.state = "RUNNING"
                known_record.heimdall_job_id = uuid.uuid4()
                known_record.sticky_exclusion_node_list = []
                storage.get_job_latest_attempt_index.return_value = -1 if new_attempt else 0
                storage.read_job_record.return_value = known_record
                storage.read_advice.return_value = None
                lifecycle = []

                def persist_job(
                    *,
                    _lifecycle=lifecycle,
                    _storage_fails=storage_fails,
                    _new_attempt=new_attempt,
                    _known_record=known_record,
                    _record=record,
                    **kwargs,
                ):
                    _lifecycle.append("job")
                    if _storage_fails:
                        raise RecoveryStorageException("storage unavailable")
                    if _new_attempt:
                        kwargs["job_record"].heimdall_job_id = _known_record.heimdall_job_id
                        return True
                    return UpdateJobResult(updated_record=_record, old_record=_known_record)

                storage.create_job_record.side_effect = persist_job
                storage.update_job_record.side_effect = persist_job

                def persist_advice(advice, _lifecycle=lifecycle, **_):
                    _lifecycle.append("advice")
                    return advice, True, None

                storage.upsert_advice.side_effect = persist_advice
                with (
                    patch(
                        "job_manager.service.time_module.time_ns", return_value=int(detection_time.timestamp()) * 10**9
                    ),
                    patch.object(
                        self._job_manager,
                        "_maybe_emit_attribution_request",
                        side_effect=lambda _record, _lifecycle=lifecycle: _lifecycle.append("attribution"),
                    ),
                ):
                    self._job_manager.get_job_update_message_callback()(
                        MagicMock(), MagicMock(), pika.BasicProperties(), record.to_json().encode()
                    )

                if storage_fails:
                    self.assertEqual(lifecycle, ["job"])
                    storage.upsert_advice.assert_not_called()
                    self._job_manager.audit_buffer.add.assert_not_called()
                    continue

                self.assertEqual(lifecycle, ["job", "advice", "attribution"])
                advice = storage.upsert_advice.call_args.args[0]
                self.assertEqual(advice.heimdall_id, known_record.heimdall_job_id)
                self.assertEqual(advice.issues[0].timestamp, detection_time)
                self.assertEqual(advice.issues[0].detected_by, "Heimdall Job Manager")
                expected_type = "CRASH" if state == "FAILED" else "END"
                self.assertEqual(advice.issues[0].issue_type, expected_type)
                advice_events = [
                    call.args[0]
                    for call in self._job_manager.audit_buffer.add.call_args_list
                    if isinstance(call.args[0], AdviceCreatedEvent)
                ]
                self.assertEqual(len(advice_events), 1)
                self.assertEqual(advice_events[0].payload.creation_reasons, [expected_type])

    def test_bus_update_repairs_advice_from_committed_anomaly(self):
        """A repeated scheduler update repairs Advice even though it carries no anomaly itself."""
        self._config["service"]["advice_enabled"] = True
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        self._job_manager.audit_buffer = MagicMock()
        storage = self._job_manager.storage
        now = datetime.now(UTC)
        incoming = create_example_slurm_job_record(state="FAILED", submit_time=now - timedelta(minutes=10))
        incoming.aps_spec.anomaly_detection_config.log_based_crash_enabled = False
        durable_job = deepcopy(incoming)
        durable_job.state = "RUNNING"
        durable_job.heimdall_job_id = uuid.uuid4()
        durable_job.sticky_exclusion_node_list = []
        storage.get_job_latest_attempt_index.return_value = 0
        storage.read_job_record.side_effect = lambda *args, **kwargs: deepcopy(durable_job)
        storage.read_advice.return_value = None
        storage.claim_attribution_request_emission.side_effect = [True, False]

        def persist_job(**updates):
            nonlocal durable_job
            old_record = deepcopy(durable_job)
            durable_job = deepcopy(incoming)
            durable_job.heimdall_job_id = old_record.heimdall_job_id
            durable_job.sticky_exclusion_node_list = old_record.sticky_exclusion_node_list
            durable_job.anomalies = deepcopy(updates["anomalies"] or old_record.anomalies)
            return UpdateJobResult(updated_record=deepcopy(durable_job), old_record=old_record)

        storage.update_job_record.side_effect = persist_job
        storage.upsert_advice.side_effect = RecoveryStorageException("Advice storage unavailable")
        callback = self._job_manager.get_job_update_message_callback()
        body = incoming.to_json().encode()
        # Keep this retry/persistence test independent of nanosecond-to-microsecond rounding.
        with patch("job_manager.service.time_module.time_ns", return_value=int(now.timestamp()) * 10**9):
            callback(MagicMock(), MagicMock(), pika.BasicProperties(), body)

        original = deepcopy(durable_job.anomalies[0])
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()
        self.assertFalse(
            any(
                isinstance(call.args[0], AdviceCreatedEvent)
                for call in self._job_manager.audit_buffer.add.call_args_list
            )
        )

        def persist_advice(advice, **_):
            storage.read_advice.return_value = deepcopy(advice)
            return advice, True, None

        storage.upsert_advice.side_effect = persist_advice
        callback(MagicMock(), MagicMock(), pika.BasicProperties(), body)
        callback(MagicMock(), MagicMock(), pika.BasicProperties(), body)

        self.assertEqual(durable_job.anomalies, [original])
        self.assertEqual(storage.upsert_advice.call_count, 2)
        issue = storage.read_advice.return_value.issues[0]
        self.assertEqual(issue.timestamp, original.timestamp)
        self.assertEqual(issue.detected_by, "Heimdall Job Manager")
        self.assertEqual(issue.relevant_log_lines, ["State-based CRASH event for failed job"])
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_called_once()
        advice_events = [
            call.args[0]
            for call in self._job_manager.audit_buffer.add.call_args_list
            if isinstance(call.args[0], AdviceCreatedEvent)
        ]
        self.assertEqual(len(advice_events), 1)
        self.assertEqual(advice_events[0].payload.creation_reasons, ["CRASH"])

    def test_job_end_event_generation_with_env_matching(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="COMPLETED",
            last_updated_at=now,
            attempt_index=0,
            environment="dev",
        )

        # Set the job manager environment name to match value specified at the job spec level.
        self._job_manager.config["service"]["environment"] = "dev"

        self.assertTrue(self._job_manager.should_send_end_event(record, record.state))

    def test_job_end_event_generation_with_env_mismatch(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="COMPLETED",
            last_updated_at=now,
            attempt_index=0,
            environment="dev",
        )

        # The environment name of job manager is prod, which doesn't match the value specified at the job spec level.
        self.assertFalse(self._job_manager.should_send_end_event(record, record.state))

    def test_job_end_event_generation_with_backward_compatibility(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            state="COMPLETED",
            last_updated_at=now,
            attempt_index=0,
        )

        self._job_manager.config["service"].pop("environment")

        # The environment name of job manager is not set, which means the job manager should send END event for all jobs.
        self.assertTrue(self._job_manager.should_send_end_event(record, record.state))

    def test_process_job_anomalies_with_file_exact_match(self):
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        anomaly = create_example_anomaly(log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log")
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=60)
        job_attempt_0 = create_example_slurm_job_record(
            submit_time=submit_time,
            last_updated_at=submit_time + timedelta(minutes=25),
            attempt_index=0,
            nvrx_enabled="disabled",
            state="FAILED",
            log_file_paths=["/var/log/heimdall/dev/8B_burn_test_1290962_date_25-12-15_time_11-00-00.log"],
        )
        job_attempt_1 = create_example_slurm_job_record(
            submit_time=submit_time + timedelta(minutes=50),
            last_updated_at=now,
            attempt_index=1,
            nvrx_enabled="disabled",
            state="RUNNING",
            log_file_paths=["/var/log/heimdall/dev/8B_burn_test_1290962_date_25-12-15_time_15-43-37.log"],
        )

        job_attempt_1.start_time = now - timedelta(minutes=9)
        storage_mock = self._job_manager.storage
        with (
            patch("job_manager.service.ANOMALY_MESSAGE_RECEIVED_COUNTER") as anomaly_counter_mock,
            patch("job_manager.service.tracer") as mock_tracer,
        ):
            storage_mock.read_job_records.return_value = ([job_attempt_1, job_attempt_0], 2)
            mock_span = MagicMock()
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

            trace_headers = {"traceparent": "00-b7f7408dda54b2bed9eccc6a27ecde77-26af15ee3d44222e-01"}

            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()
            process_job_anomalies(
                MagicMock(),
                MagicMock(),
                pika.spec.BasicProperties(headers=trace_headers),
                anomaly.model_dump_json().encode(),
            )
            storage_mock.read_job_records.assert_called_once_with(
                job_id=anomaly.attributes.job_id,
                sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
                limit=5,
                offset=0,
                join_job_events_table=False,
            )
            storage_mock.update_job_record.assert_called_once()
            persisted = storage_mock.update_job_record.call_args.kwargs["anomalies"]
            self.assertEqual(len(persisted), 1)
            self.assertEqual(persisted[0].uuid, anomaly.attributes.uuid)
            self.assertFalse(persisted[0].processed)
            request = json.loads(
                self._job_manager.adi_attribution_request_publisher.publish_message.call_args.kwargs["message"]
            )
            self.assertEqual(request["job_id"], job_attempt_1.job_id)
            self.assertEqual(request["job_attempt_index"], job_attempt_1.job_attempt_index)
            anomaly_counter_mock.inc.assert_called_once()

            mock_span.set_attribute.assert_has_calls(
                [
                    call("job_id", job_attempt_1.job_id),
                    call("job_attempt_index", job_attempt_1.job_attempt_index),
                    call("job_anomaly.type", anomaly.type),
                    call("job_anomaly.uuid", anomaly.attributes.uuid),
                    call("job_event.type", anomaly.type),
                    call("job_event.uuid", anomaly.attributes.uuid),
                ],
                any_order=True,
            )
            mock_span.set_status.assert_called_once_with(StatusCode.OK)

    def test_process_job_anomalies_skips_advice_when_disabled(self):
        anomaly = create_example_anomaly(log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log")
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=60)
        job_record = create_example_slurm_job_record(
            submit_time=submit_time,
            last_updated_at=now,
            attempt_index=1,
            nvrx_enabled="disabled",
            state="RUNNING",
            log_file_paths=["/var/log/heimdall/dev/8B_burn_test_1290962_date_25-12-15_time_15-43-37.log"],
        )

        job_record.start_time = now - timedelta(minutes=9)
        storage_mock = self._job_manager.storage
        with patch("job_manager.service.create_advice_issue_from_event") as create_advice_mock:
            storage_mock.read_job_records.return_value = ([job_record], 1)

            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()
            process_job_anomalies(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                anomaly.model_dump_json().encode(),
            )

            create_advice_mock.assert_not_called()

    def test_process_job_anomalies_persists_advice_when_enabled(self):
        self._job_manager.config["service"]["advice_enabled"] = True
        anomaly = create_example_anomaly(log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log")
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=60)
        job_record = create_example_slurm_job_record(
            submit_time=submit_time,
            last_updated_at=now,
            attempt_index=1,
            nvrx_enabled="disabled",
            state="RUNNING",
            log_file_paths=["/var/log/heimdall/dev/8B_burn_test_1290962_date_25-12-15_time_15-43-37.log"],
        )

        job_record.start_time = now - timedelta(minutes=9)
        storage_mock = self._job_manager.storage
        with patch("job_manager.service.create_advice_issue_from_event") as create_advice_mock:
            storage_mock.read_job_records.return_value = ([job_record], 1)

            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()
            process_job_anomalies(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                anomaly.model_dump_json().encode(),
            )

            create_advice_mock.assert_called_once_with(
                storage=self._job_manager.storage,
                audit_buffer=self._job_manager.audit_buffer,
                event=anomaly,
                job_record=job_record,
                timestamp_parser=unittest.mock.ANY,
                detected_by="Heimdall Watcher",
                on_advice_persisted=self._job_manager.propagate_terminated_advice_for_dag_dependents,
            )

    def test_persist_anomaly_invokes_advice_management_when_enabled(self):
        from recovery_libs.schema.job_manager.models import JobAnomaly

        self._job_manager.config["service"]["advice_management_enabled"] = True
        self._config["service"]["job_anomaly_message_relay_enabled"] = False
        now = datetime.now(UTC)
        job_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=1,
            state="RUNNING",
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        )
        job_anomaly = JobAnomaly(
            anomaly_type=EventType.CRASH,
            timestamp=now,
            uuid=str(uuid.uuid4()),
        )
        job_record.anomalies = [job_anomaly]
        self._job_manager.storage.read_application_intention.return_value = None

        with (
            patch("job_manager.service.maybe_persist_requeue_terminal_advice") as terminal_advice_mock,
        ):
            self._job_manager.persist_job_anomaly(
                job_record,
                job_anomaly,
            )
            terminal_advice_mock.assert_called_once()

    def test_attribution_emission_reconciles_application_cancellation_first(self):
        """A pending application cancellation prevents a restart sequence from starting."""
        record = create_example_slurm_job_record(state="FAILED")
        self._configure_pending_application_cancellation(record.job_id, record.job_attempt_index)
        persisted_advice = create_example_advice_record(
            job_id=record.job_id,
            attempt_index=record.job_attempt_index,
            advice_state=AdviceState.TERMINATED.value,
        )

        with (
            patch("job_manager.service.create_terminal_advice_for_job", return_value=persisted_advice),
            patch("job_manager.service.emit_attribution_request_if_needed") as emit_attribution,
        ):
            result = self._job_manager._maybe_emit_attribution_request(record)

        self.assertEqual(result, ApplicationIntentionReconciliation.TERMINATED)
        emit_attribution.assert_not_called()

    def test_persist_anomaly_retries_when_application_intention_lookup_fails(self):
        """Watcher redelivery remains available until the cancellation guard can be evaluated."""
        record = create_example_slurm_job_record(state="FAILED")
        anomaly = JobAnomaly(anomaly_type=EventType.CRASH, timestamp=datetime.now(UTC), uuid=str(uuid.uuid4()))
        record.anomalies = [anomaly]
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_application_intention.side_effect = RecoveryStorageException(
            "temporary application intention lookup failure"
        )

        with (
            patch("job_manager.service.maybe_persist_requeue_terminal_advice") as terminal_advice,
            self.assertRaisesRegex(RecoveryStorageException, "reconciliation requires retry"),
        ):
            self._job_manager.persist_job_anomaly(record, anomaly)

        terminal_advice.assert_not_called()
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_failed_anomaly_persistence_skips_advice_and_attribution(self):
        """No downstream recovery work starts before anomaly storage succeeds."""
        self._config["service"]["advice_management_enabled"] = True
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        record = create_example_slurm_job_record()
        anomaly = JobAnomaly(anomaly_type=EventType.CRASH, timestamp=datetime.now(UTC), uuid=str(uuid.uuid4()))
        record.anomalies = [anomaly]
        self._job_manager.storage.update_job_record.side_effect = RecoveryStorageException("storage unavailable")
        with (
            patch("job_manager.service.maybe_persist_requeue_terminal_advice") as advice,
            self.assertRaises(RecoveryStorageException),
        ):
            self._job_manager.persist_job_anomaly(record, anomaly)
        advice.assert_not_called()
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_process_watcher_message_redelivery(self):
        anomaly = create_example_anomaly(log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log")

        storage_mock = self._job_manager.storage
        with (
            patch("job_manager.service.rabbitmq") as rabbitmq_mock,
            patch("job_manager.service.ANOMALY_MESSAGE_REDELIVERY_COUNTER") as redelivery_counter_mock,
        ):
            storage_mock.read_job_records.return_value = ([], 0)
            storage_mock.read_job_record.return_value = None
            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()

            method_mock = MagicMock()
            method_mock.exchange.return_value = "test_exchange"
            method_mock.routing_key = "job.anomaly"

            properties_mock = MagicMock()
            # Simulate the case where the message was first seen and no redelivery has happened before.
            properties_mock.headers.return_value = {}
            process_job_anomalies(
                MagicMock(),
                method_mock,
                properties_mock,
                anomaly.model_dump_json().encode(),
            )

            storage_mock.read_job_records.assert_called_once_with(
                job_id=anomaly.attributes.job_id,
                sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
                limit=5,
                offset=0,
                join_job_events_table=False,
            )
            storage_mock.update_job_record.assert_not_called()
            rabbitmq_mock.publish_message.assert_called_once()
            redelivery_counter_mock.inc.assert_called_once()

    def test_delayed_watcher_registration_audits_original_issue_once(self):
        """Delayed delivery retains the Watcher cause and duplicate delivery adds no audit."""
        self._config["service"]["advice_enabled"] = True
        self._job_manager.audit_buffer = MagicMock()
        storage = self._job_manager.storage
        previous_attempt = create_example_slurm_job_record(attempt_index=1, state="FAILED")
        previous_attempt.heimdall_job_id = uuid.uuid4()
        storage.read_job_record.side_effect = lambda **kwargs: (
            None if "job_attempt_index" in kwargs else previous_attempt
        )
        storage.read_advice.return_value = None
        original_time = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)
        event = create_example_anomaly(anomaly_type=EventType.CRASH, slurm_job_attempt=2)
        event.timestamp = str(int(original_time.timestamp()) * 1_000_000_000)
        body = event.model_dump_json().encode()
        callback = self._job_manager.get_watcher_job_event_message_callback()
        channel, method = MagicMock(), MagicMock()

        with patch("job_manager.service.rabbitmq.publish_message") as publish:
            callback(channel, method, pika.BasicProperties(), body)
        storage.update_job_record.assert_not_called()
        storage.upsert_advice.assert_not_called()
        self._job_manager.audit_buffer.add.assert_not_called()
        self.assertIsNone(previous_attempt.anomalies)
        storage.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=2)
        publish.assert_called_once()
        replay = publish.call_args.kwargs
        self.assertEqual(replay["message_body"], body)
        self.assertEqual(replay["properties"].headers["message_delivery_times"], 1)

        record = create_example_slurm_job_record(attempt_index=2, state="FAILED")
        record.heimdall_job_id = uuid.uuid4()
        storage.read_job_record.side_effect = None
        storage.read_job_record.return_value = record
        storage.update_job_record.return_value = UpdateJobResult(
            updated_record=record,
            old_record=deepcopy(record),
        )

        def persist_advice(advice, **_):
            storage.update_job_record.assert_called()
            storage.read_advice.return_value = advice
            return advice, True, None

        storage.upsert_advice.side_effect = persist_advice

        def dispatch_after_causal_audit(_record):
            self._job_manager.audit_buffer.add.assert_called_once()

        with patch.object(self._job_manager, "_maybe_emit_attribution_request", dispatch_after_causal_audit):
            callback(channel, method, replay["properties"], replay["message_body"])
            callback(channel, method, replay["properties"], replay["message_body"])

        storage.upsert_advice.assert_called_once()
        advice = storage.upsert_advice.call_args.args[0]
        self.assertEqual(advice.heimdall_id, record.heimdall_job_id)
        self.assertEqual(advice.attempt_index, 2)
        self.assertEqual(len(advice.issues), 1)
        self.assertEqual(advice.issues[0].timestamp, original_time)
        self.assertEqual(advice.issues[0].detected_by, "Heimdall Watcher")
        self._job_manager.audit_buffer.add.assert_called_once()
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceCreatedEvent)
        self.assertEqual(audit_event.heimdall_job_id, str(record.heimdall_job_id))
        self.assertEqual(audit_event.payload.attempt_index, 2)
        self.assertEqual(audit_event.payload.creation_reasons, ["CRASH"])

    def test_watcher_storage_failure_does_not_publish_causal_advice(self):
        """A matched event must persist before its Issue or attribution is published."""
        self._config["service"]["advice_enabled"] = True
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        self._job_manager.audit_buffer = MagicMock()
        storage = self._job_manager.storage
        record = create_example_slurm_job_record(attempt_index=2, state="FAILED")
        record.heimdall_job_id = uuid.uuid4()
        storage.read_job_record.return_value = record
        storage.read_advice.return_value = None
        storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
        storage.update_job_record.side_effect = RecoveryStorageException("storage unavailable")
        event = create_example_anomaly(anomaly_type=EventType.CRASH, slurm_job_attempt=2)

        self._job_manager.get_watcher_job_event_message_callback()(
            MagicMock(), MagicMock(), pika.BasicProperties(), event.model_dump_json().encode()
        )

        storage.update_job_record.assert_called_once()
        storage.upsert_advice.assert_not_called()
        self._job_manager.audit_buffer.add.assert_not_called()
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_process_watcher_message_redeliver_attempt_exhausted(self):
        anomaly = create_example_anomaly(log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log")

        storage_mock = self._job_manager.storage
        with patch("job_manager.service.rabbitmq") as rabbitmq_mock:
            storage_mock.read_job_records.return_value = ([], 0)
            storage_mock.read_job_record.return_value = None
            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()

            method_mock = MagicMock()
            method_mock.exchange.return_value = "test_exchange"
            method_mock.routing_key = "job.anomaly"

            properties_mock = pika.BasicProperties(headers={"message_delivery_times": MESSAGE_MAX_DELIVERY_TIMES})
            process_job_anomalies(
                MagicMock(),
                method_mock,
                properties_mock,
                anomaly.model_dump_json().encode(),
            )

            storage_mock.read_job_records.assert_called_once_with(
                job_id=anomaly.attributes.job_id,
                sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
                limit=5,
                offset=0,
                join_job_events_table=False,
            )
            storage_mock.update_job_record.assert_not_called()
            rabbitmq_mock.publish_message.assert_not_called()

    def test_process_job_checkpoints(self):
        anomaly = create_example_anomaly(
            anomaly_type=EventType.CHECKPOINTSAVE,
            log_file_name="8B_burn_test_1290962_date_25-12-15_time_15-43-37.log",
        )
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        job_record = create_example_slurm_job_record(
            attempt_index=0,
            submit_time=submit_time,
            last_updated_at=now,
            log_file_paths=["/var/log/heimdall/dev/8B_burn_test_1290962_date_25-12-15_time_15-43-37.log"],
        )

        storage_mock = self._job_manager.storage
        with patch("job_manager.service.CHECKPOINT_MESSAGE_RECEIVED_COUNTER") as checkpoint_counter_mock:
            storage_mock.read_job_records.return_value = ([job_record], 1)
            process_job_anomalies = self._job_manager.get_watcher_job_event_message_callback()
            process_job_anomalies(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                anomaly.model_dump_json().encode(),
            )

            storage_mock.read_job_records.assert_called_once_with(
                job_id=anomaly.attributes.job_id,
                sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
                limit=5,
                offset=0,
                join_job_events_table=False,
            )
            storage_mock.update_job_record.assert_called_once()
            checkpoint_counter_mock.inc.assert_called_once()

    def test_find_matching_job_attempt_by_slurm_job_attempt(self):
        """Tier 1: An explicit Slurm attempt takes precedence over log file name."""
        anomaly = create_example_anomaly(
            log_file_name="shared.log",
            slurm_job_attempt="0",
        )
        now = datetime.now(UTC)
        exact_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=0,
            log_file_paths=["/logs/shared.log"],
        )
        storage_mock = self._job_manager.storage
        storage_mock.read_job_record.return_value = exact_record

        result = self._job_manager._find_matching_job_attempt_for_event(job_id=anomaly.attributes.job_id, event=anomaly)

        self.assertEqual(result, exact_record)
        storage_mock.read_job_record.assert_called_once_with(job_id=anomaly.attributes.job_id, job_attempt_index=0)
        storage_mock.read_job_records.assert_not_called()

    def test_find_matching_job_attempt_falls_back_to_latest(self):
        """Without explicit attempt identity, unmatched filenames use the latest attempt."""
        anomaly = create_example_anomaly(log_file_name="bash_1730_0")
        now = datetime.now(UTC)
        job_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=2,
            log_file_paths=["/home/user/test_job.sh_1730_date_26-03-04_time_16-51-59.log"],
        )

        job_record.start_time = now - timedelta(minutes=9)
        storage_mock = self._job_manager.storage
        storage_mock.read_job_records.side_effect = [([job_record], 1), ([], 0)]
        storage_mock.read_job_record.return_value = job_record

        result = self._job_manager._find_matching_job_attempt_for_event(job_id=anomaly.attributes.job_id, event=anomaly)

        self.assertEqual(result, job_record)
        storage_mock.read_job_record.assert_called_once_with(job_id=anomaly.attributes.job_id)

    def test_find_matching_job_attempt_shared_paths_skip_filename_identity(self):
        """Shared paths require source-time evidence for the current attempt."""
        anomaly = create_example_anomaly(log_file_name="shared.log")
        now = datetime.now(UTC)
        latest_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=1,
            log_file_paths=["/logs/shared.log"],
        )
        latest_record.aps_spec.log_file_paths_shared_across_attempts = True
        latest_record.start_time = now - timedelta(minutes=9)
        previous_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=20),
            last_updated_at=now - timedelta(minutes=10),
            attempt_index=0,
            log_file_paths=["/logs/shared.log"],
        )

        storage_mock = self._job_manager.storage
        storage_mock.read_job_records.return_value = ([latest_record, previous_record], 2)

        with (
            patch("job_manager.service.EVENT_ATTEMPT_MATCH_BY_LOG_FILE_NAME") as filename_counter,
            patch("job_manager.service.EVENT_ATTEMPT_MATCH_BY_LATEST_ATTEMPT") as latest_counter,
        ):
            result = self._job_manager._find_matching_job_attempt_for_event(
                job_id=anomaly.attributes.job_id,
                event=anomaly,
            )

        self.assertEqual(result, latest_record)
        filename_counter.labels.assert_not_called()
        latest_counter.labels.assert_not_called()
        storage_mock.read_job_record.assert_not_called()

    def test_watcher_replay_deduplicates_persisted_evidence_after_storage_retry(self):
        """Replay retries persistence without appending duplicate durable evidence."""
        for event_type, field in [(EventType.CRASH, "anomalies"), (EventType.CHECKPOINTSAVE, "checkpoints")]:
            with self.subTest(event_type=event_type):
                durable = create_example_slurm_job_record(attempt_index=0)
                self._job_manager.storage.read_job_record.side_effect = lambda _durable=durable, **_: deepcopy(_durable)
                event = create_example_anomaly(anomaly_type=event_type, slurm_job_attempt=0)
                callback = self._job_manager.get_watcher_job_event_message_callback()
                writes = 0

                def persist(*args, _durable=durable, _field=field, **updates):
                    nonlocal writes
                    writes += 1
                    if writes == 1:
                        raise RecoveryStorageException("temporary storage failure")
                    setattr(_durable, _field, deepcopy(updates[_field]))
                    return True

                with patch.object(self._job_manager, "update_job_record", side_effect=persist):
                    for _ in range(3):
                        callback(MagicMock(), MagicMock(), MagicMock(headers={}), event.model_dump_json().encode())
                    assert len(getattr(durable, field)) == 1
                    assert getattr(durable, field)[0].uuid == event.attributes.uuid
                    # A distinct source event still survives (including same body).
                    event.attributes.uuid = str(uuid.uuid4())
                    callback(MagicMock(), MagicMock(), MagicMock(headers={}), event.model_dump_json().encode())
                    assert len(getattr(durable, field)) == 2
                assert writes == 4

    def test_shared_log_replay_cannot_attach_to_newer_attempt(self):
        """Restart replay and a rapid retry share paths, but never action identity."""
        now = datetime.now(UTC)
        older = create_example_slurm_job_record(attempt_index=0, log_file_paths=["/logs/shared.log"])
        newer = create_example_slurm_job_record(attempt_index=1, log_file_paths=["/logs/shared.log"])
        older.start_time = now - timedelta(minutes=4)
        older.end_time = now - timedelta(minutes=2)
        newer.start_time = now - timedelta(minutes=1)
        newer.aps_spec.log_file_paths_shared_across_attempts = True
        self._job_manager.storage.read_job_records.side_effect = lambda **kw: (
            ([newer, older], 2) if kw.get("offset", 0) == 0 else ([], 0)
        )
        self._job_manager.storage.read_job_record.return_value = newer
        callback = self._job_manager.get_watcher_job_event_message_callback()
        with patch.object(self._job_manager, "persist_job_anomaly") as persist:
            for source_time, expected_attempt in [(now - timedelta(minutes=3), 0), (now, 1)]:
                event = create_example_anomaly(log_file_name="shared.log")
                event.timestamp = str(int(source_time.timestamp() * 1e9))
                # A replay's detection time must not substitute for source time.
                event.attributes.detection_time = now.isoformat()
                callback(MagicMock(), MagicMock(), MagicMock(), event.model_dump_json().encode())
                assert persist.call_args.kwargs["latest_job_record"].job_attempt_index == expected_attempt

    def test_unbound_replay_waits_without_mutating_latest_attempt(self):
        """Missing/invalid source time and timestamps in a retry gap fail closed."""
        now = datetime.now(UTC)
        latest = create_example_slurm_job_record(attempt_index=1, log_file_paths=["/logs/shared.log"])
        latest.start_time = now - timedelta(seconds=30)
        latest.aps_spec.log_file_paths_shared_across_attempts = True
        self._job_manager.storage.read_job_records.side_effect = lambda **kw: (
            ([latest], 1) if kw.get("offset", 0) == 0 else ([], 0)
        )
        self._job_manager.storage.read_job_record.return_value = latest
        callback = self._job_manager.get_watcher_job_event_message_callback()
        with patch("job_manager.service.rabbitmq.publish_message") as retry:
            for timestamp in [None, "invalid", str(int((now - timedelta(minutes=2)).timestamp() * 1e9))]:
                event = create_example_anomaly(log_file_name="shared.log")
                event.timestamp = timestamp
                callback(MagicMock(), MagicMock(), MagicMock(headers={}), event.model_dump_json().encode())
            assert retry.call_count == 3
        self._job_manager.storage.update_job_record.assert_not_called()
        self._job_manager.storage.upsert_advice.assert_not_called()

    def test_find_matching_job_attempt_slurm_attempt_not_in_db(self):
        """An unknown explicit attempt waits for registration rather than changing identity."""
        anomaly = create_example_anomaly(
            log_file_name="bash_1730_0",
            slurm_job_attempt="5",
        )
        now = datetime.now(UTC)
        latest_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=1,
            log_file_paths=["/home/user/test_job.sh_1730_date_26-03-04_time_16-51-59.log"],
        )

        storage_mock = self._job_manager.storage
        storage_mock.read_job_records.side_effect = [([latest_record], 1), ([], 0)]
        # A fallback lookup would return the older attempt, but must not run.
        storage_mock.read_job_record.side_effect = [None, latest_record]

        result = self._job_manager._find_matching_job_attempt_for_event(job_id=anomaly.attributes.job_id, event=anomaly)

        self.assertIsNone(result)
        storage_mock.read_job_record.assert_called_once_with(job_id=anomaly.attributes.job_id, job_attempt_index=5)
        storage_mock.read_job_records.assert_not_called()

    def test_find_matching_job_attempt_no_log_file_name_uses_slurm_attempt(self):
        """An explicit Slurm attempt does not require a log filename."""
        anomaly = create_example_anomaly(slurm_job_attempt="1")
        now = datetime.now(UTC)
        job_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=1,
        )

        storage_mock = self._job_manager.storage
        storage_mock.read_job_record.return_value = job_record

        result = self._job_manager._find_matching_job_attempt_for_event(job_id=anomaly.attributes.job_id, event=anomaly)

        self.assertEqual(result, job_record)
        storage_mock.read_job_records.assert_not_called()
        storage_mock.read_job_record.assert_called_once_with(job_id=anomaly.attributes.job_id, job_attempt_index=1)

    def test_find_matching_job_attempt_returns_none_when_no_records_exist(self):
        """All tiers return None when no job records exist at all."""
        anomaly = create_example_anomaly(log_file_name="bash_1730_0")

        storage_mock = self._job_manager.storage
        storage_mock.read_job_records.return_value = ([], 0)
        storage_mock.read_job_record.return_value = None

        result = self._job_manager._find_matching_job_attempt_for_event(job_id=anomaly.attributes.job_id, event=anomaly)

        self.assertIsNone(result)

    def test_event_attempt_match_counters(self):
        """Per-tier counters are incremented with job_id label for each resolution tier."""
        now = datetime.now(UTC)
        job_record = create_example_slurm_job_record(
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
            attempt_index=0,
            log_file_paths=["/var/log/test.log"],
        )
        job_record.start_time = now - timedelta(minutes=9)
        storage_mock = self._job_manager.storage
        storage_mock.read_job_record.return_value = job_record

        tier_map = {
            "log_file_name": create_example_anomaly(log_file_name="test.log"),
            "slurm_job_attempt": create_example_anomaly(log_file_name="no_match", slurm_job_attempt="0"),
            "latest_attempt": create_example_anomaly(log_file_name="no_match"),
        }
        for tier, anomaly in tier_map.items():
            storage_mock.read_job_records.side_effect = [([job_record], 1), ([], 0)]
            with patch(f"job_manager.service.EVENT_ATTEMPT_MATCH_BY_{tier.upper()}") as counter:
                self._job_manager._find_matching_job_attempt_for_event(job_id="12345", event=anomaly)
                counter.labels.assert_called_once_with(job_id="12345")

    def test_process_anomaly_attribution(self):
        attribution_result = create_example_anoamly_attribution_result()

        with (
            patch(
                "job_manager.service.ANOMALY_ATTRIBUTION_MESSAGE_RECEIVED_COUNTER"
            ) as attribution_message_counter_mock,
            patch("job_manager.service.tracer") as mock_tracer,
            patch("job_manager.service.extract_trace_context_from_rabbitmq_properties") as mock_extractor,
        ):
            mock_span = MagicMock()
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
            mock_context = MagicMock()
            mock_extractor.return_value = mock_context

            properties = pika.spec.BasicProperties(
                headers={"traceparent": "00-b7f7408dda54b2bed9eccc6a27ecde77-26af15ee3d44222e-01"}
            )
            process_attribution_results = self._job_manager.get_anomaly_isolator_message_callback()
            process_attribution_results(
                MagicMock(),
                MagicMock(),
                properties,
                attribution_result.to_json().encode(),
            )

            mock_tracer.start_as_current_span.assert_called_once()
            self.assertEqual(mock_tracer.start_as_current_span.call_args.kwargs["context"], mock_context)
            mock_extractor.assert_called_once_with(
                event_body=attribution_result.to_json().encode(), properties=properties
            )

            attribution_message_counter_mock.inc.assert_called_once()
            # Store attribution is called once per non-empty attribution dict with errors
            # In this test, we only have node_attributions with 1 error, no global_attributions
            self._job_manager.attribution_storage.store_attribution.assert_called_once()

            mock_span.set_attribute.assert_has_calls(
                [
                    call("job_id", attribution_result.job_id),
                    call("job_attempt_index", attribution_result.job_attempt_index),
                    call("job_anomaly.type", attribution_result.anomaly_type),
                    call("job_anomaly.uuid", attribution_result.anomaly_uuid),
                ],
                any_order=True,
            )

    def test_application_intention_wins_over_late_restart_attribution(self):
        attribution_result = create_example_anoamly_attribution_result()
        attempt = create_example_slurm_job_record(
            job_id=attribution_result.job_id,
            attempt_index=attribution_result.job_attempt_index,
            state="FAILED",
        )
        intention = ApplicationIntentionRecord(
            job_id=attribution_result.job_id,
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            reason="application cannot continue",
            created_at=datetime.now(UTC),
        )
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_job_record.return_value = attempt
        self._job_manager.storage.read_application_intention.return_value = intention
        self._job_manager.storage.read_advice_by_job.return_value = None

        with (
            patch("job_manager.service.create_terminal_advice_for_job") as persist_terminal,
            patch("job_manager.service.maybe_persist_job_released_terminal_advice_from_attribution") as persist_restart,
        ):
            persist_terminal.return_value = JobTerminalAdvice.APPLICATION_REQUESTED_CANCELLATION
            callback = self._job_manager.get_anomaly_isolator_message_callback()
            callback(
                MagicMock(),
                MagicMock(),
                pika.spec.BasicProperties(),
                attribution_result.to_json().encode(),
            )

        self._job_manager.storage.read_job_record.assert_called_with(
            job_id=attribution_result.job_id,
            job_attempt_index=attribution_result.job_attempt_index,
        )
        persist_terminal.assert_called_once()
        persist_restart.assert_not_called()

    def test_failed_application_termination_write_still_blocks_late_restart_attribution(self):
        attribution_result = create_example_anoamly_attribution_result()
        attempt = create_example_slurm_job_record(
            job_id=attribution_result.job_id,
            attempt_index=attribution_result.job_attempt_index,
            state="FAILED",
        )
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_job_record.return_value = attempt
        self._job_manager.storage.read_application_intention.return_value = ApplicationIntentionRecord(
            job_id=attribution_result.job_id,
            desired_state=ApplicationIntentionDesiredState.CANCELLED.value,
            created_at=datetime.now(UTC),
        )
        self._job_manager.storage.read_advice_by_job.return_value = None
        method = MagicMock(exchange="aie_eventbus_exchange")
        properties = pika.spec.BasicProperties()

        with (
            patch("job_manager.service.create_terminal_advice_for_job", return_value=None),
            patch("job_manager.service.maybe_persist_job_released_terminal_advice_from_attribution") as restart,
            patch("job_manager.service.rabbitmq.publish_message") as redeliver,
        ):
            callback = self._job_manager.get_anomaly_isolator_message_callback()
            callback(
                MagicMock(),
                method,
                properties,
                attribution_result.to_json().encode(),
            )

        restart.assert_not_called()
        self._job_manager.attribution_storage.store_attribution.assert_not_called()
        redeliver.assert_called_once()
        self.assertEqual(redeliver.call_args.kwargs["routing_key"], "test_dead_letter")
        self.assertEqual(redeliver.call_args.kwargs["properties"].headers["message_delivery_times"], 1)

    def test_transient_application_intention_lookup_redelivers_then_persists_restart(self):
        attribution_result = create_example_anoamly_attribution_result()
        attempt = create_example_slurm_job_record(
            job_id=attribution_result.job_id,
            attempt_index=attribution_result.job_attempt_index,
            state="FAILED",
        )
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_job_record.return_value = attempt
        self._job_manager.storage.read_application_intention.side_effect = [
            RecoveryStorageException("temporary application intention lookup failure"),
            None,
        ]
        method = MagicMock(exchange="aie_eventbus_exchange")
        properties = pika.spec.BasicProperties()

        with (
            patch("job_manager.service.maybe_persist_job_released_terminal_advice_from_attribution") as restart,
            patch("job_manager.service.rabbitmq.publish_message") as redeliver,
        ):
            callback = self._job_manager.get_anomaly_isolator_message_callback()
            callback(
                MagicMock(),
                method,
                properties,
                attribution_result.to_json().encode(),
            )
            retry_properties = redeliver.call_args.kwargs["properties"]
            callback(
                MagicMock(),
                method,
                retry_properties,
                attribution_result.to_json().encode(),
            )

        redeliver.assert_called_once()
        self.assertEqual(redeliver.call_args.kwargs["exchange_name"], "aie_eventbus_exchange")
        self.assertEqual(redeliver.call_args.kwargs["routing_key"], "test_dead_letter")
        self.assertEqual(retry_properties.headers["message_delivery_times"], 1)
        self._job_manager.attribution_storage.store_attribution.assert_called_once()
        restart.assert_called_once()

    def test_application_intention_retry_remains_pending_beyond_watcher_limit(self):
        """An exact attribution result is retained until reconciliation can complete."""
        attribution_result = create_example_anoamly_attribution_result()
        attempt = create_example_slurm_job_record(
            job_id=attribution_result.job_id,
            attempt_index=attribution_result.job_attempt_index,
            state="FAILED",
        )
        self._config["service"]["advice_management_enabled"] = True
        self._job_manager.storage.read_job_record.return_value = attempt
        self._job_manager.storage.read_application_intention.side_effect = RecoveryStorageException(
            "persistent application intention lookup failure"
        )

        with (
            patch("job_manager.service.rabbitmq.publish_message") as redeliver,
            patch("job_manager.service.maybe_persist_job_released_terminal_advice_from_attribution") as restart,
        ):
            callback = self._job_manager.get_anomaly_isolator_message_callback()
            callback(
                MagicMock(),
                MagicMock(exchange="aie_eventbus_exchange"),
                pika.spec.BasicProperties(headers={"message_delivery_times": MESSAGE_MAX_DELIVERY_TIMES}),
                attribution_result.to_json().encode(),
            )

        redeliver.assert_called_once()
        self.assertEqual(
            redeliver.call_args.kwargs["properties"].headers["message_delivery_times"],
            MESSAGE_MAX_DELIVERY_TIMES + 1,
        )
        self._job_manager.attribution_storage.store_attribution.assert_not_called()
        restart.assert_not_called()

    def test_find_stale_jobs_not_executed(self):
        self._job_manager.is_leader = False

        self._job_manager.find_stale_jobs(run_once=True)
        self._job_manager.storage.read_job_records.assert_not_called()

    def test_find_stale_jobs_found(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=30)
        # stale_job_threshold_seconds is set as 300s, job with last_updated_at set as 350 will be a stale job.
        record_1 = create_example_slurm_job_record(
            job_id="10001",
            submit_time=submit_time,
            last_updated_at=now - timedelta(seconds=350),
            attempt_index=0,
            state="RUNNING",
        )
        record_2 = create_example_slurm_job_record(
            job_id="10002",
            submit_time=submit_time,
            last_updated_at=now - timedelta(seconds=350),
            attempt_index=0,
            state="PENDING",
        )
        # Record 3 isn't considered stale because the last_updated_at is within the threshold.
        record_3 = create_example_slurm_job_record(
            job_id="10010",
            submit_time=submit_time,
            last_updated_at=now - timedelta(seconds=10),
            attempt_index=0,
            state="PENDING",
        )
        storage_mock = self._job_manager.storage
        with patch("job_manager.service.STALE_RUNNING_JOBS_GAUGE") as gauge_mock:
            # Scenario where stale jobs are found in both RUNNING and PENDING states.
            storage_mock.read_job_records.side_effect = [([record_1, record_2, record_3], 3), ([], 3)]
            self._job_manager.find_stale_jobs(run_once=True)
            gauge_mock.set.assert_called_with(2)

            # Verify the storage read_job_records is called twice.
            self.assertEqual(storage_mock.read_job_records.call_count, 2)
            storage_mock.read_job_records.assert_has_calls(
                [
                    call(states=[s for s in SlurmJobNonFinalStates], offset=0),
                    call(states=[s for s in SlurmJobNonFinalStates], offset=3),
                ]
            )

            self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()
            self._job_manager.event_bus_consumers[WORKFLOW_CONTROLLER].get_queue_length.assert_not_called()
            storage_mock.update_job_record.assert_not_called()

    def test_find_stale_jobs_not_found(self):
        self._job_manager.is_leader = True
        storage_mock = self._job_manager.storage

        with patch("job_manager.service.STALE_RUNNING_JOBS_GAUGE") as gauge_mock:
            # Scenario where stale jobs are not found.
            storage_mock.read_job_records.return_value = ([], 0)
            self._job_manager.find_stale_jobs(run_once=True)
            gauge_mock.set.assert_called_with(0)

    def test_find_stale_jobs_ignores_legacy_refresh_settings(self):
        # Old overrides must not reactivate publishing to an unconsumed queue.
        self._config["service"]["on_demand_job_update_request_enabled"] = True
        self._config["service"]["on_demand_job_update_queue_length_cutoff"] = 100000
        self.test_find_stale_jobs_found()

    def test_find_stale_jobs_cleanup(self):
        self._job_manager.is_leader = True
        self._config["service"]["stale_job_cleanup_enabled"] = True
        record = create_example_slurm_job_record(
            job_id="10001",
            last_updated_at=datetime.now(UTC) - timedelta(seconds=350),
            attempt_index=0,
            state="RUNNING",
        )
        self._job_manager.storage.read_job_records.side_effect = [([record], 1), ([], 1)]
        with patch.object(self._job_manager, "update_job_record") as update:
            self._job_manager.find_stale_jobs(run_once=True)

        update.assert_called_once()
        self.assertEqual(update.call_args.kwargs["job_id"], "10001")
        self.assertEqual(update.call_args.kwargs["job_attempt_index"], 0)
        self.assertEqual(update.call_args.kwargs["state"], "CANCELLED_STALE")
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_get_system_max_requeue_times(self):
        self.assertEqual(self._job_manager.get_system_max_requeue_times(), 100)

        self._job_manager.config["service"].pop("system_max_requeue_times")
        self.assertEqual(self._job_manager.get_system_max_requeue_times(), DEFAULT_SYSTEM_MAX_REQUEUE_TIMES)

    def test_get_checkpoint_based_requeue_stop_threshold(self):
        self.assertEqual(self._job_manager.get_checkpoint_based_requeue_stop_threshold(), 5)

        self._job_manager.config["service"].pop("checkpoint_based_requeue_stop_threshold")
        self.assertIsNone(self._job_manager.get_checkpoint_based_requeue_stop_threshold())

    def test_get_requeue_on_completion_config(self):
        self.assertFalse(self._job_manager.get_requeue_on_completion_config())

        self._job_manager.config["service"]["requeue_on_completion"] = True
        self.assertTrue(self._job_manager.get_requeue_on_completion_config())

    def test_requeue_early_stop_detected(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        # The job was only requeued twice while the limit is 5.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=2,
                submit_time=now - timedelta(minutes=60),
                # The timedelta between now and end_time is 11 minutes, which is longer than the threshold of 10 minutes
                end_time=now - timedelta(minutes=11),
                last_updated_at=now - timedelta(minutes=10),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=180),
                end_time=now - timedelta(minutes=125),
                last_updated_at=now - timedelta(minutes=120),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
        ]

        # The 1st and 3rd calls are made at the outermost for loop, the 2nd call is made when checking the checkpoint
        # based requeue stop threshold.
        storage_mock.read_job_records.side_effect = [
            (job_attemps, len(job_attemps)),
            (job_attemps, len(job_attemps)),
            ([], len(job_attemps)),
        ]
        storage_mock.read_job_record.return_value = job_attemps[0]
        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 3)
            storage_mock.read_job_record.assert_called_once()

            # The database query must include attempts that crossed the 10-minute threshold since the previous
            # 1-minute scan. Previously the lookback was only 2 minutes, so this 11-minute-old attempt could never
            # have been returned by MySQL even though the mocked response made the test pass.
            candidate_time_filter = storage_mock.read_job_records.call_args_list[0].kwargs["time_filter"]
            self.assertLessEqual(candidate_time_filter.from_time, job_attemps[0].end_time)
            self.assertGreater(candidate_time_filter.to_time, job_attemps[0].end_time)

            early_stop_counter_mock.set.assert_called_once_with(1)

    def test_requeue_early_stop_refreshes_time_window_between_scans(self):
        self._job_manager.is_leader = True
        first_scan_time = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
        second_scan_time = first_scan_time + timedelta(minutes=1)

        storage_mock = self._job_manager.storage
        storage_mock.read_job_records.return_value = ([], 0)

        with (
            patch("job_manager.service.datetime") as datetime_mock,
            patch("job_manager.service.time_module.sleep", side_effect=[None, RuntimeError("stop periodic scan")]),
            patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE"),
            self.assertRaisesRegex(RuntimeError, "stop periodic scan"),
        ):
            datetime_mock.now.side_effect = [first_scan_time, second_scan_time]
            self._job_manager.detect_requeue_early_stop()

        first_time_filter = storage_mock.read_job_records.call_args_list[0].kwargs["time_filter"]
        second_time_filter = storage_mock.read_job_records.call_args_list[1].kwargs["time_filter"]
        self.assertEqual(second_time_filter.from_time - first_time_filter.from_time, timedelta(minutes=1))
        self.assertEqual(second_time_filter.to_time - first_time_filter.to_time, timedelta(minutes=1))

    def test_no_requeue_early_stop_detected_not_managed_by_requeue(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        # The job was only requeued twice while the limit is 5.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=60),
                # The timedelta between now and end_time is 15 minutes, which is longer than the threshold of 10 minutes
                end_time=now - timedelta(minutes=15),
                last_updated_at=now - timedelta(minutes=10),
                state="CANCELLED",
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="FAILED",
            ),
            create_example_slurm_job_record(
                job_id="9999",
                attempt_index=0,
                submit_time=now - timedelta(minutes=180),
                end_time=now - timedelta(minutes=125),
                last_updated_at=now - timedelta(minutes=120),
                state="COMPLETED",
            ),
        ]

        storage_mock.read_job_records.side_effect = [(job_attemps, len(job_attemps)), ([], len(job_attemps))]
        storage_mock.read_job_record.return_value = job_attemps[0]

        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 2)
            storage_mock.read_job_record.assert_not_called()

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_no_requeue_early_stop_detected_limit_reached(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        # The job was requeued twice and the max requeue times is 2.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=2,
                submit_time=now - timedelta(minutes=60),
                end_time=now - timedelta(minutes=15),
                last_updated_at=now - timedelta(minutes=10),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=2,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=2,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=180),
                end_time=now - timedelta(minutes=125),
                last_updated_at=now - timedelta(minutes=120),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=2,
            ),
        ]

        # The 1st and 3rd calls are made at the outermost for loop, the 2nd call is made when checking the checkpoint
        # based requeue stop threshold.
        storage_mock.read_job_records.side_effect = [
            (job_attemps, len(job_attemps)),
            (job_attemps, len(job_attemps)),
            ([], len(job_attemps)),
        ]
        storage_mock.read_job_record.return_value = job_attemps[0]
        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 3)
            storage_mock.read_job_record.assert_called_once()

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_no_requeue_early_stop_detected_explicit_cancelled(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        # The job was explicitly cancelled even though it was only requeued once and the limit is 5.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=60),
                end_time=now - timedelta(minutes=15),
                last_updated_at=now - timedelta(minutes=10),
                state="CANCELLED",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
        ]

        storage_mock.read_job_records.side_effect = [(job_attemps, len(job_attemps)), ([], len(job_attemps))]
        storage_mock.read_job_record.return_value = job_attemps[0]
        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 2)
            storage_mock.read_job_record.assert_not_called()

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_no_requeue_early_stop_detected_no_requeue_on_completion(self):
        self._job_manager.is_leader = True
        self._job_manager.config["service"]["requeue_on_completion"] = False

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        # The job was in COMPLETED state and the system is configured to not requeue on job completion.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=60),
                end_time=now - timedelta(minutes=15),
                last_updated_at=now - timedelta(minutes=10),
                state="COMPLETED",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
        ]

        storage_mock.read_job_records.side_effect = [(job_attemps, len(job_attemps)), ([], len(job_attemps))]
        storage_mock.read_job_record.return_value = job_attemps[0]
        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 2)
            storage_mock.read_job_record.assert_not_called()

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_no_requeue_early_stop_detected_checkpoint_early_stop_qualified(self):
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage

        # Job level max requeue times is set to 10 and the checkpoint based requeue stop threshold is set to 5.
        # All attempts failed with no checkpoint event saved.
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=4 - i,
                submit_time=now - timedelta(minutes=60 * (i + 1)),
                end_time=now - timedelta(minutes=60 * i + 15),
                last_updated_at=now - timedelta(minutes=60 * i + 10),
                state="FAILED",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=10,
            )
            for i in range(5)
        ]

        storage_mock.read_job_records.side_effect = [
            (job_attemps, len(job_attemps)),
            (job_attemps, len(job_attemps)),
            ([], len(job_attemps)),
        ]
        storage_mock.read_job_record.return_value = job_attemps[0]
        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 3)

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_no_requeue_early_stop_detected_running_attempt(self):
        # This test simulates the scenario where the latest job attempt (3) is in RUNNING state with 3 previous attempts
        # that were requeued. The job should not be considered as an unexpected early stop.
        self._job_manager.is_leader = True

        now = datetime.now(UTC)

        storage_mock = self._job_manager.storage
        job_attemps = [
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=2,
                submit_time=now - timedelta(minutes=60),
                end_time=now - timedelta(minutes=15),
                last_updated_at=now - timedelta(minutes=10),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=1,
                submit_time=now - timedelta(minutes=120),
                end_time=now - timedelta(minutes=65),
                last_updated_at=now - timedelta(minutes=60),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
            create_example_slurm_job_record(
                job_id="10000",
                attempt_index=0,
                submit_time=now - timedelta(minutes=180),
                end_time=now - timedelta(minutes=125),
                last_updated_at=now - timedelta(minutes=120),
                state="TIMEOUT",
                auto_resume_mode=AutoResumeModeEnum.REQUEUE,
                max_requeue_times=5,
            ),
        ]

        storage_mock.read_job_records.side_effect = [(job_attemps, len(job_attemps)), ([], len(job_attemps))]
        storage_mock.read_job_record.return_value = create_example_slurm_job_record(
            job_id="10000",
            attempt_index=3,
            submit_time=now - timedelta(minutes=5),
            last_updated_at=now - timedelta(seconds=30),
            state="RUNNING",
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
            max_requeue_times=5,
        )

        with patch("job_manager.service.UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE") as early_stop_counter_mock:
            self._job_manager.detect_requeue_early_stop(run_once=True)

            self.assertEqual(storage_mock.read_job_records.call_count, 2)
            storage_mock.read_job_record.assert_called_once()

            early_stop_counter_mock.set.assert_called_once_with(0)

    def test_resolve_effective_final_state(self):
        now = datetime.now(UTC)
        cases = [
            ("TIMEOUT", "None", None, "TIMEOUT"),
            ("COMPLETING", "TimeLimit", None, "TIMEOUT"),
            ("COMPLETING", "NonZeroExitCode", None, "FAILED"),
            ("COMPLETING", "RaisedSignal", None, "FAILED"),
            ("COMPLETING", "CancelledByUser", None, None),
            ("RUNNING", "None", None, None),
            (
                "FAILED",
                "None",
                {"scheduler": "slurm", "slurm_state": "TIMEOUT", "slurm_reason": "TimeLimit"},
                "TIMEOUT",
            ),
            (
                "RUNNING",
                "None",
                {"scheduler": "slurm", "slurm_state": "COMPLETING", "slurm_reason": "NonZeroExitCode"},
                "FAILED",
            ),
        ]

        for state, reason, scheduler_metadata, expected in cases:
            with self.subTest(state=state, reason=reason, scheduler_metadata=scheduler_metadata):
                record = create_example_slurm_job_record(
                    state=state,
                    state_reason=reason,
                    submit_time=now - timedelta(minutes=10),
                    last_updated_at=now,
                )
                record.scheduler_metadata = scheduler_metadata
                self.assertEqual(resolve_effective_final_state(record), expected)

    def test_should_send_crash_anomaly_deduplicates_actionable_anomalies(self):
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            state="FAILED",
            submit_time=now - timedelta(minutes=10),
            last_updated_at=now,
        )
        record.aps_spec = ApsSlurmJobSpec(
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False)
        )

        record.anomalies = [JobAnomaly(anomaly_type=EventType.HANG, timestamp=now, uuid=str(uuid.uuid4()))]
        self.assertFalse(self._job_manager.should_send_crash_anomaly(record, "FAILED"))

        record.anomalies = [JobAnomaly(anomaly_type=EventType.STRAGGLER, timestamp=now, uuid=str(uuid.uuid4()))]
        self.assertTrue(self._job_manager.should_send_crash_anomaly(record, "FAILED"))

    def test_job_update_emits_end_during_completing_time_limit(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        record = create_example_slurm_job_record(
            state="COMPLETING",
            state_reason="TimeLimit",
            submit_time=submit_time,
            end_time=now,
            last_updated_at=now,
        )
        known_record = create_example_slurm_job_record(
            state="RUNNING",
            state_reason="None",
            submit_time=submit_time,
            last_updated_at=now - timedelta(minutes=1),
        )
        known_record.sticky_exclusion_node_list = []
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        updated_anomalies = self._job_manager.storage.update_job_record.call_args.kwargs["anomalies"]
        self.assertEqual(len(updated_anomalies), 1)
        self.assertEqual(updated_anomalies[0].anomaly_type, EventType.END)
        self.assertEqual(self._job_manager.storage.update_job_record.call_args.kwargs["state"], "COMPLETING")

    def test_job_update_completing_crash_has_persistence_span(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        record = create_example_slurm_job_record(
            state="COMPLETING",
            state_reason="NonZeroExitCode",
            submit_time=submit_time,
            end_time=now,
            last_updated_at=now,
        )
        known_record = create_example_slurm_job_record(
            state="RUNNING",
            state_reason="None",
            submit_time=submit_time,
            last_updated_at=now - timedelta(minutes=1),
        )
        for job_record in (record, known_record):
            job_record.aps_spec = ApsSlurmJobSpec(
                anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False)
            )
        known_record.sticky_exclusion_node_list = []
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record

        with (
            patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"),
            patch("job_manager.service.tracer") as mock_tracer,
        ):
            persistence_span = MagicMock()
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = persistence_span
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        updated_anomalies = self._job_manager.storage.update_job_record.call_args.kwargs["anomalies"]
        mock_tracer.start_as_current_span.assert_called_once_with(
            name="job_anomaly.persistence",
            kind=SpanKind.INTERNAL,
            attributes={
                "job_id": record.job_id,
                "job_attempt_index": record.job_attempt_index,
                "job_anomaly.type": EventType.CRASH,
                "job_anomaly.uuid": updated_anomalies[0].uuid,
            },
        )
        persistence_span.set_status.assert_called_once_with(StatusCode.OK)

    def test_job_update_does_not_duplicate_end_after_completing(self):
        now = datetime.now(UTC)
        submit_time = now - timedelta(minutes=10)
        record = create_example_slurm_job_record(
            state="TIMEOUT",
            state_reason="TimeLimit",
            submit_time=submit_time,
            end_time=now,
            last_updated_at=now,
        )
        known_record = create_example_slurm_job_record(
            state="COMPLETING",
            state_reason="TimeLimit",
            submit_time=submit_time,
            last_updated_at=now - timedelta(minutes=1),
        )
        known_record.sticky_exclusion_node_list = []
        known_record.anomalies = [
            JobAnomaly(anomaly_type=EventType.END, timestamp=now - timedelta(seconds=30), uuid=str(uuid.uuid4()))
        ]
        self._job_manager.storage.get_job_latest_attempt_index.return_value = 0
        self._job_manager.storage.read_job_record.return_value = known_record

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()
        anomalies = self._job_manager.storage.update_job_record.call_args.kwargs["anomalies"]
        # None leaves the stored anomaly collection untouched instead of appending another END.
        self.assertIsNone(anomalies)

    def test_generate_state_based_crash_anomaly_when_disabled(self):
        """Test that CRASH anomaly is generated immediately when anomaly_detection_config.log_based_crash_enabled is False"""
        # Create a job record in FAILED state with anomaly_detection_config.log_based_crash_enabled=False
        job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
        )
        job_record.state = "FAILED"
        job_record.end_time = datetime.now(UTC)
        job_record.aps_spec = ApsSlurmJobSpec(
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False)
        )
        job_record.anomalies = []
        self._config["service"]["advice_enabled"] = True

        # Keep anomaly generation independent of persistence and Advice.
        with (
            patch.object(self._job_manager.storage, "update_job_record") as mock_update,
            patch("job_manager.service.create_advice_issue_from_event") as create_advice_mock,
        ):
            # Call the method
            generated = self._job_manager.generate_state_based_crash_anomaly_if_needed(job_record)

            # Verify that a CRASH anomaly was added
            self.assertTrue(generated)
            self.assertEqual(len(job_record.anomalies), 1)
            self.assertEqual(job_record.anomalies[0].anomaly_type, EventType.CRASH)
            self.assertEqual(job_record.anomalies[0].handled_by, JobAnomalyHandledBy.HEIMDALL)
            self.assertTrue(job_record.anomalies[0].processed)

            mock_update.assert_not_called()
            create_advice_mock.assert_not_called()

    def test_no_crash_anomaly_when_enabled(self):
        """Test that CRASH anomaly is NOT generated when anomaly_detection_config.log_based_crash_enabled is True"""
        # Create a job record in FAILED state with anomaly_detection_config.log_based_crash_enabled=True
        job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
        )
        job_record.state = "FAILED"
        job_record.end_time = datetime.now(UTC)
        job_record.aps_spec = ApsSlurmJobSpec(
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=True)
        )
        job_record.anomalies = []

        # Call the method
        generated = self._job_manager.generate_state_based_crash_anomaly_if_needed(job_record)

        # Verify that no CRASH anomaly was added
        self.assertFalse(generated)
        self.assertEqual(len(job_record.anomalies), 0)

        # No direct attribution is triggered without a generated anomaly.
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_no_crash_anomaly_when_not_failed(self):
        """Test that CRASH anomaly is NOT generated when job is not in FAILED state"""
        # Create a job record in COMPLETED state with anomaly_detection_config.log_based_crash_enabled=False
        job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
        )
        job_record.state = "COMPLETED"
        job_record.end_time = datetime.now(UTC)
        job_record.aps_spec = ApsSlurmJobSpec(
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False)
        )
        job_record.anomalies = []

        # Call the method
        generated = self._job_manager.generate_state_based_crash_anomaly_if_needed(job_record)

        # Verify that no CRASH anomaly was added
        self.assertFalse(generated)
        self.assertEqual(len(job_record.anomalies), 0)

        # No direct attribution is triggered without a generated anomaly.
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_no_crash_anomaly_when_no_aps_spec(self):
        """Test that CRASH anomaly is NOT generated when aps_spec is None"""
        # Create a job record in FAILED state without aps_spec
        job_record = create_example_slurm_job_record(
            submit_time=datetime.now(UTC) - timedelta(minutes=10),
            last_updated_at=datetime.now(UTC),
        )
        job_record.state = "FAILED"
        job_record.end_time = datetime.now(UTC)
        job_record.aps_spec = None
        job_record.anomalies = []

        # Call the method
        generated = self._job_manager.generate_state_based_crash_anomaly_if_needed(job_record)

        # Verify that no CRASH anomaly was added
        self.assertFalse(generated)
        self.assertEqual(len(job_record.anomalies), 0)

        # No direct attribution is triggered without a generated anomaly.
        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()

    def test_watcher_advice_write_failure_replays_original_cause_before_attribution(self):
        """A committed anomaly repairs Advice on replay before emitting attribution once."""
        self._config["service"]["advice_enabled"] = True
        self._config["service"]["advice_management_enabled"] = False
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        self._job_manager.audit_buffer = MagicMock()
        storage = self._job_manager.storage
        record = create_example_slurm_job_record(attempt_index=2, state="FAILED")
        record.heimdall_job_id = uuid.uuid4()
        storage.read_job_record.return_value = record
        storage.update_job_record.return_value = UpdateJobResult(updated_record=record, old_record=deepcopy(record))
        storage.read_advice.return_value = None
        storage.upsert_advice.side_effect = RecoveryStorageException("Advice storage unavailable")
        original_time = datetime(2026, 10, 6, 10, 0, tzinfo=UTC)
        event = create_example_anomaly(anomaly_type=EventType.CRASH, slurm_job_attempt=2)
        event.timestamp = str(int(original_time.timestamp()) * 1_000_000_000)
        event.body = "original crash line"
        body = event.model_dump_json().encode()
        original_headers = {
            "traceparent": "00-b7f7408dda54b2bed9eccc6a27ecde77-26af15ee3d44222e-01",
            "upstream": "record-1",
        }
        properties = pika.BasicProperties(
            headers=original_headers.copy(), content_type="application/json", message_id="watcher-failure"
        )
        channel, method = MagicMock(), MagicMock(exchange="anomaly_exchange")
        callback = self._job_manager.get_watcher_job_event_message_callback()
        publisher = self._job_manager.adi_attribution_request_publisher

        with patch("job_manager.service.rabbitmq.publish_message") as delayed_publish:
            callback(channel, method, properties, body)

            storage.update_job_record.assert_called_once()
            storage.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=2)
            storage.claim_attribution_request_emission.assert_not_called()
            publisher.publish_message.assert_not_called()
            self._job_manager.audit_buffer.add.assert_not_called()
            delayed_publish.assert_called_once()
            replay = delayed_publish.call_args.kwargs
            self.assertEqual(replay["message_body"], body)
            self.assertEqual(replay["exchange_name"], method.exchange)
            self.assertEqual(
                replay["routing_key"], self._config["service"]["event_bus"]["anomaly_dead_letter_routing_key"]
            )
            self.assertEqual(replay["properties"].headers, {**original_headers, "message_delivery_times": 1})
            self.assertEqual(replay["properties"].content_type, "application/json")
            self.assertEqual(replay["properties"].message_id, "watcher-failure")
            self.assertEqual(properties.headers, original_headers)
            self.assertIsNot(replay["properties"], properties)
            self.assertEqual(record.anomalies[0].original_watcher_anomaly_event, body.decode())

            def persist_advice(advice, **_):
                previous = storage.read_advice.return_value
                storage.read_advice.return_value = advice
                return advice, previous is None, previous

            def dispatch_after_causal_audit(**_kwargs):
                self._job_manager.audit_buffer.add.assert_called_once()

            storage.upsert_advice.side_effect = persist_advice
            storage.claim_attribution_request_emission.side_effect = [True, False]
            publisher.publish_message.side_effect = dispatch_after_causal_audit
            callback(channel, method, replay["properties"], replay["message_body"])
            callback(channel, method, replay["properties"], replay["message_body"])

            delayed_publish.assert_called_once()

        self.assertEqual(storage.upsert_advice.call_count, 2)
        advice = storage.read_advice.return_value
        self.assertEqual(advice.heimdall_id, record.heimdall_job_id)
        self.assertEqual(advice.attempt_index, 2)
        self.assertEqual(len(advice.issues), 1)
        self.assertEqual(advice.issues[0].issue_type, "CRASH")
        self.assertEqual(advice.issues[0].timestamp, original_time)
        self.assertEqual(advice.issues[0].relevant_log_lines, ["original crash line"])
        self.assertEqual(advice.issues[0].detected_by, "Heimdall Watcher")
        self._job_manager.audit_buffer.add.assert_called_once()
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceCreatedEvent)
        self.assertEqual(audit_event.heimdall_job_id, str(record.heimdall_job_id))
        self.assertEqual(audit_event.payload.attempt_index, 2)
        self.assertEqual(audit_event.payload.creation_reasons, ["CRASH"])
        self.assertEqual(storage.claim_attribution_request_emission.call_args_list, [call("12345", 2)] * 2)
        publisher.publish_message.assert_called_once()
        request = json.loads(publisher.publish_message.call_args.kwargs["message"])
        self.assertEqual((request["job_id"], request["job_attempt_index"]), ("12345", 2))

    def test_watcher_advice_write_failure_stops_when_delayed_retry_is_unavailable(self):
        """Unavailable or exhausted retries cannot bypass the causal Advice write."""
        self._config["service"]["advice_enabled"] = True
        self._config["service"]["adi_attribution_request_emit_enabled"] = True
        retry_route = self._config["service"]["event_bus"]["anomaly_dead_letter_routing_key"]
        for routing_key, delivery_times in ((None, 0), (retry_route, MESSAGE_MAX_DELIVERY_TIMES)):
            with self.subTest(routing_key=routing_key, delivery_times=delivery_times):
                self._config["service"]["event_bus"]["anomaly_dead_letter_routing_key"] = routing_key
                self._job_manager.audit_buffer = MagicMock()
                storage = MagicMock(spec=MySQLStorage)
                self._job_manager.storage = storage
                record = create_example_slurm_job_record(attempt_index=2, state="FAILED")
                record.heimdall_job_id = uuid.uuid4()
                storage.read_job_record.return_value = record
                storage.update_job_record.return_value = UpdateJobResult(
                    updated_record=record, old_record=deepcopy(record)
                )
                storage.read_advice.return_value = None
                storage.upsert_advice.side_effect = RecoveryStorageException("Advice storage unavailable")
                event = create_example_anomaly(anomaly_type=EventType.CRASH, slurm_job_attempt=2)
                callback = self._job_manager.get_watcher_job_event_message_callback()

                with patch("job_manager.service.rabbitmq.publish_message") as delayed_publish:
                    callback(
                        MagicMock(),
                        MagicMock(),
                        pika.BasicProperties(headers={"message_delivery_times": delivery_times}),
                        event.model_dump_json().encode(),
                    )

                storage.update_job_record.assert_called_once()
                storage.upsert_advice.assert_called_once()
                storage.claim_attribution_request_emission.assert_not_called()
                delayed_publish.assert_not_called()
                self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()
                self._job_manager.audit_buffer.add.assert_not_called()


class TestLeaderElectionRetry(unittest.TestCase):
    """Tests for leader election retry logic in _run_leader_election_with_retry."""

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.ApiClient")
    @patch("kubernetes.leaderelection.leaderelection.LeaderElection")
    @patch("job_manager.service.RabbitMQPublisher")
    @patch("job_manager.service.RabbitMQConsumer")
    @patch("job_manager.service.MySQLConnection")
    def setUp(
        self, mock_connection, mock_consumer, mock_publisher, mock_leader_election, mock_api_client, mock_load_config
    ):
        self._config = create_basic_service_config()
        self._shutdown_event = Event()
        self._job_manager = JobManager(config=self._config, shutdown_event=self._shutdown_event)
        self._job_manager.storage = MagicMock(spec=MySQLStorage)

    def test_retry_on_exception(self):
        """Leader election retries after an exception and resets is_leader to False."""
        call_count = 0

        def fake_run():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("simulated k8s API error")
            # On the 3rd call, signal shutdown to exit the loop
            self._shutdown_event.set()

        self._job_manager._leader_election_cfg = MagicMock()
        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            mock_le_cls.return_value.run = fake_run
            self._job_manager._run_leader_election_with_retry()

        self.assertEqual(call_count, 3)
        self.assertFalse(self._job_manager.is_leader)

    def test_retry_on_normal_return(self):
        """Leader election re-enters election when run() returns normally."""
        call_count = 0

        def fake_run():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                self._shutdown_event.set()

        self._job_manager._leader_election_cfg = MagicMock()
        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            mock_le_cls.return_value.run = fake_run
            self._job_manager._run_leader_election_with_retry()

        self.assertEqual(call_count, 2)

    def test_shutdown_event_stops_retry(self):
        """Shutdown event prevents further retries."""
        self._shutdown_event.set()
        self._job_manager._leader_election_cfg = MagicMock()

        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            self._job_manager._run_leader_election_with_retry()
            mock_le_cls.return_value.run.assert_not_called()

    def test_exponential_backoff_capped(self):
        """Backoff doubles on each failure but is capped at MAX_BACKOFF."""
        backoffs = []
        call_count = 0

        def capture_wait(timeout):
            backoffs.append(timeout)
            nonlocal call_count
            call_count += 1
            if call_count >= 8:
                self._shutdown_event.set()
                return True
            return False

        self._shutdown_event.wait = capture_wait
        self._job_manager._leader_election_cfg = MagicMock()

        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            mock_le_cls.return_value.run.side_effect = RuntimeError("fail")
            self._job_manager._run_leader_election_with_retry()

        # Backoff: 1, 2, 4, 8, 16, 32, 60, 60
        expected = [1, 2, 4, 8, 16, 32, 60, 60]
        self.assertEqual(backoffs, expected)

    def test_leader_status_metric_set_on_exception(self):
        """Leader status metric is reset to 0 when election fails with an exception."""

        call_count = 0

        def fake_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Simulate gaining leadership before crash
                self._job_manager.is_leader = True
                LEADER_STATUS_GAUGE.set(1)
                raise RuntimeError("simulated crash")
            self._shutdown_event.set()

        self._job_manager._leader_election_cfg = MagicMock()
        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            mock_le_cls.return_value.run = fake_run
            self._job_manager._run_leader_election_with_retry()

        metric_value = LEADER_STATUS_GAUGE._value.get()
        self.assertEqual(metric_value, 0)
        self.assertFalse(self._job_manager.is_leader)

    def test_follower_exception_does_not_set_metric(self):
        """A follower crash should not touch the leader status metric."""

        # Set gauge to 1 to simulate another replica being leader
        LEADER_STATUS_GAUGE.set(1)
        call_count = 0

        def fake_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated crash as follower")
            self._shutdown_event.set()

        self._job_manager.is_leader = False
        self._job_manager._leader_election_cfg = MagicMock()
        with patch("job_manager.service.LeaderElection") as mock_le_cls:
            mock_le_cls.return_value.run = fake_run
            self._job_manager._run_leader_election_with_retry()

        # Metric should remain 1 — follower must not override the leader's value
        metric_value = LEADER_STATUS_GAUGE._value.get()
        self.assertEqual(metric_value, 1)

    def test_leader_status_metric_set_via_callbacks(self):
        """Leader status metric is set to 1/0 via on_start_leading/on_stop_leading callbacks."""

        # The setUp creates a JobManager without leader election enabled (default config).
        # Create one with leader election to test callbacks.
        config = create_basic_service_config(enable_leader_election=True)
        shutdown = Event()

        with (
            patch("kubernetes.config.load_kube_config"),
            patch("kubernetes.client.ApiClient"),
            patch("kubernetes.leaderelection.leaderelection.LeaderElection"),
            patch("job_manager.service.get_current_namespace", return_value="default"),
            patch("job_manager.service.RabbitMQPublisher"),
            patch("job_manager.service.RabbitMQConsumer"),
            patch("job_manager.service.MySQLConnection"),
        ):
            jm = JobManager(config=config, shutdown_event=shutdown)

            # Access the callbacks from the stored election config
            le_config = jm._leader_election_cfg

            # Simulate start leading
            le_config.onstarted_leading()
            self.assertTrue(jm.is_leader)
            metric_value = LEADER_STATUS_GAUGE._value.get()
            self.assertEqual(metric_value, 1)

            # Simulate stop leading
            le_config.onstopped_leading()
            self.assertFalse(jm.is_leader)
            metric_value = LEADER_STATUS_GAUGE._value.get()
            self.assertEqual(metric_value, 0)

            # Stop the background retry thread to avoid interference with other tests
            shutdown.set()
            jm.leader_election_thread.join(timeout=2)


class TestDagDependencyManagement(unittest.TestCase):
    """Tests for DAG dependency registration and evaluation in the job update callback."""

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.ApiClient")
    @patch("kubernetes.leaderelection.leaderelection.LeaderElection")
    @patch("job_manager.service.RabbitMQPublisher")
    @patch("job_manager.service.RabbitMQConsumer")
    @patch("job_manager.service.MySQLConnection")
    def setUp(
        self, mock_connection, mock_consumer, mock_publisher, mock_leader_election, mock_api_client, mock_load_config
    ):

        self._config = create_basic_service_config()
        # Enable DAG support for DAG tests
        self._config["service"]["dag_support"]["enabled"] = True
        self._job_manager = JobManager(config=self._config, shutdown_event=Event())
        self._job_manager.attribution_storage = MagicMock(spec=AttributionStorage)
        self._job_manager.notifications_client = MagicMock(spec=NotificationsApi)
        # Replace the storage mock with one that passes isinstance check for MySQLStorage
        self._job_manager.storage = MagicMock(spec=MySQLStorage)
        # Default return for upsert_dag_dependency: (record_mock, True) indicating a new record was created
        self._job_manager.storage.upsert_dag_dependency.return_value = (MagicMock(), True)
        self._job_manager.storage.read_advice_by_job.return_value = None
        self._job_manager.storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
        # Default return for read_job_record: a real initial attempt for DAG release advice.
        self._job_manager.storage.read_job_record.return_value = create_example_slurm_job_record()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_dependencies_registered_on_job_creation(self, mock_sanitize):
        """When a new job with dag_dependencies is created, dependency records should be registered."""

        now = datetime.now(UTC)
        record = create_example_slurm_job_record(submit_time=now, last_updated_at=now, attempt_index=0)
        record.aps_spec.dag_dependencies = [
            DagDependencyStep(target=JobReferenceByName(name="preprocess")),
            DagDependencyStep(target=JobReferenceByName(name="training")),
        ]
        record.aps_spec.dag_action = DagActionEnum.RELEASE

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        self._job_manager.storage.create_job_record.assert_called_once()
        # Should have 2 calls to upsert_dag_dependency, one per dependency step
        self.assertEqual(self._job_manager.storage.upsert_dag_dependency.call_count, 2)

        first_call = self._job_manager.storage.upsert_dag_dependency.call_args_list[0]
        dep_record = first_call.args[0]
        self.assertEqual(dep_record.dependent_job_id, record.job_id)
        self.assertEqual(dep_record.dependent_job_action, "release")
        self.assertEqual(dep_record.dependency_job_identifier, "preprocess")
        self.assertEqual(dep_record.dependency_job_identifier_type, "name")
        self.assertEqual(dep_record.dependency_trigger_condition, "completed")

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_no_dag_registration_without_dependencies(self, mock_sanitize):
        """When a job without dag_dependencies is created, no dependency records should be registered."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(submit_time=now, last_updated_at=now, attempt_index=0)

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        self._job_manager.storage.create_job_record.assert_called_once()
        self._job_manager.storage.upsert_dag_dependency.assert_not_called()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_evaluation_on_job_completion_all_deps_met(self, mock_sanitize):
        """When a parent job completes and all deps for a child are met, the action should be triggered."""

        now = datetime.now(UTC)
        # Create a completed job record
        record = create_example_slurm_job_record(
            submit_time=now, state="COMPLETED", end_time=now, last_updated_at=now, attempt_index=0
        )

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        # Set up a single dependency that references this completed job by name
        dep = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="Test Job",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
        )
        dep.id = 1

        # read_dag_dependencies: by parent name returns the dep, by dependent_job_id returns all deps
        def _read_dag_deps_side_effect(**kwargs):
            if (
                kwargs.get("dependency_job_identifier") == "Test Job"
                and kwargs.get("dependency_job_identifier_type") == "name"
            ):
                return ([dep], 1)
            if kwargs.get("dependent_job_id") == "99999":
                return ([dep], 1)
            return ([], 0)

        self._job_manager.storage.read_dag_dependencies.side_effect = _read_dag_deps_side_effect
        # The parent job is found and is COMPLETED
        self._job_manager.storage.read_job_records.return_value = ([record], 1)
        # The dependent job record (for building spec_update)
        dependent_record = create_example_slurm_job_record(job_id="99999")
        self._job_manager.storage.read_job_record.return_value = dependent_record

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        advice_record = self._job_manager.storage.upsert_advice.call_args.args[0]
        self.assertEqual(advice_record.job_id, "99999")
        self.assertEqual(advice_record.attempt_index, 0)
        self.assertEqual(advice_record.advice_state, AdviceState.RELEASED.value)
        self.assertEqual(
            [
                call.kwargs["action_status"]
                for call in self._job_manager.storage.update_dag_dependency_status.call_args_list
            ],
            [
                DagDependencyActionStatus.NOT_ELIGIBLE.value,
                DagDependencyActionStatus.IN_PROGRESS.value,
                DagDependencyActionStatus.SUCCESSFUL.value,
            ],
        )

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_evaluation_on_job_completion_not_all_deps_met(self, mock_sanitize):
        """When a parent job completes but not all deps for a child are met, no action should be triggered."""

        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now, state="COMPLETED", end_time=now, last_updated_at=now, attempt_index=0
        )

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        # Two dependencies for the child job — only one parent is completed
        dep_met = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="Test Job",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
        )
        dep_met.id = 1

        dep_not_met = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="other_parent",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
        )
        dep_not_met.id = 2

        def _read_dag_deps_side_effect(**kwargs):
            if (
                kwargs.get("dependency_job_identifier") == "Test Job"
                and kwargs.get("dependency_job_identifier_type") == "name"
            ):
                return ([dep_met], 1)
            if kwargs.get("dependent_job_id") == "99999":
                return ([dep_met, dep_not_met], 2)
            return ([], 0)

        self._job_manager.storage.read_dag_dependencies.side_effect = _read_dag_deps_side_effect
        # Parent lookup: "Test Job" found and COMPLETED, "other_parent" not found
        self._job_manager.storage.read_job_records.side_effect = lambda **kwargs: (
            ([record], 1) if kwargs.get("job_name") == "Test Job" else ([], 0)
        )
        # The dependent job is still PENDING
        self._job_manager.storage.read_job_record.return_value = create_example_slurm_job_record(job_id="99999")

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        # No release action is triggered, but both held gates receive a durable explanation.
        update_calls = self._job_manager.storage.update_dag_dependency_status.call_args_list
        self.assertEqual(
            [call.kwargs["reason_code"] for call in update_calls],
            [
                DagDependencyReasonCode.DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS.value,
                DagDependencyReasonCode.WAITING_FOR_PARENT_REGISTRATION.value,
            ],
        )
        self._job_manager.storage.upsert_advice.assert_not_called()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_evaluation_no_dependents(self, mock_sanitize):
        """When a job completes but no other jobs depend on it, nothing should happen."""
        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now, state="COMPLETED", end_time=now, last_updated_at=now, attempt_index=0
        )

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        # No deps reference this job
        self._job_manager.storage.read_dag_dependencies.return_value = ([], 0)

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        self._job_manager.storage.update_dag_dependency_status.assert_not_called()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_evaluation_skips_non_pending_dependent_job(self, mock_sanitize):
        """When a dependent job is no longer in PENDING state, dependency evaluation should be skipped."""

        now = datetime.now(UTC)
        record = create_example_slurm_job_record(
            submit_time=now, state="COMPLETED", end_time=now, last_updated_at=now, attempt_index=0
        )

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        dep = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="Test Job",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
        )
        dep.id = 1

        def _read_dag_deps_side_effect(**kwargs):
            if (
                kwargs.get("dependency_job_identifier") == "Test Job"
                and kwargs.get("dependency_job_identifier_type") == "name"
            ):
                return ([dep], 1)
            return ([], 0)

        self._job_manager.storage.read_dag_dependencies.side_effect = _read_dag_deps_side_effect

        # The dependent job is already RUNNING — no longer pending
        self._job_manager.storage.read_job_record.return_value = create_example_slurm_job_record(
            job_id="99999", state="RUNNING"
        )

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), record.to_json().encode()
            )

        # Should skip — never read deps for a specific dependent job (only read by parent)
        for mock_call in self._job_manager.storage.read_dag_dependencies.call_args_list:
            self.assertIsNone(
                mock_call.kwargs.get("dependent_job_id"),
                f"Expected dependent_job_id=None but got {mock_call.kwargs.get('dependent_job_id')}",
            )
        self._job_manager.storage.update_dag_dependency_status.assert_not_called()

    @patch("job_manager.service.sanitize_log_file_paths_for_retry")
    def test_dag_evaluation_uses_in_flight_state_not_db_state(self, mock_sanitize):
        """The completed_record's state should be used instead of the DB state, since the DB
        hasn't been updated yet when dependency evaluation runs."""

        now = datetime.now(UTC)
        # The job that just completed (state=COMPLETED), but not yet persisted
        completed_record = create_example_slurm_job_record(
            submit_time=now, state="COMPLETED", end_time=now, last_updated_at=now, attempt_index=0
        )

        self._job_manager.storage.get_job_latest_attempt_index.return_value = -1

        dep = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="Test Job",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
        )
        dep.id = 1

        def _read_dag_deps_side_effect(**kwargs):
            if (
                kwargs.get("dependency_job_identifier") == "Test Job"
                and kwargs.get("dependency_job_identifier_type") == "name"
            ):
                return ([dep], 1)
            if kwargs.get("dependent_job_id") == "99999":
                return ([dep], 1)
            return ([], 0)

        self._job_manager.storage.read_dag_dependencies.side_effect = _read_dag_deps_side_effect

        # Simulate the DB still having the old state (RUNNING) — the record hasn't been persisted yet
        stale_db_record = create_example_slurm_job_record(
            submit_time=now, state="RUNNING", last_updated_at=now, attempt_index=0
        )
        self._job_manager.storage.read_job_records.return_value = ([stale_db_record], 1)

        # read_job_record returns the exact dependency-blocked child attempt.
        dependent_record = create_example_slurm_job_record(job_id="99999")
        self._job_manager.storage.read_job_record.return_value = dependent_record

        with patch("job_manager.service.JOB_UPDATE_MESSAGE_RECEIVED_COUNTER"):
            self._job_manager.get_job_update_message_callback()(
                MagicMock(), MagicMock(), MagicMock(), completed_record.to_json().encode()
            )

        self._job_manager.storage.upsert_advice.assert_called_once()

    def test_dag_action_uses_existing_advice_scope_and_decision_time(self):
        from recovery_libs.schema.job_manager.models import RecoveryContext, RecoveryOperation

        child = create_example_slurm_job_record(job_id="child", attempt_index=0)
        child.scheduler_metadata = {"scheduler": "slurm", "environment": "job-scope"}
        child.end_time = datetime(2020, 1, 1, tzinfo=UTC)
        existing = create_example_advice_record(job_id="child", attempt_index=0)
        existing.advice_state = AdviceState.RELEASED.value
        existing.scheduler_metadata = {"scheduler": "slurm", "environment": "advice-scope"}
        self._job_manager.storage.read_advice_by_job.return_value = existing
        recovery = MagicMock()
        recovery.begin_for_job.return_value = RecoveryContext(
            sequence_id=str(uuid.uuid4()),
            traceparent="00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
            operation=RecoveryOperation.DAG_RELEASE,
            triggered_at=datetime.now(UTC),
        )
        self._job_manager.recovery_service = recovery
        before = datetime.now(UTC)
        self._job_manager._trigger_dag_action(child, "release", [1])
        kwargs = recovery.begin_for_job.call_args.kwargs
        self.assertEqual(kwargs["scheduler_metadata"]["environment"], "advice-scope")
        self.assertGreaterEqual(kwargs["triggered_at"], before)
        persisted = self._job_manager.storage.upsert_advice.call_args.args[0]
        self.assertEqual(str(persisted.recovery_sequence_id), recovery.begin_for_job.return_value.sequence_id)

    def test_dag_action_persists_exact_attempt_release_advice(self):
        dependent_record = create_example_slurm_job_record(job_id="99999", attempt_index=0)
        self._job_manager.storage.read_job_record.return_value = dependent_record
        self._job_manager.audit_buffer = MagicMock()

        self._job_manager._trigger_dag_action(
            dependent_record=dependent_record,
            action="release",
            dag_dep_ids=[1, 2],
        )

        self._job_manager.adi_attribution_request_publisher.publish_message.assert_not_called()
        advice_record = self._job_manager.storage.upsert_advice.call_args.args[0]
        self.assertEqual(advice_record.job_id, "99999")
        self.assertEqual(advice_record.attempt_index, 0)
        self.assertEqual(advice_record.advice_state, AdviceState.RELEASED.value)
        audit_event = self._job_manager.audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceCreatedEvent)
        self.assertEqual(audit_event.payload.advice, _expected_advice_audit_snapshot(advice_record))
        self.assertEqual(audit_event.payload.advice["advice"]["state"], AdviceState.RELEASED.value)
        self.assertEqual(audit_event.payload.creation_reasons, ["DAG_DEPENDENCIES_SATISFIED"])
        self.assertEqual(
            [
                call.kwargs["action_status"]
                for call in self._job_manager.storage.update_dag_dependency_status.call_args_list
            ],
            [
                DagDependencyActionStatus.IN_PROGRESS.value,
                DagDependencyActionStatus.IN_PROGRESS.value,
                DagDependencyActionStatus.SUCCESSFUL.value,
                DagDependencyActionStatus.SUCCESSFUL.value,
            ],
        )

    def test_completed_parent_cannot_reopen_terminal_dependent_gate(self):
        completed_parent = create_example_slurm_job_record(job_id="100", state="COMPLETED")
        dependent_record = create_example_slurm_job_record(job_id="200", state="PENDING")
        terminal_dep = DagDependency(
            dependent_job_id=dependent_record.job_id,
            dependent_job_action="release",
            dependency_job_identifier=completed_parent.job_id,
            dependency_job_identifier_type="id",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.ABORTED.value,
        )
        terminal_dep.id = 1

        def _read_dag_dependencies(**kwargs):
            if kwargs.get("dependency_job_identifier") == completed_parent.job_id:
                return ([terminal_dep], 1)
            if kwargs.get("dependent_job_id") == dependent_record.job_id:
                return ([terminal_dep], 1)
            return ([], 0)

        self._job_manager.storage.reset_mock()
        self._job_manager.storage.read_dag_dependencies.side_effect = _read_dag_dependencies
        self._job_manager.storage.read_job_record.return_value = dependent_record

        self._job_manager._evaluate_dag_dependencies_for_completed_job(completed_parent)

        self._job_manager.storage.upsert_advice.assert_not_called()
        self._job_manager.storage.update_dag_dependency_status.assert_not_called()

    def test_dag_action_refuses_retry_attempt(self):
        dependent_record = create_example_slurm_job_record(job_id="99999", attempt_index=1)

        self._job_manager._trigger_dag_action(
            dependent_record=dependent_record,
            action="release",
            dag_dep_ids=[1],
        )

        self._job_manager.storage.upsert_advice.assert_not_called()
        self._job_manager.storage.update_dag_dependency_status.assert_not_called()

    def test_dag_action_persistence_failure_remains_in_progress(self):
        self._job_manager.storage.upsert_advice.side_effect = RuntimeError("database unavailable")
        dependent_record = create_example_slurm_job_record(job_id="99999", attempt_index=0)

        self._job_manager._trigger_dag_action(
            dependent_record=dependent_record,
            action="release",
            dag_dep_ids=[1],
        )

        self.assertEqual(self._job_manager.storage.update_dag_dependency_status.call_count, 1)
        update_call = self._job_manager.storage.update_dag_dependency_status.call_args
        self.assertEqual(update_call.kwargs["action_status"], DagDependencyActionStatus.IN_PROGRESS.value)


class TestDagActionTimeoutReconciliation(unittest.TestCase):
    """Tests for timed-out DAG advice reconciliation."""

    def test_reconcile_timed_out_dag_actions(self):
        """Timed-out IN_PROGRESS actions recreate exact attempt-0 advice."""

        dep = DagDependency(
            dependent_job_id="99999",
            dependent_job_action="release",
            dependency_job_identifier="parent",
            dependency_job_identifier_type="name",
            dependency_trigger_condition="completed",
            action_status=DagDependencyActionStatus.IN_PROGRESS.value,
            action_time=datetime(2026, 4, 15, tzinfo=UTC),
        )
        dep.id = 1

        mock_storage = MagicMock()
        mock_storage.read_dag_dependencies.return_value = ([dep], 1)
        mock_storage.read_advice_by_job.return_value = None
        mock_storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
        dependent_record = create_example_slurm_job_record(job_id="99999", attempt_index=0)
        mock_storage.read_job_record.return_value = dependent_record
        audit_buffer = MagicMock()

        reconcile_timed_out_dag_actions(
            storage=mock_storage,
            timeout_secs=120,
            audit_buffer=audit_buffer,
        )

        advice_record = mock_storage.upsert_advice.call_args.args[0]
        self.assertEqual(advice_record.advice_state, AdviceState.RELEASED.value)
        self.assertEqual(advice_record.attempt_index, 0)
        audit_event = audit_buffer.add.call_args.args[0]
        self.assertIsInstance(audit_event, AdviceCreatedEvent)
        self.assertEqual(audit_event.payload.creation_reasons, ["DAG_DEPENDENCIES_SATISFIED"])
        self.assertEqual(
            [call.kwargs["action_status"] for call in mock_storage.update_dag_dependency_status.call_args_list],
            [
                DagDependencyActionStatus.IN_PROGRESS.value,
                DagDependencyActionStatus.SUCCESSFUL.value,
            ],
        )

    def test_reconcile_no_timed_out_actions(self):
        """When no actions are timed out, nothing should happen."""

        mock_storage = MagicMock()
        mock_storage.read_dag_dependencies.return_value = ([], 0)
        audit_buffer = MagicMock()

        reconcile_timed_out_dag_actions(
            storage=mock_storage,
            timeout_secs=120,
            audit_buffer=audit_buffer,
        )

        mock_storage.upsert_advice.assert_not_called()
        mock_storage.update_dag_dependency_status.assert_not_called()
        audit_buffer.add.assert_not_called()


if __name__ == "__main__":
    unittest.main()
