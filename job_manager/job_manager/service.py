# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging as _logging
import os
import re
import threading
import time as time_module
import uuid
from collections.abc import Callable
from copy import copy
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import pika
import uuid6
from kubernetes import config as k8s_config
from kubernetes.leaderelection.electionconfig import Config as LeaderElectionConfig
from kubernetes.leaderelection.leaderelection import LeaderElection
from kubernetes.leaderelection.resourcelock.configmaplock import ConfigMapLock
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, ProducerEvent
from recovery_libs.event_bus.event_bus_error import EventBusError
from recovery_libs.event_bus.event_bus_utils import (
    RabbitMQConsumer,
    RabbitMQConsumerConfig,
    RabbitMQPublisher,
    RabbitMQPublisherConfig,
    rabbitmq,
)
from recovery_libs.notifications_client import (
    ApiClient,
    Configuration,
    InvalidJobConfig,
    NotificationCreate,
    NotificationsApi,
    TemplateData,
)
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.aps_spec.spec import (
    AutoResumeModeEnum,
    DagActionEnum,
    DagTriggerConditionEnum,
)
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceState,
    ApplicationIntention,
    ApplicationIntentionDesiredState,
    ApplicationIntentionInput,
    JobAnomaly,
    JobAnomalyHandledBy,
    JobCheckpointInfo,
    JobTerminalAdvice,
    JobTimeFilter,
    JobTimeFilterTarget,
    RecoveryContext,
    RecoveryOperation,
    RecoveryTriggerType,
    SlurmJobExecutionStatus,
    SortOrderEnum,
    SystemRequeueConfig,
    filter_anomalies,
)
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    ApplicationIntentionRecord,
    DagDependency,
    DagDependencyActionStatus,
    SlurmJobExecutionRecord,
    SlurmJobFinalStates,
    SlurmJobNonFinalStates,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage, UpsertAdviceResult, UpsertJobResult
from recovery_libs.tracing.recovery import with_recovery_identity
from recovery_libs.tracing.traces import (
    JobAnomalySpanName,
    extract_trace_context_from_rabbitmq_properties,
    extract_trace_id_from_rabbitmq_properties,
    extract_trace_id_from_span,
    restore_trace_context,
)
from recovery_libs.utils.time import parse_event_timestamp, source_timestamp_in_attempt

from job_manager import periodic_tasks, storage_utils
from job_manager.attribution_request import emit_attribution_request_if_needed
from job_manager.audit.application_intentions import audit_application_termination_requested
from job_manager.config import (
    KubernetesLeaderElectionSettings,
    MySQLConnectionSettings,
    RabbitMQConnectionSettings,
)
from job_manager.constants import (
    ANOMALY_ISOLATOR,
    DEFAULT_SYSTEM_MAX_REQUEUE_TIMES,
    INFINITE_SYSTEM_REQUEUE_LIMIT,
    WATCHER,
    WORKFLOW_CONTROLLER,
)
from job_manager.dag_utils import (
    dependency_is_satisfied,
    propagate_terminated_advice,
    reconcile_registered_job,
    refresh_dag_dependency_observability,
    register_dag_dependencies,
)
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.logger import logger
from job_manager.metrics import (
    ANOMALY_ATTRIBUTION_MESSAGE_RECEIVED_COUNTER,
    ANOMALY_MESSAGE_RECEIVED_COUNTER,
    ANOMALY_MESSAGE_REDELIVERY_COUNTER,
    CHECKPOINT_MESSAGE_RECEIVED_COUNTER,
    CHECKPOINT_MESSAGE_REDELIVERY_COUNTER,
    END_ANOMALY_GENERATED_COUNTER,
    EVENT_ATTEMPT_MATCH_BY_LATEST_ATTEMPT,
    EVENT_ATTEMPT_MATCH_BY_LOG_FILE_NAME,
    EVENT_ATTEMPT_MATCH_BY_SLURM_JOB_ATTEMPT,
    EVENT_BUS_RETRYABLE_ERROR_COUNTER,
    EVENT_BUS_UNEXPECTED_ERROR_COUNTER,
    JOB_UPDATE_MESSAGE_RECEIVED_COUNTER,
    JOB_WITH_INVALID_CONFIG_COUNTER,
    LEADER_STATUS_GAUGE,
    STALE_RUNNING_JOBS_GAUGE,
    UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE,
)
from job_manager.notification_utils import build_recovery_notification, invalid_job_config_messages
from job_manager.recovery import RecoveryService
from job_manager.storage_utils import (
    create_advice_issue_from_event,
    create_terminal_advice_for_job,
    maybe_persist_job_released_terminal_advice_from_attribution,
    maybe_persist_requeue_terminal_advice,
    persist_dag_release_advice,
    populate_sticky_exclusion_node_list,
    read_all_dag_dependencies,
    sanitize_log_file_paths_for_retry,
)

MESSAGE_MAX_DELIVERY_TIMES = 5
DEFAULT_MESSAGE_DELIVERY_RETRY_DELAY = 30
SLURM_JOB_FINAL_STATE_SET = {s.value for s in SlurmJobFinalStates}


class ApplicationIntentionReconciliation(Enum):
    """Outcome of applying a durable application intention to one attempt."""

    NOT_REQUESTED = "not_requested"
    TERMINATED = "terminated"
    RETRY_REQUIRED = "retry_required"

    @property
    def blocks_recovery(self) -> bool:
        """Whether a competing recovery decision must be suppressed."""
        return self is not ApplicationIntentionReconciliation.NOT_REQUESTED


# Slurm <23.11 reports a bare COMPLETING state while the epilog runs, masking
# the base final state. The state reason is already available during that
# window, so use it to overlap anomaly handling with the epilog and preserve
# Slurm's MinJobAge requeue window.
COMPLETING_STATE_REASON_TO_FINAL_STATE = {
    "TimeLimit": SlurmJobFinalStates.TIMEOUT.value,
    "NonZeroExitCode": SlurmJobFinalStates.FAILED.value,
    "RaisedSignal": SlurmJobFinalStates.FAILED.value,
}


def resolve_effective_final_state(record: SlurmJobExecutionRecord) -> str | None:
    """Return the terminal state that state-based detection should act on.

    Advisor API records carry scheduler-neutral state in ``record.state`` and
    retain the Slurm base state and reason in scheduler metadata. Legacy job
    updates instead carry the raw Slurm state directly. Supporting both forms
    here keeps terminal-state policy in one place without adding Slurm fields
    to the public Advisor contract.
    """
    scheduler_metadata = record.scheduler_metadata or {}
    if scheduler_metadata.get("scheduler") == "slurm":
        raw_state = scheduler_metadata.get("slurm_state")
        state_reason = scheduler_metadata.get("slurm_reason")
        if isinstance(raw_state, str):
            if raw_state in SLURM_JOB_FINAL_STATE_SET:
                return raw_state
            if raw_state == SlurmJobNonFinalStates.COMPLETING.value and isinstance(state_reason, str):
                return COMPLETING_STATE_REASON_TO_FINAL_STATE.get(state_reason)

    if record.state in SLURM_JOB_FINAL_STATE_SET:
        return record.state
    if record.state == SlurmJobNonFinalStates.COMPLETING.value:
        return COMPLETING_STATE_REASON_TO_FINAL_STATE.get(record.state_reason or "")
    return None


tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()


class _LeaderElectionLogFilter(_logging.Filter):
    """Suppress INFO-level logs emitted by the kubernetes LeaderElection module.

    The upstream library calls ``logging.info()`` on the root logger in a tight
    retry loop, producing excessive noise.  This filter drops any record at INFO
    level (or below) whose source file is the leaderelection module.  WARNING
    and above are still propagated so genuine issues remain visible.
    """

    _SUPPRESSED_MODULE = "leaderelection"

    def filter(self, record: _logging.LogRecord) -> bool:
        return not (record.module == self._SUPPRESSED_MODULE and record.levelno <= _logging.INFO)


def _suppress_leaderelection_root_logs() -> None:
    """Install a filter on the root logger to quiet LeaderElection chatter."""
    root = _logging.getLogger()
    if not any(isinstance(f, _LeaderElectionLogFilter) for f in root.filters):
        root.addFilter(_LeaderElectionLogFilter())


class StorageMode(str, Enum):
    MYSQL = "MYSQL"


def get_current_namespace(context: str | None = None) -> str | None:
    ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    try:
        contexts, active_context = k8s_config.list_kube_config_contexts()
        if contexts is None:
            return active_context["context"]["namespace"]
        selected_context = next(ctx for ctx in contexts if ctx["name"] == context)
        return selected_context["context"]["namespace"]
    except (KeyError, StopIteration):
        return "default"


class JobManager:
    def __init__(
        self,
        config: dict[str, Any],
        shutdown_event: threading.Event,
        audit_buffer: AuditBuffer[ProducerEvent] = NoOpAuditBuffer[ProducerEvent](),
    ) -> None:
        self.config = config
        self.shutdown_event = shutdown_event
        self.audit_buffer = audit_buffer

        self.leader_election_identity: str = str(uuid.uuid4())
        self.is_leader: bool = True  # If leader election is disabled, always act as leader
        self.leader_elector: LeaderElection | None = None
        # Initialize leader election
        if self.config["service"].get("kubernetes_leader_election"):
            # Initialize leader status
            self.is_leader = False

            logger.info(f"Attempting to acquire leader election with identity: {self.leader_election_identity}")

            k8s_settings = KubernetesLeaderElectionSettings(**self.config["service"]["kubernetes_leader_election"])
            # Configure Kubernetes client
            if k8s_settings.in_cluster:
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config(k8s_settings.kubeconfig_path)

            # get the current k8s namespace from the apiserver
            k8s_namespace = get_current_namespace()

            # Create callbacks for leader election events
            def on_start_leading():
                logger.info(f"Started leading with identity: {self.leader_election_identity}")
                self.is_leader = True
                LEADER_STATUS_GAUGE.set(1)

            def on_stop_leading():
                logger.info(f"Stopped leading with identity: {self.leader_election_identity}")
                self.is_leader = False
                LEADER_STATUS_GAUGE.set(0)

            election_cfg = LeaderElectionConfig(
                lock=ConfigMapLock(
                    name=k8s_settings.lease_name,
                    namespace=k8s_namespace,
                    identity=self.leader_election_identity,
                ),
                lease_duration=k8s_settings.lease_duration_seconds,
                renew_deadline=k8s_settings.renew_deadline_seconds,
                retry_period=k8s_settings.retry_period_seconds,
                onstarted_leading=on_start_leading,
                onstopped_leading=on_stop_leading,
            )

            _suppress_leaderelection_root_logs()

            self.leader_elector = LeaderElection(election_config=election_cfg)
            self._leader_election_cfg = election_cfg
            # Start leader election in a background thread with retry logic
            self.leader_election_thread = threading.Thread(target=self._run_leader_election_with_retry, daemon=True)
            self.leader_election_thread.start()
        else:
            logger.info("Leader election is disabled, will act as leader.")

        self.event_bus_consumers: dict[str, RabbitMQConsumer] = {}
        self.adi_attribution_request_publisher: RabbitMQPublisher | None = (
            self.initialize_adi_attribution_request_publisher()
        )
        self.initialize_storage()
        self.recovery_service = RecoveryService(self.storage)
        self.initialize_event_bus_consumers()
        self.initialize_notifications_client()

        self.background_job_threads: dict[str, threading.Thread] = {}

    def _is_dag_support_enabled(self) -> bool:
        return self.config["service"].get("dag_support", {}).get("enabled", False)

    def upsert_advice_record(
        self,
        advice_record: AdviceRecord,
        *,
        audit_reason: str | None = None,
    ) -> UpsertAdviceResult:
        """Persist Advice and audit its committed semantic transition."""
        return storage_utils.persist_advice_record(
            self.storage,
            self.audit_buffer,
            advice_record,
            audit_reason=audit_reason,
        )

    def upsert_job_record(
        self,
        job_record: SlurmJobExecutionRecord,
        *,
        populate_heimdall_id_log_path: bool = False,
    ) -> UpsertJobResult:
        """Persist a Job attempt through the audited storage boundary."""
        return storage_utils.persist_job_record(
            self.storage,
            self.audit_buffer,
            job_record,
            populate_heimdall_id_log_path=populate_heimdall_id_log_path,
        )

    def update_job_record(
        self,
        job_id: str,
        job_attempt_index: int | None,
        **updates: Any,
    ) -> bool:
        """Apply a partial Job update through the audited storage boundary."""
        return storage_utils.update_job_record(
            self.storage,
            self.audit_buffer,
            job_id,
            job_attempt_index,
            **updates,
        )

    def create_application_intention(self, request: ApplicationIntentionInput) -> ApplicationIntention:
        """Persist an application cancellation request without waiting for action.

        The intention is job-scoped and may arrive before Job Manager has seen an
        attempt. Storage therefore accepts it independently of the job table.
        Advice creation is deliberately excluded from this request path so the
        application receives an acknowledgement as soon as the intention is
        durable.
        """
        stored, _created = self.storage.create_or_get_application_intention(
            ApplicationIntentionRecord(
                job_id=request.job_id,
                desired_state=request.desired_state.value,
                reason=request.reason,
                created_at=datetime.now(UTC),
            )
        )
        return ApplicationIntention(
            id=stored.id,
            job_id=stored.job_id,
            desired_state=ApplicationIntentionDesiredState(stored.desired_state),
            reason=stored.reason,
            created_at=stored.created_at,
        )

    def audit_application_intention_for_attempt(self, record: SlurmJobExecutionRecord) -> None:
        """Emit the job-level request event once, tagged to the latest known attempt."""
        try:
            intention, claimed = self.storage.claim_application_intention_audit_attempt(
                record.job_id,
                record.job_attempt_index,
            )
        except RecoveryStorageException as error:
            logger.exception(
                "Failed to associate application termination request audit event with job %s attempt %s",
                record.job_id,
                record.job_attempt_index,
                extra={
                    "job_id": record.job_id,
                    "job_attempt_index": record.job_attempt_index,
                    "error": str(error),
                },
            )
            return
        if claimed and intention is not None:
            audit_application_termination_requested(intention, self.audit_buffer)

    def reconcile_latest_application_intention(self, job_id: str) -> None:
        """Best-effort background reconciliation for a newly accepted request.

        The durable intention remains authoritative if this task is interrupted.
        A later scheduler upsert or attribution result evaluates it again, so an
        HTTP 200 response never depends on this task completing successfully.
        """
        try:
            latest_attempt = self.storage.read_job_record(job_id=job_id)
            if latest_attempt is not None:
                self.audit_application_intention_for_attempt(latest_attempt)
                self.reconcile_application_intention(latest_attempt)
        except Exception as error:
            logger.exception(
                "Background reconciliation failed for application intention on job %s; "
                "a later job update will retry it",
                job_id,
                extra={"job_id": job_id, "error": str(error)},
            )

    def reconcile_application_intention(
        self,
        record: SlurmJobExecutionRecord,
        *,
        fail_closed_on_lookup_error: bool = False,
    ) -> ApplicationIntentionReconciliation:
        """Apply a durable application intention to one observed job attempt.

        The result distinguishes a completed terminal write from one that must
        be retried. Both outcomes block competing recovery Advice. A scheduler
        upsert retries ``RETRY_REQUIRED`` on the next poll, including for an
        attempt first observed after the request was stored.

        Storage lookup failures fail open for ordinary scheduler updates. This
        keeps job ingestion available during a rolling migration where the new
        table may not exist yet. Late attribution passes
        ``fail_closed_on_lookup_error=True`` because publishing competing
        restart Advice before cancellation intent can be checked would violate
        the application's terminal request.
        """
        if not self._is_advice_management_enabled():
            return ApplicationIntentionReconciliation.NOT_REQUESTED

        try:
            intention = self.storage.read_application_intention(record.job_id)
        except RecoveryStorageException as error:
            logger.exception(
                "Failed to read application intention for job %s; continuing without application reconciliation",
                record.job_id,
                extra={"job_id": record.job_id, "error": str(error)},
            )
            return (
                ApplicationIntentionReconciliation.RETRY_REQUIRED
                if fail_closed_on_lookup_error
                else ApplicationIntentionReconciliation.NOT_REQUESTED
            )

        if intention is None:
            return ApplicationIntentionReconciliation.NOT_REQUESTED
        if intention.audit_attempt_index is None:
            self.audit_application_intention_for_attempt(record)
        if intention.desired_state != ApplicationIntentionDesiredState.CANCELLED.value:
            logger.error(
                "Ignoring unsupported application intention state %s for job %s",
                intention.desired_state,
                record.job_id,
            )
            return ApplicationIntentionReconciliation.NOT_REQUESTED

        try:
            existing_advice = self.storage.read_advice_by_job(
                job_id=record.job_id,
                attempt_index=record.job_attempt_index,
            )
        except RecoveryStorageException as error:
            logger.exception(
                "Failed to check application termination Advice for job %s attempt %s",
                record.job_id,
                record.job_attempt_index,
                extra={
                    "job_id": record.job_id,
                    "job_attempt_index": record.job_attempt_index,
                    "error": str(error),
                },
            )
            return ApplicationIntentionReconciliation.RETRY_REQUIRED

        if existing_advice is not None and existing_advice.advice_state == AdviceState.TERMINATED.value:
            return ApplicationIntentionReconciliation.TERMINATED

        persisted = create_terminal_advice_for_job(
            self.storage,
            audit_buffer=self.audit_buffer,
            job_id=record.job_id,
            job_attempt_index=record.job_attempt_index,
            terminal_advice=JobTerminalAdvice.APPLICATION_REQUESTED_CANCELLATION,
            audit_reason=intention.reason,
            status=storage_to_http_job_status(record),
            on_advice_persisted=self.propagate_terminated_advice_for_dag_dependents,
            on_advice_ready=self._begin_sequence_for_advice,
        )
        if persisted is None:
            return ApplicationIntentionReconciliation.RETRY_REQUIRED
        return ApplicationIntentionReconciliation.TERMINATED

    def propagate_terminated_advice_for_dag_dependents(
        self,
        advice_record: AdviceRecord,
        terminal_reason: str | None = None,
    ) -> None:
        """Propagate one durable terminal Advice through the configured DAG backend.

        The callback owns the Job Manager boundary checks so event ingestion
        paths need not depend on the concrete storage adapter.

        Args:
            advice_record: Parent terminal Advice to propagate from.
            terminal_reason: Optional terminal cause included in descendant
                causality evidence.
        """
        if not self._is_dag_support_enabled() or not isinstance(self.storage, MySQLStorage):
            return
        propagate_terminated_advice(
            self.storage,
            advice_record,
            audit_buffer=self.audit_buffer,
            terminal_reason=terminal_reason,
        )

    def register_dag_dependencies_for_new_job(self, record: SlurmJobExecutionRecord) -> None:
        """Register a newly created DAG child and apply already-known parent outcomes.

        This runs only for the initial creation of a job record. Re-running
        registration for every scheduler poll would overwrite a dependency
        action that has already progressed beyond its initial state.
        """
        if not self._is_dag_support_enabled() or not isinstance(self.storage, MySQLStorage):
            return
        if record.aps_spec and record.aps_spec.dag_dependencies:
            self._register_dag_dependencies(record, reconcile_existing_parents=True)

    _LEADER_ELECTION_MAX_BACKOFF_SECONDS = 60

    def _run_leader_election_with_retry(self) -> None:
        """Run leader election in a loop, restarting on failure with exponential backoff."""
        backoff = 1
        while not self.shutdown_event.is_set():
            try:
                logger.info(f"Starting leader election loop with identity: {self.leader_election_identity}")
                self.leader_elector = LeaderElection(election_config=self._leader_election_cfg)
                self.leader_elector.run()
                # run() returns when leadership is lost normally; reset backoff and briefly pause before retrying
                logger.warning("Leader election run() returned, will re-enter election.")
                backoff = 1
                if self.shutdown_event.wait(timeout=2):
                    break
            except Exception:
                was_leader = self.is_leader
                self.is_leader = False
                if was_leader:
                    LEADER_STATUS_GAUGE.set(0)
                logger.exception(
                    f"Leader election failed, will retry in {backoff}s. Identity: {self.leader_election_identity}"
                )
                if self.shutdown_event.wait(timeout=backoff):
                    break
                backoff = min(backoff * 2, self._LEADER_ELECTION_MAX_BACKOFF_SECONDS)

        logger.info("Leader election retry loop exiting due to shutdown.")

    def initialize_notifications_client(self) -> None:
        if not self.config["service"].get("notification_service_url"):
            logger.info("Notification service URL is not configured, skip initializing notifications client.")
            self.notifications_client = None
            return

        notification_service_url = self.config["service"].get("notification_service_url")
        logger.info(f"Initializing notifications client with service URL: {notification_service_url}")
        self.notifications_client = NotificationsApi(ApiClient(Configuration(host=notification_service_url)))

    def initialize_storage(self) -> None:
        logger.info("Initializing storage.")

        storage_mode = self.config["service"]["storage"]["mode"]

        self.storage: MySQLStorage
        match storage_mode:
            case StorageMode.MYSQL:
                mysql_config = MySQLConnectionSettings(**self.config["service"]["storage"]["sql"])
                echo = False
                if self.config["service"].get("log_level", "info").upper() == "DEBUG":
                    echo = True

                self.connection = MySQLConnection(
                    host=mysql_config.host,
                    port=mysql_config.port,
                    username=mysql_config.username,
                    password=mysql_config.password.get_secret_value(),
                    database_name=mysql_config.database_name,
                    echo=echo,
                    ssl_enabled=mysql_config.ssl_enabled,
                    ssl_ca=mysql_config.ssl_ca_path,
                    write_pool_enabled=mysql_config.write_pool_enabled,
                    write_pool_size=mysql_config.write_pool_size,
                    write_max_overflow=mysql_config.write_max_overflow,
                )
                self.storage = MySQLStorage(connection=self.connection)
                self.attribution_storage = AttributionStorage(connection=self.connection)
            case _:
                raise RuntimeError(f"Storage mode {storage_mode} is not supported.")

        try:
            # Use schema upgrade to bootstrap the storage in the first call, and check if the schema is up-to-date in the second call.
            ready = self.storage.bootstrap(schema_upgrade=True) and (
                self.attribution_storage is None or self.attribution_storage.bootstrap(schema_upgrade=False)
            )

            if not ready:
                raise RuntimeError("Storage bootstrap failed with unknown error.")
        except RecoveryStorageException as e:
            logger.exception("Storage bootstrap failed", extra={"error": str(e)})
            ready = self.attribution_storage is None or self.attribution_storage.bootstrap(schema_upgrade=False)
            if not ready:
                raise RuntimeError("Storage bootstrap failed with unknown error.")

    def initialize_adi_attribution_request_publisher(self) -> RabbitMQPublisher | None:
        """Publisher for the AttributionRequest event ADI consumes (its own exchange).

        Built whenever the event bus is configured; emission is gated separately by
        the ``adi_attribution_request_emit_enabled`` flag, so this stays inert until cutover.
        """
        event_bus = self.config["service"].get("event_bus", {})
        if not event_bus:
            return None
        rabbitmq_settings = RabbitMQConnectionSettings(**event_bus)
        return RabbitMQPublisher(
            publisher_config=RabbitMQPublisherConfig(
                host=rabbitmq_settings.host,
                username=rabbitmq_settings.username,
                password=rabbitmq_settings.password.get_secret_value(),
                vhost=rabbitmq_settings.vhost,
                exchange_name=event_bus.get("adi_attribution_request_exchange", "attribution_request_exchange"),
            )
        )

    def _maybe_emit_attribution_request(
        self,
        record: SlurmJobExecutionRecord,
    ) -> ApplicationIntentionReconciliation:
        """Reconcile application intent, then emit attribution once when recovery remains eligible.

        Safe to call from any anomaly-record or terminal-transition path after the
        record is persisted. The fail-closed intention check prevents an attribution
        request from starting a restart sequence while cancellation state is unavailable.
        The emitter then gates on anomaly eligibility and de-dups via an atomic claim, so
        redundant calls are no-ops.
        """
        application_intention_result = self.reconcile_application_intention(
            record,
            fail_closed_on_lookup_error=True,
        )
        if application_intention_result.blocks_recovery:
            return application_intention_result

        event_bus = self.config["service"].get("event_bus", {})
        emit_attribution_request_if_needed(
            record,
            storage=self.storage,
            publisher=self.adi_attribution_request_publisher,
            routing_key=event_bus.get("adi_attribution_request_routing_key", "job.updates.attribution_request"),
            enabled=self.config["service"].get("adi_attribution_request_emit_enabled", False),
            straggler_enabled=self.config["service"].get("adi_straggler_emit_enabled", False),
            recovery_context_provider=lambda: self._recovery_context_for_attribution(record),
        )
        return application_intention_result

    def _recovery_context_for_attribution(self, record: SlurmJobExecutionRecord) -> Context | None:
        anomalies = record.anomalies or []
        trigger = anomalies[0] if anomalies else None
        recovery_context = self.recovery_service.begin_for_job(
            record,
            operation=RecoveryOperation.RESTART,
            trigger_type=(RecoveryTriggerType.ANOMALY if trigger is not None else RecoveryTriggerType.JOB_STATE),
            trigger_identifier=(str(trigger.uuid) if trigger is not None and trigger.uuid is not None else None),
            triggered_at=(trigger.timestamp if trigger is not None else record.end_time),
        )
        parent = restore_trace_context(recovery_context.traceparent, recovery_context.tracestate)
        return (
            with_recovery_identity(parent, recovery_context.sequence_id, recovery_context.operation.value)
            if parent is not None
            else None
        )

    def _begin_sequence_for_advice(
        self,
        advice: Advice,
        *,
        trigger_type: RecoveryTriggerType | None = None,
    ) -> RecoveryContext | None:
        """Attach the canonical durable context before actionable Advice is written."""
        try:
            record = self.storage.read_job_record(
                job_id=advice.job_id,
                job_attempt_index=advice.restart_count,
            )
            if record is None:
                logger.warning(
                    "Cannot begin recovery sequence for Advice %s: job %s attempt %s is missing.",
                    advice.id,
                    advice.job_id,
                    advice.restart_count,
                )
                return None

            state = advice.advice.state if advice.advice is not None else None
            operation_by_state: dict[AdviceState | None, RecoveryOperation] = {
                AdviceState.RESTARTED: RecoveryOperation.RESTART,
                AdviceState.TERMINATED: RecoveryOperation.TERMINATE,
                AdviceState.RELEASED: RecoveryOperation.DAG_RELEASE,
            }
            operation = operation_by_state.get(state)
            if operation is None:
                return None

            anomalies = record.anomalies or []
            trigger = anomalies[0] if anomalies else None
            effective_trigger_type = trigger_type
            if effective_trigger_type is None:
                if operation == RecoveryOperation.DAG_RELEASE:
                    effective_trigger_type = RecoveryTriggerType.DEPENDENCY
                elif trigger is not None:
                    effective_trigger_type = RecoveryTriggerType.ANOMALY
                else:
                    effective_trigger_type = RecoveryTriggerType.JOB_STATE

            return self.recovery_service.begin_for_job(
                record,
                operation=operation,
                scheduler_metadata=advice.scheduler_metadata,
                trigger_type=effective_trigger_type,
                trigger_identifier=(str(trigger.uuid) if trigger is not None and trigger.uuid is not None else None),
                triggered_at=(
                    None
                    if operation == RecoveryOperation.DAG_RELEASE
                    else trigger.timestamp
                    if trigger is not None
                    else record.end_time
                ),
            )
        except Exception as error:
            logger.exception(
                "Failed to begin recovery sequence for Advice %s; continuing without correlation",
                advice.id,
                extra={"advice_id": advice.id, "error": str(error)},
            )
            return None

    def initialize_event_bus_consumers(self) -> None:
        logger.info("Initializing event bus consumers.")
        service_config = self.config["service"]

        rabbitmq_settings = RabbitMQConnectionSettings(**service_config["event_bus"])

        watcher_consumer_config = RabbitMQConsumerConfig(
            host=rabbitmq_settings.host,
            username=rabbitmq_settings.username,
            password=rabbitmq_settings.password.get_secret_value(),
            vhost=rabbitmq_settings.vhost,
            exchange_name=service_config["event_bus"]["anomaly_exchange"],
            queue_name=service_config["event_bus"]["anomaly_queue"],
            routing_keys=[
                service_config["event_bus"]["anomaly_routing_key"],
            ],
            callback_func=self.get_watcher_job_event_message_callback(),
            delay_secs=service_config["event_bus"].get(
                "anomaly_queue_delivery_retry_delay_secs",
                DEFAULT_MESSAGE_DELIVERY_RETRY_DELAY,
            ),
            dead_letter_routing_key=service_config["event_bus"].get("anomaly_dead_letter_routing_key"),
            dead_letter_redelivery_routing_key=service_config["event_bus"].get(
                "anomaly_dead_letter_redelivery_routing_key"
            ),
        )

        workflow_controller_consumer_config = RabbitMQConsumerConfig(
            host=rabbitmq_settings.host,
            username=rabbitmq_settings.username,
            password=rabbitmq_settings.password.get_secret_value(),
            vhost=rabbitmq_settings.vhost,
            exchange_name=service_config["event_bus"]["workflow_controller_exchange"],
            exchange_type=service_config["event_bus"].get("workflow_controller_exchange_type", "direct"),
            queue_name=service_config["event_bus"]["job_queue"],
            routing_keys=[service_config["event_bus"]["job_routing_key"]],
            callback_func=self.get_job_update_message_callback(),
        )

        anomaly_isolator_consumer_config = RabbitMQConsumerConfig(
            host=rabbitmq_settings.host,
            username=rabbitmq_settings.username,
            password=rabbitmq_settings.password.get_secret_value(),
            vhost=rabbitmq_settings.vhost,
            exchange_name=service_config["event_bus"]["anomaly_isolator_exchange"],
            exchange_type=service_config["event_bus"].get("anomaly_isolator_exchange_type", "direct"),
            queue_name=service_config["event_bus"]["anomaly_attribution_queue"],
            routing_keys=[service_config["event_bus"]["anomaly_attribution_routing_key"]],
            callback_func=self.get_anomaly_isolator_message_callback(),
            delay_secs=service_config["event_bus"].get(
                "anomaly_queue_delivery_retry_delay_secs",
                DEFAULT_MESSAGE_DELIVERY_RETRY_DELAY,
            ),
            dead_letter_routing_key=service_config["event_bus"].get("anomaly_dead_letter_routing_key"),
            dead_letter_redelivery_routing_key=service_config["event_bus"].get(
                "anomaly_dead_letter_redelivery_routing_key"
            ),
        )

        self.event_bus_consumers[WATCHER] = RabbitMQConsumer(
            consumer_config=watcher_consumer_config,
            thread_event=self.shutdown_event,
            EVENT_BUS_RETRYABLE_ERROR_COUNTER=EVENT_BUS_RETRYABLE_ERROR_COUNTER,
            EVENT_BUS_UNEXPECTED_ERROR_COUNTER=EVENT_BUS_UNEXPECTED_ERROR_COUNTER,
        )
        self.event_bus_consumers[WORKFLOW_CONTROLLER] = RabbitMQConsumer(
            consumer_config=workflow_controller_consumer_config,
            thread_event=self.shutdown_event,
            EVENT_BUS_RETRYABLE_ERROR_COUNTER=EVENT_BUS_RETRYABLE_ERROR_COUNTER,
            EVENT_BUS_UNEXPECTED_ERROR_COUNTER=EVENT_BUS_UNEXPECTED_ERROR_COUNTER,
        )
        self.event_bus_consumers[ANOMALY_ISOLATOR] = RabbitMQConsumer(
            consumer_config=anomaly_isolator_consumer_config,
            thread_event=self.shutdown_event,
            EVENT_BUS_RETRYABLE_ERROR_COUNTER=EVENT_BUS_RETRYABLE_ERROR_COUNTER,
            EVENT_BUS_UNEXPECTED_ERROR_COUNTER=EVENT_BUS_UNEXPECTED_ERROR_COUNTER,
        )

    def should_send_end_event(self, record: SlurmJobExecutionRecord, state: str) -> bool:
        """
        Method to determine whether an end event should be sent for a given job record.

        Args:
            record (SlurmJobExecutionRecord): The job record to check.
            state (str): The current state the job is at.

        Returns:
            bool: True if an end event should be sent, False otherwise.
        """

        filtered_anomalies = filter_anomalies(anomalies=record.anomalies, later_than=record.submit_time)

        if not filtered_anomalies:
            if not self.config["service"].get("job_end_event_generation_enabled", True):
                return False

            if state != SlurmJobFinalStates.COMPLETED.value and state != SlurmJobFinalStates.TIMEOUT.value:
                logger.info("Job is not in the COMPLETED or TIMEOUT state, skip sending the job end event.")
                return False

            job_manager_environment = self.config["service"].get("environment", None)
            # This is the default behavior before the "environment" config field was not introduced. Keeping it for
            # backward compatibility.
            if not job_manager_environment:
                logger.debug(
                    f"Job manager environment is not configured, sending the job END event for {record.job_id}."
                )
                return True

            # Check if record's environment matches the job manager's environment.
            if record.aps_spec and record.aps_spec.environment:
                return record.aps_spec.environment == job_manager_environment

            # Fallback to "prod" if record's environment is unspecified.
            return job_manager_environment == "prod"
        else:
            logger.info(
                f"Previous job anomalies found for job {record.job_id}, attempt {record.job_attempt_index}: "
                f"{[str((anomaly.uuid, anomaly.anomaly_type, anomaly.handled_by)) for anomaly in filtered_anomalies]}, "
                "skipping job end event generation."
            )
            return False

    def _is_advice_enabled(self) -> bool:
        """Return whether automatic advice persistence is enabled in service config.

        Reads ``self.config["service"].get("advice_enabled", False)`` and returns
        a boolean indicating whether automatic advice persistence should run.
        Defaults to ``False`` when the key is absent and has no side effects.
        """
        return self.config["service"].get("advice_enabled", False)

    def _is_advice_management_enabled(self) -> bool:
        """Return whether JM advice management (terminal advice persistence) is enabled."""
        return self.config["service"].get("advice_management_enabled", False)

    def _is_exclusion_node_stickiness_enabled(self) -> bool:
        return self.config["service"].get("exclusion_node_stickiness_enabled", True)

    def _is_recovery_notifications_enabled(self) -> bool:
        """Whether JM emits the dashboard recovery notifications (requeued / max-requeue).

        Default off so it can be enabled at cutover.
        """
        return self.config["service"].get("recovery_notifications_enabled", False)

    def _maybe_send_recovery_notification(
        self, job_record: SlurmJobExecutionRecord, terminal_advice: JobTerminalAdvice
    ) -> None:
        """Persist the dashboard notification for a recovery terminal advice (gated, no-op if off)."""
        if not self._is_recovery_notifications_enabled() or self.notifications_client is None:
            return
        template_data = build_recovery_notification(job_record, terminal_advice)
        if template_data is not None:
            self.send_notification(template_data=template_data, job_record=job_record)

    def _build_system_requeue_config(self) -> SystemRequeueConfig:
        return SystemRequeueConfig(
            max_requeue_times=self.get_system_max_requeue_times(),
            checkpoint_based_requeue_stop_threshold=self.get_checkpoint_based_requeue_stop_threshold(),
            requeue_on_completion=self.get_requeue_on_completion_config(),
        )

    def _get_previous_attempt_status(self, job_id: str, job_attempt_index: int) -> SlurmJobExecutionStatus | None:
        record = self.storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)
        if record is None:
            return None
        return storage_to_http_job_status(record)

    def _get_jobs_for_requeue_check(
        self, job_id: str, limit: int, offset: int, **_kwargs: Any
    ) -> list[SlurmJobExecutionStatus]:
        records, _ = self.storage.read_job_records(
            job_id=job_id,
            sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
            limit=limit,
            offset=offset,
        )
        return [storage_to_http_job_status(record) for record in records]

    def send_notification(self, template_data: TemplateData, job_record: SlurmJobExecutionRecord) -> None:
        try:
            self.notifications_client.notification_create(  # type: ignore[union-attr]
                NotificationCreate(
                    recipient=job_record.user,
                    template_data=template_data,
                    job_id=job_record.job_id,
                )
            )
        except Exception as e:
            logger.exception(
                f"Failed to send notification for job {job_record.job_id}, attempt index {job_record.job_attempt_index}",
                extra={
                    "job_id": job_record.job_id,
                    "job_attempt_index": job_record.job_attempt_index,
                    "error": str(e),
                },
            )

    def should_send_crash_anomaly(self, record: SlurmJobExecutionRecord, state: str) -> bool:
        """Return whether a state-based CRASH should be emitted for ``state``."""
        if state != SlurmJobFinalStates.FAILED.value:
            return False

        if not record.aps_spec or record.aps_spec.anomaly_detection_config.log_based_crash_enabled is not False:
            logger.debug(
                f"Job {record.job_id} attempt {record.job_attempt_index}: "
                f"log_based_crash_enabled is {record.aps_spec.anomaly_detection_config.log_based_crash_enabled if record.aps_spec else 'N/A (no aps_spec)'}, skipping state-based CRASH generation."
            )
            return False

        filtered_anomalies = filter_anomalies(anomalies=record.anomalies, later_than=record.submit_time)
        if filtered_anomalies:
            logger.info(
                f"Previous job anomalies found for job {record.job_id}, attempt {record.job_attempt_index}: "
                f"{[str((anomaly.uuid, anomaly.anomaly_type, anomaly.handled_by)) for anomaly in filtered_anomalies]}, "
                "skipping state-based CRASH anomaly generation."
            )
            return False

        return True

    def _generate_anomaly_event(
        self,
        record: SlurmJobExecutionRecord,
        event_type: EventType,
        base_anomalies: list[JobAnomaly] | None,
        *,
        event_body: str | None = None,
        mark_detection_span_ok: bool = False,
        handled_by: JobAnomalyHandledBy = JobAnomalyHandledBy.UNHANDLED,
        processed: bool = False,
    ) -> Event:
        """Generate and trace an anomaly for the caller to persist before Advice or ADI emission."""
        detection_time_ns = time_module.time_ns()
        event_kwargs: dict[str, Any] = {
            "type": event_type,
            "timestamp": str(detection_time_ns),
            "attributes": Attributes(
                job_id=record.job_id,
                detection_time=str(detection_time_ns),
                uuid=str(uuid6.uuid7()),
                slurm_job_attempt=record.job_attempt_index,
                job_manager_generated=True,
            ),
        }
        if event_body is not None:
            event_kwargs["body"] = event_body
        event = Event(**event_kwargs)

        detection_span = tracer.start_span(
            name=f"job_anomaly.{JobAnomalySpanName.DETECTION}",
            kind=SpanKind.PRODUCER,
            attributes={
                "job_id": record.job_id,
                "job_attempt_index": record.job_attempt_index,
                "job_anomaly.type": event.type,
                "job_anomaly.uuid": event.attributes.uuid,  # type: ignore[union-attr]
            },
            start_time=(int(record.end_time.timestamp() * 1_000_000_000) if record.end_time else detection_time_ns),
        )
        if mark_detection_span_ok:
            detection_span.set_status(StatusCode.OK)
        detection_span.end(end_time=detection_time_ns)

        anomalies = list(base_anomalies) if base_anomalies else []
        anomalies.append(
            JobAnomaly(
                anomaly_type=event.type,
                timestamp=datetime.fromtimestamp(int(event.timestamp) / 1e9, UTC),  # type: ignore[arg-type]
                uuid=event.attributes.uuid,  # type: ignore[union-attr]
                handled_by=handled_by,
                processed=processed,
                trace_id=extract_trace_id_from_span(detection_span),
                original_watcher_anomaly_event=event.model_dump_json(),
            )
        )
        record.anomalies = anomalies
        return event

    def persist_anomaly_advice(
        self,
        record: SlurmJobExecutionRecord,
        event: Event,
        *,
        detected_by: str = "Heimdall Watcher",
    ) -> None:
        """Record the original event's causal Issue after its Job write has succeeded."""
        if self._is_advice_enabled():
            create_advice_issue_from_event(
                storage=self.storage,
                audit_buffer=self.audit_buffer,
                event=event,
                job_record=record,
                timestamp_parser=parse_event_timestamp,
                detected_by=(
                    "Heimdall Job Manager"
                    if event.attributes and (event.attributes.model_extra or {}).get("job_manager_generated") is True
                    else detected_by
                ),
                on_advice_persisted=self.propagate_terminated_advice_for_dag_dependents,
            )

    def reconcile_anomaly_advice(self, record: SlurmJobExecutionRecord) -> None:
        """Retry missing Advice from committed causes before terminal recovery or attribution."""
        if self._is_advice_enabled():
            storage_utils.create_advice_issues_from_anomalies(
                self.storage,
                record,
                audit_buffer=self.audit_buffer,
                on_advice_persisted=self.propagate_terminated_advice_for_dag_dependents,
            )

    def _generate_crash_event(
        self,
        record: SlurmJobExecutionRecord,
        base_anomalies: list[JobAnomaly] | None,
    ) -> Event:
        logger.info(
            f"Job {record.job_id} attempt {record.job_attempt_index} reached FAILED state "
            f"(raw state: {record.state}, state_reason: {record.state_reason}) with "
            "anomaly_detection_config.log_based_crash_enabled=False. Generating immediate CRASH anomaly."
        )
        event = self._generate_anomaly_event(
            record,
            EventType.CRASH,
            base_anomalies,
            event_body="State-based CRASH event for failed job",
            mark_detection_span_ok=True,
            handled_by=JobAnomalyHandledBy.HEIMDALL,
            processed=True,
        )
        logger.info(
            f"State-based CRASH anomaly {event.attributes.uuid} created for job "  # type: ignore[union-attr]
            f"{record.job_id} attempt {record.job_attempt_index}."
        )
        return event

    def _generate_end_event(
        self,
        record: SlurmJobExecutionRecord,
        base_anomalies: list[JobAnomaly] | None,
    ) -> Event:
        event = self._generate_anomaly_event(
            record,
            EventType.END,
            base_anomalies,
        )
        END_ANOMALY_GENERATED_COUNTER.inc(1)
        return event

    def generate_state_based_crash_anomaly_if_needed(self, record: SlurmJobExecutionRecord) -> bool:
        """Compatibility wrapper for callers that already hold a raw FAILED record."""
        state = resolve_effective_final_state(record)
        if state is None or not self.should_send_crash_anomaly(record=record, state=state):
            return False
        self._generate_crash_event(record=record, base_anomalies=record.anomalies)
        return True

    def get_anomaly_isolator_message_callback(
        self,
    ) -> Callable[
        [
            pika.adapters.blocking_connection.BlockingChannel,
            pika.spec.Basic.Deliver,
            pika.spec.BasicProperties,
            bytes,
        ],
        None,
    ]:
        def process_anomaly_isolator_message(
            channel: pika.channel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            trace_context = extract_trace_context_from_rabbitmq_properties(event_body=body, properties=properties)
            with tracer.start_as_current_span(
                name=f"job_anomaly.{JobAnomalySpanName.ATTRIBUTION_PERSISTENCE}",
                kind=SpanKind.CONSUMER,
                context=trace_context,
            ) as span:
                try:
                    ANOMALY_ATTRIBUTION_MESSAGE_RECEIVED_COUNTER.inc(1)
                    message = body.decode()
                    logger.info(f"Received anomaly attribution result message: {message}")
                    json_dict = json.loads(message)
                    res = AnomalyAttributionResult.from_json(json_dict)

                    span.set_attribute("job_id", res.job_id)
                    span.set_attribute("job_attempt_index", res.job_attempt_index)
                    span.set_attribute("job_anomaly.type", res.anomaly_type)
                    span.set_attribute("job_anomaly.uuid", res.anomaly_uuid)

                    # The application may have requested cancellation after
                    # this attempt was observed but before attribution
                    # completed. Re-check the durable job-level intent for the
                    # exact attributed attempt so a late result cannot publish
                    # Restarted Advice after the application asked recovery to
                    # stop.
                    attributed_record = self.storage.read_job_record(
                        job_id=res.job_id,
                        job_attempt_index=res.job_attempt_index,
                    )
                    application_intention_result = (
                        self.reconcile_application_intention(
                            attributed_record,
                            fail_closed_on_lookup_error=True,
                        )
                        if attributed_record is not None
                        else ApplicationIntentionReconciliation.NOT_REQUESTED
                    )
                    if application_intention_result is ApplicationIntentionReconciliation.RETRY_REQUIRED:
                        self._redeliver_anomaly_attribution_result(channel, method, properties, body, res)
                        span.set_status(
                            Status(StatusCode.ERROR, "Application intention reconciliation requires retry.")
                        )
                        return

                    # Reconcile the cancellation guard before these writes so
                    # delayed redelivery cannot duplicate attribution data.
                    for job_execution_error, node_events in res.node_attributions.items():
                        self.attribution_storage.store_attribution(job_execution_error, node_events)  # type: ignore[arg-type]
                    for job_execution_error, global_events in res.global_attributions.items():
                        self.attribution_storage.store_attribution(job_execution_error, global_events)  # type: ignore[arg-type]

                    logger.info(
                        f"Successfully persisted the anomaly attribution for job {res.job_id} result in the database."
                    )

                    if self._is_advice_management_enabled() and not application_intention_result.blocks_recovery:
                        maybe_persist_job_released_terminal_advice_from_attribution(
                            self.storage,
                            res,
                            audit_buffer=self.audit_buffer,
                            exclusion_stickiness_enabled=self._is_exclusion_node_stickiness_enabled(),
                            get_previous_attempt=self._get_previous_attempt_status,
                            system_requeue_config=self._build_system_requeue_config(),
                            get_jobs=self._get_jobs_for_requeue_check,
                            on_persisted=self._maybe_send_recovery_notification,
                            on_advice_persisted=self.propagate_terminated_advice_for_dag_dependents,
                            on_advice_ready=self._begin_sequence_for_advice,
                        )

                    span.set_status(StatusCode.OK)
                except Exception as e:
                    logger.exception(
                        f"Failed to process anomaly attribution result message. Raw message: {body.decode()}",
                        extra={"error": str(e)},
                    )
                    span.set_status(Status(StatusCode.ERROR, "Failed to persist anomaly attribution result."))

        return process_anomaly_isolator_message

    def _redeliver_anomaly_attribution_result(
        self,
        channel: pika.channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
        result: AnomalyAttributionResult,
    ) -> None:
        """Delay a transiently blocked attribution result using the existing retry route."""
        # This exact result is the only input that can produce its recovery
        # Advice, so it remains retryable until reconciliation succeeds.
        self._redeliver_message(
            channel,
            method,
            properties,
            body,
            description=f"Anomaly attribution for job {result.job_id}",
            max_delivery_times=None,
        )

    def _redeliver_message(
        self,
        channel: pika.channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
        *,
        description: str,
        max_delivery_times: int | None,
    ) -> bool:
        """Publish one message through the configured delayed route when eligible."""
        routing_key = self.config["service"]["event_bus"].get("anomaly_dead_letter_routing_key")
        if not routing_key:
            logger.error("%s cannot be retried because the dead-letter route is not configured", description)
            return False

        headers = properties.headers or {}
        delivery_times = int(headers.get("message_delivery_times", 0))
        if max_delivery_times is not None and delivery_times >= max_delivery_times:
            logger.error("%s exceeded its maximum delayed deliveries", description)
            return False

        retry_properties = copy(properties)
        retry_properties.headers = {**headers, "message_delivery_times": delivery_times + 1}
        rabbitmq.publish_message(
            channel=channel,
            exchange_name=method.exchange,
            routing_key=routing_key,
            message_body=body,  # type: ignore[arg-type]
            properties=retry_properties,
        )
        return True

    def get_job_update_message_callback(
        self,
    ) -> Callable[
        [
            pika.adapters.blocking_connection.BlockingChannel,
            pika.spec.Basic.Deliver,
            pika.spec.BasicProperties,
            bytes,
        ],
        None,
    ]:
        def process_job_updates(
            channel: pika.channel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            try:
                JOB_UPDATE_MESSAGE_RECEIVED_COUNTER.inc(1)
                # Per contract, the event_data should be the JSON serialization of the
                # SlurmJobExecutionRecord so it can be deserialized directly.
                json_dict = json.loads(body.decode())

                record = SlurmJobExecutionRecord.from_json(json_dict)
                logger.debug(f"Persisting a job update message to storage, raw message: {json_dict}")
                latest_job_attempt_index = self.storage.get_job_latest_attempt_index(record.job_id)
                logger.debug(f"Latest job attempt index for job '{record.job_id}' is: {latest_job_attempt_index}")
                # job_attempt_index is marked as optional in the data class but it should always be set, this is to
                # guard against the rare case when it's not available.
                job_end_event: Event | None = None
                job_crash_event: Event | None = None
                if record.job_attempt_index is None:
                    logger.error(
                        f"Malformed job record data: job_attempt_index for job {record.job_name} "
                        "must be provided, skipping save."
                    )
                    logger.error(f"Raw message: {json_dict}.")
                elif record.job_attempt_index > latest_job_attempt_index:
                    effective_final_state = resolve_effective_final_state(record)
                    if effective_final_state is not None:
                        logger.info(
                            f"First time seeing job {record.job_id} reach terminal state "
                            f"(effective final state: {effective_final_state}, raw state: {record.state}, "
                            f"state_reason: {record.state_reason}). "
                            f"Raw update message: {body.decode()}"
                        )
                        if self.should_send_crash_anomaly(record=record, state=effective_final_state):
                            job_crash_event = self._generate_crash_event(record=record, base_anomalies=None)
                        elif self.should_send_end_event(record=record, state=effective_final_state):
                            job_end_event = self._generate_end_event(record=record, base_anomalies=None)

                    # Note to future: Unless absolutely necessary, all job config validations should be done at the
                    # producer side to simplify the logic here.
                    if record.config_validation_failure_types:
                        notification_segments = []
                        for failure_type in record.config_validation_failure_types:
                            JOB_WITH_INVALID_CONFIG_COUNTER.labels(failure_type=failure_type.name).inc(1)
                            if failure_type in invalid_job_config_messages:
                                notification_segments.append(
                                    f"{failure_type.name}: {invalid_job_config_messages[failure_type]}"
                                )
                            else:
                                logger.warning(
                                    f"No explanation message defined for config validation failure type {failure_type.name}."
                                )

                        if notification_segments and self.notifications_client:
                            self.send_notification(
                                template_data=TemplateData(
                                    InvalidJobConfig(
                                        job_id=record.job_id,
                                        job_attempt_index=record.job_attempt_index,
                                        job_name=record.job_name,
                                        reason="\n".join(notification_segments),
                                        user=record.user,
                                    )
                                ),
                                job_record=record,
                            )

                    record.last_state_transition_time = record.last_updated_at
                    logger.info(
                        f"Creating job record for job {record.job_id}, attempt {record.job_attempt_index}, "
                        f"raw payload: {json_dict}"
                    )
                    sanitize_log_file_paths_for_retry(record, self.storage)
                    self.storage.create_job_record(job_record=record)
                    self.reconcile_anomaly_advice(record)

                    if self._is_dag_support_enabled():
                        # Register DAG dependencies if the job has them
                        if record.aps_spec and record.aps_spec.dag_dependencies:
                            self._register_dag_dependencies(record)

                        if record.state != SlurmJobFinalStates.COMPLETED.value:
                            self.refresh_dag_dependency_observability_for_observed_job(record)

                        # Evaluate DAG dependencies if the job completed (new attempt already in final COMPLETED state)
                        if record.state == SlurmJobFinalStates.COMPLETED.value:
                            self._evaluate_dag_dependencies_for_completed_job(record)

                    # A request can be accepted before this consumer first
                    # observes the job. Register DAG edges before creating
                    # terminal Advice, then suppress any competing attribution
                    # request for the cancelled attempt.
                    self._maybe_emit_attribution_request(record)
                else:
                    known_record = self.storage.read_job_record(record.job_id, record.job_attempt_index)
                    if known_record is None:
                        logger.warning(
                            f"Job record not found for job {record.job_id}, attempt {record.job_attempt_index} "
                            f"despite latest attempt index being {latest_job_attempt_index}. "
                            f"Creating as new record. Raw message: {json_dict}"
                        )
                        record.last_state_transition_time = record.last_updated_at
                        sanitize_log_file_paths_for_retry(record, self.storage)
                        self.storage.create_job_record(job_record=record)

                        if self._is_dag_support_enabled():
                            # Register DAG dependencies if the job has them
                            # TODO: Handle corner cases where dag action condition is already met or will never be met
                            if record.aps_spec and record.aps_spec.dag_dependencies:
                                self._register_dag_dependencies(record)

                            self.refresh_dag_dependency_observability_for_observed_job(record)

                        self.reconcile_application_intention(record)
                        return
                    effective_final_state = resolve_effective_final_state(record)
                    known_effective_final_state = resolve_effective_final_state(known_record)
                    if effective_final_state is not None and known_effective_final_state is None:
                        logger.info(
                            f"First time seeing job {record.job_id} reach terminal state "
                            f"(effective final state: {effective_final_state}, raw state: {record.state}, "
                            f"state_reason: {record.state_reason}). "
                            f"Raw update message: {body.decode()}"
                        )
                        if self.should_send_crash_anomaly(record=known_record, state=effective_final_state):
                            job_crash_event = self._generate_crash_event(
                                record=record, base_anomalies=known_record.anomalies
                            )
                        elif self.should_send_end_event(record=known_record, state=effective_final_state):
                            job_end_event = self._generate_end_event(
                                record=record, base_anomalies=known_record.anomalies
                            )

                    # Downstream final-state work remains tied to the actual
                    # state transition, not the early COMPLETING observation.
                    if (
                        (
                            record.state in SLURM_JOB_FINAL_STATE_SET
                            and known_record.state not in SLURM_JOB_FINAL_STATE_SET
                            and record.state != known_record.state
                        )
                        and self._is_dag_support_enabled()
                        and record.state == SlurmJobFinalStates.COMPLETED.value
                    ):
                        self._evaluate_dag_dependencies_for_completed_job(record)

                    if record.state != known_record.state or record.state_reason != known_record.state_reason:
                        # When either state or state_reason changes, the last_state_transition_time should be
                        # updated to the last_updated_at time.
                        record.last_state_transition_time = record.last_updated_at

                    if record.state == SlurmJobNonFinalStates.RUNNING:
                        # Slurm sometimes doesn't clear the pervious state reason after the job starts running, to avoid
                        # showing confusing information, we explicitly clear it at RUNNING state. This doesn't prevent
                        # the state_reason to be reset in later stages of the job.
                        logger.debug(
                            f"Explicitly setting the state_reason for job {record.job_id} from {record.state_reason} "
                            "to 'None' since it's already running."
                        )
                        record.state_reason = "None"

                    # Only checks whether the value is None to avoid unnecessary function call when the value is set
                    # to an empty list.
                    if known_record.sticky_exclusion_node_list is None:
                        populate_sticky_exclusion_node_list(
                            self.storage,
                            self.attribution_storage,
                            record,
                            known_record,
                        )

                    # Only validate log file paths if they were not set before.
                    if not known_record.log_file_paths and record.log_file_paths:
                        sanitize_log_file_paths_for_retry(record, self.storage)

                    # Note to future: Unless absolutely necessary, all job config validations should be done at the
                    # producer side to simplify the consumer logic here.
                    if record.config_validation_failure_types and not known_record.config_validation_failure_types:
                        notification_segments = []
                        for failure_type in record.config_validation_failure_types:
                            JOB_WITH_INVALID_CONFIG_COUNTER.labels(failure_type=failure_type.name).inc(1)
                            if failure_type in invalid_job_config_messages:
                                notification_segments.append(
                                    f"{failure_type.name}: {invalid_job_config_messages[failure_type]}"
                                )
                            else:
                                logger.warning(
                                    f"No explanation message defined for config validation failure type {failure_type.name}."
                                )

                        if notification_segments and self.notifications_client:
                            self.send_notification(
                                template_data=TemplateData(
                                    InvalidJobConfig(
                                        job_id=record.job_id,
                                        job_attempt_index=record.job_attempt_index,
                                        job_name=record.job_name,
                                        reason="\n".join(notification_segments),
                                        user=record.user,
                                    )
                                ),
                                job_record=record,
                            )

                    if (
                        record.state != known_record.state
                        or (record.log_file_paths and record.log_file_paths != known_record.log_file_paths)
                        or record.start_time != known_record.start_time
                        or record.end_time != known_record.end_time
                    ):
                        logger.info(
                            f"Updating job record for job {record.job_id}, attempt {record.job_attempt_index}, "
                            f"raw payload: {json_dict}"
                        )

                    # Trace persistence for either state-based event. END and
                    # CRASH are mutually exclusive for a single transition.
                    persisted_anomaly_event = job_end_event or job_crash_event
                    if persisted_anomaly_event:
                        with tracer.start_as_current_span(
                            name=f"job_anomaly.{JobAnomalySpanName.PERSISTENCE}",
                            kind=SpanKind.INTERNAL,
                            attributes={
                                "job_id": record.job_id,
                                "job_attempt_index": record.job_attempt_index,
                                "job_anomaly.type": persisted_anomaly_event.type,
                                "job_anomaly.uuid": persisted_anomaly_event.attributes.uuid,  # type: ignore[union-attr]
                            },
                        ) as persistence_span:
                            self.update_job_record(**asdict(record))
                            persistence_span.set_status(StatusCode.OK)
                    else:
                        self.update_job_record(**asdict(record))

                    if not record.anomalies:
                        record.anomalies = known_record.anomalies
                    self.reconcile_anomaly_advice(record)
                    self._maybe_emit_attribution_request(record)
            except EventBusError as e:
                logger.exception(f"Event bus error. Raw message: {body.decode()}", extra={"error": str(e)})
            except RecoveryStorageException as e:
                logger.exception(f"Persistence failed. Raw message: {body.decode()}", extra={"error": str(e)})
            except Exception as e:
                logger.exception(
                    f"Failed to process job update message. Raw message: {body.decode()}",
                    extra={"error": str(e)},
                )

        return process_job_updates

    def _find_matching_job_attempt_for_event(self, job_id: str, event: Event) -> SlurmJobExecutionRecord | None:
        """
        Find the matching job attempt for the given event using a three-tier strategy:
          1. If slurm_job_attempt is present, look up by (job_id, job_attempt_index).
          2. Match a log filename within its attempt execution window. Shared paths
             require a source timestamp and a known execution start.
          3. Unknown filenames require the same time evidence for the latest attempt.

        An explicit attempt that has not registered yet must wait for registration;
        it must never fall through to a different attempt.
        """
        if not event.attributes:
            logger.warning(
                f"Event attributes are missing for event ({event}), unable to find matching job attempt of '{job_id}'."
            )
            return None

        try:
            # Explicit identity is authoritative, including while registration is pending.
            if (event.attributes.model_extra or {}).get("slurm_job_attempt") is not None:
                return self._match_by_slurm_job_attempt(job_id, event)

            # --- Tier 2: log file name matching ---
            attempt = self._match_by_log_file_name(job_id, event)
            if attempt is not None:
                return attempt

            # --- Tier 3: latest job attempt ---
            attempt = self._match_latest_job_attempt(job_id, event)
            if attempt is not None:
                return attempt

            logger.warning(
                f"No matching job attempt found for event ({event.attributes.uuid}) for job '{job_id}' "
                "after exhausting all matching strategies."
            )
            return None

        except Exception as e:
            logger.exception(
                f"Error finding matching job attempt for job '{job_id}' and event {event}",
                extra={"job_id": job_id, "error": str(e)},
            )
            return None

    def _match_by_log_file_name(self, job_id: str, event: Event) -> SlurmJobExecutionRecord | None:
        """Tier 2: Match by filename, or bypass filename identity for shared logs."""
        assert event.attributes is not None
        event_log_file_name = event.attributes.log_file_name
        if not event_log_file_name:
            logger.debug(
                f"Log file name is missing for event ({event.attributes.uuid}), skipping log file name matching."
            )
            return None

        offset = 0
        shared_logs = False
        newer_start = None
        while job_attempts := self.storage.read_job_records(
            job_id=job_id,
            sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
            offset=offset,
            limit=5,
            join_job_events_table=False,
        )[0]:
            offset += len(job_attempts)

            if offset == len(job_attempts):
                latest_attempt = job_attempts[0]
                shared_logs = bool(
                    latest_attempt.aps_spec and latest_attempt.aps_spec.log_file_paths_shared_across_attempts
                )

            for attempt in job_attempts:
                # A newer registered start also bounds older records whose end
                # update has not arrived yet. Never use detection time here.
                end_time = attempt.end_time or newer_start
                within_attempt = source_timestamp_in_attempt(event.timestamp, attempt.start_time, end_time)
                newer_start = attempt.start_time or newer_start
                if shared_logs:
                    if within_attempt:
                        return attempt
                    continue
                # An initial attempt's unique filename remains usable before its
                # start update arrives. Retries need an execution-time fence.
                if not within_attempt and (attempt.start_time is not None or attempt.job_attempt_index > 0):
                    continue
                log_file_paths = attempt.log_file_paths or []
                for log_file_path in log_file_paths:
                    base_log_file_name = os.path.basename(log_file_path)
                    if event_log_file_name == base_log_file_name:
                        logger.info(
                            f"A matching job attempt of job id '{job_id}' found for event (uuid: {event.attributes.uuid}) "
                            f"based on exact log file name match: {event_log_file_name}."
                        )
                        EVENT_ATTEMPT_MATCH_BY_LOG_FILE_NAME.labels(job_id=job_id).inc()
                        return attempt

                    base_log_file_name_prefix, base_log_file_name_suffix = os.path.splitext(base_log_file_name)

                    # TODO: Move the regex suffix to a configuration.
                    log_file_pattern = re.compile(
                        rf"^{base_log_file_name_prefix}_cycle(\d+){base_log_file_name_suffix}$"
                    )
                    match = log_file_pattern.match(event_log_file_name)
                    if match:
                        nested_restart_attempt = int(match.group(1))
                        logger.info(
                            f"A matching job attempt of job id '{job_id}' found for event (uuid: {event.attributes.uuid}) "
                            f"based on log file name pattern match: {log_file_pattern.pattern} with nested restart {nested_restart_attempt}."
                        )
                        EVENT_ATTEMPT_MATCH_BY_LOG_FILE_NAME.labels(job_id=job_id).inc()
                        return attempt

            if len(job_attempts) < 5:
                break

        logger.warning(
            f"No matching job attempt found for event ({event.attributes.uuid}) for job '{job_id}' "
            "based on log file name."
        )
        return None

    def _match_by_slurm_job_attempt(self, job_id: str, event: Event) -> SlurmJobExecutionRecord | None:
        """Tier 1: Match event to a job attempt using the slurm_job_attempt attribute."""
        assert event.attributes is not None
        slurm_job_attempt = (event.attributes.model_extra or {}).get("slurm_job_attempt")
        if slurm_job_attempt is None:
            return None

        try:
            if isinstance(slurm_job_attempt, bool) or not isinstance(slurm_job_attempt, (str, int)):
                raise ValueError("Attempt identity must be an integer")  # noqa: TRY004
            job_attempt_index = int(slurm_job_attempt)
            if job_attempt_index < 0:
                raise ValueError("Attempt identity must be nonnegative")
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid slurm_job_attempt value '{slurm_job_attempt}' for event ({event.attributes.uuid}) "
                f"of job '{job_id}', skipping slurm_job_attempt matching."
            )
            return None

        attempt = self.storage.read_job_record(job_id=job_id, job_attempt_index=job_attempt_index)
        if attempt is not None:
            logger.info(
                f"A matching job attempt of job id '{job_id}' found for event (uuid: {event.attributes.uuid}) "
                f"based on slurm_job_attempt={job_attempt_index}."
            )
            EVENT_ATTEMPT_MATCH_BY_SLURM_JOB_ATTEMPT.labels(job_id=job_id).inc()
        else:
            logger.warning(
                f"No job attempt found for event ({event.attributes.uuid}) of job '{job_id}' "
                f"with slurm_job_attempt={job_attempt_index}."
            )
        return attempt

    def _match_latest_job_attempt(self, job_id: str, event: Event) -> SlurmJobExecutionRecord | None:
        """Tier 3: Fall back to the latest job attempt in the DB."""
        assert event.attributes is not None
        attempt = self.storage.read_job_record(job_id=job_id)
        if attempt is not None and not source_timestamp_in_attempt(
            event.timestamp, attempt.start_time, attempt.end_time
        ):
            logger.warning(
                "Ignoring unbound event %s for job %s: source time is outside the latest attempt",
                event.attributes.uuid,
                job_id,
            )
            return None
        if attempt is not None:
            logger.info(
                f"Associating event ({event.attributes.uuid}) of job '{job_id}' with latest job attempt "
                f"(job_attempt_index={attempt.job_attempt_index})."
            )
            EVENT_ATTEMPT_MATCH_BY_LATEST_ATTEMPT.labels(job_id=job_id).inc()
        return attempt

    def _redeliver_watcher_event(self, channel, method, properties, body: bytes, event: Event) -> None:
        """Reuse the bounded delayed route for missing registration or failed durable writes."""
        if not self._redeliver_message(
            channel,
            method,
            properties,
            body,
            description=f"Watcher event {event.model_dump_json()}",
            max_delivery_times=MESSAGE_MAX_DELIVERY_TIMES,
        ):
            return
        if event.type == EventType.CHECKPOINTSAVE:
            CHECKPOINT_MESSAGE_REDELIVERY_COUNTER.inc(1)
        else:
            ANOMALY_MESSAGE_REDELIVERY_COUNTER.inc(1)

    def get_watcher_job_event_message_callback(
        self,
    ) -> Callable[
        [
            pika.adapters.blocking_connection.BlockingChannel,
            pika.spec.Basic.Deliver,
            pika.spec.BasicProperties,
            bytes,
        ],
        None,
    ]:
        def process_watcher_job_events(
            channel: pika.channel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            trace_context = extract_trace_context_from_rabbitmq_properties(event_body=body, properties=properties)

            with tracer.start_as_current_span(
                name=f"job_event.{JobAnomalySpanName.PERSISTENCE}", kind=SpanKind.CONSUMER, context=trace_context
            ) as span:
                try:
                    # Per contract, the event_data should be the JSON serialization of the Event class so it can be
                    # deserialized directly.
                    json_str = body.decode()
                    logger.info(f"Received watcher notification event: {json_str}.")
                    watcher_event: Event = Event.model_validate_json(json_str)

                    span.set_attribute("job_event.type", watcher_event.type)
                    if watcher_event.attributes:
                        span.set_attribute("job_event.uuid", watcher_event.attributes.uuid)

                    if not watcher_event.attributes or not watcher_event.attributes.job_id:
                        logger.error("Event.attributes.job_id cannot be empty.")
                        span.set_status(Status(StatusCode.ERROR, "Event.attributes.job_id is empty."))
                        return

                    matching_job_record = self._find_matching_job_attempt_for_event(
                        job_id=watcher_event.attributes.job_id, event=watcher_event
                    )

                    # When no matching job record was found, the message will be sent to a dead-letter queue to wait for
                    # some time before gets re-routed back to try again. The total number of requests will be tracked
                    # as a header.
                    if not matching_job_record:
                        self._redeliver_watcher_event(channel, method, properties, body, watcher_event)
                        span.set_status(Status(StatusCode.ERROR, "Matching job attempt is not registered."))
                        return

                    span.set_attribute("job_id", matching_job_record.job_id)
                    span.set_attribute("job_attempt_index", matching_job_record.job_attempt_index)

                    # Currently checkpoint event is sharing the same schema as the anomaly, we need to differentiate
                    # them by the anomaly_type field and store them at different locations.
                    match watcher_event.type:
                        case EventType.CHECKPOINTSAVE:
                            CHECKPOINT_MESSAGE_RECEIVED_COUNTER.inc(1)
                            checkpoint = JobCheckpointInfo(
                                timestamp=(
                                    datetime.fromtimestamp(int(watcher_event.timestamp) / 1e9, UTC)
                                    if watcher_event.timestamp
                                    else datetime.now(tz=UTC)
                                ),
                                uuid=watcher_event.attributes.uuid,
                            )
                            matching_job_record.checkpoints = matching_job_record.checkpoints or []
                            if not any(
                                existing.uuid == checkpoint.uuid for existing in matching_job_record.checkpoints
                            ):
                                matching_job_record.checkpoints.append(checkpoint)

                            logger.info("Persisting a job checkpoint event in storage.")
                            self.update_job_record(
                                job_id=watcher_event.attributes.job_id,
                                job_attempt_index=matching_job_record.job_attempt_index,
                                checkpoints=matching_job_record.checkpoints,
                                last_updated_at=datetime.now(tz=UTC),
                            )

                        case EventType.CHECKPOINTLOAD:
                            logger.info(
                                f"Received checkpoint load event {watcher_event.type}, logging it only and not persisting it in the storage"
                            )

                        case _:
                            ANOMALY_MESSAGE_RECEIVED_COUNTER.inc(1)
                            # Add additional attributes with job_anomaly prefix to facilitate easier querying and visualization
                            span.set_attribute("job_anomaly.uuid", watcher_event.attributes.uuid)
                            span.set_attribute("job_anomaly.type", watcher_event.type)

                            job_anomaly = JobAnomaly(
                                anomaly_type=watcher_event.type,
                                timestamp=(
                                    datetime.fromtimestamp(int(watcher_event.timestamp) / 1e9, UTC)
                                    if watcher_event.timestamp
                                    else datetime.now(
                                        tz=UTC
                                    )  # This should never really happen as we will always have a timestamp coming from watcher, so something that could be improved in future
                                ),
                                uuid=(
                                    watcher_event.attributes.uuid if hasattr(watcher_event.attributes, "uuid") else None
                                ),
                                original_watcher_anomaly_event=json_str,
                                handled_by=JobAnomalyHandledBy.HEIMDALL,
                                processed=False,
                                trace_id=extract_trace_id_from_rabbitmq_properties(properties),
                            )
                            matching_job_record.anomalies = matching_job_record.anomalies or []
                            if not any(existing.uuid == job_anomaly.uuid for existing in matching_job_record.anomalies):
                                matching_job_record.anomalies.append(job_anomaly)

                            self.persist_job_anomaly(
                                latest_job_record=matching_job_record,
                                job_anomaly=job_anomaly,
                                watcher_event=watcher_event,
                            )

                    if watcher_event.type in (EventType.CHECKPOINTSAVE, EventType.CHECKPOINTLOAD):
                        self.persist_anomaly_advice(matching_job_record, watcher_event)

                    span.set_status(StatusCode.OK)
                except RecoveryStorageException as e:
                    logger.exception(
                        f"Watcher event persistence failed. Raw message: {body.decode()}",
                        extra={"error": str(e)},
                    )
                    self._redeliver_watcher_event(channel, method, properties, body, watcher_event)
                    span.set_status(Status(StatusCode.ERROR, "Watcher event persistence failed due to storage error."))
                except Exception as e:
                    logger.exception(
                        f"Failed to process watcher event message. Raw message: {body.decode()}",
                        extra={"error": str(e)},
                    )
                    span.set_status(
                        Status(StatusCode.ERROR, "Failed to process watcher event message due to unexpected error.")
                    )

        return process_watcher_job_events

    def persist_job_anomaly(
        self,
        latest_job_record: SlurmJobExecutionRecord,
        job_anomaly: JobAnomaly,
        *,
        watcher_event: Event | None = None,
    ) -> None:
        """Persist a Watcher anomaly, notify ADI, and reconcile terminal Advice.

        The caller has appended the anomaly to the matching attempt. ADI must
        observe durable anomaly data. Record the causal Advice Issue before
        dispatching attribution; terminal Advice remains independent
        of the retired ARM relay. Direct ADI emission uses an atomic storage claim.
        """
        logger.info(
            f"Persisting a job anomaly (uuid {job_anomaly.uuid}) (type {job_anomaly.anomaly_type}) for job {latest_job_record.job_id} in storage."
        )
        self.update_job_record(
            job_id=latest_job_record.job_id,
            job_attempt_index=latest_job_record.job_attempt_index,
            anomalies=latest_job_record.anomalies,
            last_updated_at=datetime.now(tz=UTC),
        )

        if watcher_event is not None:
            self.persist_anomaly_advice(latest_job_record, watcher_event)

        application_intention_result = self._maybe_emit_attribution_request(latest_job_record)
        if application_intention_result is ApplicationIntentionReconciliation.RETRY_REQUIRED:
            # The Watcher consumer auto-acknowledges deliveries. Surface a
            # storage failure so its existing delayed-redelivery path retains
            # the anomaly until the cancellation guard can be evaluated.
            raise RecoveryStorageException(
                f"Application intention reconciliation requires retry for job {latest_job_record.job_id}"
            )
        if application_intention_result.blocks_recovery:
            return

        if self._is_advice_management_enabled():
            maybe_persist_requeue_terminal_advice(
                self.storage,
                latest_job_record,
                audit_buffer=self.audit_buffer,
                system_requeue_config=self._build_system_requeue_config(),
                get_jobs=self._get_jobs_for_requeue_check,
                on_persisted=self._maybe_send_recovery_notification,
                on_advice_persisted=self.propagate_terminated_advice_for_dag_dependents,
                on_advice_ready=self._begin_sequence_for_advice,
            )

    def _register_dag_dependencies(
        self,
        record: SlurmJobExecutionRecord,
        *,
        reconcile_existing_parents: bool = False,
    ) -> None:
        """
        Register DAG dependencies for a job that has dag_dependencies in its APS spec.
        Creates a DagDependency record for each dependency step.

        Args:
            record: The initial job execution record with DAG dependencies in its APS spec.
            reconcile_existing_parents: When true, immediately applies a parent
                completion or terminal Advice already persisted before this child
                was registered.
        """
        if not isinstance(self.storage, MySQLStorage):
            logger.warning(
                f"DAG dependency registration skipped for job {record.job_id}: "
                "storage does not support DAG dependencies."
            )
            return
        register_dag_dependencies(self.storage, record)
        refresh_dag_dependency_observability(self.storage, record)
        if reconcile_existing_parents:
            for completed_parent in reconcile_registered_job(
                self.storage,
                record,
                audit_buffer=self.audit_buffer,
            ):
                self._evaluate_dag_dependencies_for_completed_job(completed_parent)

    def refresh_dag_dependency_observability_for_observed_job(
        self,
        observed_record: SlurmJobExecutionRecord,
    ) -> None:
        """Refresh held children that may reference a newly observed parent.

        ID and name queries deliberately produce candidates rather than proving
        parent identity. ``refresh_dag_dependency_observability`` resolves each
        candidate against the child's workflow and latest attempts. Restricting
        refreshes to PENDING attempt-zero children preserves the established
        invariant that dependency-gated children have not started running.
        """
        if not self._is_dag_support_enabled() or not isinstance(self.storage, MySQLStorage):
            return

        matching_deps = read_all_dag_dependencies(
            self.storage,
            dependency_job_identifier=observed_record.job_id,
            dependency_job_identifier_type="id",
        )
        if observed_record.job_name:
            matching_deps.extend(
                read_all_dag_dependencies(
                    self.storage,
                    dependency_job_identifier=observed_record.job_name,
                    dependency_job_identifier_type="name",
                )
            )

        for dependent_job_id in {dep.dependent_job_id for dep in matching_deps}:
            dependent_record = self.storage.read_job_record(
                job_id=dependent_job_id,
                job_attempt_index=0,
            )
            if dependent_record is None or dependent_record.state != SlurmJobNonFinalStates.PENDING.value:
                continue
            refresh_dag_dependency_observability(
                self.storage,
                dependent_record,
                observed_parent=observed_record,
            )

    def _evaluate_dag_dependencies_for_completed_job(self, completed_record: SlurmJobExecutionRecord) -> None:
        """
        Evaluate held children that may reference a just-completed parent.

        The initial ID/name lookup only narrows the candidate set. Each edge is
        resolved again using the child's workflow and the uncommitted completed
        event. The dependency explanation is refreshed before release
        eligibility is checked; if every edge is satisfied, release Advice and
        action-state transitions are handled by ``_trigger_dag_action``.

        Args:
            completed_record: The job execution record that just completed.
        """
        if not isinstance(self.storage, MySQLStorage):
            return

        # Find all dependency records that reference this completed job — by ID and by name
        matching_deps: list[DagDependency] = []
        matching_deps.extend(
            read_all_dag_dependencies(
                self.storage, dependency_job_identifier=completed_record.job_id, dependency_job_identifier_type="id"
            )
        )
        if completed_record.job_name:
            matching_deps.extend(
                read_all_dag_dependencies(
                    self.storage,
                    dependency_job_identifier=completed_record.job_name,
                    dependency_job_identifier_type="name",
                )
            )

        if not matching_deps:
            return

        # Group by dependent_job_id to check each child job
        dependent_job_ids = {dep.dependent_job_id for dep in matching_deps}
        logger.info(f"Job {completed_record.job_id} completed — checking DAG dependents: {dependent_job_ids}")

        for dependent_job_id in dependent_job_ids:
            dependent_record = self.storage.read_job_record(dependent_job_id)
            if dependent_record is None or dependent_record.state != SlurmJobNonFinalStates.PENDING.value:
                logger.info(
                    f"Skipping DAG dependency evaluation for job {dependent_job_id}: "
                    f"job is no longer in PENDING state (current state: "
                    f"{dependent_record.state if dependent_record else 'not found'})."
                )
                continue

            all_deps = read_all_dag_dependencies(self.storage, dependent_job_id=dependent_job_id)
            if any(dep.action_status == DagDependencyActionStatus.ABORTED.value for dep in all_deps):
                logger.info(f"Skipping DAG release for job {dependent_job_id}: its dependency gate was aborted.")
                continue
            if any(dep.action_status != DagDependencyActionStatus.NOT_ELIGIBLE.value for dep in all_deps):
                continue
            refresh_dag_dependency_observability(
                self.storage,
                dependent_record,
                observed_parent=completed_record,
            )
            all_satisfied = True

            for dep in all_deps:
                # Check if this dependency's trigger condition is met
                if dep.dependency_trigger_condition == DagTriggerConditionEnum.COMPLETED.value:
                    parent_satisfied = self._is_dependency_satisfied(dep, completed_record)
                    if not parent_satisfied:
                        all_satisfied = False
                else:
                    logger.warning(
                        f"Unknown DAG trigger condition '{dep.dependency_trigger_condition}' "
                        f"for dependency {dep.dependency_job_identifier} of job {dependent_job_id}, "
                        "treating as not satisfied."
                    )
                    all_satisfied = False

            if all_satisfied:
                action = all_deps[0].dependent_job_action
                logger.info(f"All DAG dependencies satisfied for job {dependent_job_id}, triggering action: {action}")
                self._trigger_dag_action(
                    dependent_record=dependent_record,
                    action=action,
                    dag_dep_ids=[dep.id for dep in all_deps],
                )

    def _is_dependency_satisfied(self, dep: DagDependency, completed_record: SlurmJobExecutionRecord) -> bool:
        """
        Check if a single dependency's trigger condition is satisfied by looking up the parent job.

        Because this is called before the completed_record is persisted to the database, the DB may
        still have the old state. When the looked-up parent matches the completed_record (same job_id
        and job_attempt_index), we use the completed_record's state instead of the DB state.

        Args:
            dep: The DAG dependency record to check.
            completed_record: The job record that just completed but may not be persisted yet.

        Returns:
            True if the parent job satisfies the trigger condition.
        """
        dependent_record = self.storage.read_job_record(
            job_id=dep.dependent_job_id,
            job_attempt_index=0,
        )
        if dependent_record is None:
            return False
        return dependency_is_satisfied(
            self.storage,
            dep,
            dependent_record,
            observed_parent=completed_record,
        )

    def _trigger_dag_action(
        self,
        dependent_record: SlurmJobExecutionRecord,
        action: str,
        dag_dep_ids: list[int],
    ) -> None:
        """
        Persist exact-attempt DAG advice and update the dependency action status.

        Args:
            dependent_record: The current exact attempt of the dependent child.
            action: The action to perform (e.g., "release").
            dag_dep_ids: The IDs of the DagDependency records to update.
        """
        if action != DagActionEnum.RELEASE.value:
            logger.warning(f"Unsupported DAG action: {action} for job {dependent_record.job_id}")
            return
        if dependent_record.job_attempt_index != 0:
            logger.error(
                f"Refusing DAG release for job {dependent_record.job_id} attempt "
                f"{dependent_record.job_attempt_index}: dependency release is scoped to attempt 0."
            )
            return

        try:
            now = datetime.now(UTC)
            for dep_id in dag_dep_ids:
                self.storage.update_dag_dependency_status(
                    id=dep_id,
                    action_status=DagDependencyActionStatus.IN_PROGRESS.value,
                    action_time=now,
                )

            persist_dag_release_advice(
                self.storage,
                dependent_record,
                audit_buffer=self.audit_buffer,
                on_advice_ready=lambda advice: self.recovery_service.begin_for_job(
                    dependent_record,
                    operation=RecoveryOperation.DAG_RELEASE,
                    trigger_type=RecoveryTriggerType.DEPENDENCY,
                    triggered_at=now,
                    scheduler_metadata=advice.scheduler_metadata,
                ),
            )

            for dep_id in dag_dep_ids:
                self.storage.update_dag_dependency_status(
                    id=dep_id,
                    action_status=DagDependencyActionStatus.SUCCESSFUL.value,
                    action_time=datetime.now(UTC),
                )

        except Exception as e:
            logger.exception(
                f"Failed to persist DAG action '{action}' for job {dependent_record.job_id} "
                f"attempt {dependent_record.job_attempt_index}",
                extra={
                    "job_id": dependent_record.job_id,
                    "job_attempt_index": dependent_record.job_attempt_index,
                    "error": str(e),
                },
            )

    def find_stale_jobs(self, run_once: bool = False) -> None:
        """Report stale non-final attempts and optionally mark them cancelled.

        Only the elected leader scans storage. Scheduler refreshes belong to the
        Advisor Controller; this watcher neither requests updates nor consults
        broker queues. ``run_once`` performs a single scan instead of polling.
        """
        interval = int(self.config["service"].get("stale_job_scan_frequency_seconds", 300))
        stale_threshold_seconds = int(self.config["service"].get("stale_job_threshold_seconds", 300))
        stale_threshold = timedelta(seconds=stale_threshold_seconds)
        while True:
            try:
                if self.is_leader:
                    logger.info("Running the stale job search query.")

                    stale_jobs: list[SlurmJobExecutionRecord] = []
                    offset = 0
                    now = datetime.now(tz=UTC)

                    while non_terminated_jobs := self.storage.read_job_records(
                        states=[s for s in SlurmJobNonFinalStates],
                        offset=offset,
                    )[0]:
                        offset += len(non_terminated_jobs)

                        previous_stale_jobs_count = len(stale_jobs)
                        stale_jobs.extend(
                            [
                                job
                                for job in non_terminated_jobs
                                if job.last_updated_at and job.last_updated_at < now - stale_threshold
                            ]
                        )
                        logger.debug(
                            f"Found {len(stale_jobs) - previous_stale_jobs_count} more stale jobs in this batch, total stale jobs so far: {len(stale_jobs)}."
                        )

                    if stale_jobs:
                        logger.warning(
                            f"Identified {len(stale_jobs)} unfinished job attempts that are not updated in the last "
                            f"{stale_threshold_seconds} seconds."
                        )
                        logger.warning(
                            f"Stale job attempts: {[str(job.job_id) + ' (' + str(job.job_attempt_index) + ')' for job in stale_jobs]}"
                        )
                        STALE_RUNNING_JOBS_GAUGE.set(len(stale_jobs))

                        if self.config["service"].get("stale_job_cleanup_enabled", False):
                            logger.info("Cleaning up stale jobs.")
                            for job in stale_jobs:
                                self.update_job_record(
                                    job_id=job.job_id,
                                    job_attempt_index=job.job_attempt_index,
                                    state="CANCELLED_STALE",
                                    last_updated_at=datetime.now(tz=UTC),
                                )

                    else:
                        logger.info("No stale running jobs identified.")
                        STALE_RUNNING_JOBS_GAUGE.set(0)
                else:
                    logger.debug("Skipping the stale job search query.")
            except RecoveryStorageException as e:
                logger.exception("Storage error", extra={"error": str(e)})
            except Exception as e:
                logger.exception("Uncategorized error", extra={"error": str(e)})

            if run_once:
                break
            else:
                time_module.sleep(interval)

    def detect_requeue_early_stop(self, run_once: bool = False) -> None:
        """
        This function detects if there are any jobs managed by requeue stopped before hitting either the system
        level requeue limit or the job level requeue limit, and not explicitly cancelled.

        The filtering was done by:
        1. Find all the jobs that crossed the early-stop threshold since the previous scan.
        2. For jobs found in step 1, find all the jobs that are managed by requeue.
        3. For jobs found in step 2, find the latest attempt of each job whose state is in the terminal state.
        4. For jobs found in step 3, check if the job has hit one of the conditions below:
           - Reached the system level requeue limit
           - Reached the job level requeue limit
           - Was explicitly CANCELLED
           - Qualified for checkpoint-based early stop, i.e. the job has failed to reach a checkpoint in Z consecutive
             attempts.
           If none of the conditions are met and the job has been stopped for longer
           than Y minutes, then the job is considered as stopped early.

        A Prometheus gauge will be exposed to indicate the number of job groups that are stopped early.

        Both X, Y, Z are configurable values.
        """

        interval = int(self.config["service"].get("requeue_early_stop_scan_frequency_seconds", 60))
        threshold = int(self.config["service"].get("requeue_early_stop_threshold_seconds", 600))

        while True:
            try:
                if self.is_leader:
                    logger.info("Running queries to detect requeue early stop.")
                    lastest_attempt_index_evaluated_for_jobs: dict[str, int] = {}
                    now = datetime.now(UTC)
                    system_max_requeue_times = self.get_system_max_requeue_times()
                    checkpoint_based_threshold = self.get_checkpoint_based_requeue_stop_threshold()
                    unexpected_early_stop_jobs = []
                    offset = 0

                    while terminated_job_attempts := self.storage.read_job_records(
                        states=list(SLURM_JOB_FINAL_STATE_SET),
                        time_filter=JobTimeFilter(
                            job_time_target=JobTimeFilterTarget.END_TIME,
                            # Search the interval in which jobs first became old enough to qualify. The extra 60
                            # seconds allows for scheduler drift between scans. Including the threshold in the lower
                            # bound is essential: without it, a job can leave the query window before it is old enough
                            # to be classified as an early stop.
                            from_time=now - timedelta(seconds=(threshold + interval + 60)),
                            to_time=now - timedelta(seconds=threshold),
                        ),
                        # Sort the result by job_id in ascending order and then job_attempt_index in descending order.
                        sort_by=[
                            ("job_id", SortOrderEnum.ASC),
                            ("job_attempt_index", SortOrderEnum.DESC),
                        ],
                        offset=offset,
                    )[0]:
                        offset += len(terminated_job_attempts)
                        for job in terminated_job_attempts:
                            # Skip evaluation if the job is not managed by requeue.
                            if (
                                not job.aps_spec
                                or not job.aps_spec.auto_resume_mode
                                or job.aps_spec.auto_resume_mode != AutoResumeModeEnum.REQUEUE
                            ):
                                continue

                            # Use the lastest_attempt_index_evaluated_for_jobs dictionary to optimize the processing.
                            # Skip the job attempt if a higher job attempt index of the same job id has been evaluated
                            # in the previous iteration.
                            if (
                                job.job_id not in lastest_attempt_index_evaluated_for_jobs
                                or job.job_attempt_index > lastest_attempt_index_evaluated_for_jobs[job.job_id]
                            ):
                                lastest_attempt_index_evaluated_for_jobs[job.job_id] = job.job_attempt_index

                            if job.job_attempt_index < lastest_attempt_index_evaluated_for_jobs[job.job_id]:
                                continue

                            # Job explicitly cancelled doesn't qualify unexpected early stop.
                            if job.state == SlurmJobFinalStates.CANCELLED:
                                logger.debug(
                                    f"Job {job.job_id}, attempt {job.job_attempt_index} is explicitly cancelled, No "
                                    "unexpected requeue early stop detected."
                                )
                                continue

                            if (
                                job.state == SlurmJobFinalStates.COMPLETED
                                and not self.get_requeue_on_completion_config()
                            ):
                                logger.debug(
                                    f"Job {job.job_id}, attempt {job.job_attempt_index} is completed and requeue on "
                                    "completion is disabled, No unexpected requeue early stop detected."
                                )
                                continue

                            # Find the latest job attempt of the same job id.
                            latest_job_attempt = self.storage.read_job_record(job_id=job.job_id)
                            if not latest_job_attempt:
                                continue
                            if latest_job_attempt.state not in SLURM_JOB_FINAL_STATE_SET:
                                logger.debug(
                                    f"The latest job attempt {latest_job_attempt.job_attempt_index} of job "
                                    f"{job.job_id} is not in the terminal state. No unexpected early stop detected."
                                )
                                continue

                            # Check if the job has hit the checkpoint-based requeue stop threshold.
                            if checkpoint_based_threshold:
                                recent_attempts, _ = self.storage.read_job_records(
                                    job_id=job.job_id,
                                    sort_by=[("job_attempt_index", SortOrderEnum.DESC)],
                                    limit=checkpoint_based_threshold,
                                )

                                if len(recent_attempts) == checkpoint_based_threshold:
                                    checkpoint_reached = False
                                    for job_attempt in recent_attempts:
                                        if job_attempt.checkpoints:
                                            checkpoint_reached = True
                                            break

                                    if not checkpoint_reached:
                                        logger.info(
                                            f"Job {job.job_id} has failed to reach a checkpoint in the last "
                                            f"{checkpoint_based_threshold} attempts, the checkpoint-based early stop "
                                            "condition is met, no unexpected requeue early stop detected."
                                        )
                                        continue
                                else:
                                    logger.info(
                                        "Not enough job attempts to evaluate the checkpoint-based early stop "
                                        f"condition for job {job.job_id}, the threshold is {checkpoint_based_threshold}. "
                                        f"but only {len(recent_attempts)} attempts are found."
                                    )

                            # Job that hit the requeue limit are not considered as an unexpected early stop.
                            effecitve_requeue_times_limit = system_max_requeue_times
                            if job.aps_spec.max_requeue_times:
                                if effecitve_requeue_times_limit == INFINITE_SYSTEM_REQUEUE_LIMIT:
                                    effecitve_requeue_times_limit = job.aps_spec.max_requeue_times
                                else:
                                    effecitve_requeue_times_limit = min(
                                        effecitve_requeue_times_limit,
                                        job.aps_spec.max_requeue_times,
                                    )

                            if (
                                effecitve_requeue_times_limit != INFINITE_SYSTEM_REQUEUE_LIMIT
                                and latest_job_attempt.job_attempt_index >= effecitve_requeue_times_limit
                            ):
                                logger.info(
                                    f"Job {job.job_id}, attempt {job.job_attempt_index} has hit the requeue limit. "
                                    "No unexpected requeue early stop detected."
                                )
                                continue

                            if latest_job_attempt.end_time and now - latest_job_attempt.end_time >= timedelta(
                                seconds=threshold
                            ):
                                logger.info(
                                    f"Job {latest_job_attempt.job_id}, attempt {latest_job_attempt.job_attempt_index} "
                                    "is run under auto-resume mode REQUEUE with an effective requeue limit of "
                                    f"{effecitve_requeue_times_limit}. However the requeue sequence is stopped at  "
                                    f"attempt {latest_job_attempt.job_attempt_index} and a new job attempt is not "
                                    f"created for longer than {threshold} seconds. The requeue sequence is considered "
                                    "as unexpected early stop."
                                )
                                unexpected_early_stop_jobs.append(job.job_id)

                    UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE.set(len(unexpected_early_stop_jobs))
                    if unexpected_early_stop_jobs:
                        logger.warning(
                            f"Detected {len(unexpected_early_stop_jobs)} jobs stopped early under requeue mode: "
                            f"{unexpected_early_stop_jobs}."
                        )
                    else:
                        logger.info("No unexpected early stop detected for jobs managed by requeue.")
                else:
                    logger.debug(
                        "Current job manager instance is not the leader, skipping requeue early stop detection."
                    )
            except RecoveryStorageException as e:
                logger.exception("Storage error", extra={"error": str(e)})
            except Exception as e:
                logger.exception("Uncategorized error", extra={"error": str(e)})

            if run_once:
                break
            else:
                time_module.sleep(interval)

    def generate_anomaly_event_for_failed_jobs_executor(self, run_once: bool = False) -> None:
        """
        Executes the periodic task to generate end events for failed jobs that do not have an anomaly to keep the auto
        resume sequence going.
        """
        time_window_secs = int(self.config["service"].get("job_state_based_auto_resume_time_window_secs", 900))
        threshold_secs = int(self.config["service"].get("job_state_based_auto_resume_threshold_secs", 300))
        interval_secs = int(self.config["service"].get("job_state_based_auto_resume_scan_interval_secs", 30))

        while True:
            if not self.is_leader:
                logger.debug(
                    "Current job manager instance is not the leader, skipping the anomaly event generation for "
                    "failed jobs."
                )
            else:
                logger.info("Running period task to generate anomaly event for failed jobs without anomaly.")
                try:
                    periodic_tasks.generate_anomaly_event_for_failed_jobs(
                        storage=self.storage,
                        audit_buffer=self.audit_buffer,
                        time_window_secs=time_window_secs,
                        threshold_secs=threshold_secs,
                        on_anomaly_persisted=self._maybe_emit_attribution_request,
                        advice_enabled=self._is_advice_enabled(),
                    )
                except RecoveryStorageException as e:
                    logger.exception("Storage error", extra={"error": str(e)})
                except Exception as e:
                    logger.exception("Uncategorized error", extra={"error": str(e)})

            if run_once:
                break
            else:
                time_module.sleep(interval_secs)

    def reconcile_timed_out_dag_actions_executor(self, run_once: bool = False) -> None:
        """
        Reconcile timed-out DAG actions that are stuck in IN_PROGRESS state.
        """
        dag_support_config = self.config["service"].get("dag_support", {})
        timeout_secs = int(dag_support_config.get("action_timeout_seconds", 120))
        interval_secs = int(dag_support_config.get("retry_scan_frequency_seconds", 60))

        while True:
            if not self.is_leader:
                logger.debug("Current job manager instance is not the leader, skipping DAG action reconciliation.")
            else:
                logger.debug("Running periodic task to reconcile timed-out DAG actions.")
                try:
                    periodic_tasks.reconcile_timed_out_dag_actions(
                        storage=self.storage,
                        timeout_secs=timeout_secs,
                        audit_buffer=self.audit_buffer,
                    )
                except RecoveryStorageException as e:
                    logger.exception("Storage error during DAG action reconciliation", extra={"error": str(e)})
                except Exception as e:
                    logger.exception("Error during DAG action reconciliation", extra={"error": str(e)})

            if run_once:
                break
            else:
                time_module.sleep(interval_secs)

    def start_consuming(self) -> None:
        logger.info("Starting consuming messages from the event bus.")
        for consumer in self.event_bus_consumers.values():
            consumer.start_consuming()

    def stop(self) -> None:
        logger.info("Stopping consuming messages from the event bus.")
        for consumer in self.event_bus_consumers.values():
            consumer.stop_consuming()

        if self.adi_attribution_request_publisher:
            self.adi_attribution_request_publisher.cleanup()

        # Clean up leader election resources if enabled
        if self.is_leader:
            # For Kubernetes, the leaderelection package automatically handles cleanup via its run method
            # When the main thread exits, the daemon thread will be terminated
            logger.info("Kubernetes leader election thread will be terminated when main thread exits.")

    def start_background_jobs(self) -> None:
        logger.info("Starting background jobs.")
        self.background_job_threads["stale_job_watcher"] = threading.Thread(target=self.find_stale_jobs)

        service_config = self.config["service"]
        if service_config.get("requeue_early_stop_scan_frequency_seconds", 0) > 0:
            self.background_job_threads["requeue_early_stop_detection"] = threading.Thread(
                target=self.detect_requeue_early_stop
            )

        if service_config.get("job_state_based_auto_resume_time_window_secs", 0) > 0:
            self.background_job_threads["end_event_generation_for_failed_jobs"] = threading.Thread(
                target=self.generate_anomaly_event_for_failed_jobs_executor
            )

        if (
            self._is_dag_support_enabled()
            and service_config.get("dag_support", {}).get("retry_scan_frequency_seconds", 0) > 0
        ):
            self.background_job_threads["dag_action_reconciliation"] = threading.Thread(
                target=self.reconcile_timed_out_dag_actions_executor
            )

        for thread in self.background_job_threads.values():
            thread.start()

    def get_system_max_requeue_times(self) -> int:
        if (value := self.config["service"].get("system_max_requeue_times")) is not None:
            return value
        else:
            return DEFAULT_SYSTEM_MAX_REQUEUE_TIMES

    def get_checkpoint_based_requeue_stop_threshold(self) -> int | None:
        return self.config["service"].get("checkpoint_based_requeue_stop_threshold")

    def get_requeue_on_completion_config(self) -> bool:
        return self.config["service"].get("requeue_on_completion", False)
