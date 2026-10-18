# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import re
import statistics
import time
import traceback
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from numbers import Number
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from pika.spec import BasicProperties
from pytz import timezone
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer
from recovery_libs.log_query import BackendName, QueryFailure
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Event
from recovery_libs.storage.dataclass import (
    JobConfigValidationFailureTypes,
    SlurmJobNonFinalStates,
)
from recovery_libs.tracing.traces import (
    JobAnomalySpanName,
    populate_headers_with_span_context,
)
from recovery_libs.utils.time import source_timestamp_in_attempt

from watcher.lib.plugins.job_manager import get_job_attempts_sync, get_job_info
from watcher.lib.plugins.log_query import LogQueryBatcher
from watcher.lib.plugins.rabbitmq import EventBusPublisher
from watcher.lib.utils.anomaly import IterationThroughputMonitor
from watcher.lib.utils.aps_spec import (
    get_nooutput_timeout,
    is_event_detection_disabled,
    parse_aps_spec_from_job_info,
)
from watcher.lib.utils.audit import try_add_audit_event_from_anomaly
from watcher.lib.utils.constants import (
    EVENT_TYPES_REQUIRING_DETECTION_CHECK,
    JOB_ATTRIBUTES_NOOUTPUT_PREFIX,
    JOB_LAST_EVENT_PREFIX,
    JOB_PHASE_ORDERED,
    JOB_PHASE_VALID_TRANSITIONS,
    JOB_STATE_CACHE_CLEANUP_KEY_PREFIXES,
    LOG_QUERY_HIGH_EVENT_COUNT_THRESHOLD,
    LOG_QUERY_VALIDATION_BACKOFF_SECS,
    LOG_QUERY_VALIDATION_MAX_RETRIES,
    MAINTENANCE_TASKS_LAST_RUN_KEY,
    METRICS_CLEANUP_KEY,
)
from watcher.lib.utils.data import (
    TIMED_COUNTER_DEFAULT_TIMEOUT,
    ActionCache,
    EventDeduplicator,
    RunningAverage,
    TimedCounter,
    compare_operator,
    convert_datetime_to_timestamp_ns,
    convert_timestamp_to_datetime_fromtimestamp_format,
    does_datetime_isoformat_contain_timezone,
    extract_structured_data_from_text,
    generate_job_status_detection_time_now,
    generate_sha256_signature,
    generate_unique_cache_key,
    generate_uuid7,
)
from watcher.lib.utils.job import get_job_attempt_for_event, try_get_job_identity_for_event
from watcher.lib.utils.logging import logger
from watcher.lib.utils.loki_stats import LOKI_DEGRADED, LokiQueryPerformanceMonitor
from watcher.lib.utils.metrics import (
    LOG_QUERY_RUN_DURATION,
    SEND_MESSAGE_EVENTBUS_DURATION,
    WORKFLOW_CACHE_ACTION_SIZE,
    WORKFLOW_CACHE_KV_SIZE,
    WORKFLOW_HANGNOOUTPUT_JOB_NOT_RUNNING_STATE,
    WORKFLOW_HANGNOOUTPUT_LAST_ITERATION,
    WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR,
    WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED,
    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND,
    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND,
    WORKFLOW_LOG_QUERY_EVENTS,
    WORKFLOW_LOG_QUERY_EVENTS_DISCARDED_PRE_PROCESSING,
    WORKFLOW_LOG_QUERY_FAILURES,
    WORKFLOW_LOG_QUERY_INGESTION_DELAY,
    WORKFLOW_PROCESSING_TIME,
    WORKFLOW_RABBITMQ_MESSAGES,
    WORKFLOW_RABBITMQ_MESSAGES_SKIPPED,
    WORKFLOW_STEP_COMPARE_LAST_DATAPOINT_DURATION,
    WORKFLOW_STEP_EMIT_METRIC_DURATION,
    WORKFLOW_STEP_MOVING_AVG_DURATION,
    WORKFLOW_STEP_NOOUTPUT_DURATION,
    WORKFLOW_STEP_REGEX_PARSER_DURATION,
    WORKFLOW_STEP_SIMPLE_ACTION_DURATION,
    WORKFLOW_STEP_STORE_DATAPOINT_DURATION,
    increase_metric_counter,
    measure_duration,
    set_metric_gauge,
)
from watcher.lib.utils.workflow_parser import first_events_by_job_id_and_logfile_name
from watcher.maintenance_tasks import (
    update_certificate_expiration_metrics,
    update_deduplicator_capacity_metrics,
)
from watcher.models.audit import WatcherAnomalyAuditEvent
from watcher.models.config import WatcherConfig, WatcherEventSource
from watcher.models.exceptions import WorkflowStepLastSampleKeyError
from watcher.models.workflow import (
    Checkpoint,
    LogRecord,
    MetricType,
    WorkflowItem,
    WorkflowStep,
    WorkflowStepNooutputEventType,
    WorkflowStepNooutputJobAttributes,
    WorkflowStepNooutputJobPhase,
    WorkflowStepRegexConfig,
    WorkflowStepType,
)

tracer = trace.get_tracer(__name__)


class RulesEngine:
    def __init__(
        self,
        publisher: EventBusPublisher,
        kvstore: dict[str, Any],
        config: WatcherConfig,
        action_cache: ActionCache,
        log_query_batcher: LogQueryBatcher | None = None,
        audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = NoOpAuditBuffer[WatcherAnomalyAuditEvent](),
    ):
        self.config = config
        self.log_query_client = log_query_batcher
        self.publisher = publisher
        self.kvstore = kvstore
        self.action_cache = action_cache
        self.event_source = config.event_source
        self.deduplicator = EventDeduplicator()
        self.loki_perf_monitor = LokiQueryPerformanceMonitor(kvstore=kvstore)
        self.audit_event_buffer = audit_event_buffer

    @property
    def log_query_backend(self) -> BackendName:
        """Return the active backend while treating legacy Watcher config as Loki."""

        return self.config.datasources.log_query_backend

    def _render_nooutput_validation_query(self, template: str, *, job_id: str) -> str:
        """Substitute the job identifier without interpreting native query syntax.

        LogsQL uses ordinary braces in stream filters, so ``str.format`` would
        parse provider syntax as Python replacement fields. Loki retains its
        legacy formatting behavior for compatibility.
        """

        if self.log_query_backend == BackendName.LOKI:
            return template.format(job_id=job_id)
        placeholder = "{job_id}"
        if placeholder not in template:
            raise ValueError(
                f"VictoriaLogs nooutput_validation_query must contain the literal placeholder {placeholder}"
            )
        return template.replace(placeholder, job_id)

    def run_maintenance_tasks(self):
        """
        Run periodic maintenance tasks with configurable interval.

        Tasks are throttled to run at most once per interval
        to avoid excessive execution overhead.
        """
        current_time = datetime.now()
        maintenance_interval = self.config.service.maintenance_tasks_interval_secs

        # Check if enough time has passed since last run
        if MAINTENANCE_TASKS_LAST_RUN_KEY in self.kvstore:
            last_run = datetime.fromtimestamp(self.kvstore[MAINTENANCE_TASKS_LAST_RUN_KEY])
            time_since_last_run = (current_time - last_run).total_seconds()

            if time_since_last_run < maintenance_interval:
                logger.debug(
                    f"[run_maintenance_tasks] Skipping - ran {time_since_last_run:.1f}s ago "
                    f"(interval: {maintenance_interval}s)"
                )
                return

        logger.debug("running RulesEngine maintenance tasks")
        self.kvstore[MAINTENANCE_TASKS_LAST_RUN_KEY] = current_time.timestamp()

        # Enhanced RabbitMQ publisher health check and maintenance
        self.check_and_maintain_publisher_connection()

        # update cache stats
        WORKFLOW_CACHE_ACTION_SIZE.set(len(self.action_cache))
        WORKFLOW_CACHE_KV_SIZE.set(len(self.kvstore))

        # cleanup metrics gauges
        metrics_clean_threshold_secs = self.config.metrics_cleanup_threshold_secs
        self.cleanup_metric_gauges(threshold_secs=metrics_clean_threshold_secs)

        # update certificate expiration metrics
        update_certificate_expiration_metrics(log_query_client=self.log_query_client)

        # update deduplicator capacity metrics
        update_deduplicator_capacity_metrics(deduplicator=self.deduplicator)

        # Provider-specific Loki scan/queue diagnostics have no VictoriaLogs
        # equivalent and must not affect VictoriaLogs no-output decisions.
        if self.log_query_backend == BackendName.LOKI:
            self.loki_perf_monitor.evaluate_if_due()

    def check_and_maintain_publisher_connection(self):
        """
        Check RabbitMQ publisher connection health and reconnect if necessary.

        This method verifies that the publisher connection is healthy and attempts
        to reconnect if it's not. This runs in both service and scheduler modes.
        """
        try:
            # Check publisher connection health
            publisher_healthy = (
                self.publisher.connection
                and self.publisher.connection.is_open
                and self.publisher.channel
                and self.publisher.channel.is_open
            )

            if not publisher_healthy:
                logger.warning("[Health Check] Publisher connection is unhealthy, attempting to reset...")
                try:
                    self.publisher.reset_connection_and_channel()
                    logger.info("[Health Check] Publisher connection reset successfully")
                except Exception as e:
                    logger.exception("[Health Check] Failed to reset publisher connection", extra={"error": str(e)})
            else:
                # Keep connection alive with ping (original functionality)
                self.publisher.send_ping()
                logger.debug("[Health Check] Publisher connection is healthy")

        except Exception as e:
            logger.exception(
                "[Health Check] Error checking RabbitMQ publisher connection health", extra={"error": str(e)}
            )

    def _get_bucket_keys_from_workflow_item(self, bucket_keys: list[str], workflow_item: WorkflowItem) -> list[str]:
        """
        Get bucket key suffix from workflow item.

        Args:
            bucket_keys (str): List of bucket keys.
            workflow_input (WorkflowItem): The input data for the workflow.

        Returns:
            List of bucket labels from current workflow item.
        """

        attributes = workflow_item.record.attributes.model_dump().keys()  # type: ignore[union-attr]
        labels_available = list(set(bucket_keys) & set(attributes))
        return [str(getattr(workflow_item.record.attributes, label)).lower() for label in labels_available]

    def _get_bucket_id(
        self,
        key_name: str,
        bucket_keys: list[str],
        workflow_item: WorkflowItem,
    ) -> str:
        """
        Get bucket key suffix from workflow item.

        Args:
            bucket_keys (str): List of bucket keys.
            workflow_input (WorkflowItem): The input data for the workflow.

        Returns:
            (str): Bucket ID
        """
        if not bucket_keys:
            return key_name

        labels = self._get_bucket_keys_from_workflow_item(bucket_keys=bucket_keys, workflow_item=workflow_item)

        if not labels:
            return key_name

        return ":".join([key_name] + labels)

    @staticmethod
    def _get_event(workflow_item: WorkflowItem, event_type: EventType) -> Event:
        # set `type` to [WorkflowItem].routing_suffix if available, else defaults to `event_type`
        evt_type = workflow_item.routing_suffix if workflow_item.routing_suffix is not None else event_type
        # A timestamp-less record has no stable occurrence identity. Keep its
        # generated UUID unique so two otherwise identical real occurrences are
        # not collapsed as one anomaly or one audit event.
        event_attributes = workflow_item.record.attributes.model_copy(deep=True)  # type: ignore[union-attr]
        event_attributes.uuid = generate_uuid7()
        return Event(
            body=workflow_item.record.body,
            type=evt_type,
            timestamp=workflow_item.record.timestamp,
            attributes=event_attributes,
        )

    @WORKFLOW_PROCESSING_TIME.time()
    async def execute_workflow(
        self,
        workflow: list[dict[str, Any]],
        workflow_input: WorkflowItem | list[WorkflowItem] | None = None,
        bucket_keys: list[str] | None = None,
        start: None | datetime = None,
        end: None | datetime = None,
    ) -> None:
        """
        Execute a workflow.

        Args:
            workflow (List[Dict[str, Any]]): A list of workflow steps, where each
                                       step is a dictionary containing the
                                       step name, type, attributes, and
                                       optional children steps.
            workflow_input (Optional[List[WorkflowItem]]): The input data for
                                                           the workflow.
                                                           Default is None.
            start (Optional[datetime]): When invoked from ``LogQueryPoller`` (via
                ``poller_workflow_callback``), the chunk's start bound. Threaded
                down to ``run_log_query`` so the log-query is bounded to the
                chunk window (the configured chunk size) instead of falling
                back to the in-batcher (cursor → now) range — which can grow
                unbounded if the cursor stalls.
            end (Optional[datetime]): Chunk end bound. Same semantics as ``start``.

        Raises:
            QueryFailure: The query failed or could not establish completeness.
                Propagation keeps the poller from committing this chunk's cursor.
        """
        checkpoint = Checkpoint()
        self.run_maintenance_tasks()
        logger.debug(f"initiating workflow run: {workflow}")
        logger.debug(f"workflow_input data: {workflow_input}")

        for step in workflow:
            workflow_step = WorkflowStep(**step)

            # check if enable bucketing is enabled
            if not bucket_keys:
                bucket_keys = workflow_step.bucket_by

            if not checkpoint.value and workflow_input:
                checkpoint.value = workflow_input
                checkpoint.key = workflow_step.name

            match workflow_step.type:
                case WorkflowStepType.log_query | WorkflowStepType.loki_query:
                    if self.event_source.uses_log_query:
                        query_prefix = self.config.datasources.log_query_prefix
                        query = self.config.datasources.workflow_queries(workflow_step.name).query
                        filter_events_by_jobid = workflow_step.attributes.get(
                            "filter_events_by_jobid_before_processing", False
                        )
                        try:
                            query_results = await self.run_log_query(
                                query=query,
                                query_id=workflow_step.name,
                                query_prefix=query_prefix,
                                filter_events_by_jobid=filter_events_by_jobid,
                                start=start,
                                end=end,
                            )
                        except Exception as err:
                            logger.warning(
                                f"[{workflow_step.name}] {self.log_query_backend.value} query failed. "
                                f"Skipping workflow run: {err}"
                            )
                            WORKFLOW_LOG_QUERY_FAILURES.inc()
                            # A failed query is not a processed window. The poller
                            # owns retries and commits only successful callbacks.
                            if isinstance(err, QueryFailure):
                                raise
                            continue

                        checkpoint.key = workflow_step.name
                        checkpoint.value = query_results

                case WorkflowStepType.regex_parser:
                    try:
                        self.workflow_step_regex_parser(workflow_step=workflow_step, checkpoint=checkpoint)
                    except ValueError as err:
                        logger.warning(f"[{workflow_step.name}] skipping remaining steps: {err}")
                        continue

                case WorkflowStepType.compare_last_datapoint:
                    try:
                        self.workflow_step_compare_last_datapoint(
                            workflow_step=workflow_step, checkpoint=checkpoint, bucket_keys=bucket_keys
                        )
                    except ValueError as err:
                        logger.warning(f"[{workflow_step.name}]: {err}")
                        # no checkpoint value available - skip loop
                        continue
                    except IndexError as err:
                        logger.warning(f"[{workflow_step.name}] {err}")
                    except WorkflowStepLastSampleKeyError as err:
                        logger.error(f"[{workflow_step.name}]: {err}")

                case WorkflowStepType.store_datapoint:
                    logger.debug(
                        f"[{workflow_step.name}] step store_datapoint - current output data: {checkpoint.value}"
                    )
                    self.workflow_step_store_datapoint(
                        workflow_step=workflow_step, checkpoint=checkpoint, bucket_keys=bucket_keys
                    )

                case WorkflowStepType.nooutput:
                    logger.debug(f"[{workflow_step.name}] step nooutput - current output data: {checkpoint.value}")
                    try:
                        await self.workflow_step_nooutput(
                            workflow_step=workflow_step,
                            checkpoint=checkpoint,
                            bucket_keys=bucket_keys,
                            start=start,
                            end=end,
                        )
                    except ValueError as err:
                        logger.warning(f"[{workflow_step.name}] {err}")
                        # no checkpoint value available - skip loop
                        continue

                case WorkflowStepType.simple_action:
                    try:
                        self.workflow_step_simple_action(workflow_step=workflow_step, checkpoint=checkpoint)
                    except ValueError as err:
                        logger.warning(f"[{workflow_step.name}] skipping remaining steps: {err}")
                        # no checkpoint value available - skip loop
                        continue

                case WorkflowStepType.moving_avg:
                    try:
                        self.workflow_step_moving_avg(
                            workflow_step=workflow_step, checkpoint=checkpoint, bucket_keys=bucket_keys
                        )
                    except ValueError as err:
                        logger.warning(f"[{workflow_step.name}]: {err}")
                        # no checkpoint value available - skip loop
                        continue

                case WorkflowStepType.emit_metric:
                    logger.debug(f"[{workflow_step.name}] step emit_metric - current output data: {checkpoint.value}")
                    self.workflow_step_emit_metric(
                        workflow_step=workflow_step,
                        checkpoint=checkpoint,
                        bucket_keys=bucket_keys,
                    )

            # Handle workflow with child steps
            if workflow_step.children:
                logger.debug(f"[{workflow_step.name}] Found children step: {workflow_step.name}")
                logger.debug(
                    f"[{workflow_step.name}] Children step {workflow_step.name} - current output: {checkpoint.value}"
                )
                # Spawn a workflow for each output item in the current step
                if isinstance(checkpoint.value, list):
                    logger.debug(
                        f"[{workflow_step.name}] Children step {workflow_step.name} - current output is a list"
                    )
                    for item in checkpoint.value:
                        logger.debug(
                            f"[{workflow_step.name}] Children step {workflow_step.name} - spawing {step['children']} with input data: {item}"
                        )
                        await self.execute_workflow(
                            workflow=step["children"],
                            workflow_input=item,
                            bucket_keys=bucket_keys,
                            start=start,
                            end=end,
                        )
                else:
                    logger.debug(
                        f"[{workflow_step.name}] Children step {workflow_step.name} - current output is a singleton"
                    )
                    logger.debug(
                        f"[{workflow_step.name}] Children step {workflow_step.name} - spawing {step['children']} with input data: {checkpoint.value}"
                    )
                    await self.execute_workflow(
                        workflow=step["children"],
                        workflow_input=checkpoint.value,
                        bucket_keys=bucket_keys,
                        start=start,
                        end=end,
                    )

    @measure_duration(LOG_QUERY_RUN_DURATION)
    async def run_log_query(
        self,
        query: list[str] | str,
        query_id: str,
        query_prefix: str,
        start: None | datetime = None,
        end: None | datetime = None,
        limit: None | int = None,
        ignore_query_delay: bool = False,
        filter_events_by_jobid: bool = False,
        log_empty_response: bool = False,
    ) -> list[WorkflowItem] | None:
        """
        Execute a log-query and return list of WorkflowItem objects.

        Args:
            query (Union[List[str], str]): Provider-native queries to execute.
            query_id (str): A unique identifier for the query.
            query_prefix (str): The prefix to be added to the query
                                (to avoid duplication in the config)
            start (Union[None, datetime], optional): Start.
            end (Union[None, datetime], optional): End.
            limit (Union[None, int]): number of max entries in response.
            ignore_query_delay (bool): ignore query delay param set at log-query client.
            filter_events_by_jobid (bool): filters workflow items (events) results to reduce number of
                                           children workflows to be processed.
            log_empty_response (bool): when True, logs provider diagnostics at
                INFO level when the backend returns no data.

        Returns:
            Optional[List[WorkflowItem]]: A list of WorkflowItem objects
                                              representing the query results.
                                          Returns None if an exception occurs
                                              during the query execution or
                                              results are empty.
        """
        if self.log_query_client is None:
            raise RuntimeError("log_query_client is not initialized but run_log_query was called")

        workflow_items = []

        if not isinstance(query, list):
            query = [query]

        for idx, query_partial in enumerate(sorted(query)):
            query_partial = f"{query_prefix} {query_partial}"
            logger.debug(f"Running {self.log_query_backend.value} query: {query_partial}")
            results = await self.log_query_client.batch_query(
                query=query_partial,
                query_id=f"{query_id}_{idx}",
                start=start,
                end=end,
                limit=limit,
                ignore_query_delay=ignore_query_delay,
                log_empty_response=log_empty_response,
            )

            if not results:
                continue

            workflow_items_partial = [WorkflowItem(record=LogRecord(**row)) for row in results]
            WORKFLOW_LOG_QUERY_EVENTS.labels(query=query_id).inc(len(workflow_items_partial))
            if len(workflow_items_partial) > LOG_QUERY_HIGH_EVENT_COUNT_THRESHOLD:
                logger.info(
                    f"[LogQuery] High event count: query_id={query_id} returned {len(workflow_items_partial)} events"
                )
            # Filters workflow items (events) results to reduce number of children workflows to be processed
            if filter_events_by_jobid:
                workflow_items_partial_pre_filtering = workflow_items_partial[:]
                logger.debug(
                    f"filter_events_by_jobid - number of events pre filtering: {len(workflow_items_partial_pre_filtering)}"
                )
                workflow_items_partial = first_events_by_job_id_and_logfile_name(workflow_items_partial_pre_filtering)
                logger.debug(f"filter_events_by_jobid - number of events post filtering: {len(workflow_items_partial)}")
                WORKFLOW_LOG_QUERY_EVENTS_DISCARDED_PRE_PROCESSING.inc(
                    len(workflow_items_partial_pre_filtering) - len(workflow_items_partial)
                )

            workflow_items += workflow_items_partial

        if len(workflow_items) > 1:
            logger.info(f"Sorting {len(workflow_items)} items by timestamp")
            workflow_items = sorted(
                workflow_items, key=lambda item: int(item.record.timestamp) if item.record.timestamp else 0
            )

        # emit ingestion delay metrics
        log_query_policy = self.config.datasources.selected_log_query_policy
        ingestion_delay_timestamp_regex = (
            log_query_policy.ingestion_delay_timestamp_regex if log_query_policy is not None else None
        )
        ingestion_delay_timestamp_timezone = (
            log_query_policy.ingestion_delay_timestamp_timezone if log_query_policy is not None else None
        )
        ingestion_delay_metric_collection_interval_secs = (
            log_query_policy.ingestion_delay_metric_collection_interval_secs if log_query_policy is not None else None
        )
        if (
            ingestion_delay_timestamp_regex
            and ingestion_delay_timestamp_timezone
            and ingestion_delay_metric_collection_interval_secs
        ):
            # instantiate non-blocking async task to emit ingestion delay metrics
            asyncio.create_task(
                self.emit_ingestion_delay_metrics(
                    workflow_items=workflow_items,
                    ingestion_delay_timestamp_regex=ingestion_delay_timestamp_regex,
                    ingestion_delay_timestamp_timezone=ingestion_delay_timestamp_timezone,
                    ingestion_delay_metric_collection_interval_secs=ingestion_delay_metric_collection_interval_secs,
                )
            )

        return workflow_items

    @measure_duration(SEND_MESSAGE_EVENTBUS_DURATION)
    def send_message_eventbus(
        self,
        events: list[Event],
        routing_suffix: str | None = None,
        routing_prefix: str | None = None,
        workflow_step_name: str | None = None,
        unique: bool = False,
        dedup: bool = False,
        dedup_signature_pattern: str | None = None,
        dedup_strip_patterns: list[str] | None = None,
    ) -> None:
        """
        Send a message to RabbitMQ based on workflow items and routing suffix.

        Args:
            events (List[Event]): Events to send.
            routing_suffix (str, optional): The suffix to be appended to
                                            the routing key. Defaults to None.
            routing_prefix (str, optional): The prefix to be appended to
                                            the routing key. Defaults to None.
                                            Determines which queues a message is sent to.
            workflow_step_name (str): The name of workflow_step emitting
                                      message. Used to dedup messages.
            unique (bool): Dedup messages sent to eventbus.
                           Defaults to False.
            dedup (bool): Enable content-based deduplication using EventDeduplicator.
                          Defaults to False.
            dedup_signature_pattern (str, optional): Regex pattern with capture group to extract
                                                     signature. If None, uses full content.
            dedup_strip_patterns (List[str], optional): List of regex patterns to strip
                                                        before computing signature.

        Returns:
            None
        """
        routing_suffix = routing_suffix.upper() if routing_suffix else None
        routing = routing_prefix if routing_prefix else self.config.datasources.rabbitmq.publisher.routing_prefix  # type: ignore[union-attr]
        if routing_suffix:
            routing += str(routing_suffix)

        logger.debug(f"Sending {len(events)} events to {routing}, unique={unique}")

        should_check_detection = routing_suffix and routing_suffix in EVENT_TYPES_REQUIRING_DETECTION_CHECK
        needs_identity = unique or dedup
        job_manager_config = self.config.datasources.job_manager
        jm_endpoint = (
            job_manager_config.endpoint if job_manager_config and (should_check_detection or needs_identity) else None
        )
        # ephemeral cache - only lives through iteration over list of events passed on instantiation
        job_attempts_cache: dict[str, list[SlurmJobExecutionStatus] | None] = {}

        for original_event in events:
            event = original_event.model_copy(deep=True)
            job_id = str(event.job_id)

            job_identity = try_get_job_identity_for_event(event)
            if jm_endpoint and (should_check_detection or job_identity is None) and job_id not in job_attempts_cache:
                job_attempts_cache[job_id] = get_job_attempts_sync(endpoint=jm_endpoint, job_id=job_id)

            job_attempts = job_attempts_cache.get(job_id)
            latest_job_attempt = job_attempts[0] if job_attempts else None

            if (
                should_check_detection
                and latest_job_attempt
                and routing_suffix
                and is_event_detection_disabled(latest_job_attempt.aps_spec, routing_suffix)
            ):
                logger.info(
                    f"[Action] [RabbitMQ] Dropping event for job {job_id}: "
                    f"detection disabled by aps_spec for event type {routing_suffix}. "
                    f"Event: {event}"
                )
                WORKFLOW_RABBITMQ_MESSAGES_SKIPPED.inc()
                continue

            # Delivery and deduplication retain explicit attempt or timestamp matches
            # against only the latest attempt, so Job Manager remains the source of
            # truth for resolution.
            #
            # Historical resolution is only for audit as it is best-effort.
            deliver_job_identity = job_identity or try_get_job_identity_for_event(
                event,
                match_time_with_job_attempt=latest_job_attempt,
            )

            audit_identity = job_identity
            if audit_identity is None and job_attempts:
                matched_attempt = get_job_attempt_for_event(event, job_attempts)
                audit_identity = try_get_job_identity_for_event(event, match_time_with_job_attempt=matched_attempt)

            # Resolve the audit identity first so this inferred label cannot
            # bypass the historical selector's ambiguity check.
            if deliver_job_identity is not None and event.attributes:
                event.attributes.slurm_job_attempt = deliver_job_identity.attempt_index  # type: ignore[attr-defined]
            step_signature = None
            if unique and deliver_job_identity is not None:
                step_signature_key = f"{workflow_step_name}:{routing_suffix}:{deliver_job_identity.heimdall_job_id}:{event.log_file_name}"
                step_signature = generate_sha256_signature(step_signature_key)
                if step_signature in self.action_cache:
                    WORKFLOW_RABBITMQ_MESSAGES_SKIPPED.inc()
                    continue

            # Replayed source records keep the same event ID across process
            # restarts, allowing persisted anomalies/checkpoints to deduplicate.
            # Do not include inferred attempt identity: registration can arrive
            # between deliveries of the same source record.
            if event.attributes and event.timestamp:
                source = [
                    event.timestamp,
                    event.type.value,
                    event.body,
                    original_event.attributes.model_dump(  # type: ignore[union-attr]
                        mode="json", exclude={"uuid", "detection_time"}, exclude_none=True
                    ),
                ]
                event.attributes.uuid = str(uuid5(NAMESPACE_URL, json.dumps(source, sort_keys=True)))

            message = event.model_dump_json(exclude_unset=True)
            detection_time_ns = time.time_ns()

            # Update notification counter and attach more attributes to the span.
            detection_span = tracer.start_span(
                name=f"job_event.{JobAnomalySpanName.DETECTION}",
                kind=SpanKind.PRODUCER,
                attributes={
                    "job_id": job_id,
                    "workflow_step": workflow_step_name,  # type: ignore[dict-item]
                },
                # Event timestamps are nanoseconds. Although optional in the schema,
                # they are set for production events.
                start_time=int(event.timestamp) if event.timestamp else detection_time_ns,
            )
            increase_metric_counter(metric_name=f"WORKFLOW_NOTIFICATION_{routing_suffix}", job_id=job_id)

            # Expose both attributes for consistency across event queries.
            detection_span.set_attribute("job_anomaly.type", event.type.value)
            detection_span.set_attribute("job_event.type", event.type.value)
            if event.attributes and event.attributes.uuid:
                detection_span.set_attribute("job_event.uuid", event.attributes.uuid)
                detection_span.set_attribute("job_anomaly.uuid", event.attributes.uuid)
            else:
                logger.warning(
                    "[Action] [Tracing] Event uuid attribute not set for event detections tracing span. "
                    f"Event.attributes or Event.attributes.uuid not set: {event}."
                )

            detection_span.set_status(StatusCode.OK)
            detection_span.end(end_time=detection_time_ns)

            # Populate the trace context in the message properties to associate downstream spans to the same trace.
            headers = populate_headers_with_span_context(span=detection_span)

            # Content-based deduplication
            if dedup and deliver_job_identity is not None:
                dedup_content = event.body
                dedup_context = {
                    "workflow_step": workflow_step_name,
                    "routing_suffix": routing_suffix,
                    "job_id": str(deliver_job_identity.heimdall_job_id),
                    "log_file_name": event.log_file_name,
                }
                if dedup_content and self.deduplicator.is_duplicate(
                    content=dedup_content,
                    context=dedup_context,
                    signature_pattern=dedup_signature_pattern,
                    strip_patterns=dedup_strip_patterns,
                ):
                    WORKFLOW_RABBITMQ_MESSAGES_SKIPPED.inc()
                    logger.info(f"[Action] [RabbitMQ] Skipping content-duplicate: {dedup_content}")
                    continue

            _ = try_add_audit_event_from_anomaly(
                self.audit_event_buffer,
                event,
                workflow_step_name,
                detection_time_ns,
                audit_identity,
            )

            logger.debug(f"[Action] [RabbitMQ] Publishing event (routing={routing}): {message}")
            self.publisher.publish_message(
                routing_key=routing,
                message=message,
                properties=BasicProperties(headers=headers) if headers else None,
            )
            if step_signature is not None:
                self.action_cache.add(key=step_signature, value=1)
            logger.info(f"[Action] [RabbitMQ] Message published (routing={routing}, job_id={job_id}): {message}")

            # Update rabbitmq messages counter
            WORKFLOW_RABBITMQ_MESSAGES.inc()

    @measure_duration(WORKFLOW_STEP_REGEX_PARSER_DURATION)
    def workflow_step_regex_parser(self, workflow_step: WorkflowStep, checkpoint: Checkpoint) -> None:
        """
        Perform a regex parsing step on the workflow item.

        Args:
            workflow_step (WorkflowStep): The current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the workflow item.

        Raises:
            ValueError: If the checkpoint value is empty.

        Returns:
            None

        Notes:
        1- This workflow step supports two modes of operation:
            - Standard mode: Provide a single regex pattern via the 'regex' attribute.
            - Advanced parser mode: Provide a list of advanced regex configurations via the 'advanced_regex'
                attribute along with additional event-related configuration through field 'events'.
        2- The attribute 'publish_events' is only supported in Advanced parser mode.
        """
        regex_parser_step = WorkflowStepRegexConfig(**workflow_step.model_dump())
        logger.debug(f"[{regex_parser_step.name}] current output data: {checkpoint.value}")
        if not checkpoint.value:
            raise ValueError("[workflow_step_regex_parser] Checkpoint value is empty")

        workflow_item = checkpoint.value
        checkpoint.value = None
        advanced_regex_matches = []

        # Execute simple regex parser step
        if regex_parser_step.attributes.advanced_regex is None:
            regex = rf"{regex_parser_step.attributes.regex}"
            logger.debug(f"[{regex_parser_step.name}] regex: {regex}")
            match = re.search(regex, workflow_item.record.body or "")
            if match:
                logger.debug(f"[{regex_parser_step.name}] found match: {match.group(1)}")
                workflow_item.step_value = match.group(1)

        # Execute advanced regex parser step
        elif regex_parser_step.attributes.advanced_regex is not None:
            for event_config in regex_parser_step.attributes.advanced_regex:
                structured_data_matches = extract_structured_data_from_text(
                    data=workflow_item.record.body or "",
                    regexes=event_config.regexes or [],
                    stop_after_first_match=regex_parser_step.attributes.stop_after_first_match or True,
                    include_data=False,
                )
                logger.debug(
                    f"[{regex_parser_step.name}] advanced regex parser matches for filter {event_config.regexes}: {structured_data_matches}"
                )
                advanced_regex_matches.extend(structured_data_matches)

                if not regex_parser_step.attributes.publish_events:
                    continue

                if event_config.event_type is None:
                    continue

                events = []
                for match in structured_data_matches:  # type: ignore[assignment]
                    event_attributes = workflow_item.record.attributes.model_copy(deep=True)
                    event_attributes.uuid = generate_uuid7()
                    events.append(
                        Event(
                            body=json.dumps(match),
                            type=event_config.event_type,
                            timestamp=workflow_item.record.timestamp,
                            attributes=event_attributes,
                        )
                    )

                logger.info(
                    f"[{regex_parser_step.name}] forwarding events ({generate_sha256_signature(str(events))}) to RMQ publisher: {events}"
                )
                self.send_message_eventbus(
                    events=events,
                    routing_suffix=event_config.event_type,
                    workflow_step_name=regex_parser_step.name,
                    unique=False,
                    dedup=regex_parser_step.attributes.dedup,
                    dedup_signature_pattern=regex_parser_step.attributes.dedup_signature_pattern,
                    dedup_strip_patterns=regex_parser_step.attributes.dedup_strip_patterns,
                )

            workflow_item.step_value = advanced_regex_matches

        # update checkpoint
        workflow_item.step_name = regex_parser_step.name
        checkpoint.key = regex_parser_step.name
        checkpoint.value = workflow_item

    @measure_duration(WORKFLOW_STEP_SIMPLE_ACTION_DURATION)
    def workflow_step_simple_action(self, workflow_step: WorkflowStep, checkpoint: Checkpoint) -> None:
        """
        Executes an action on a workflow item.

        Args:
            workflow_step (WorkflowStep): The current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the workflow item.

        Raises:
            ValueError: If the checkpoint value is empty.

        Returns:
            None

        Notes:
            1- This is an immutable workflow method - meaning it doesn't change the state of checkpoint values.
        """
        logger.debug(f"[{workflow_step.name}] current output data: {checkpoint.value}")
        if not checkpoint.value:
            raise ValueError(f"[{workflow_step.name}] Checkpoint value is empty")

        workflow_item = checkpoint.value

        if not workflow_step.attributes.get("action"):
            logger.warning(f"[{workflow_step.name}] action key not defined in attributes")
            return

        logger.debug(f"[{workflow_step.name}] sending event to notification handler: {workflow_item}")
        routing_prefix = workflow_step.attributes["action"].get("routing_prefix", None)
        dedup_notification = workflow_step.attributes["action"].get("unique", False)
        action, routing_suffix = next(iter(workflow_step.attributes["action"].items()))
        event = RulesEngine._get_event(workflow_item, routing_suffix)
        action_method_reference = getattr(self, action)
        action_method_reference(
            events=[event],
            routing_suffix=routing_suffix,
            routing_prefix=routing_prefix,
            workflow_step_name=workflow_step.name,
            unique=dedup_notification,
        )

    @measure_duration(WORKFLOW_STEP_COMPARE_LAST_DATAPOINT_DURATION)
    def workflow_step_compare_last_datapoint(
        self, workflow_step: WorkflowStep, checkpoint: Checkpoint, bucket_keys: list[str] | None = None
    ) -> None:
        """
        Compare the current datapoint with last datapoint in the cache.

        Args:
            workflow_step (WorkflowStep): Current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the current input data.
            bucket_keys (Optional[List[str]]): List of keys to identify
                                               the cache bucket.

        Raises:
            ValueError: If the checkpoint value is empty.
            WorkflowStepLastSampleKeyError: If the cache key is not found
                                            in the kv_store.
        """
        logger.debug(f"[{workflow_step.name}] current output data: {checkpoint.value}")

        if not checkpoint.value:
            raise ValueError(f"[{workflow_step.name}] checkpoint value is empty")

        workflow_item = checkpoint.value
        key_prefix = workflow_step.attributes["cache_key"]
        key = self._get_bucket_id(key_name=key_prefix, bucket_keys=bucket_keys or [], workflow_item=workflow_item)
        caching_timeout = workflow_step.attributes.get("interval", TIMED_COUNTER_DEFAULT_TIMEOUT)

        if not self.timed_counter_does_key_exist(key=key):
            self.timed_counter_add(key=key, item=workflow_item.step_value, timeout=caching_timeout)
            logger.info(f"[{workflow_step.name}] key {key} not in kv_store")
            raise WorkflowStepLastSampleKeyError(error_type="key_not_found", message=f"key {key} not in kv_store")

        previous_datapoint: Any = None
        if "previous" in workflow_step.attributes["operation"]:
            try:
                previous_datapoint = self.timed_counter_get_newest(key)
            except IndexError as err:
                self.timed_counter_add(key=key, item=workflow_item.step_value, timeout=caching_timeout)
                logger.warning(f"[{workflow_step.name}] <previous> value not found: {err}")

        # Update step name in checkpoint
        checkpoint.key = workflow_step.name
        # compare current datapoint to last datapoint stored in kvstore
        current_datapoint = workflow_item.step_value
        operation_pattern = workflow_step.attributes["operation"]
        operation = operation_pattern.format(previous=previous_datapoint, current=current_datapoint)
        logger.debug(f"[{workflow_step.name}] operation: {operation}")
        result = compare_operator(operation)
        logger.info(f"[{workflow_step.name}] operation returned: {result}")
        # Add datapoint to kvstore
        self.timed_counter_add(key=key, item=current_datapoint, timeout=caching_timeout)

        # Run action
        if not result or not workflow_step.attributes.get("action"):
            return

        logger.debug(f"[{workflow_step.name}] sending event to notification handler: {workflow_item}")
        action, routing_suffix = next(iter(workflow_step.attributes["action"].items()))
        action_method_reference = getattr(self, action)
        event = RulesEngine._get_event(workflow_item, routing_suffix)
        action_method_reference(
            events=[event],
            routing_suffix=routing_suffix,
            workflow_step_name=workflow_step.name,
            unique=workflow_step.attributes["action"].get("unique", False),
        )

    @measure_duration(WORKFLOW_STEP_MOVING_AVG_DURATION)
    def workflow_step_moving_avg(
        self,
        workflow_step: WorkflowStep,
        checkpoint: Checkpoint,
        bucket_keys: list[str] | None = None,
    ) -> None:
        """
        Compare the current datapoint with moving avg for last N events in the cache.

        Args:
            workflow_step (WorkflowStep): Current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the current input data.
            bucket_keys (Optional[List[str]]): List of keys to identify
                                               the cache bucket.

        Raises:
            ValueError: If the checkpoint value is empty.
        """
        logger.debug(f"[{workflow_step.name}] current output data: {checkpoint.value}")

        if not checkpoint.value:
            raise ValueError(f"[{workflow_step.name}] checkpoint value is empty")

        workflow_item = checkpoint.value
        # Update step name in checkpoint
        checkpoint.key = workflow_step.name
        # get current datapoint
        current_datapoint = workflow_item.step_value
        # get job_id
        job_id = workflow_item.record.attributes.job_id

        # Ensure current_datapoint is numeric for statistical calculations
        try:
            if isinstance(current_datapoint, str):
                current_datapoint = float(current_datapoint)
            elif not isinstance(current_datapoint, (int, float)):
                raise ValueError(f"Invalid datapoint type (job_id={job_id}): {type(current_datapoint)}")  # noqa: TRY004
        except (ValueError, TypeError) as error:
            logger.error(
                f"[{workflow_step.name}] Unable to convert step_value to numeric (job_id={job_id}): {workflow_item.step_value} (type: {type(workflow_item.step_value)}). Error: {error}"
            )
            raise ValueError(
                f"[{workflow_step.name}] current_datapoint must be convertible to numeric value (job_id={job_id}), got: {workflow_item.step_value} (type: {type(workflow_item.step_value)})"
            )

        # Read parameters from workflow step config
        window_size = workflow_step.attributes.get("window_size", 100)
        base_percentile_threshold = workflow_step.attributes.get("base_percentile_threshold", 35)
        anomaly_percent_threshold = workflow_step.attributes.get("anomaly_percent_threshold", 10)
        alert_smoothing_count = workflow_step.attributes.get("alert_smoothing_count", 5)
        warmup_grace_period_secs = workflow_step.attributes.get("warmup_grace_period_secs", 600)
        min_window_size_for_detection = workflow_step.attributes.get("min_window_size_for_detection", 10)
        min_interval_between_datapoints_secs = workflow_step.attributes.get("min_interval_between_datapoints_secs", 30)
        skip_too_close_datapoints = workflow_step.attributes.get("skip_too_close_datapoints", False)
        reset_after_good_values = workflow_step.attributes.get("reset_after_good_values", 5)
        notify_once = workflow_step.attributes.get("notify_once", False)

        # generate job unique ID
        key_prefix = workflow_step.attributes["cache_key"]  # caching key prefix
        key = self._get_bucket_id(key_name=key_prefix, bucket_keys=bucket_keys or [], workflow_item=workflow_item)
        cache_key = f"it_thr_mon:{key}"

        if cache_key not in self.kvstore:
            self.kvstore[cache_key] = IterationThroughputMonitor(
                window_size=window_size,
                base_percentile_threshold=base_percentile_threshold,
                anomaly_percent_threshold=anomaly_percent_threshold,
                alert_smoothing_count=alert_smoothing_count,
                warmup_grace_period_secs=warmup_grace_period_secs,
                min_window_size_for_detection=min_window_size_for_detection,
                min_interval_between_datapoints_secs=min_interval_between_datapoints_secs,
                skip_too_close_datapoints=skip_too_close_datapoints,
                reset_after_good_values=reset_after_good_values,
                notify_once=notify_once,
            )

        is_datapoint_anomaly = self.kvstore[cache_key].add_and_validate(value=current_datapoint)

        # Log iteration_throughput_monitor stats if available
        try:
            iteration_throughput_monitor_stats = self.kvstore[cache_key].stats
            if iteration_throughput_monitor_stats:
                logger.info(
                    f"[{workflow_step.name}] iteration_throughput_monitor stats (job_id={job_id}): {iteration_throughput_monitor_stats}"
                )
        except Exception as error:
            logger.warning(
                f"[{workflow_step.name}] Error accessing IterationThroughputMonitor stats (job_id={job_id})",
                exc_info=True,
                extra={"job_id": job_id, "error": str(error)},
            )
            logger.info(
                f"[{workflow_step.name}] History content (job_id={job_id}): {self.kvstore[cache_key].get_history()}"
            )

        if not is_datapoint_anomaly or not workflow_step.attributes.get("action"):
            return

        if is_datapoint_anomaly:
            # send notification to eventbus
            logger.debug(
                f"[{workflow_step.name}] sending event to notification handler (job_id={job_id}): {workflow_item}"
            )
            action, routing_suffix = next(iter(workflow_step.attributes["action"].items()))
            action_method_reference = getattr(self, action)
            event = RulesEngine._get_event(workflow_item, routing_suffix)
            action_method_reference(
                events=[event],
                routing_suffix=routing_suffix,
                workflow_step_name=workflow_step.name,
                unique=workflow_step.attributes["action"].get("unique", False),
            )

    @measure_duration(WORKFLOW_STEP_NOOUTPUT_DURATION)
    async def workflow_step_nooutput(
        self,
        workflow_step: WorkflowStep,
        checkpoint: Checkpoint,
        bucket_keys: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> None:
        """
        Checks if no updates made to a given query/bucket.

        Args:
            workflow_step (WorkflowStep): Current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing the
                                     current input data.
            bucket_keys (Optional[List[str]]): List of keys to identify
                                               the cache bucket.
            start (Optional[datetime]): Chunk start bound forwarded from
                ``execute_workflow``. Threaded into the per-phase ``run_log_query``
                calls below so they execute against the same chunk window as the
                rest of the workflow instead of falling back to the batcher cursor
                (which can drift unboundedly when stuck). Does NOT affect the
                validation-query window further down, which is intentionally tied
                to the per-job effective_timeout.
            end (Optional[datetime]): Chunk end bound. Same semantics as ``start``.

        Raises:
            ValueError: If the checkpoint value is empty.
        """
        logger.debug(f"[{workflow_step.name}] current output data: {checkpoint.value}")
        # required configuration
        all_jobs_cache_key_name_seed = workflow_step.attributes["cache_key"]
        all_jobs_cache_key_name = generate_unique_cache_key(key_suffix=all_jobs_cache_key_name_seed)
        interval_config = workflow_step.attributes["interval"]
        jm_endpoint = self.config.datasources.job_manager.endpoint  # type: ignore[union-attr]
        last_iteration_enabled = workflow_step.attributes.get("last_iteration_enabled", False)
        last_iteration_regex = workflow_step.attributes.get("last_iteration_regex", None)
        last_iteration_operation_schema = workflow_step.attributes.get("last_iteration_operation_schema", None)
        query_config = workflow_step.attributes.get("query", {})
        query_prefix = self.config.datasources.log_query_prefix
        checkpoint.key = workflow_step.name
        # init local variables
        query_failed = False
        query_results: dict[str, Any] = {}
        cache_timeout: dict[Any, float] = {}
        # ephemeral per-job aps_spec cache, scoped to a single workflow_step_nooutput invocation.
        # Used to avoid redundant Job Manager lookups when computing per-job timeout overrides.
        aps_spec_cache: dict[str, ApsSlurmJobSpec | None] = {}

        async def _get_aps_spec(job_id: str) -> ApsSlurmJobSpec | None:
            if job_id in aps_spec_cache:
                return aps_spec_cache[job_id]
            aps_spec: ApsSlurmJobSpec | None = None
            if jm_endpoint:
                jm_info = await get_job_info(endpoint=jm_endpoint, job_id=job_id)
                aps_spec = parse_aps_spec_from_job_info(jm_info)
            aps_spec_cache[job_id] = aps_spec
            return aps_spec

        async def _clean_job_id_from_state_cache(key: str):
            for prefix in JOB_STATE_CACHE_CLEANUP_KEY_PREFIXES:
                state_cache_key = f"{prefix}{key}"
                try:
                    if state_cache_key in self.kvstore:
                        del self.kvstore[state_cache_key]
                except KeyError as error:
                    logger.warning(
                        f"workflow_step_nooutput - unable to clean up key ({state_cache_key}) from state cache: {error}"
                    )

            self.timed_counter_delete(key)

        async def _process_query_results(
            results: list[WorkflowItem],
            job_phase: WorkflowStepNooutputJobPhase,
        ):
            for event in results:
                workflow_item = event
                job_id = str(workflow_item.record.attributes.job_id)  # type: ignore[union-attr]
                key = self._get_bucket_id(
                    key_name=all_jobs_cache_key_name, bucket_keys=bucket_keys or [], workflow_item=workflow_item
                )
                # add last seen event to cache to be parsed for eventbus message body content
                self.kvstore[f"{JOB_LAST_EVENT_PREFIX}{key}"] = workflow_item

                # increase job events metric counter
                increase_metric_counter(metric_name="WORKFLOW_NOTIFICATION_JOB_EVENTS", job_id=job_id)

                # decision tree: new job detected and add timer
                if not self.timed_counter_does_key_exist(key=key):
                    logger.info(
                        f"[{workflow_step.name}] new job detected ({key}) (job_phase={job_phase.value}): {job_id}"
                    )
                # decision tree: new event for existing job detected and reset timer
                else:
                    logger.debug(
                        f"[{workflow_step.name}] detected new event for job ({key}) (job_phase={job_phase.value}): {job_id}"
                    )

                # add/update timer in timed cache, honoring per-job aps_spec threshold override
                aps_spec = await _get_aps_spec(job_id)
                effective_timeout = get_nooutput_timeout(
                    aps_spec=aps_spec,
                    job_phase=job_phase,
                    default_timeout=int(cache_timeout[job_phase]),
                )
                self.timed_counter_add(
                    key=key,
                    item=WorkflowStepNooutputEventType.OBSERVED,  # type: ignore[arg-type]
                    timeout=effective_timeout,
                )

                # update job attributes in cache
                self.kvstore[f"{JOB_ATTRIBUTES_NOOUTPUT_PREFIX}{key}"] = WorkflowStepNooutputJobAttributes(
                    job_id=job_id,
                    job_phase=job_phase,
                    detection_time=generate_job_status_detection_time_now(),
                )
                logger.info(f"[{workflow_step.name}] job phase set for job {job_id} ({key}): {job_phase.value}")

        # Parse interval config
        for k, v in interval_config.items():
            cache_timeout[WorkflowStepNooutputJobPhase(k)] = v

        # Parse provider-native queries by phase.
        phase_to_log_queries: dict[str, Any] = {}
        log_queries = (
            self.config.datasources.workflow_queries(workflow_step.name).query
            if self.event_source.uses_log_query
            else []
        )
        for item in log_queries:
            if isinstance(item, str):
                phase_to_log_queries.setdefault(WorkflowStepNooutputJobPhase.TRAINING_LOOP, []).append(item)
            elif isinstance(item, dict):
                for phase, query_str in item.items():
                    phase_to_log_queries.setdefault(WorkflowStepNooutputJobPhase[phase], []).append(query_str)

        # Parse regex queries by phase
        phase_to_regexes: dict[str, Any] = {}
        regex_queries = query_config.get("regex", [])
        for item in regex_queries:
            if isinstance(item, str):
                phase_to_regexes.setdefault(WorkflowStepNooutputJobPhase.TRAINING_LOOP, []).append(item)
            elif isinstance(item, dict):
                for phase, regex_str in item.items():
                    phase_to_regexes.setdefault(WorkflowStepNooutputJobPhase[phase], []).append(regex_str)

        if self.event_source.uses_log_query:
            # Log-query backend flow
            try:
                for job_phase in JOB_PHASE_ORDERED:
                    if job_phase not in phase_to_log_queries:
                        continue

                    for query_str in phase_to_log_queries[job_phase]:
                        _query_result = await self.run_log_query(
                            query=query_str,
                            query_id=workflow_step.name,
                            query_prefix=query_prefix,
                            start=start,
                            end=end,
                        )
                        if _query_result:
                            if job_phase in query_results:
                                query_results[job_phase].extend(_query_result)
                            else:
                                query_results[job_phase] = _query_result
                        logger.debug(
                            f"[{workflow_step.name}] {self.log_query_backend.value} query results for phase "
                            f"{job_phase}: {len(_query_result) if _query_result else 0} items"
                        )
            except Exception as err:
                query_failed = True
                logger.exception(
                    f"[{self.log_query_backend.value}] Query failed ({phase_to_log_queries})",
                    extra={"error": str(err)},
                )
                print(traceback.format_exc())
                WORKFLOW_LOG_QUERY_FAILURES.inc()
        elif self.event_source == WatcherEventSource.RABBITMQ:
            # Regex flow
            for job_phase in JOB_PHASE_ORDERED:
                if job_phase not in phase_to_regexes:
                    continue

                regex_patterns = phase_to_regexes[job_phase]
                if not checkpoint.value:
                    continue

                for event in checkpoint.value:
                    for pattern in regex_patterns:
                        if re.search(pattern, str(event)):
                            logger.debug(
                                f"[{workflow_step.name}] Regex match for job_phase {job_phase}: pattern '{pattern}' found in event '{event}'"
                            )
                            if job_phase in query_results:
                                query_results[job_phase].append(event)
                            else:
                                query_results[job_phase] = [event]

        for job_phase in JOB_PHASE_ORDERED:
            if job_phase not in phase_to_log_queries and job_phase not in phase_to_regexes:
                continue

            if job_phase in query_results:
                await _process_query_results(results=query_results[job_phase], job_phase=job_phase)

        # scan all jobs keys in cache
        logger.debug(f"[{workflow_step.name}] scanning nooutput job cache")
        for job_attribute_key in list(self.kvstore):
            # run workflow on NOOUTPUT job keys only
            if not job_attribute_key.startswith(JOB_ATTRIBUTES_NOOUTPUT_PREFIX):
                continue

            # get required objects/data for the scan code flow
            key = job_attribute_key.removeprefix(JOB_ATTRIBUTES_NOOUTPUT_PREFIX)
            workflow_item = self.kvstore[f"{JOB_LAST_EVENT_PREFIX}{key}"]
            job_id = workflow_item.record.attributes.job_id
            job_filename = workflow_item.record.attributes.log_file_name

            # get job_phase from job attributes
            job_attributes = self.kvstore.get(f"{JOB_ATTRIBUTES_NOOUTPUT_PREFIX}{key}")
            if job_attributes is None:
                logger.error(
                    f"[{workflow_step.name}] No job attributes found for key {key}. Skipping. KVStore state: {self.kvstore}"
                )
                WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR.labels(job_id=job_id).inc()
                await _clean_job_id_from_state_cache(key=key)
                continue

            job_phase = job_attributes.job_phase

            # Update the job counter when a backend query fails to avoid false positives.
            if query_failed:
                self.timed_counter_add(key=key, item=WorkflowStepNooutputEventType.QUERY_FAILURE, timeout=60)  # type: ignore[arg-type]
                logger.info(f"[{workflow_step.name}] Log query failed. Auto-incrementing event count for {key}")
                continue

            # Query failures count as activity so an unavailable backend cannot
            # produce a false-positive no-output notification.
            number_of_updates = self.timed_counter_len(key)

            if number_of_updates > 0:  # type: ignore[operator]
                logger.debug(
                    f"[{workflow_step.name}] {number_of_updates} event(s) observed in the last {cache_timeout[job_phase]} seconds for key {key}"
                )
                continue

            if not workflow_step.attributes.get("action"):
                logger.info(f"[{workflow_step.name}] no actions configured. skipping")
                continue

            last_seen_event = workflow_item.record.body
            logger.info(
                f"[{workflow_step.name}] HANGNOOUTPUT timeout violation observed for key {key} - initiating workflow processing for job_id: {job_id} (default timeout: {interval_config})"
            )
            # check if last seen event is last iteration
            if last_iteration_enabled:
                is_last_iteration = self.is_last_iteration(
                    event=str(last_seen_event),
                    job_id=job_id,
                    last_iteration_operation_schema=last_iteration_operation_schema,  # type: ignore[arg-type]
                    last_iteration_regex=last_iteration_regex,  # type: ignore[arg-type]
                )
                logger.debug(
                    f"[{workflow_step.name}] is_last_iteration - returned (job_id={job_id}): {is_last_iteration}"
                )

                if is_last_iteration:
                    logger.info(
                        f"[{workflow_step.name}] is_last_iteration - last iteration detected for job {job_id}. skipping notification: {last_seen_event}"
                    )
                    WORKFLOW_HANGNOOUTPUT_LAST_ITERATION.labels(job_id=job_id).inc()
                    await _clean_job_id_from_state_cache(key=key)
                    continue

            # get job_info from Job Manager service
            jm_job_info = await get_job_info(endpoint=jm_endpoint, job_id=job_id)
            logger.info(f"[{workflow_step.name}] current job_info in Job Manager: {jm_job_info}")
            # refresh per-job aps_spec cache while we have a fresh job_info; downstream uses
            # the same effective_timeout to keep validation/MFU windows consistent with the
            # per-job no-output threshold honored at timer-add time.
            aps_spec = parse_aps_spec_from_job_info(jm_job_info)
            aps_spec_cache[job_id] = aps_spec
            effective_timeout = get_nooutput_timeout(
                aps_spec=aps_spec,
                job_phase=job_phase,
                default_timeout=int(cache_timeout[job_phase]),
            )
            if not jm_job_info:
                logger.warning(
                    f"[{workflow_step.name}] job_info not available for job. {job_id}. please check Job Manager/WFC health as well as user job configuration. removing job {job_id} from tracking cache and skipping notification: {last_seen_event}"
                )
                WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR.labels(job_id=job_id).inc()
                await _clean_job_id_from_state_cache(key=key)
                continue

            # validate log_file_paths in job_info response
            config_validation_failures = jm_job_info.get("config_validation_failure_types", []) or []
            if JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND.name in config_validation_failures:
                logger.warning(
                    f"[{workflow_step.name}] job {job_id} has {JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND.name} validation failure. "
                    f"skipping notification: {last_seen_event}"
                )
                WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR.labels(job_id=job_id).inc()
                await _clean_job_id_from_state_cache(key=key)
                continue

            jm_log_file_paths = jm_job_info.get("log_file_paths", []) or []
            if not any(job_filename in path for path in jm_log_file_paths):
                logger.warning(
                    f"[{workflow_step.name}] job {job_id} log file '{job_filename}' not found in job_info log_file_paths: {jm_log_file_paths}. "
                    f"skipping notification: {last_seen_event}"
                )
                WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR.labels(job_id=job_id).inc()
                await _clean_job_id_from_state_cache(key=key)
                continue

            # make sure job status is not terminated (job manager)
            job_state_jm = jm_job_info["state"]
            if job_state_jm.upper() != SlurmJobNonFinalStates.RUNNING.value:
                logger.info(
                    f"[{workflow_step.name}] job {job_id} is not in RUNNING state. removing job {job_id} from tracking cache and skipping notification: {last_seen_event}"
                )
                WORKFLOW_HANGNOOUTPUT_JOB_NOT_RUNNING_STATE.labels(job_id=job_id).inc()
                await _clean_job_id_from_state_cache(key=key)
                continue

            # A timer armed by a previous attempt must not become a new attempt's
            # hang merely because we replace its timestamp with detection time.
            attempt_start = jm_job_info.get("start_time")
            if isinstance(attempt_start, str):
                attempt_start = datetime.fromisoformat(attempt_start)
            tracked_attempt = (workflow_item.record.attributes.model_extra or {}).get("slurm_job_attempt")
            if (
                tracked_attempt is not None and str(tracked_attempt) != str(jm_job_info.get("job_attempt_index"))
            ) or not source_timestamp_in_attempt(workflow_item.record.timestamp, attempt_start):
                logger.info("[%s] Discarding stale no-output state for job %s", workflow_step.name, job_id)
                await _clean_job_id_from_state_cache(key=key)
                continue
            workflow_item.record.attributes.slurm_job_attempt = jm_job_info["job_attempt_index"]

            #####################################################################################################
            # note (@mmelo): validate there are no events for a given job_id in the time window configured in the
            #                NOOUTPUT workflow
            #
            # 1- Query the selected backend with the job_id predicate
            # 2- Time window for query is set to [current_time] - [noprogress_interval]
            # 3- If events are found it means this is a false positive, so reset key in cache and SKIP sending
            #    notification to Eventbus
            #####################################################################################################
            if self.event_source.uses_log_query:
                validation_query_template = self.config.datasources.workflow_queries(
                    workflow_step.name
                ).nooutput_validation_query
                if validation_query_template is None:
                    raise ValueError(f"[{workflow_step.name}] provider nooutput validation query is required")
                validation_query = self._render_nooutput_validation_query(validation_query_template, job_id=job_id)
                validation_query_id = f"nooutput_val_{workflow_step.name}"
                end_time = datetime.now(UTC)
                start_time = end_time - timedelta(seconds=effective_timeout)
                validation_results: list[WorkflowItem] | None = None
                for idx in range(LOG_QUERY_VALIDATION_MAX_RETRIES):
                    if idx > 0:
                        await asyncio.sleep(LOG_QUERY_VALIDATION_BACKOFF_SECS * idx)
                    logger.info(
                        f"[{workflow_step.name}] running validation query [{idx + 1}/{LOG_QUERY_VALIDATION_MAX_RETRIES}] "
                        f"(start={start_time}, end={end_time}, start_ts={start_time.timestamp()}, "
                        f"end_ts={end_time.timestamp()}, job_id={job_id}): {validation_query}"
                    )
                    validation_results = await self.run_log_query(
                        query=validation_query,
                        query_id=validation_query_id,
                        query_prefix=query_prefix,
                        start=start_time,
                        end=end_time,
                        limit=1,
                        ignore_query_delay=True,
                        log_empty_response=True,
                    )
                    if validation_results:
                        logger.info(
                            f"[{workflow_step.name}] validation query [{idx + 1}/{LOG_QUERY_VALIDATION_MAX_RETRIES}] "
                            f"returned {len(validation_results)} event(s) for job_id {job_id}"
                        )
                        break
                    else:
                        logger.info(
                            f"[{workflow_step.name}] validation query [{idx + 1}/{LOG_QUERY_VALIDATION_MAX_RETRIES}] "
                            f"returned 0 events for job_id {job_id}"
                        )

                if validation_results:
                    logger.info(
                        f"[{workflow_step.name}] validation query results (job_id={job_id}): {validation_results}"
                    )
                    logger.info(
                        f"[{workflow_step.name}] validation: events found for job_id {job_id} "
                        f"in the last {effective_timeout}s - skipping anomaly notification"
                    )
                    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND.labels(job_id=job_id).inc()
                    continue
                else:
                    if self.log_query_backend == BackendName.LOKI and LOKI_DEGRADED._value.get() > 0:
                        logger.warning(
                            f"[{workflow_step.name}] [LOKI_DEGRADED] Suppressing HANGNOOUTPUT anomaly "
                            f"for job_id {job_id} (validation returned no events but Loki health is compromised)"
                        )
                        WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED.labels(job_id=job_id).inc()
                        continue
                    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND.labels(job_id=job_id).inc()
                    logger.info(
                        f"[{workflow_step.name}] validation query results (job_id={job_id}): "
                        f"no events found in the last {effective_timeout}s after "
                        f"{LOG_QUERY_VALIDATION_MAX_RETRIES} attempts - proceeding with anomaly notification"
                    )

            # send event to eventbus notification handler
            workflow_item.record.body = f"nooutput event detected for job_id {job_id}"
            workflow_item.record.timestamp = convert_datetime_to_timestamp_ns(dt=datetime.now())
            logger.debug(
                f"[{workflow_step.name}] sending event to notification handler (job_id={job_id}): {workflow_item}"
            )
            action, routing_suffix = next(iter(workflow_step.attributes["action"].items()))
            action_method_reference = getattr(self, action)
            event = RulesEngine._get_event(workflow_item, routing_suffix)
            action_method_reference(
                events=[event],
                routing_suffix=routing_suffix,
                workflow_step_name=workflow_step.name,
                unique=workflow_step.attributes["action"].get("unique", False),
            )
            await _clean_job_id_from_state_cache(key=key)
            # reset job throughput metric gauge
            set_metric_gauge(metric_name="WORKFLOW_JOB_THROUGHPUT", job_id=str(job_id), amount=0)

    @measure_duration(WORKFLOW_STEP_STORE_DATAPOINT_DURATION)
    def workflow_step_store_datapoint(
        self, workflow_step: WorkflowStep, checkpoint: Checkpoint, bucket_keys: list[str] | None = None
    ) -> None:
        """
        Store a datapoint in the key-value store.

        Args:
            workflow_step (WorkflowStep): The current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the workflow item.
            bucket_keys (Optional[List[str]]): Optional list of keys to
                                               identify the bucket.

        Returns:
            None
        """
        if not checkpoint.value:
            logger.warning(f"[{workflow_step.name}] no checkpoint value available. skipping")
            return

        workflow_item = checkpoint.value
        cache_key_seed = workflow_step.attributes["cache_key"]
        key_prefix = generate_unique_cache_key(cache_key_seed)
        caching_timeout = workflow_step.attributes.get("interval", TIMED_COUNTER_DEFAULT_TIMEOUT)
        key = self._get_bucket_id(key_name=key_prefix, bucket_keys=bucket_keys or [], workflow_item=workflow_item)
        value = workflow_item.step_value
        logger.info(f"[{workflow_step.name}] Storing {key}: {value}")
        self.timed_counter_add(key=key, item=value, timeout=int(caching_timeout))

    @measure_duration(WORKFLOW_STEP_EMIT_METRIC_DURATION)
    def workflow_step_emit_metric(
        self,
        workflow_step: WorkflowStep,
        checkpoint: Checkpoint,
        bucket_keys: list[str] | None = None,
    ) -> None:
        """
        Emit a metric datapoint.

        Args:
            workflow_step (WorkflowStep): The current workflow step.
            checkpoint (Checkpoint): The checkpoint object containing
                                     the workflow item.
            bucket_keys (Optional[List[str]]): Optional list of keys to
                                               identify the bucket.

        Returns:
            None

        Notes:
            1- This is an immutable workflow method - meaning it doesn't change the state of checkpoint values.
        """
        metric_name = workflow_step.attributes["metric_name"]
        metric_type = workflow_step.attributes["metric_type"]
        enable_metric_cleanup = workflow_step.attributes.get("enable_metric_cleanup")

        if not checkpoint.value:
            logger.warning(f"[{workflow_step.name}] no checkpoint value available. skipping")
            return

        workflow_item = checkpoint.value

        if not workflow_item.step_value:
            logger.warning(f"[{workflow_step.name}]] no value available. skipping")
            return

        if not workflow_item.record:
            logger.warning(f"[{workflow_step.name}] no record available. skipping")
            return

        value = workflow_item.step_value
        job_id = workflow_item.record.attributes.job_id

        match metric_type.lower():
            case MetricType.GAUGE:
                logger.info(f"[{workflow_step.name}] emitting gauge metric {metric_name} for job {job_id}: {value}")
                set_metric_gauge(metric_name=metric_name, job_id=str(job_id), amount=float(value))
            case MetricType.COUNTER:
                logger.info(f"[{workflow_step.name}] emitting counter metric {metric_name} for job {job_id}: {value}")
                increase_metric_counter(metric_name=metric_name, job_id=str(job_id), amount=float(value))

        if enable_metric_cleanup:
            self.kvstore.setdefault(METRICS_CLEANUP_KEY, {})
            self.kvstore[METRICS_CLEANUP_KEY].setdefault(metric_name, {})
            self.kvstore[METRICS_CLEANUP_KEY][metric_name][job_id] = datetime.now().timestamp()

    def timed_counter_get_previous(self, key: str) -> str | Number | None:
        """
        Get one-before-last item from timed counter.

        Args:
            key (str): The key of the timed counter.

        Returns:
            Union[str, Number, None]: The previous item in the timed counter,
                                      or None if not found.
        """
        counter = self.kvstore.get(key)
        if counter is None:
            return None
        return counter.get_item(index=-2)

    def timed_counter_get_oldest(self, key: str) -> str | Number | None:
        """
        Get the oldest item from timed counter.

        Args:
            key (str): The key of the timed counter.

        Returns:
            Union[str, Number, None]: The oldest item in the timed counter,
                                      or None if not found.
        """
        counter = self.kvstore.get(key)
        if counter is None:
            return None
        return counter.oldest

    def timed_counter_get_newest(self, key: str) -> str | Number | None:
        """
        Get the newest item from timed counter.

        Args:
            key (str): The key of the timed counter.

        Returns:
            Union[str, Number, None]: The newest item in the timed counter,
                                      or None if not found.
        """
        counter = self.kvstore.get(key)
        if counter is None:
            return None
        return counter.newest

    def timed_counter_get(self, key: str, index: int) -> str | Number | None:
        """
        Get an item from the timed counter by index.

        Args:
            key (str): The key of the timed counter.
            index (int): The index of the item to retrieve.

        Returns:
            Union[str, Number, None]: The item at the specified index,
                                      or None if not found.
        """
        counter = self.kvstore.get(key)
        if counter is None:
            return None
        return counter.get_item(index=index)

    def timed_counter_len(self, key: str) -> Number | None:
        """
        Get the length of the timed counter.

        Args:
            key (str): The key of the timed counter.

        Returns:
            Optional[Number]: The length of the timed counter,
                              or None if the key is not found.
        """
        return len(self.kvstore[key])  # type: ignore[return-value]

    def timed_counter_add(self, key: str, item: str | Number, timeout: int) -> None:
        """
        Add an item to timed counter.

        Args:
            key (str): The key of the timed counter.
            item (Union[str, Number]): The item to add to the timed counter.
            timeout (int): The timeout value for the timed counter.

        Returns:
            None
        """
        if key not in self.kvstore:
            self.kvstore[key] = TimedCounter(timeout=timeout)

        self.kvstore[key].add(item=item, timeout=timeout)

    def timed_counter_create_key(self, key: str, timeout: int) -> None:
        """
        Create timed counter key.

        Args:
            key (str): The key of the timed counter.
            timeout (int): The timeout value for the timed counter.

        Returns:
            None
        """
        if key not in self.kvstore:
            self.kvstore[key] = TimedCounter(timeout=timeout)

    def timed_counter_delete(self, key: str) -> None:
        """
        Delete an item from timed counter.

        Args:
            key (str): The key of the timed counter to delete.

        Returns:
            None
        """
        if key not in self.kvstore:
            return

        del self.kvstore[key]

    def timed_counter_does_key_exist(self, key: str) -> bool:
        """
        Check if a key exists in the timed counter.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return key in self.kvstore

    def timed_counter_get_all_values(self, key: str) -> list[str] | list[Number] | None:
        """
        Check if a key exists in the timed counter.

        Args:
            key (str): The key to check.

        Returns:
            Optional[Union[List[str], List[Number]]]: List with timed items.
        """
        if key not in self.kvstore:
            return []

        return [item for item in self.kvstore[key]]

    def timed_counter_expire_oldest(self, key: str) -> None:
        """
        Expire oldest value in the timed counter.

        Args:
            key (str): The key to check.
        """
        if key not in self.kvstore:
            return

        self.kvstore[key].expire()

    def timed_counter_clear_all(self, key: str) -> None:
        """
        Clear all values in the timed counter.

        Args:
            key (str): The key to check.
        """
        if key not in self.kvstore:
            return

        self.kvstore[key].clear()

    def is_last_iteration(
        self, event: str, job_id: str, last_iteration_operation_schema: str, last_iteration_regex: str
    ) -> bool:
        """
        Determines if the current event matches the last iteration criteria based on a regex pattern
        and a formatted operation schema.

        This function uses a regex pattern to extract values from the event string, maps them to an
        operation schema, and evaluates if event corresponds to the last iteration.

        Args:
            event (str): The event string to be evaluated.
            job_id (str): The job ID.
            last_iteration_operation_schema (str): The schema for the operation to determine the last iteration.
                                                This schema uses placeholders that will be replaced by values
                                                extracted from the event using regex.
            last_iteration_regex (str): The regex pattern to extract relevant values from the event.

        Returns:
            bool: True if the event matches the last iteration criteria, False otherwise.
        """
        last_iteration_attrs = re.findall(r"\{(.*?)\}", last_iteration_operation_schema)
        last_iteration_regex_re_format = rf"{last_iteration_regex}"
        last_iteration_re_match_values = re.findall(last_iteration_regex_re_format, event)
        if not last_iteration_re_match_values:
            logger.warning(
                f"[is_last_iteration] skipping. regex match not found (job_id={job_id}, regex={last_iteration_regex_re_format}): {event}"
            )
            return False

        last_iteration_values = dict(zip(last_iteration_attrs, last_iteration_re_match_values[0]))
        last_iteration_operation = last_iteration_operation_schema.format_map(last_iteration_values)
        logger.info(f"[is_last_iteration] operation data (job_id={job_id}): {last_iteration_operation}")
        return compare_operator(last_iteration_operation)

    async def emit_ingestion_delay_metrics(
        self,
        workflow_items: list[WorkflowItem],
        ingestion_delay_timestamp_regex: str,
        ingestion_delay_timestamp_timezone: str,
        ingestion_delay_metric_collection_interval_secs: int,
    ) -> None:
        """
        Emit metrics tracking ingestion delay observed in events.

        Args:
            workflow_items (List[WorkflowItem]): A list of WorkflowItem
                                                objects.
            ingestion_delay_timestamp_regex (str): Timestamp parsing regex.
            ingestion_delay_timestamp_timezone (str): Timestamp parsing timezone.
            ingestion_delay_metric_collection_interval_secs (int): Aggregation moving window interval in secs.

        Notes:
            1- This is an immutable workflow method - meaning it doesn't change the state of checkpoint values.
        """
        caching_key = generate_unique_cache_key(key_suffix="ingestion_delay_metrics")
        last_update_key = "ingestion_delay_metrics_last_update"
        current_delay_avg = 0
        found_at_least_one_body_ts_match = False
        tz_offset = timezone(ingestion_delay_timestamp_timezone).localize(datetime.now()).strftime("%z")
        running_avg = RunningAverage()
        record = namedtuple("record", "event_timestamp body")
        records = [record(item.record.timestamp, item.record.body) for item in workflow_items]

        for record in records:  # type: ignore[assignment, misc]
            match = re.search(ingestion_delay_timestamp_regex, record.body)
            if match:
                found_at_least_one_body_ts_match = True
                body_datetime_match_result = match.group(0)
                if does_datetime_isoformat_contain_timezone(dt=body_datetime_match_result):
                    body_timestamp = datetime.fromisoformat(match.group(0))
                else:
                    body_timestamp = datetime.fromisoformat(f"{match.group(0)}{tz_offset}")

                event_timestamp = datetime.fromtimestamp(
                    convert_timestamp_to_datetime_fromtimestamp_format(record.event_timestamp),
                    tz=timezone(ingestion_delay_timestamp_timezone),
                )
                logger.debug(
                    f"[emit_ingestion_delay_metrics] event_datetime: {event_timestamp}, body_datetime: {body_timestamp}, event_timestamp: {event_timestamp.timestamp()}, body_timestamp: {body_timestamp.timestamp()}"
                )
                delta = event_timestamp - body_timestamp
                current_delay_avg = running_avg.update_average(delta.seconds)  # type: ignore[assignment]

        # did it find at least one event with body timestamp
        if found_at_least_one_body_ts_match:
            logger.info(
                f"[emit_ingestion_delay_metrics] Avg ingestion delay in current event stream: {current_delay_avg} secs"
            )
            self.timed_counter_add(
                key=caching_key,
                item=current_delay_avg,  # type: ignore[arg-type]
                timeout=ingestion_delay_metric_collection_interval_secs,
            )

        current_time = datetime.now()
        time_window_values = self.timed_counter_get_all_values(key=caching_key)
        logger.debug(f"[emit_ingestion_delay_metrics] Avg ingestion delay in current time window: {time_window_values}")

        last_update_delta: timedelta | None = None
        if last_update_time_from_cache := self.kvstore.get(last_update_key, None):
            last_update_time = datetime.fromtimestamp(last_update_time_from_cache)
            last_update_delta = current_time - last_update_time
            logger.debug(f"[emit_ingestion_delay_metrics] last update time: {last_update_time}")
            logger.debug(f"[emit_ingestion_delay_metrics] {last_update_delta.seconds} secs elapsed since last update")

        if not last_update_time_from_cache or (
            last_update_delta is not None
            and last_update_delta.seconds >= int(ingestion_delay_metric_collection_interval_secs)
        ):
            if time_window_values:
                time_window_avg = statistics.mean(time_window_values)  # type: ignore[type-var]
            else:
                time_window_avg = 0

            logger.debug(
                f"[emit_ingestion_delay_metrics] emitting ingestion delay in current time window: {round(time_window_avg, 1)} secs"  # type: ignore[call-overload]
            )
            WORKFLOW_LOG_QUERY_INGESTION_DELAY.set(round(time_window_avg, 1))  # type: ignore[call-overload]
            self.timed_counter_clear_all(key=caching_key)
            # store last update timestamp in cache/kvstore
            self.kvstore[last_update_key] = current_time.timestamp()

    def is_job_phase_transition_valid(
        self,
        current_phase: WorkflowStepNooutputJobPhase,
        transition_to: WorkflowStepNooutputJobPhase,
    ) -> bool:
        """
        Checks if the transition from the current job phase to the specified job phase is valid.

        Args:
            current_phase (WorkflowStepNooutputJobPhase): The current job phase.
            transition_to (WorkflowStepNooutputJobPhase): The job phase to which the transition is requested.

        Returns:
            bool: `True` if the transition from `current_phase` to `transition_to` is valid, otherwise `False`.
        """
        if current_phase == transition_to:
            return True

        return transition_to in JOB_PHASE_VALID_TRANSITIONS.get(current_phase, [])

    def cleanup_metric_gauges(self, threshold_secs: int):
        """
        Reset metric gauges for any job_id that has not been updated
        in the last threshold_secs.

        Args:
            threshold_secs (int): Time in seconds after which a gauge is considered stale.
        """
        if METRICS_CLEANUP_KEY not in self.kvstore:
            return

        current_time = datetime.now()
        # Use list() to safely iterate over keys while possibly modifying the dict.
        for metric in list(self.kvstore[METRICS_CLEANUP_KEY]):
            for job_id in list(self.kvstore[METRICS_CLEANUP_KEY][metric]):
                last_update = datetime.fromtimestamp(self.kvstore[METRICS_CLEANUP_KEY][metric][job_id])
                last_update_delta = current_time - last_update
                if last_update_delta.seconds >= int(threshold_secs):
                    logger.info(f"[cleanup_metric_gauges] Resetting metric: {metric}/{job_id}")
                    set_metric_gauge(metric_name=metric, job_id=job_id, amount=0)
                    del self.kvstore[METRICS_CLEANUP_KEY][metric][job_id]
