# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time
from collections.abc import Callable
from functools import wraps
from os import environ
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, Summary

from watcher.lib.utils.logging import logger

# Basic label setup
LABELS_NOTIFICATIONS = ["job_id"]
LABELS_QUERY = ["query"]

# Transport-level HTTP metrics (fires per attempt, including retries)
HTTP_REQUESTS_TOTAL = Counter(
    "watcher_downstream_http_requests_total",
    "Total HTTP requests including retry attempts",
    ["method", "host", "path", "service", "status_code"],
)

# Downstream metrics
LOG_QUERY_API_EXCEPTIONS = Counter(
    "watcher_downstream_log_query_api_exceptions", "Number of exceptions while reading log-query data"
)
LOG_QUERY_API_LATENCY = Histogram(
    "watcher_downstream_log_query_api_latency", "Time spent waiting for log-query data", LABELS_QUERY
)
LOG_QUERY_ERRORS = Counter("watcher_downstream_log_query_errors", "Number of errors while querying logs", LABELS_QUERY)
LOG_QUERY_ERROR_REASONS = Counter(
    "watcher_downstream_log_query_error_reasons",
    "Number of failed log queries, by backend and reason",
    ["backend", "reason"],
)
LOG_QUERY_PROCESSING_TIME = Summary(
    "watcher_downstream_log_query_processing_seconds", "Time spent processing log queries", LABELS_QUERY
)
LOG_QUERY_RECORDS_RECEIVED = Counter(
    "watcher_downstream_log_query_records_received",
    "Total raw log records received",
)
LOG_QUERY_RECORDS_DUPLICATE_DISCARDED = Counter(
    "watcher_downstream_log_query_records_duplicate_discarded",
    "Duplicate log records removed",
)
LOG_QUERY_EMPTY_RESPONSE = Counter(
    "watcher_downstream_log_query_empty_response",
    "Number of log queries that returned a successful but empty response",
    LABELS_QUERY,
)
LOG_QUERY_REQUESTS = Counter(
    "watcher_log_query_requests_total",
    "Number of backend-neutral log-query requests by bounded outcome",
    ["backend", "query_id", "outcome"],
)
LOG_QUERY_LATENCY = Histogram(
    "watcher_log_query_latency_seconds",
    "End-to-end latency of a backend-neutral log-query request",
    ["backend", "query_id"],
)
LOG_QUERY_RECORDS = Counter(
    "watcher_log_query_records_total",
    "Number of normalized records returned by log-query backends",
    ["backend", "query_id"],
)
JOB_MANAGER_API_REQUESTS = Counter(
    "watcher_downstream_job_manager_api_requests",
    "Total number of API calls to the Job Manager service",
    LABELS_NOTIFICATIONS,
)
JOB_MANAGER_API_ERRORS = Counter(
    "watcher_downstream_job_manager_api_errors",
    "Number of failed API calls to the Job Manager service",
    LABELS_NOTIFICATIONS,
)
JOB_MANAGER_API_LATENCY = Histogram(
    "watcher_downstream_job_manager_api_latency_seconds",
    "Latency of API calls to the Job Manager service",
    LABELS_NOTIFICATIONS,
)
RABBITMQ_MESSAGES_EXCEPTIONS = Counter(
    "watcher_downstream_rabbitmq_msg_exceptions", "Number of exceptions while sending messages to RabbitMQ"
)
RABBITMQ_HEALTHCHECK_FAILED = Counter(
    "watcher_downstream_rabbitmq_hc_failed", "Number of RabbitMQ health check failures"
)

# Workflow metrics
WORKFLOW_HANGNOOUTPUT_JOB_NOT_RUNNING_STATE = Counter(
    "watcher_workflow_hangnooutput_job_not_running_state", "HANGNOOUTPUT Job not in RUNNING State", LABELS_NOTIFICATIONS
)
WORKFLOW_HANGNOOUTPUT_LAST_ITERATION = Counter(
    "watcher_workflow_hangnooutput_last_iteration", "HANGNOOUTPUT last iteration detected", LABELS_NOTIFICATIONS
)
WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND = Counter(
    "watcher_workflow_hangnooutput_validation_events_found",
    "Number of HANGNOOUTPUT validations where the log-query backend returned events",
    LABELS_NOTIFICATIONS,
)
WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND = Counter(
    "watcher_workflow_hangnooutput_validation_events_not_found",
    "Number of HANGNOOUTPUT validations where the log-query backend returned no events",
    LABELS_NOTIFICATIONS,
)
WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED = Counter(
    "watcher_workflow_hangnooutput_skipped_loki_degraded",
    "Number of HANGNOOUTPUT anomalies suppressed because Loki was in a degraded state",
    LABELS_NOTIFICATIONS,
)
WORKFLOW_HANGNOOUTPUT_RUNTIME_ERROR = Counter(
    "watcher_workflow_hangnooutput_runtime_error", "Number of HANGNOOUTPUT Runtime Errors", LABELS_NOTIFICATIONS
)
WORKFLOW_LOG_QUERY_EVENTS = Counter(
    "watcher_workflow_log_query_events", "Number of log events returned by log queries", LABELS_QUERY
)
WORKFLOW_LOG_QUERY_EVENTS_DISCARDED_PRE_PROCESSING = Counter(
    "watcher_workflow_events_discarded_preprocessing", "Number of events discarded pre processing"
)
WORKFLOW_LOG_QUERY_INGESTION_DELAY = Gauge(
    "watcher_workflow_log_query_ingestion_delay_seconds",
    "Log ingestion delay",
)
WORKFLOW_LOG_QUERY_FAILURES = Counter(
    "watcher_workflow_log_query_failures",
    "Log-query failures",
)
WORKFLOW_NOTIFICATION_CHECKPOINTLOAD = Counter(
    "watcher_workflow_notification_checkpointload", "Number of CHECKPOINTLOAD events detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_CHECKPOINTSAVE = Counter(
    "watcher_workflow_notification_checkpointsave", "Number of CHECKPOINTSAVE events detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_CRASH = Counter(
    "watcher_workflow_notification_crash", "Number of CRASH events detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_HANGAPP = Counter(
    "watcher_workflow_notification_hangapp", "Number of HANGAPP detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_HANGNOOUTPUT = Counter(
    "watcher_workflow_notification_hangnooutput", "Number of HANGNOOUTPUT detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_HANGPUSHRECV = Counter(
    "watcher_workflow_notification_hangpushrecv", "Number of HANGPUSHRECV detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_NESTED_RESTARTER = Counter(
    "watcher_workflow_notification_nested_restarter", "Number of NESTED_RESTARTER detected", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_JOB_EVENTS = Counter(
    "watcher_workflow_notification_job_events", "Number of events detected for a given job", LABELS_NOTIFICATIONS
)
WORKFLOW_NOTIFICATION_STRAGGLER = Counter(
    "watcher_workflow_notification_straggler", "Number of STRAGGLER events detected", LABELS_NOTIFICATIONS
)
WORKFLOW_PROCESSING_TIME = Summary("watcher_workflow_processing_seconds", "Time spent processing workflow")
WORKFLOW_RUN = Counter("watcher_workflow_run", "Number of workflow runs")
WORKFLOW_RABBITMQ_MESSAGES = Counter("watcher_workflow_rabbitmq_messages", "Number of messages sent to RabbitMQ")
WORKFLOW_RABBITMQ_MESSAGES_SKIPPED = Counter(
    "watcher_workflow_rabbitmq_messages_skipped", "Number of duplicated messages skipped"
)
WORKFLOW_JOB_THROUGHPUT = Gauge(
    "watcher_workflow_job_throughput", "Track training jobs throughput", LABELS_NOTIFICATIONS
)
WORKFLOW_CACHE_ACTION_SIZE = Gauge("watcher_workflow_cache_action_size", "Number of keys in Workflow Action Cache")
WORKFLOW_CACHE_KV_SIZE = Gauge("watcher_workflow_cache_kv_size", "Number of keys in Workflow KV Cache")

# Duration metrics for workflow methods
WORKFLOW_STEP_REGEX_PARSER_DURATION = Histogram(
    "watcher_workflow_step_regex_parser_seconds", "Duration of workflow_step_regex_parser execution"
)
WORKFLOW_STEP_SIMPLE_ACTION_DURATION = Histogram(
    "watcher_workflow_step_simple_action_seconds", "Duration of workflow_step_simple_action execution"
)
WORKFLOW_STEP_COMPARE_LAST_DATAPOINT_DURATION = Histogram(
    "watcher_workflow_step_compare_last_datapoint_seconds", "Duration of workflow_step_compare_last_datapoint execution"
)
WORKFLOW_STEP_MOVING_AVG_DURATION = Histogram(
    "watcher_workflow_step_moving_avg_seconds", "Duration of workflow_step_moving_avg execution"
)
WORKFLOW_STEP_NOOUTPUT_DURATION = Histogram(
    "watcher_workflow_step_nooutput_seconds", "Duration of workflow_step_nooutput execution"
)
WORKFLOW_STEP_STORE_DATAPOINT_DURATION = Histogram(
    "watcher_workflow_step_store_datapoint_seconds", "Duration of workflow_step_store_datapoint execution"
)
WORKFLOW_STEP_EMIT_METRIC_DURATION = Histogram(
    "watcher_workflow_step_emit_metric_seconds", "Duration of workflow_step_emit_metric execution"
)
SEND_MESSAGE_EVENTBUS_DURATION = Histogram(
    "watcher_send_message_eventbus_seconds", "Duration of send_message_eventbus execution"
)
LOG_QUERY_RUN_DURATION = Histogram(
    "watcher_log_query_run_seconds",
    "Duration of log-query execution",
)
E2E_SYNTHETIC_APPLICATION_LOG_RCVD = Counter(
    "watcher_e2e_synthetic_application_log_rcvd", "Number of synthetic application log events received"
)

# Metrics using lazy initialization - initialized lazily only when feature flag is enabled
LOKI_CERT_EXPIRATION_DAYS = None
LOKI_CA_CERT_EXPIRATION_DAYS = None
EVENT_DEDUP_CACHE_UTILIZATION = None


def _initialize_cert_metrics() -> None:
    """Initialize certificate expiration metrics if not already created.

    This function is called lazily when certificate-based authentication is detected.
    """
    global LOKI_CERT_EXPIRATION_DAYS, LOKI_CA_CERT_EXPIRATION_DAYS

    if LOKI_CERT_EXPIRATION_DAYS is None:
        LOKI_CERT_EXPIRATION_DAYS = Gauge(
            "watcher_loki_cert_expiration_days", "Days until Loki client certificate expires"
        )

    if LOKI_CA_CERT_EXPIRATION_DAYS is None:
        LOKI_CA_CERT_EXPIRATION_DAYS = Gauge(
            "watcher_loki_ca_cert_expiration_days", "Days until Loki CA certificate expires"
        )


def _initialize_deduplicator_metrics() -> None:
    """Initialize EventDeduplicator capacity metrics if not already created."""
    global EVENT_DEDUP_CACHE_UTILIZATION

    if EVENT_DEDUP_CACHE_UTILIZATION is None:
        EVENT_DEDUP_CACHE_UTILIZATION = Gauge(
            "watcher_event_dedup_cache_utilization_percent", "EventDeduplicator cache utilization percentage"
        )


# Provider-independent poller metrics.
LOG_QUERY_POLLER_BACKLOG = Gauge(
    "watcher_log_query_poller_backlog_seconds",
    "Log-query cursor backlog",
    LABELS_QUERY,
)
LOG_QUERY_POLLER_CHUNK_DURATION = Histogram(
    "watcher_log_query_poller_chunk_duration_seconds",
    "Log-query chunk duration",
    LABELS_QUERY,
)
LOG_QUERY_POLLER_RETRY_COUNT = Counter(
    "watcher_log_query_poller_retry_count",
    "Log-query retries",
    LABELS_QUERY,
)
LOG_QUERY_POLLER_AUTH_RESTARTS = Counter(
    "watcher_log_query_poller_auth_restarts",
    "Log-query auth restarts",
)
LOG_QUERY_POLLER_CURSOR_TIMESTAMP = Gauge(
    "watcher_log_query_poller_cursor_timestamp",
    "Log-query cursor timestamp",
    LABELS_QUERY,
)
LOG_QUERY_POLLER_ITERATIONS = Counter(
    "watcher_log_query_poller_iterations",
    "Log-query poller iterations",
)
LOG_QUERY_POLLER_CHUNKS_PROCESSED = Counter(
    "watcher_log_query_poller_chunks_processed",
    "Log-query chunks processed",
    LABELS_QUERY,
)
LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED = Counter(
    "watcher_log_query_poller_iteration_budget_exceeded",
    "Log-query iteration budgets exceeded",
    LABELS_QUERY,
)
LOG_QUERY_WINDOW_CAPPED = Counter(
    "watcher_log_query_window_capped_seconds",
    "Seconds of cursor lag fast-forwarded by the HTTP_MAX_QUERY_WINDOW_SECS safety cap in batch_query",
    LABELS_QUERY,
)


def increase_metric_counter(metric_name: str, job_id: str, amount: float = 1) -> None:
    """
    Increment a counter metric by a specified amount.

    Args:
        metric_name (str): The metric name.
        job_id (str): Job ID.
        amount (float, optional): The amount by which to increment the metric.
                                  Defaults to 1.
    """
    try:
        metric = globals()[metric_name]
    except KeyError:
        logger.warning(f"metric not found: {metric_name}")
        return

    metric.labels(job_id=job_id).inc(amount)


def set_metric_gauge(metric_name: str, job_id: str, amount: float) -> None:
    """
    Increment a counter metric by a specified amount.

    Args:
        metric_name (str): The metric name.
        job_id (str): Job ID.
        amount (float, int): The amount to set the gauge.
    """
    try:
        metric = globals()[metric_name]
    except KeyError:
        logger.warning(f"metric not found: {metric_name}")
        return

    metric.labels(job_id=job_id).set(amount)


def emit_app_info_metrics(environment_variables: str | list[str]) -> None:
    """
    Emits App Info metrics.

    Args:
        metric_name (Union[str, List[str]]): The name of environment variables to be exported as metrics.
    """
    metric_name = "app_info"

    if isinstance(environment_variables, str):
        environment_variables = [environment_variables]

    APP_INFO = Gauge(metric_name, "App Information", ["name"])

    for env_var in environment_variables:
        if env_var_value := environ.get(env_var):
            logger.info(
                f"[emit_app_info_metrics] Emitting app info metric: {metric_name}/{env_var.lower()}_{env_var_value}"
            )
            APP_INFO.labels(name=f"{env_var.lower()}_{env_var_value}").set(1)


def measure_duration(metric: Histogram):
    """
    Decorator to measure the execution duration of a function and record it in a Prometheus Histogram.

    Args:
        metric (Histogram): The Prometheus Histogram metric to observe the duration.

    Returns:
        Callable: The decorator function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.perf_counter() - start_time
                metric.observe(duration)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.perf_counter() - start_time
                metric.observe(duration)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
