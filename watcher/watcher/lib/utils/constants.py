# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from recovery_libs.schema.event_type import EventType

from watcher.models.workflow import WorkflowStepNooutputJobPhase

## Job Manager
##
JOB_MANAGER_PATH_GET_STATUS = "job_status"
JOB_MANAGER_DATETIME_MASK = "%Y-%m-%dT%H:%M:%SZ"

## Workflow
##
JOB_ATTRIBUTES_NOOUTPUT_PREFIX = "job_attrs_nooutput_"
# Pending discovery stores the retry deadline and latest observed activity time.
JOB_NOOUTPUT_PENDING_PREFIX = "job_nooutput_pending_"
# Allow more than one 30-second discovery interval before falling back.
JOB_NOOUTPUT_DISCOVERY_GRACE_SECS = 31
JOB_LAST_EVENT_PREFIX = "job_last_event_"
JOB_LEVEL_DEDUP_CACHE_PREFIX = "job_terminal_exception_"
JOB_PHASE_VALID_TRANSITIONS = {
    WorkflowStepNooutputJobPhase.PRE_TRAINING: [WorkflowStepNooutputJobPhase.TRAINING_LOOP],
}
JOB_PHASE_ORDERED = (
    WorkflowStepNooutputJobPhase.PRE_TRAINING,
    WorkflowStepNooutputJobPhase.TRAINING_LOOP,
)
JOB_STATE_CACHE_CLEANUP_KEY_PREFIXES = (
    JOB_ATTRIBUTES_NOOUTPUT_PREFIX,
    JOB_LAST_EVENT_PREFIX,
    JOB_NOOUTPUT_PENDING_PREFIX,
)
METRICS_CLEANUP_KEY = "garbage_collected_metrics"
METRICS_CLEANUP_THRESHOLD_SECS = 600
MAINTENANCE_TASKS_INTERVAL_SECS = 30
MAINTENANCE_TASKS_LAST_RUN_KEY = "maintenance_tasks_last_run"
STRAGGLER_ANOMALY_DEDUP_CACHE_PREFIX = "straggler_dedup_"

## Event Detection
##
# Event types whose publication is gated by the per-job aps_spec.anomaly_detection_config flags.
# When the corresponding flag is explicitly disabled, the watcher will drop events of these types
# at publication time. Mapping from event type -> flag lives in watcher.lib.utils.aps_spec.
EVENT_TYPES_REQUIRING_DETECTION_CHECK = [EventType.CRASH, EventType.HANGNOOUTPUT]

## App Info Environment Variables
##
APP_INFO_ENV_VARS = [
    "LOG_LEVEL",
    "VERSION",
]

## Watcher Service
##
DEFAULT_QUEUE_TIMEOUT_SECS = 15
DEFAULT_LOG_QUERY_TIMEOUT_SECS = 35
DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS = 5

## Log-query poller
##
LOG_QUERY_POLLER_CHUNK_SIZE_SECS = 60  # Maximum time window per chunk
LOG_QUERY_POLLER_MAX_BACKOFF_SECS = 300  # Maximum backoff delay (5 minutes)
LOG_QUERY_POLLER_INITIAL_BACKOFF_SECS = 2  # Initial backoff delay
LOG_QUERY_AUTH_RESTART_COOLDOWN_SECS = 60  # Minimum time between client restarts triggered by auth failures
LOG_QUERY_POLLER_ITERATION_BUDGET_SECS = 300  # Per-query wall-clock budget (asyncio.wait_for)
LOG_QUERY_VALIDATION_MAX_RETRIES = 3  # Max retry attempts for validation queries
LOG_QUERY_VALIDATION_BACKOFF_SECS = 1  # Base backoff multiplier between validation retries (idx * N)
LOG_QUERY_HIGH_EVENT_COUNT_THRESHOLD = 50  # Log INFO when a single query returns more events than this
# Hard ceiling on one backend query window.
HTTP_MAX_QUERY_WINDOW_SECS = 300

## Loki Performance Monitor
##
LOKI_PERF_MONITOR_INTERVAL_SECS = 60  # How often the monitor evaluates (seconds)
LOKI_PERF_MONITOR_WINDOW_SECS = 300  # Sliding window for observation analysis (seconds)
LOKI_PERF_MONITOR_EMPTY_THRESHOLD = 5  # Min empty responses in window to fire CONSECUTIVE_EMPTY_RESPONSES
LOKI_PERF_MONITOR_ZERO_SCAN_THRESHOLD = 3  # Min zero-scan empties in window to fire ZERO_SCAN_EMPTY
LOKI_PERF_MONITOR_HIGH_QUEUE_THRESHOLD = 3  # Min high-queue queries in window to fire HIGH_QUEUE_TIME
LOKI_PERF_MONITOR_QUEUE_TIME_RATIO = 0.8  # queue/(exec+queue) ratio above which a query is "high queue"
LOKI_PERF_MONITOR_MAX_OBSERVATIONS = 1000  # Ring buffer capacity for recent query observations

## Event Deduplicator
##
EVENT_DEDUP_DEFAULT_WINDOW_SECS = 60
EVENT_DEDUP_DEFAULT_MAX_SIZE = 10000
