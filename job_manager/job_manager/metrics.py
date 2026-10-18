# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from prometheus_client import Counter, Gauge, Histogram

NAMESPACE = "aps"
SUBSYSTEM = "job_manager"

STALE_RUNNING_JOBS_GAUGE = Gauge(
    name="stale_running_jobs",
    documentation="The number of potentially stale running jobs in job manager.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
UNEXPECTED_EARLY_STOP_REQUEUE_JOBS_GAUGE = Gauge(
    name="unexpected_early_stopp_requeue_jobs",
    documentation="The number of jobs managed by requeue where the requeue process stopped earlier than expected.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
JOB_UPDATE_MESSAGE_RECEIVED_COUNTER = Counter(
    name="job_update_message_received",
    documentation="The number of job update event bus messages received from WFC.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
ANOMALY_MESSAGE_RECEIVED_COUNTER = Counter(
    name="anomaly_message_received",
    documentation="The number of job anomaly event bus messages received from Watcher.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
ANOMALY_MESSAGE_REDELIVERY_COUNTER = Counter(
    name="anomaly_message_redelivery",
    documentation="The number of job anomaly event bus messages redelivery attempts from rabbitmq dead letter queue",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
END_ANOMALY_GENERATED_COUNTER = Counter(
    name="end_anomaly_generated",
    documentation="The number of job end anomalies generated.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
CRASH_ANOMALY_GENERATED_COUNTER = Counter(
    name="crash_anomaly_for_failed_jobs_generated",
    documentation="The number of job crash anomalies persisted for failed jobs without anomalies.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
CHECKPOINT_MESSAGE_RECEIVED_COUNTER = Counter(
    name="checkpoint_message_received",
    documentation="The number of checkpoint event bus messages received from Watcher.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
CHECKPOINT_MESSAGE_REDELIVERY_COUNTER = Counter(
    name="checkpoint_message_redelivery",
    documentation="The number of job checkpoint event bus messages redelivery attempts from rabbitmq dead letter queue",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
ANOMALY_ATTRIBUTION_MESSAGE_RECEIVED_COUNTER = Counter(
    name="anomaly_attribution_message_received",
    documentation="The number of anomaly attribution event bus messages received from the anomaly isolator.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
EVENT_BUS_RETRYABLE_ERROR_COUNTER = Counter(
    name="event_bus_retryable_error",
    documentation="The number of event bus retryable errors encountered by job manager when consuming messages.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
EVENT_BUS_UNEXPECTED_ERROR_COUNTER = Counter(
    name="event_bus_unexpected_error",
    documentation="The number of event bus unexpected errors encountered by job manager when consuming messages.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
JOB_WITH_INVALID_CONFIG_COUNTER = Counter(
    name="job_with_invalid_config",
    documentation="The number of jobs with invalid configuration detected by job manager.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
    labelnames=["failure_type"],
)

EVENT_ATTEMPT_MATCH_BY_LOG_FILE_NAME = Counter(
    name="event_attempt_match_by_log_file_name",
    documentation="Events matched to job attempts via log file name (primary).",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
    labelnames=["job_id"],
)
EVENT_ATTEMPT_MATCH_BY_SLURM_JOB_ATTEMPT = Counter(
    name="event_attempt_match_by_slurm_job_attempt",
    documentation="Events matched to job attempts via explicit slurm_job_attempt identity.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
    labelnames=["job_id"],
)
LEADER_STATUS_GAUGE = Gauge(
    name="leader_status",
    documentation="Whether there is a leader in the deployment (1 = yes, 0 = no).",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

EVENT_ATTEMPT_MATCH_BY_LATEST_ATTEMPT = Counter(
    name="event_attempt_match_by_latest_attempt",
    documentation="Events matched to job attempts via latest attempt in DB (last resort).",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
    labelnames=["job_id"],
)

ATTRIBUTION_REQUEST_EMITTED_COUNTER = Counter(
    name="attribution_request_emitted",
    documentation="The number of AttributionRequest events emitted to ADI by job manager.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
ATTRIBUTION_REQUEST_SKIPPED_COUNTER = Counter(
    name="attribution_request_skipped",
    documentation="The number of AttributionRequest emissions skipped by the emit gate, labeled by reason.",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
    labelnames=["reason"],
)
ATTRIBUTION_REQUEST_PUBLISH_FAILED_COUNTER = Counter(
    name="attribution_request_publish_failed",
    documentation=(
        "The number of AttributionRequest emissions that won the atomic claim but failed to publish "
        "(the silent-gap signal for missed ADI triggers)."
    ),
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

RECOVERY_SEQUENCES_COUNTER = Counter(
    name="nvidia_recovery_sequences_total",
    documentation="Durable recovery sequence begin operations.",
    labelnames=["operation", "trigger_type", "outcome"],
)
RECOVERY_MILESTONES_RECORDED_COUNTER = Counter(
    name="nvidia_recovery_milestones_recorded_total",
    documentation="Durable recovery milestone records accepted by Job Manager.",
    labelnames=["operation", "action", "outcome", "record_result"],
)
RECOVERY_END_TO_END_DURATION = Histogram(
    name="nvidia_recovery_end_to_end_duration_seconds",
    documentation="Trigger-to-explicit-terminal-milestone duration for completed recovery sequences.",
    labelnames=["operation"],
)
