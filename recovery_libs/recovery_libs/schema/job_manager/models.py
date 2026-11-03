# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, PositiveInt, model_validator

from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.workflow_identity import CanonicalWorkflowID

## This set maintains the list of actionable anomaly types.
## Note: STRAGGLER is intentionally excluded from this set because straggler anomalies
## do not trigger any remediation action (no requeuing), so they should not prevent
## END event generation when jobs finish or timeout.
ACTIONABLE_ANOMALY_TYPES = {
    EventType.CRASH.value,
    EventType.CRASHAPPEXCEPTION.value,
    EventType.CRASHOOM.value,
    EventType.CRASHSEGV.value,
    EventType.CRASHNAN.value,
    EventType.END.value,
    EventType.FLTINJECTION.value,
    EventType.FLTINJECTIONINFRA.value,
    EventType.HANG.value,
    EventType.HANGAPP.value,
    EventType.HANGCOMM.value,
    EventType.HANGNOOUTPUT.value,
    EventType.HANGPUSHRECV.value,
}


class JobAnomalyHandledBy(str, Enum):
    HEIMDALL = "HEIMDALL"
    UNHANDLED = "UNHANDLED"


class JobTerminalAdvice(str, Enum):
    """Advice types for job terminal state transitions.

    These are used to communicate the reason for a job's terminal state to the job manager.
    """

    USER_CANCELED = "user_canceled"
    """Job was cancelled by the user."""

    MAX_REQUEUE_REACHED = "max_requeue_reached"
    """Job has reached the maximum requeue limit."""

    JOB_RELEASED = "job_released"
    """Job was released to start a new attempt."""

    MANUAL_KILL = "manual_kill"
    """Job was manually killed via API."""

    MANUAL_RELEASE = "manual_release"
    """Job was manually released via API."""

    APPLICATION_REQUESTED_CANCELLATION = "application_requested_cancellation"
    """The application requested that the scheduler job and its retries stop."""


class ApplicationIntentionDesiredState(str, Enum):
    """Desired states that an application may request for its scheduler job."""

    CANCELLED = "CANCELLED"


class ApplicationIntentionInput(BaseModel):
    """Application request to stop a scheduler job and prevent later retries."""

    job_id: str = Field(min_length=1, max_length=64)
    desired_state: ApplicationIntentionDesiredState
    reason: str | None = Field(default=None, max_length=1024)


class ApplicationIntention(BaseModel):
    """Durably accepted application intention for a scheduler job."""

    id: int = Field(json_schema_extra={"format": "int64"})
    job_id: str
    desired_state: ApplicationIntentionDesiredState
    reason: str | None = None
    created_at: datetime


class JobAnomaly(BaseModel):
    """
    Class to represent an anomaly that happened during the job execution.

    Note: Today given this model is very simple, it's also directly returned
    from job manager's REST API. We can introduce a new model and migrate the
    internal data model to it when we need to distinguish them.

    Note that for newly created JobAnomaly the default value for `processed` is False,
    but old data read from the storage, the default value will be set to None to provide
    correct semantic because they will never be set to True or False.
    """

    anomaly_type: str
    timestamp: datetime  # Time when this event was ingested into the loki database
    uuid: str | None = None
    processed: bool | None = False
    isolation_result_json: str | None = None
    original_watcher_anomaly_event: str | None = None
    handled_by: JobAnomalyHandledBy | None = JobAnomalyHandledBy.UNHANDLED
    trace_id: str | None = None


class JobCheckpointInfo(BaseModel):
    """
    Data structure that represents a job checkpoint event.
    """

    timestamp: datetime
    uuid: str


class JobEvent(BaseModel):
    """
    The public facing class for job events.
    """

    uuid: uuid.UUID
    type: EventType
    time: datetime
    payload: str
    log_file_name: str | None = None


class Workflow(BaseModel):
    """
    A workflow is a collection of jobs for a particular workload or logical job.

    For example, a job maybe requeued multiple times, resulting in a workflow.
    """

    id: CanonicalWorkflowID
    start_time: datetime | None = None
    end_time: datetime | None = None


class GetWorkflowsResponse(BaseModel):
    """
    The public facing data model of the jobs by name response that gets
    returned from job manager's REST APIs. It contains a list of JobsByName
    objects.
    """

    item_count: int
    workflows: list[Workflow]


class SlurmJobExecutionStatus(BaseModel):
    """
    The public facing data model of the Slurm job execution status that gets
    returned from job manager's REST APIs. It contains a subset fields of the
    internal data model (SlurmJobExecutionRecord class) to achieve separation.
    """

    job_id: str
    heimdall_job_id: uuid.UUID | None = None
    job_attempt_index: int
    workflow_id: CanonicalWorkflowID
    job_group_id: str | None = None

    job_name: str | None = None
    state: str | None = None
    state_reason: str | None = None

    submit_time: datetime | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    cluster: str | None = None
    partition: str | None = None
    reservation: str | None = None
    namespace: str | None = None
    worker_cluster_name: str | None = None
    user: str | None = None
    command: str | None = None
    dependency: str | None = None

    node_count: int | None = None
    total_gpu_count: int | None = None
    total_cpu_count: int | None = None
    total_memory_mb: int | None = None
    allocated_node_list: list[str] | None = None
    inclusion_node_list: list[str] | None = None
    exclusion_node_list: list[str] | None = None
    failed_node_list: list[str] | None = None
    sticky_exclusion_node_list: list[str] | None = None

    scheduler_metadata: dict = Field(default={}, serialization_alias="schedulerMetadata")

    anomalies: list[JobAnomaly] | None = None
    checkpoints: list[JobCheckpointInfo] | None = None
    aps_spec: ApsSlurmJobSpec | None = None
    config_validation_failure_types: list[str] | None = None

    retry_of: str | None = Field(default=None, deprecated=True)

    last_updated_at: datetime | None = None
    log_file_paths: list[str] | None = None
    filename: str | None = None


class JobExecutionStatuses(BaseModel):
    """
    This class is used to return a list of job execution statuses from the job manager's REST API. Since the response is
    paginated, this class also contains the total number of items that match the query.
    """

    item_count: int
    statuses: list[SlurmJobExecutionStatus]


class JobGroupStatus(BaseModel):
    """
    The public facing data model of a job group's aggregated metadata, derived from
    the job execution records belonging to the group.
    """

    job_group_id: str
    unique_job_count: int
    total_job_attempt_count: int


class JobGroupStatuses(BaseModel):
    """
    This class is used to return a list of job group statuses from the job manager's REST API. Since the response is
    paginated, this class also contains the total number of items that match the query.
    """

    item_count: int
    job_groups: list[JobGroupStatus]


class JobTimeFilterTarget(str, Enum):
    """
    The target job time to use in a time filter query. Jobs have different times, such
    as the submit, start, end times and the last updated time. This enum
    allows selecting one of these for time filter queries when retrieving jobs by time.
    """

    SUBMIT_TIME = "SUBMIT_TIME"
    START_TIME = "START_TIME"
    END_TIME = "END_TIME"
    LAST_UPATED_TIME = "LAST_UPDATE_AT"


class JobTimeFilter(BaseModel):
    """
    A job time filter query is composed by the job time target, and
    either a from time, or a to time, or both.
    If both from and to time are missing, then an exception will be raised.
    If both are present, then from needs to be before to. Both times need to
    be in UTC.
    """

    job_time_target: JobTimeFilterTarget
    from_time: datetime | None = None  # in utc
    to_time: datetime | None = None  # in utc

    @model_validator(mode="after")
    def validate(self):
        if self.from_time is None and self.to_time is None:
            raise ValueError("Either from or to time required for a time filter query")
        if self.from_time and self.to_time and self.from_time >= self.to_time:
            raise ValueError(f"From time {self.from_time} must be before to time {self.to_time}")
        if self.from_time and self.from_time.strftime("%Z") != "UTC":
            raise ValueError(f"From time needs to be in UTC, got tz={self.from_time.strftime('%Z')}")
        if self.to_time and self.to_time.strftime("%Z") != "UTC":
            raise ValueError(f"To time needs to be in UTC, got tz={self.to_time.strftime('%Z')}")

        return self


class SortOrderEnum(str, Enum):
    ASC = "asc"
    DESC = "desc"


class SystemRequeueConfig(BaseModel):
    """
    Configuration class for system requeue behavior settings.

    Attributes:
    - max_requeue_times: Controls the maximum number of requeue that the system allows a job to have. Together
        with the job level requeue limit, the lower value will be used to determine the actual requeue limit. In the
        rare case of it set to None, it means the system allows unlimited requeue. "-1" is a special value that means
        infinitely requeue.

    - checkpoint_based_requeue_stop_threshold: Controls after how many job attempts without a successful checkpoint,
        the system will stop requeueing the job. This is a system wide setting and will be applied to all jobs. If not
        configured, the system will not use checkpoint signal to stop the requeue sequence.

    - requeue_on_completion: Controls whether the system should continue requeuing a job when it's final state is
      COMPLETED.
    """

    max_requeue_times: int | None = None
    checkpoint_based_requeue_stop_threshold: PositiveInt | None = None
    requeue_on_completion: bool = True


def filter_anomalies(
    anomalies: list[JobAnomaly] | None,
    later_than: datetime | None = None,
    processed: bool | None = None,
) -> list[JobAnomaly]:
    """
    Filters the anomalies based on the provided timestamp and returns a list of actionable anomalies.
    """
    if not anomalies:
        return []

    result = [anomaly for anomaly in anomalies if anomaly.anomaly_type in ACTIONABLE_ANOMALY_TYPES]
    if later_than:
        result = [anomaly for anomaly in result if anomaly.timestamp > later_than]

    if processed is not None:
        result = [anomaly for anomaly in result if anomaly.processed == processed]

    return result


class JobExecutionError(BaseModel):
    """
    Represents an error that occurred during job execution.

    This model captures details about errors including their type, code, signature,
    and descriptive information to help with debugging and error tracking.

    Attributes:
        code: The error code if available, zero otherwise
        message: Short error message
        description: Detailed description of the error
        signature: A unique signature identifying this specific error
    """

    # The error code, a unique identifier for the error, if available, zero otherwise
    # This would be the XID code for XID errors, or the NCCL code for NCCL errors, etc.
    code: int
    # A short message describing the error
    message: str
    # A detailed description of the error
    description: str
    # A unique signature identifying this specific error, like a regex pattern or metric name
    signature: str


class NodeEvent(BaseModel):
    """
    Represents an event that occurred on a node during job execution.

    This model captures details about node events including error attribution,
    timestamps, and relationships to jobs and errors.

    Attributes:
        job_id: ID of the job associated with this event
        job_attempt_index: Index of the job attempt
        node: Name of the node where the event occurred
        timestamp: When the event occurred
        attribution: Classification of the error (hardware/software/system/unknown)
        attribution_type: Type of telemetry data used for attribution
        attribution_origin: Origin of the attribution
        attribution_sources: Sources of the attribution
        actions: Actions taken by ADI
        fact_status: Status of the FACT service
        error: The associated error details
    """

    # The name of the node where the event occurred
    node: str
    # The timestamp when the error event was reported
    timestamp: datetime
    # The unique identifier for the job that incurred the error
    job_id: str
    # The index of the run for the job that incurred the error
    job_attempt_index: int
    # The start time of the job that incurred the error
    job_start_time: datetime | None = None
    # The end time of the job that incurred the error
    job_end_time: datetime | None = None
    # The associated error that occurred on the node during job execution
    job_execution_error: JobExecutionError
    # The cluster where the job was run
    cluster: str | None = None
    # The attribution of the error
    error_attribution: str
    # The type of telemetry data used for error attribution
    error_attribution_type: str
    # The origin of the error attribution
    error_attribution_origin: str
    # The sources of the error attribution
    error_attribution_sources: list[str]
    # The type of exclusion taken by ADI
    exclusion_type: str
    # The original type of exclusion before sanitization
    original_exclusion_type: str | None = None
    # The status of the FACT service
    fact_status: str


class GlobalEvent(BaseModel):
    """
    Represents a global (job-level) event that occurred during job execution.

    Unlike NodeEvent, GlobalEvent represents issues that affect the entire job
    rather than being attributable to specific nodes. These events don't have
    node-specific fields like node name, exclusion_type, job_start_time, job_end_time, or cluster.

    Attributes:
        job_id: ID of the job associated with this event
        job_attempt_index: Index of the job attempt
        timestamp: When the event occurred
        job_execution_error: The associated error details
        error_attribution: Classification of the error
        error_attribution_type: Type of telemetry data used for attribution
        error_attribution_origin: Origin of the attribution
        error_attribution_sources: Sources of the attribution
        fact_status: Status of the FACT service
    """

    # The ID of the job this event is associated with
    job_id: str
    # The index of the job attempt this event is associated with
    job_attempt_index: int
    # The timestamp when the event occurred
    timestamp: datetime
    # The associated error that occurred during job execution
    job_execution_error: JobExecutionError
    # The attribution of the error
    error_attribution: str
    # The type of telemetry data used for error attribution
    error_attribution_type: str
    # The origin of the error attribution
    error_attribution_origin: str
    # The sources of the error attribution
    error_attribution_sources: list[str]
    # The status of the FACT service
    fact_status: str


class NodeEvents(BaseModel):
    """
    Represents a collection of node events and global events.

    This model contains lists of NodeEvent and GlobalEvent objects and provides metadata
    about the total number of items and the page size.

    TODO: Rename this class to something more generic like `AttributionEvents` or
    `ErrorAttributions` since it now contains both node-level and global/job-level events,
    not just node events.
    """

    item_count: int
    attribution_summary: str
    attribution_insight: str
    events: list[NodeEvent]
    global_events: list[GlobalEvent] | None = None


# ============================================================================
# Advice Models - For NVIDIA Recovery Advice CRD integration
# ============================================================================


class RecoveryOperation(str, Enum):
    RESTART = "restart"
    TERMINATE = "terminate"
    DAG_RELEASE = "dag_release"


class RecoveryTriggerType(str, Enum):
    ANOMALY = "anomaly"
    JOB_STATE = "job_state"
    DEPENDENCY = "dependency"
    OPERATOR = "operator"


class RecoverySequenceStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"


class RecoverySchedulerAction(str, Enum):
    SLURM_REQUEUE_HOLD = "slurm.requeue_hold"
    SLURM_UPDATE_CONSTRAINTS = "slurm.update_constraints"
    SLURM_RELEASE = "slurm.release"
    SLURM_CANCEL = "slurm.cancel"


class RecoverySchedulerActionOutcome(str, Enum):
    PLANNED = "planned"
    SUCCESS = "success"
    ERROR = "error"
    ALREADY_SATISFIED = "already_satisfied"


class RecoveryContext(BaseModel):
    sequence_id: str = Field(
        min_length=1,
        max_length=64,
        serialization_alias="sequenceId",
        validation_alias=AliasChoices("sequenceId", "sequence_id"),
    )
    traceparent: str = Field(min_length=1, max_length=128)
    tracestate: str | None = Field(default=None, max_length=512)
    triggered_at: datetime = Field(
        serialization_alias="triggeredAt",
        validation_alias=AliasChoices("triggeredAt", "triggered_at"),
    )
    operation: RecoveryOperation


class GetRecoverySequenceRequest(BaseModel):
    """Exact natural-key lookup; never creates recovery state."""

    model_config = ConfigDict(extra="forbid")

    scheduler_system: str = Field(
        min_length=1,
        max_length=32,
        serialization_alias="schedulerSystem",
        validation_alias=AliasChoices("schedulerSystem", "scheduler_system"),
    )
    scheduler_scope: str = Field(
        min_length=1,
        max_length=256,
        serialization_alias="schedulerScope",
        validation_alias=AliasChoices("schedulerScope", "scheduler_scope"),
    )
    job_id: str = Field(
        min_length=1,
        max_length=256,
        serialization_alias="jobId",
        validation_alias=AliasChoices("jobId", "job_id"),
    )
    source_attempt_index: int = Field(
        ge=0,
        serialization_alias="sourceAttemptIndex",
        validation_alias=AliasChoices("sourceAttemptIndex", "source_attempt_index"),
    )
    operation: RecoveryOperation


class GetRecoverySequenceResponse(BaseModel):
    """Canonical context of an existing backend-owned recovery sequence."""

    recovery_context: RecoveryContext = Field(
        serialization_alias="recoveryContext",
        validation_alias=AliasChoices("recoveryContext", "recovery_context"),
    )


class RecordRecoveryMilestoneRequest(BaseModel):
    sequence_id: str = Field(
        min_length=1,
        max_length=64,
        serialization_alias="sequenceId",
        validation_alias=AliasChoices("sequenceId", "sequence_id"),
    )
    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        serialization_alias="idempotencyKey",
        validation_alias=AliasChoices("idempotencyKey", "idempotency_key"),
    )
    operation: RecoveryOperation
    action: RecoverySchedulerAction
    outcome: RecoverySchedulerActionOutcome
    completes_sequence: bool = Field(
        serialization_alias="completesSequence",
        validation_alias=AliasChoices("completesSequence", "completes_sequence"),
    )
    completed_at: datetime = Field(
        serialization_alias="completedAt",
        validation_alias=AliasChoices("completedAt", "completed_at"),
    )


class RecordRecoveryMilestoneResponse(BaseModel):
    duplicate: bool
    sequence_completed: bool = Field(
        serialization_alias="sequenceCompleted",
        validation_alias=AliasChoices("sequenceCompleted", "sequence_completed"),
    )
    end_to_end_duration_seconds: float | None = Field(
        default=None,
        ge=0,
        serialization_alias="endToEndDurationSeconds",
        validation_alias=AliasChoices("endToEndDurationSeconds", "end_to_end_duration_seconds"),
    )


class AdviceState(str, Enum):
    """
    Possible states for job advice from the NVIDIA Recovery system.

    Matches the Go client schema for NVIDIA Recovery Advice CRD.
    """

    RESTARTED = "Restarted"
    TERMINATED = "Terminated"
    RELEASED = "Released"


class AdviceReconciliationDisposition(str, Enum):
    """Lifecycle disposition reported by an Advice reconciler."""

    PENDING = "PENDING"
    CONVERGED = "CONVERGED"
    ABANDONED = "ABANDONED"


class AdviceReconciliationReason(str, Enum):
    """Bounded machine-readable reasons for Advice reconciliation status."""

    DESIRED_STATE_OBSERVED = "DESIRED_STATE_OBSERVED"
    ALREADY_SATISFIED = "ALREADY_SATISFIED"
    ADVICE_EXPIRED = "ADVICE_EXPIRED"
    STALE_ATTEMPT = "STALE_ATTEMPT"
    TARGET_STATE_FINAL = "TARGET_STATE_FINAL"
    SUCCESSOR_ALREADY_RUNNING = "SUCCESSOR_ALREADY_RUNNING"
    INVALID_RECOMMENDATION = "INVALID_RECOMMENDATION"
    UNSUPPORTED_RECOMMENDATION = "UNSUPPORTED_RECOMMENDATION"


class ResourceDetail(BaseModel):
    """
    Represents details about a specific resource involved in a remediation action.

    This model is compatible with the NVIDIA Recovery Advice CRD and matches the Go client schema.
    """

    resource_type: str = Field(
        serialization_alias="resourceType",
        validation_alias=AliasChoices("resourceType", "resource_type"),
    )
    resource_id: str = Field(
        serialization_alias="resourceId",
        validation_alias=AliasChoices("resourceId", "resource_id"),
    )
    details: str | None = None


class RemediationAction(BaseModel):
    """
    Describes a specific action to take to remediate an issue.

    This model is compatible with the NVIDIA Recovery Advice CRD and matches the Go client schema.
    """

    action_type: str = Field(
        serialization_alias="actionType",
        validation_alias=AliasChoices("actionType", "action_type"),
    )
    timestamp: datetime
    reason: str
    resource_details: list[ResourceDetail] | None = Field(
        default=None,
        serialization_alias="resourceDetails",
        validation_alias=AliasChoices("resourceDetails", "resource_details"),
    )


# TODO: Check about unifying this issue data with anomaly data.
class Issue(BaseModel):
    """
    Describes a detected issue for a job.

    This model is compatible with the NVIDIA Recovery Advice CRD and matches the Go client schema.
    Issue types include: CRASH, HANG, STRAGGLER, etc.
    """

    issue_type: str = Field(
        serialization_alias="issueType",
        validation_alias=AliasChoices("issueType", "issue_type"),
    )
    timestamp: datetime
    detected_by: str | None = Field(
        default=None,
        serialization_alias="detectedBy",
        validation_alias=AliasChoices("detectedBy", "detected_by"),
    )
    relevant_log_lines: list[str] | None = Field(
        default=None,
        serialization_alias="relevantLogLines",
        validation_alias=AliasChoices("relevantLogLines", "relevant_log_lines"),
    )
    remediation_actions: list[RemediationAction] | None = Field(
        default=None,
        serialization_alias="remediationActions",
        validation_alias=AliasChoices("remediationActions", "remediation_actions"),
    )


class NodeConstraints(BaseModel):
    """
    Contains scheduling constraints for a job.

    This model is compatible with the NVIDIA Recovery Advice CRD and matches the Go client schema.
    """

    exclusions: list[str] | None = None
    inclusions: list[str] | None = None


class AdviceRecommendation(BaseModel):
    """
    Represents the recommendation portion of advice.

    This model is compatible with the NVIDIA Recovery Advice CRD and matches the Go client schema.
    """

    state: AdviceState | None = None
    node_constraints: NodeConstraints | None = Field(
        default=None,
        serialization_alias="nodeConstraints",
        validation_alias=AliasChoices("nodeConstraints", "node_constraints"),
    )


class AdviceReconciliationStatus(BaseModel):
    """The reconciliation observation for an Advice generation."""

    observed_generation: int = Field(
        ge=1,
        serialization_alias="observedGeneration",
        validation_alias=AliasChoices("observedGeneration", "observed_generation"),
    )
    disposition: AdviceReconciliationDisposition
    reason: AdviceReconciliationReason
    observed_at: datetime = Field(
        serialization_alias="observedAt",
        validation_alias=AliasChoices("observedAt", "observed_at"),
    )
    transitioned_at: datetime = Field(
        serialization_alias="transitionedAt",
        validation_alias=AliasChoices("transitionedAt", "transitioned_at"),
    )
    diagnostic_message: str | None = Field(
        default=None,
        max_length=1024,
        serialization_alias="diagnosticMessage",
        validation_alias=AliasChoices("diagnosticMessage", "diagnostic_message"),
    )


class Advice(BaseModel):
    """
    Represents advice for a job from the NVIDIA Recovery system.

    This model matches the NVIDIA Recovery Go client schema (flattened structure) rather than
    the CRD structure (nested spec). The API returns a flattened version where jobId and
    restartCount are at the top level alongside the advice recommendation and issues.

    This is the public facing model returned from the job manager's REST API.
    """

    id: str
    visible: bool = Field(default=False)
    job_id: str = Field(
        serialization_alias="jobId",
        validation_alias=AliasChoices("jobId", "job_id"),
    )
    restart_count: int = Field(
        serialization_alias="restartCount",
        validation_alias=AliasChoices("restartCount", "restart_count"),
    )
    scheduler_metadata: dict = Field(
        default={},
        serialization_alias="schedulerMetadata",
        validation_alias=AliasChoices("schedulerMetadata", "scheduler_metadata"),
    )
    advice: AdviceRecommendation
    generation: int = Field(default=1, ge=1)
    desired_state_changed_at: datetime | None = Field(
        default=None,
        serialization_alias="desiredStateChangedAt",
        validation_alias=AliasChoices("desiredStateChangedAt", "desired_state_changed_at"),
    )
    reconciliation_status: AdviceReconciliationStatus | None = Field(
        default=None,
        serialization_alias="reconciliationStatus",
        validation_alias=AliasChoices("reconciliationStatus", "reconciliation_status"),
    )
    issues: list[Issue] | None = None
    fact_attribution: dict | None = Field(
        default=None,
        serialization_alias="factAttribution",
        validation_alias=AliasChoices("factAttribution", "fact_attribution"),
    )
    recovery_context: RecoveryContext | None = Field(
        default=None,
        serialization_alias="recoveryContext",
        validation_alias=AliasChoices("recoveryContext", "recovery_context"),
    )
    created_at: datetime | None = Field(
        default=None,
        serialization_alias="createdAt",
        validation_alias=AliasChoices("createdAt", "created_at"),
    )
    updated_at: datetime | None = Field(
        default=None,
        serialization_alias="updatedAt",
        validation_alias=AliasChoices("updatedAt", "updated_at"),
    )


class ListAdviceResponse(BaseModel):
    """
    Response model for listing advice records.
    """

    item_count: int
    advice: list[Advice]
    next_page_token: str | None = Field(
        default=None,
        serialization_alias="nextPageToken",
        validation_alias=AliasChoices("nextPageToken", "next_page_token"),
    )


class NodeAnalysisResult(BaseModel):
    """
    Result of node set analysis containing the computed node set.
    """

    nodes: list[str] = Field(description="List of node names in the result set")
    count: int = Field(description="Total number of nodes in the result set")


class NodeAnalysisResponse(BaseModel):
    """
    Response model for node set analysis endpoint.

    This response contains the result of evaluating a set algebra expression
    over job node lists, optionally grouped by rack.
    """

    expression: str = Field(description="The original expression that was evaluated")
    result: NodeAnalysisResult = Field(description="The computed node set result")
    grouped_by_rack: dict[str, list[str]] | None = Field(
        default=None,
        serialization_alias="groupedByRack",
        description="Nodes grouped by rack identifier (only present when group_by=rack)",
    )
    jobs_queried: list[str] = Field(
        serialization_alias="jobsQueried",
        description="List of job IDs (format: job_id/attempt_index) that were queried",
    )


class DagDependencyActionStatusEnum(str, Enum):
    """
    The action status of a DAG dependency record.

    NOT_ELIGIBLE: The dependency trigger condition has not been met yet.
    IN_PROGRESS: The action has been sent to WFC and is awaiting confirmation.
    SUCCESSFUL: The action was executed successfully.
    FAILED: The action failed after exhausting all retries.
    ABORTED: A parent reached terminal Advice, so the dependent action must not run.
    """

    NOT_ELIGIBLE = "not_eligible"
    IN_PROGRESS = "in_progress"
    SUCCESSFUL = "successful"
    FAILED = "failed"
    ABORTED = "aborted"


class DagDependencyReasonCodeEnum(str, Enum):
    """Machine-readable explanation for the latest DAG dependency evaluation."""

    WAITING_FOR_PARENT_REGISTRATION = "WAITING_FOR_PARENT_REGISTRATION"
    WAITING_FOR_PARENT_COMPLETION = "WAITING_FOR_PARENT_COMPLETION"
    DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS = "DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS"
    PARENT_NAME_AMBIGUOUS = "PARENT_NAME_AMBIGUOUS"
    PARENT_TERMINATED = "PARENT_TERMINATED"


class DagDependencyResponse(BaseModel):
    """
    Public-facing data model for a DAG dependency record.

    Each record represents a dependency relationship between a dependent (child) job and a dependency (parent) job.
    When the parent job satisfies the trigger condition, the dependent job becomes eligible for the specified action.
    """

    dependent_job_id: str
    dependent_job_action: str
    dependency_job_identifier: str
    dependency_job_identifier_type: str
    dependency_trigger_condition: str
    action_status: str = Field(
        description=(
            "Dependency action status: not_eligible, in_progress, successful, failed, or aborted. "
            "Aborted is absorbing and means a parent reached terminal Advice, so this gate cannot release."
        )
    )
    action_time: datetime | None = None
    reason_code: DagDependencyReasonCodeEnum | None = Field(
        default=None,
        description="Stable machine-readable explanation for the latest dependency evaluation.",
    )
    reason_message: str | None = Field(
        default=None,
        description="Human-readable explanation for the latest dependency evaluation; clients must not parse it.",
    )
    reason_details: dict[str, Any] | None = Field(
        default=None,
        description="Structured context for the latest dependency evaluation.",
    )
    last_evaluated_at: datetime | None = Field(
        default=None,
        description="UTC timestamp of the latest dependency evaluation; null before the first evaluation.",
    )


class ListDagDependenciesResponse(BaseModel):
    """
    Response model for listing DAG dependency records. The response is paginated —
    item_count reflects the total number of matching records, not the page size.
    """

    item_count: int
    dag_dependencies: list[DagDependencyResponse]
