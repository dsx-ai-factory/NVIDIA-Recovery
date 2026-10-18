# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PositiveInt, model_validator

DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_PRETRAINING_SECS = 960
DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_TRAININGLOOP_SECS = 840
StrictPositiveInt = Annotated[int, Field(gt=0, strict=True)]


class AutoResumeModeEnum(str, Enum):
    """
    This enum class defines the auto resume mode for the job.

    SINGLETON_DEPENDENCY: NVIDIA Recovery will use the existing singleton dependency jobs to resume the progress.
    REQUEUE: NVIDIA Recovery will requeue the same job to resume the progress.
    """

    SINGLETON_DEPENDENCY = "singleton_dependency"
    REQUEUE = "requeue"


class AnomalyDetectionConfig(BaseModel):
    """
    Anomaly detection configuration that the user can control per job.

    Attributes:
        log_based_crash_enabled (bool): Whether to enable log-based crash detection for the job.
            If set to False, a CRASH anomaly will be generated immediately when the job enters FAILED
            state, without waiting for log analysis. Default is True (log-based detection enabled).
        log_based_timeout_enabled (bool): Whether to enable log-based no-output timeout detection
            (HANGNOOUTPUT) for the job. When set to False, the watcher service will suppress
            HANGNOOUTPUT anomaly publication for the job. Default is True (detection enabled).
        log_based_timeout_threshold_pretraining (int): Per-job no-output timeout threshold (in
            seconds) applied during the pre-training phase. The watcher uses this value as the
            silence window after which a HANGNOOUTPUT anomaly may be raised. Must be a positive
            integer. Default is 960 seconds (16 minutes).
        log_based_timeout_threshold_trainingloop (int): Per-job no-output timeout threshold (in
            seconds) applied during the training loop phase. The watcher uses this value as the
            silence window after which a HANGNOOUTPUT anomaly may be raised. Must be a positive
            integer. Default is 840 seconds (14 minutes).
    """

    log_based_crash_enabled: bool = True
    log_based_timeout_enabled: bool = True
    log_based_timeout_threshold_pretraining: StrictPositiveInt = DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_PRETRAINING_SECS
    log_based_timeout_threshold_trainingloop: StrictPositiveInt = DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_TRAININGLOOP_SECS


class DagActionEnum(str, Enum):
    """
    The action to take on the target job in a DAG step.

    RELEASE: Release the target job so it becomes eligible for scheduling.
    """

    RELEASE = "release"


class JobReferenceByName(BaseModel):
    """Identify a target job by its name."""

    type: Literal["name"] = "name"
    name: str


class JobReferenceById(BaseModel):
    """Identify a target job by its Slurm job ID."""

    type: Literal["id"] = "id"
    id: str


JobReference = Annotated[
    JobReferenceByName | JobReferenceById,
    Field(discriminator="type"),
]


class DagTriggerConditionEnum(str, Enum):
    """
    The condition under which a DAG step should be triggered.

    COMPLETED: Trigger when the current job finishes successfully (Slurm COMPLETED state).
    """

    COMPLETED = "completed"


class DagDependencyStep(BaseModel):
    """
    A single DAG dependency step that describes a dependency that must meet a trigger condition
    before the current job can run.

    Attributes:
        target (JobReference): The dependency job, identified by name or id.
        trigger (DagTriggerConditionEnum): The condition that triggers this step. Defaults to COMPLETED.
    """

    target: JobReference
    trigger: DagTriggerConditionEnum = DagTriggerConditionEnum.COMPLETED


class ApsSlurmJobSpec(BaseModel):
    """
    APS Slurm Job Spec, today this spec should be passed to APS as a JSON string included in the "comment" of a Slurm
    job.

    Attributes:
        workflow_id (Optional[str]): The workflow this Slurm job belongs to.
        workflow_name (Optional[str]): Deprecated alias for workflow_id.
        environment (Optional[str]): The APS/NVIDIA Recovery environment that the job should be managed by, if not specified,
            the job will be managed by the "prod" environment.
        auto_resume_mode (Optional[AutoResumeModeEnum]): The auto resume mode for the job.
        max_requeue_times (Optional[PositiveInt]): The max number of requeue that will happen to the job. Only
            applicable when auto_resume_mode is set to REQUEUE. If not specified, the system wide max job attempt limit
            will be used. Note that the max requeue times will be capped at the system wide limit too.
        requeue_on_completion (Optional[bool]): Whether to requeue the job on normal completion. Default is None which
            means it follows the system wide default. Setting it to either True or False will override the system wide
            default. Only meaningful when auto_resume_mode is set to REQUEUE.
        log_file_paths_shared_across_attempts (bool): Whether requeued attempts intentionally write to the same log
            files. Defaults to False, which requires unique per-attempt paths and protects event correlation from
            stale paths returned by Slurm during a requeue race.
        nvrx (Optional[bool]): Whether the job is enabled for NVRX. Default is "disabled".
        anomaly_detection_config (AnomalyDetectionConfig): Anomaly detection configuration. Defaults to
            AnomalyDetectionConfig() (all log-based detections enabled with default thresholds). Use
            AnomalyDetectionConfig(log_based_crash_enabled=False) to generate a CRASH anomaly immediately when the
            job enters FAILED state. Use AnomalyDetectionConfig(log_based_timeout_enabled=False) to suppress
            log-based no-output (HANGNOOUTPUT) anomalies, or override
            log_based_timeout_threshold_pretraining / log_based_timeout_threshold_trainingloop to tune the
            per-phase silence window before a HANGNOOUTPUT anomaly may fire.
        dag_dependencies (Optional[list[DagDependencyStep]]): DAG dependencies that must be satisfied before this
            job can run. Each step specifies a dependency job and a trigger condition. The current job will only be
            eligible for execution when all dependencies are met.
        dag_action (Optional[DagActionEnum]): The action to take on this job after all its dag_dependencies are
            satisfied. For example, RELEASE will release the job so it becomes eligible for scheduling.
    """

    model_config = {"extra": "ignore"}

    workflow_id: str | None = None
    workflow_name: str | None = Field(default=None, json_schema_extra={"deprecated": True})
    environment: str | None = None
    auto_resume_mode: AutoResumeModeEnum | None = None
    max_requeue_times: PositiveInt | None = None
    requeue_on_completion: bool | None = None
    log_file_paths_shared_across_attempts: bool = False
    nvrx: str | None = "disabled"  # Supported values: "enabled", "disabled", "true", "false"
    anomaly_detection_config: AnomalyDetectionConfig = Field(default_factory=AnomalyDetectionConfig)
    dag_dependencies: list[DagDependencyStep] | None = None
    dag_action: DagActionEnum | None = None

    @model_validator(mode="after")
    def validate_nvrx(self):
        allowed_values = {"enabled", "disabled", "true", "false", None}
        if self.nvrx not in allowed_values:
            raise ValueError(f"nvrx must be one of {allowed_values - {None}} or None, got '{self.nvrx}'")
        return self
