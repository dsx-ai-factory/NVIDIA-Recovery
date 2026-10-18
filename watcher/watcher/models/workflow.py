# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from recovery_libs.schema.watcher.model import Attributes


class LogRecord(BaseModel):
    """Backend-neutral log record consumed by Watcher workflows."""

    body: str | None = None
    timestamp: str | None = None
    attributes: Attributes | None = None


class WorkflowItem(BaseModel):
    record: LogRecord
    routing_suffix: str | None = None
    step_name: str | None = None
    step_value: Any | None = None


class Checkpoint(BaseModel):
    key: str | None = None
    value: Any | None = None


class WorkflowStepType(str, Enum):
    compare_last_datapoint = "compare_last_datapoint"
    count_over_time = "count_over_time"
    emit_metric = "emit_metric"
    log_query = "log_query"
    loki_query = "loki_query"
    moving_avg = "moving_avg"
    nooutput = "nooutput"
    regex_parser = "regex_parser"
    store_datapoint = "store_datapoint"
    simple_action = "simple_action"


class WorkflowStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: WorkflowStepType
    attributes: dict[str, Any] = Field(default_factory=dict)
    bucket_by: list[str] | None = None
    children: list[dict[str, Any]] | None = None


class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    SUMMARY = "summary"


## Nooutput Workflow Step
##
class WorkflowStepNooutputEventType(Enum):
    OBSERVED = 1
    QUERY_FAILURE = 2


class WorkflowStepNooutputJobPhase(str, Enum):
    def __str__(self):
        return str(self.value)

    PRE_TRAINING = "PRE_TRAINING"
    TRAINING_LOOP = "TRAINING_LOOP"


class WorkflowStepNooutputJobAttributes(BaseModel):
    job_id: str
    job_phase: WorkflowStepNooutputJobPhase
    detection_time: str


class WorkflowStepNooutputConfig(WorkflowStep):
    """
    Configuration for a Nooutput Detection Workflow Step.
    """

    attributes: WorkflowStepNooutputConfigAttributes  # type: ignore[assignment]


class WorkflowStepNooutputConfigAttributes(BaseModel):
    """
    Attribute definition for Nooutput Detection Workflow Step.

    Detects when a job produces no output within configured time intervals per job phase.
    """

    model_config = ConfigDict(extra="forbid")

    cache_key: str
    query: dict[Literal["regex"], list] = Field(default_factory=dict)
    interval: dict[str, int]
    last_iteration_enabled: bool = False
    last_iteration_regex: str | None = None
    last_iteration_operation_schema: str | None = None
    false_positive_scan_cache_key: str | None = None
    false_positive_scan_timer: int | None = None
    action: WorkflowStepConfigAction


## Regex Workflow Step
##
class WorkflowStepRegexConfig(WorkflowStep):
    """
    Configuration for a Regex Parser Workflow Step.

    This workflow step supports two modes of operation:
    - Standard mode: Uses the simple regex defined in the 'regex' attribute.
    - Advanced parser mode: Uses the advanced regex parser defined in the 'advanced_regex' attribute.

    Note: Only one parser type (standard or advanced_parser) can be set in a workflow step.
    """

    attributes: WorkflowStepRegexConfigAttributes  # type: ignore[assignment]


class WorkflowStepRegexConfigAttributes(BaseModel):
    """
    Attribute definition for Regex Parser Workflow Step.

    Notes:
    1- Standard mode: Provide a single regex pattern via the 'regex' attribute.
    2- Advanced parser mode: Provide a list of advanced regex configurations via the 'advanced_regex'
       attribute along with additional event-related configuration through field 'events'.
    3- the attribute 'publish_events' is only supported in Advanced parser mode.
    """

    model_config = ConfigDict(extra="forbid")

    advanced_regex: list[WorkflowStepRegexConfigAttributesAdvancedRegex] | None = None
    dedup: bool = False
    dedup_signature_pattern: str | None = None
    dedup_strip_patterns: list[str] | None = None
    event_type: str | None = None
    publish_events: bool | None = False
    regex: str | None = None
    stop_after_first_match: bool | None = True

    @model_validator(mode="before")
    @classmethod
    def validate_advanced_parser_requirements(cls, values):
        """
        Validate advanced parser requirements and dedup regex patterns.

        If the 'advanced_regex_parser' flag is set to True, then 'events' must be provided and each event
        must include non-empty 'regexes'.
        """
        advanced_parser = values.get("advanced_regex_parser")
        events = values.get("events")

        if advanced_parser is True:
            if events is None or len(events) == 0:
                raise ValueError("When advanced_regex_parser is True, events must be provided")

            for i, event in enumerate(events):
                if event["regexes"] is None or len(event["regexes"]) == 0:
                    raise ValueError(
                        f"When advanced_regex_parser is True, events:regexes must be provided for each event type. Missing for event type {i}"
                    )

                for regex_pattern in event["regexes"]:
                    try:
                        re.compile(regex_pattern)
                    except re.error:
                        raise ValueError(f"Invalid regex pattern: {regex_pattern}")

        # Validate dedup_signature_pattern
        if sig_pattern := values.get("dedup_signature_pattern"):
            try:
                re.compile(sig_pattern)
            except re.error:
                raise ValueError(f"Invalid dedup_signature_pattern: {sig_pattern}")

        # Validate dedup_strip_patterns
        if strip_patterns := values.get("dedup_strip_patterns"):
            for pattern in strip_patterns:
                try:
                    re.compile(pattern)
                except re.error:
                    raise ValueError(f"Invalid dedup_strip_pattern: {pattern}")

        return values


class WorkflowStepRegexConfigAttributesAdvancedRegex(BaseModel):
    """
    Represents an advanced regex configuration for a specific event type.

    This model is used when WorkflowStepRegexConfig is operating in advanced parser mode (advanced_regex).
    """

    model_config = ConfigDict(extra="forbid")

    event_type: str | None = None
    regexes: list[str] | None = None


## Straggler Detection Step
##
class WorkflowStepStragglerConfig(WorkflowStep):
    """
    Configuration for a Straggler Detection Workflow Step using moving average analysis.
    """

    attributes: WorkflowStepStragglerConfigAttributes  # type: ignore[assignment]


class WorkflowStepStragglerConfigAttributes(BaseModel):
    """
    Attribute definition for Straggler Detection Workflow Step.

    This step uses moving average analysis to detect stragglers based on performance metrics.
    """

    model_config = ConfigDict(extra="forbid")

    cache_key: str
    window_size: int
    base_percentile_threshold: float
    anomaly_percent_threshold: float
    alert_smoothing_count: int
    warmup_grace_period_secs: int
    min_window_size_for_detection: int
    min_interval_between_datapoints_secs: int
    skip_too_close_datapoints: bool
    reset_after_good_values: int
    notify_once: bool = False
    action: WorkflowStepConfigAction


class WorkflowStepConfigAction(BaseModel):
    """
    Generic action configuration for workflow steps.
    """

    model_config = ConfigDict(extra="forbid")

    send_message_eventbus: str
    unique: bool | None = None
