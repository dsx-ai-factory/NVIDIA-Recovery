# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Request models for the workload attribution HTTP interface (design §6).

Everything crossing the HTTP boundary speaks "workload" vocabulary, not "job"
(design §D6). Heimdall-internal code keeps job_id / SlurmJobExecutionStatus.
"""

from datetime import datetime

from pydantic import BaseModel, Field

from recovery_libs.schema.attribution_interface.enums import AttributionPhase
from recovery_libs.schema.workflow_identity import CanonicalWorkflowID


class WorkloadScope(BaseModel):
    """The nodes / ranks / time window an attribution request applies to (design §6).

    start_time and end_time are Optional because SlurmJobExecutionStatus.start_time /
    end_time may be None (correction C12): callers that know the time pass it; callers
    working from an in-progress job may not.
    """

    nodes: list[str]
    ranks: list[int] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None


class FailureContext(BaseModel):
    """When/how the failure was detected (design §6). detection_signal drives STRAGGLER handling on NV side."""

    detected_at: datetime
    detection_signal: str


class WorkloadContext(BaseModel):
    """Typed context FACT's AttributorInfo needs (design §6).

    job_end_time is DISTINCT from scope.end_time (which may be anomaly_time); it is the
    observed_at no-source fallback on the NV side (design §7.2).
    """

    workflow_id: CanonicalWorkflowID
    attempt_index: int
    command: str = ""
    cluster: str = ""
    workload_name: str = ""
    state: str = ""
    node_count: int | None = None
    total_gpu_count: int | None = None
    job_end_time: datetime | None = None
    anomaly_uuid: str = ""


class WorkloadAttributionRequest(BaseModel):
    """A single FAST or SLOW attribution request from the Policy Engine to attribution-adapter (design §6)."""

    schema_version: str
    workload_id: str
    user_id: str = ""
    scope: WorkloadScope
    failure_context: FailureContext
    timeout_ms: int
    workload_context: WorkloadContext
    continue_session: str | None = None
    phase: AttributionPhase
