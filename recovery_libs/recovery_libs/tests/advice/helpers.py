# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import UTC, datetime

from recovery_libs.schema.anomaly_isolator.attribution import (
    AnomalyAttributionResult,
    JobExecutionError,
    NodeEvent,
)
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.storage.node_events import ErrorAttribution, ErrorAttributionType, ExclusionType


def random_job_execution_status(
    job_id: str = "12345",
    state: str = "RUNNING",
    job_attempt_index: int = 0,
    sticky_exclusion_node_list: list[str] | None = None,
    auto_resume_mode: AutoResumeModeEnum = AutoResumeModeEnum.REQUEUE,
) -> SlurmJobExecutionStatus:
    return SlurmJobExecutionStatus(
        job_id=job_id,
        job_attempt_index=job_attempt_index,
        workflow_id="workflow-test",
        state=state,
        job_name="job-name",
        submit_time="2024-01-02 00:00:00",  # type: ignore[arg-type]
        allocated_node_list=["rank0_hostname"],
        sticky_exclusion_node_list=sticky_exclusion_node_list,
        user="test_user",
        aps_spec=ApsSlurmJobSpec(auto_resume_mode=auto_resume_mode),
    )


def create_anomaly_attribution(
    job_id: str = "123456",
    job_attempt_index: int = 0,
    node_event_count: int = 2,
) -> AnomalyAttributionResult:
    return AnomalyAttributionResult(
        job_id=job_id,
        job_attempt_index=job_attempt_index,
        anomaly_type=EventType.CRASH,
        anomaly_uuid=str(uuid.uuid4()),
        attribution_type=ErrorAttributionType.FAST,
        node_attributions={
            JobExecutionError.create(
                code=31,
                message="test error message",
                signature="GPU fell off the bus",
            ): [
                NodeEvent.create(
                    node=f"node_{i}",
                    timestamp=datetime.now(UTC),
                    job_id=job_id,
                    job_attempt_index=job_attempt_index,
                    error_attribution=ErrorAttribution.HARDWARE,
                    exclusion_type=ExclusionType.TEMPORARY,
                    error_attribution_type=ErrorAttributionType.FAST,
                )
                for i in range(node_event_count)
            ]
        },
        global_attributions={},
    )
