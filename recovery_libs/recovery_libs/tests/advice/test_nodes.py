# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

from recovery_libs.advice.nodes import compute_exclusion_nodes, compute_sticky_exclusion_nodes
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult, JobExecutionError, NodeEvent
from recovery_libs.schema.aps_spec.spec import AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.node_events import ErrorAttributionType, ExclusionType
from recovery_libs.tests.advice.helpers import create_anomaly_attribution, random_job_execution_status


def test_compute_exclusion_nodes_uses_attribution_nodes_only_when_stickiness_disabled():
    status = random_job_execution_status(job_id="12345", job_attempt_index=0)
    attribution = create_anomaly_attribution(job_id="12345", node_event_count=2)

    result = compute_exclusion_nodes(
        status,
        attribution,
        exclusion_stickiness_enabled=False,
    )

    assert result == {"node_0", "node_1"}


def test_compute_sticky_exclusion_nodes_singleton_uses_current_attempt_list():
    status = random_job_execution_status(
        job_id="12345",
        job_attempt_index=1,
        sticky_exclusion_node_list=["user-node-1", "user-node-2"],
        auto_resume_mode=AutoResumeModeEnum.SINGLETON_DEPENDENCY,
    )

    result = compute_sticky_exclusion_nodes(status)

    assert result == ["user-node-1", "user-node-2"]


def test_compute_exclusion_nodes_inherits_sticky_nodes_from_previous_attempt():
    status = random_job_execution_status(job_id="12345", job_attempt_index=1)
    attribution = create_anomaly_attribution(job_id="12345", node_event_count=1)
    previous = random_job_execution_status(
        job_id="12345",
        job_attempt_index=0,
        sticky_exclusion_node_list=["sticky_node_1", "sticky_node_2"],
    )

    result = compute_exclusion_nodes(
        status,
        attribution,
        exclusion_stickiness_enabled=True,
        get_previous_attempt=lambda job_id, attempt_index: previous if attempt_index == 0 else None,
    )

    assert result == {"sticky_node_1", "sticky_node_2", "node_0"}


def test_compute_exclusion_nodes_filters_non_exclusion_types():
    status = random_job_execution_status(job_id="12345", job_attempt_index=0)
    attribution = AnomalyAttributionResult(
        job_id="12345",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        anomaly_uuid="test-uuid",
        attribution_type=ErrorAttributionType.FAST,
        node_attributions={
            JobExecutionError.create(signature="sig1"): [
                NodeEvent.create(
                    node="node-temp",
                    timestamp=datetime.now(UTC),
                    job_id="12345",
                    job_attempt_index=0,
                    exclusion_type=ExclusionType.TEMPORARY,
                ),
                NodeEvent.create(
                    node="node-none",
                    timestamp=datetime.now(UTC),
                    job_id="12345",
                    job_attempt_index=0,
                    exclusion_type=ExclusionType.NONE,
                ),
            ]
        },
        global_attributions={},
    )

    result = compute_exclusion_nodes(
        status,
        attribution,
        exclusion_stickiness_enabled=False,
    )

    assert result == {"node-temp"}
