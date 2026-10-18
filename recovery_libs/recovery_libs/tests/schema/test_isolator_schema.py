# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import json
from datetime import datetime

from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)


def test_attribution_result_serialization_non_empty_events():
    now = datetime.now()
    attribution_result = AnomalyAttributionResult(
        job_id="12345",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        anomaly_uuid="test-anomaly-uuid",
        attribution_type=ErrorAttributionType.FAST,
        node_attributions={
            JobExecutionError.create(
                code=123,
                message="Test error message",
                description="Test description",
                signature="test_signature",
            ): [
                NodeEvent.create(
                    node=f"test_node_{i}",
                    job_id="123456",
                    job_attempt_index=0,
                    timestamp=now,
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.FACT,
                    error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
                    exclusion_type=ExclusionType.TEMPORARY,
                    fact_status=FactStatus.OK,
                )
                for i in range(3)
            ]
        },
        global_attributions={},
    )

    json_str = attribution_result.to_json()
    json_dict = json.loads(json_str)

    node_events_json = next(iter(json_dict["node_attributions"].values()))

    for i in range(3):
        assert node_events_json[i]["node"] == f"test_node_{i}"
        assert node_events_json[i]["job_id"] == "123456"
        assert node_events_json[i]["job_attempt_index"] == 0
        assert node_events_json[i]["timestamp"] == now.isoformat()
        assert node_events_json[i]["error_attribution"] == ErrorAttribution.UNDEFINED
        assert node_events_json[i]["error_attribution_type"] == ErrorAttributionType.FAST
        assert node_events_json[i]["error_attribution_origin"] == ErrorAttributionOrigin.FACT
        assert node_events_json[i]["error_attribution_sources"] == [
            ErrorAttributionSource.APPLICATION_LOGS,
            ErrorAttributionSource.SYSLOGS,
        ]
        assert node_events_json[i]["exclusion_type"] == ExclusionType.TEMPORARY
        assert node_events_json[i]["fact_status"] == FactStatus.OK

    job_execution_error_str = next(iter(json_dict["node_attributions"].keys()))
    job_execution_error_json_dict = json.loads(job_execution_error_str)

    assert job_execution_error_json_dict["code"] == 123
    assert job_execution_error_json_dict["message"] == "Test error message"
    assert job_execution_error_json_dict["description"] == "Test description"
    assert job_execution_error_json_dict["signature"] == "test_signature"

    assert json_dict["job_id"] == "12345"
    assert json_dict["job_attempt_index"] == 0
    assert json_dict["anomaly_type"] == EventType.CRASH.value
    assert json_dict["anomaly_uuid"] == "test-anomaly-uuid"
    assert json_dict["attribution_type"] == ErrorAttributionType.FAST.value

    reconstructed_attribution_result = AnomalyAttributionResult.from_json(json.loads(json_str))

    node_events_reconstructed = next(iter(reconstructed_attribution_result.node_attributions.values()))
    for i in range(3):
        assert node_events_reconstructed[i].node == f"test_node_{i}"
        assert node_events_reconstructed[i].job_id == "123456"
        assert node_events_reconstructed[i].job_attempt_index == 0
        assert node_events_reconstructed[i].timestamp == now
        assert node_events_reconstructed[i].error_attribution == ErrorAttribution.UNDEFINED
        assert node_events_reconstructed[i].error_attribution_type == ErrorAttributionType.FAST
        assert node_events_reconstructed[i].error_attribution_origin == ErrorAttributionOrigin.FACT
        assert node_events_reconstructed[i].error_attribution_sources == [
            ErrorAttributionSource.APPLICATION_LOGS,
            ErrorAttributionSource.SYSLOGS,
        ]
        assert node_events_reconstructed[i].exclusion_type == ExclusionType.TEMPORARY
        assert node_events_reconstructed[i].fact_status == FactStatus.OK

    job_execution_error_reconstructed = next(iter(reconstructed_attribution_result.node_attributions.keys()))
    assert job_execution_error_reconstructed.code == 123
    assert job_execution_error_reconstructed.message == "Test error message"
    assert job_execution_error_reconstructed.description == "Test description"
    assert job_execution_error_reconstructed.signature == "test_signature"
    assert reconstructed_attribution_result.attribution_type == ErrorAttributionType.FAST


def test_attribution_result_serialization_empty_events():
    attribution_result = AnomalyAttributionResult(
        job_id="12345",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        attribution_type=ErrorAttributionType.FAST,
        anomaly_uuid="test-anomaly-uuid",
        node_attributions={
            JobExecutionError.create(
                code=123,
                message="Test error message",
                description="Test description",
                signature="test_signature",
            ): []
        },
        global_attributions={},
    )

    json_str = attribution_result.to_json()
    reconstructed_attribution_result = AnomalyAttributionResult.from_json(json.loads(json_str))

    assert reconstructed_attribution_result.job_id == "12345"
    assert reconstructed_attribution_result.job_attempt_index == 0
    assert reconstructed_attribution_result.anomaly_type == EventType.CRASH
    assert reconstructed_attribution_result.anomaly_uuid == "test-anomaly-uuid"
    assert reconstructed_attribution_result.attribution_type == ErrorAttributionType.FAST
    assert len(reconstructed_attribution_result.node_attributions) == 1
    assert next(iter(reconstructed_attribution_result.node_attributions.values())) == []
