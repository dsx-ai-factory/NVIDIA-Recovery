# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest_asyncio
from recovery_libs.schema.watcher.model import Attributes
from watcher.models.workflow import Checkpoint, LogRecord, WorkflowItem, WorkflowStep, WorkflowStepType

# CRASH setup
crash_events = [
    "121: [rank121]: RuntimeError: PytorchStreamReader failed reading zip archive: failed finding central directory",
]


@pytest_asyncio.fixture
def workflow_step_crash():
    return WorkflowStep(
        name="crash_action",
        type=WorkflowStepType.simple_action,
        attributes={"action": {"send_message_eventbus": "CRASH", "unique": True}},
        bucket_by=None,
        children=None,
    )


@pytest_asyncio.fixture
def loki_log_crash():
    return LogRecord(
        body="2618: [cls-h100-004-001-026:1129553] Signal: Aborted (6)",
        timestamp="1730329215704736095",
        attributes=Attributes(
            job_id="804949",
            detection_time=None,
            log_file_name="gpt_15b_804949_date_24-10-30_time_14-29-33.log",
            log_file_path="/home/test-service-user/otelfilelog_paths/prod/megatron-gpt3-15b-8t-bs-1152/gpt_15b_804949_date_24-10-30_time_14-29-33.log",
            uuid="0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee",
        ),
    )


# WorkflowStepType.log_query (workflow.py) returns: List[WorkflowItem]
@pytest_asyncio.fixture
def workflow_item_crash(loki_log_crash):
    return WorkflowItem(
        record=loki_log_crash,
        step_name=None,
        step_value=None,
    )


@pytest_asyncio.fixture
def checkpoint_crash(workflow_item_crash):
    return Checkpoint(
        key="crash_action",
        value=workflow_item_crash,
    )


@pytest_asyncio.fixture
def crash_events_workflow_items_format():
    return [
        WorkflowItem(
            record=LogRecord(
                body="1952: [rank1952]: RuntimeError: Rank 1952, node cls1-h100-001-293-026, device 0: Unexpected result nan (message='found NaN in local forward loss calculation')",
                timestamp="1736367350109318640",
                attributes={
                    "job_id": "1366324",
                    "log.file.name": "56B_hybrid_20T_phase1_1366324_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366324_date_25-01-08_time_05-58-28.log",
                    "uuid": "0194526d-14df-73ca-8d68-2581f7170cd6",
                },
            ),
            step_name=None,
            step_value=None,
        ),
        WorkflowItem(
            record=LogRecord(
                body="3745: [rank3745]: RuntimeError: Rank 3745, node cls1-h100-002-251-026, device 1: Unexpected result nan (message='found NaN in local forward loss calculation')",
                timestamp="1736367350109349962",
                attributes={
                    "job_id": "1366325",
                    "log.file.name": "56B_hybrid_20T_phase1_1366325_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366325_date_25-01-08_time_05-58-28.log",
                    "uuid": "0194526d-14e0-7659-b47f-e3607d4efde5",
                },
            ),
            step_name=None,
            step_value=None,
        ),
        WorkflowItem(
            record=LogRecord(
                body="1959: [rank1959]: RuntimeError: Rank 1959, node cls1-h100-001-293-026, device 7: Unexpected result nan (message='found NaN in local forward loss calculation')",
                timestamp="1736367350114104679",
                attributes={
                    "job_id": "1366324",
                    "log.file.name": "56B_hybrid_20T_phase1_1366324_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366324_date_25-01-08_time_05-58-28.log",
                    "uuid": "0194526d-14e1-776f-8dc3-23982b6786a8",
                },
            ),
            step_name=None,
            step_value=None,
        ),
        WorkflowItem(
            record=LogRecord(
                body="1959: [rank1983]: RuntimeError: Rank 1959, node cls1-h100-001-014-027, device 9: Unexpected result nan (message='found NaN in local forward loss calculation')",
                timestamp="1736919646628518975",
                attributes={
                    "job_id": "1366324",
                    "log.file.name": "56B_hybrid_20T_phase1_1366324_2_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366324_2_date_25-01-08_time_05-58-28.log",
                    "uuid": "0194687b-39ee-7257-b211-41996a0f40ca",
                },
            ),
            step_name=None,
            step_value=None,
        ),
    ]
