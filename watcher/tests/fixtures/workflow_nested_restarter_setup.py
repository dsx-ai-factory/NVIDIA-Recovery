# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest_asyncio
from recovery_libs.schema.watcher.model import Attributes
from watcher.models.workflow import Checkpoint, LogRecord, WorkflowItem


@pytest_asyncio.fixture
def loki_log_nested_restarter_init():
    return LogRecord(
        body="2614: [cls-h100-004-001-027:3459553] [NestedRestarter] name=[InProcess|InJob] state=initialize",
        timestamp="1730329215704736095",
        attributes=Attributes(
            job_id="804973",
            detection_time=None,
            log_file_name="gpt_15b_804973_date_24-10-30_time_14-29-33.log",
            log_file_path="/home/test-service-user/otelfilelog_paths/prod/megatron-gpt3-15b-8t-bs-1152/gpt_15b_804973_date_24-10-30_time_14-29-33.log",
            uuid="0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee",
        ),
    )


# WorkflowStepType.log_query (workflow.py) returns: List[WorkflowItem]
@pytest_asyncio.fixture
def workflow_item_nested_restarter_init(loki_log_nested_restarter_init):
    return WorkflowItem(
        record=loki_log_nested_restarter_init,
        step_name=None,
        step_value=None,
    )


@pytest_asyncio.fixture
def checkpoint_nested_restarter_init(workflow_item_nested_restarter_init):
    return Checkpoint(
        key="nested_restart_init_action",
        value=workflow_item_nested_restarter_init,
    )


@pytest_asyncio.fixture
def nested_restarter_init_events_workflow_items_format():
    return [
        WorkflowItem(
            record=LogRecord(
                body="2614: [cls-h100-004-001-027:3459553] [NestedRestarter] name=[InProcess|InJob] state=initialize",
                timestamp="1736367350109318640",
                attributes={
                    "job_id": "1366321",
                    "log.file.name": "56B_hybrid_20T_phase1_1366321_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366321_date_25-01-08_time_05-58-28.log",
                    "uuid": "0194526d-14df-73ca-8d68-2581f7170cd6",
                },
            ),
            step_name=None,
            step_value=None,
        ),
    ]


@pytest_asyncio.fixture
def workflow_step_nested_restarter_config_dict(rules_engine):
    workflow_config_rule = None

    for rule in rules_engine.config.rules:
        if rule.name == "local":
            workflow_config_rule = rule

    for step in workflow_config_rule.workflow:
        if step.name == "nested_restarter":
            for child in step.children:
                if child.name == "nested_restarter_regex_parse":
                    return child.to_json()

    return None
