# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import AsyncGenerator

import pytest_asyncio
from recovery_libs.schema.watcher.model import Attributes
from watcher.models.config import WorkflowNativeQueries
from watcher.models.workflow import (
    Checkpoint,
    LogRecord,
    WorkflowItem,
    WorkflowStep,
    WorkflowStepType,
)

# NOOUTPUT setup
pre_training_events = [
    "0: GlobalConfig.configure_distributed_args() using world size: 32 and model-parallel size: 8",
]

training_loop_events = [
    "2047:  [2024-11-05 18:10:20] iteration  1573500/ 2384185 | consumed samples:   1208448000 | elapsed time per iteration (ms): 431.5 | throughput per GPU (TFLOP/s/GPU): 338.3 | learning rate: 2.130589E-04 | global batch size:   768 | lm loss: 1.408516E+00 | loss scale: 1.0 | grad norm: 0.274 | num zeros: 294941024.0 | params norm: 2822.210 | number of skipped iterations:   0 | number of nan iterations:   0 |",
    "2099:  [2024-11-05 18:11:15] iteration  2384185/ 3194870 | consumed samples:   1208448000 | elapsed time per iteration (ms): 430.5 | throughput per GPU (TFLOP/s/GPU): 331.7 | learning rate: 2.130589E-04 | global batch size:   768 | lm loss: 1.408516E+00 | loss scale: 1.0 | grad norm: 0.274 | num zeros: 294941024.0 | params norm: 2822.210 | number of skipped iterations:   0 | number of nan iterations:   0 |",
]


@pytest_asyncio.fixture
def workflow_step_nooutput_training_loop_phase_async(rules_engine_async) -> AsyncGenerator[str, None]:
    rules_engine_async.config.datasources.log_query.selected_provider.queries.workflows["hang_nooutput"] = (
        WorkflowNativeQueries(
            query=[{"TRAINING_LOOP": '|~ "(?i)throughput per GPU|checkpoint|using world size|on_train_start"'}],
            nooutput_validation_query="|= `{job_id}`",
        )
    )
    return WorkflowStep(
        name="hang_nooutput",
        type=WorkflowStepType.nooutput,
        bucket_by=["job_id"],
        attributes={
            "cache_key": "hang_nooutput",
            "query": {
                "regex": [
                    {"TRAINING_LOOP": "throughput per GPU"},
                    {"TRAINING_LOOP": "checkpoint"},
                    {"TRAINING_LOOP": "using world size"},
                    {"TRAINING_LOOP": "on_train_start"},
                ],
            },
            "query_delay": 15,
            "interval": {
                "TRAINING_LOOP": 2,
            },
            "last_iteration_enabled": True,
            "last_iteration_regex": r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|",
            "last_iteration_operation_schema": "{current} > 0.99 * {total}",
            "false_positive_scan_cache_key": "false_positive",
            "false_positive_scan_timer": 300,
            "action": {
                "send_message_eventbus": "HANGNOOUTPUT",
            },
        },
        children=None,
    )


@pytest_asyncio.fixture
def workflow_step_nooutput_last_iter_disabled_training_loop_phase_async(
    rules_engine_async,
) -> AsyncGenerator[str, None]:
    rules_engine_async.config.datasources.log_query.selected_provider.queries.workflows["hang_nooutput"] = (
        WorkflowNativeQueries(
            query=[{"TRAINING_LOOP": '|~ "(?i)throughput per GPU|checkpoint|using world size|on_train_start"'}],
            nooutput_validation_query="|= `{job_id}`",
        )
    )
    return WorkflowStep(
        name="hang_nooutput",
        type=WorkflowStepType.nooutput,
        bucket_by=["job_id"],
        attributes={
            "cache_key": "hang_nooutput",
            "query": {
                "regex": [
                    {"TRAINING_LOOP": "throughput per GPU"},
                    {"TRAINING_LOOP": "checkpoint"},
                    {"TRAINING_LOOP": "using world size"},
                    {"TRAINING_LOOP": "on_train_start"},
                ],
            },
            "query_delay": 15,
            "interval": {
                "TRAINING_LOOP": 2,
            },
            "false_positive_scan_cache_key": "false_positive",
            "false_positive_scan_timer": 300,
            "action": {
                "send_message_eventbus": "HANGNOOUTPUT",
            },
        },
        children=None,
    )


@pytest_asyncio.fixture
def workflow_step_nooutput_all_phases_async(rules_engine_async) -> AsyncGenerator[str, None]:
    rules_engine_async.config.datasources.log_query.selected_provider.queries.workflows["hang_nooutput"] = (
        WorkflowNativeQueries(
            query=[
                {"PRE_TRAINING": '|~ "(?i)using world size|RANK 0.*Initialized distributed training"'},
                {"TRAINING_LOOP": '|~ "(?i)throughput per GPU|checkpoint|using world size|on_train_start"'},
            ],
            nooutput_validation_query="|= `{job_id}`",
        )
    )
    return WorkflowStep(
        name="hang_nooutput",
        type=WorkflowStepType.nooutput,
        bucket_by=["job_id"],
        attributes={
            "cache_key": "hang_nooutput",
            "query": {
                "regex": [
                    {"PRE_TRAINING": "using world size"},
                    {"PRE_TRAINING": "RANK 0.*Initialized distributed training"},
                    {"TRAINING_LOOP": "throughput per GPU"},
                    {"TRAINING_LOOP": "checkpoint"},
                    {"TRAINING_LOOP": "using world size"},
                    {"TRAINING_LOOP": "on_train_start"},
                ],
            },
            "query_delay": 15,
            "interval": {
                "PRE_TRAINING": 6,
                "TRAINING_LOOP": 2,
            },
            "last_iteration_enabled": True,
            "last_iteration_regex": r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|",
            "last_iteration_operation_schema": "{current} > 0.99 * {total}",
            "false_positive_scan_cache_key": "false_positive",
            "false_positive_scan_timer": 300,
            "action": {
                "send_message_eventbus": "HANGNOOUTPUT",
            },
        },
        children=None,
    )


@pytest_asyncio.fixture
def loki_log_nooutput_training_loop_phase_async() -> AsyncGenerator[str, None]:
    return [  # type: ignore[return-value]
        LogRecord(
            body=event,
            timestamp="1730329215704736095",
            attributes=Attributes(  # type: ignore[call-arg]
                job_id="612685",
                detection_time=None,
                log_file_name="8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log",
                log_file_path="/home/test-service-user/otelfilelog_paths/prod/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log",
                uuid="49a96762-4fc5-46d1-8f1d-bbae84b0f982",
            ),
        )
        for event in training_loop_events
    ]


@pytest_asyncio.fixture
def loki_log_nooutput_pre_training_phase_async() -> AsyncGenerator[str, None]:
    return [  # type: ignore[return-value]
        LogRecord(
            body=event,
            timestamp="1730329215704736095",
            attributes=Attributes(  # type: ignore[call-arg]
                job_id="612685",
                detection_time=None,
                log_file_name="8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log",
                log_file_path="/home/test-service-user/otelfilelog_paths/prod/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log",
                uuid="49a96762-4fc5-46d1-8f1d-bbae84b0f982",
            ),
        )
        for event in pre_training_events
    ]


@pytest_asyncio.fixture
def workflow_item_nooutput_training_loop_phase_async(
    loki_log_nooutput_training_loop_phase_async,
) -> AsyncGenerator[WorkflowItem, None]:
    return [  # type: ignore[return-value]
        WorkflowItem(
            record=loki_log_event,
            step_name=None,
            step_value=None,
        )
        for loki_log_event in loki_log_nooutput_training_loop_phase_async
    ]


@pytest_asyncio.fixture
def workflow_item_nooutput_pre_training_phase_async(
    loki_log_nooutput_pre_training_phase_async,
) -> AsyncGenerator[WorkflowItem, None]:
    return [  # type: ignore[return-value]
        WorkflowItem(
            record=loki_log_event,
            step_name=None,
            step_value=None,
        )
        for loki_log_event in loki_log_nooutput_pre_training_phase_async
    ]


@pytest_asyncio.fixture
def checkpoint_nooutput_async() -> AsyncGenerator[str, None]:
    return Checkpoint(
        key="nooutput_action",
        value=None,
    )
