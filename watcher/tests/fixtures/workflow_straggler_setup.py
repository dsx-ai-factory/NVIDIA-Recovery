# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest_asyncio
from recovery_libs.schema.watcher.model import Attributes
from watcher.models.workflow import Checkpoint, LogRecord, WorkflowItem, WorkflowStep, WorkflowStepType

# STRAGGLER setup
straggler_events = [
    "6143:  [2025-02-04 18:16:04] iteration  2564300/ 3178914 | consumed samples:   1969382400 | skipped samples:        49152 | elapsed time per iteration (ms): 1045.2 | throughput per GPU (TFLOP/s/GPU): 337.3 | learning rate: 3.914993E-05 | global batch size:   768 | lm loss: 1.221473E+00 | loss scale: 1.0 | grad norm: 0.315 | num zeros: 690296704.0 | params norm: 3917.219 | number of skipped iterations:   0 | number of nan iterations:   0 |",
]


@pytest_asyncio.fixture
def workflow_step_straggler_emit_tflop():
    return WorkflowStep(
        name="straggler_action",
        type=WorkflowStepType.emit_metric,
        attributes={
            "metric_name": "WORKFLOW_JOB_THROUGHPUT",
            "metric_type": "gauge",
            "enable_metric_cleanup": True,
        },
        bucket_by=None,
        children=None,
    )


@pytest_asyncio.fixture
def loki_log_straggler():
    return LogRecord(
        body="6143:  [2025-02-04 18:16:04] iteration  2564300/ 3178914 | consumed samples:   1969382400 | skipped samples:        49152 | elapsed time per iteration (ms): 1045.2 | throughput per GPU (TFLOP/s/GPU): 337.3 | learning rate: 3.914993E-05 | global batch size:   768 | lm loss: 1.221473E+00 | loss scale: 1.0 | grad norm: 0.315 | num zeros: 690296704.0 | params norm: 3917.219 | number of skipped iterations:   0 | number of nan iterations:   0 |",
        timestamp="1730329215704736095",
        attributes=Attributes(
            job_id="804947",
            detection_time=None,
            log_file_name="gpt_15b_804947_date_24-10-30_time_14-29-33.log",
            log_file_path="/home/test-service-user/otelfilelog_paths/prod/megatron-gpt3-15b-8t-bs-1152/gpt_15b_804947_date_24-10-30_time_14-29-33.log",
            uuid="0192dfe1-7a96-7c8f-b84b-0da6cd4cdbaa",
        ),
    )


# WorkflowStepType.log_query (workflow.py) returns: List[WorkflowItem]
@pytest_asyncio.fixture
def workflow_item_straggler_emit_tflop(loki_log_straggler):
    return WorkflowItem(
        record=loki_log_straggler,
        step_name="get_tflop",
        step_value="337.3",
    )


@pytest_asyncio.fixture
def checkpoint_straggler_emit_tflop(workflow_item_straggler_emit_tflop):
    return Checkpoint(
        key="straggler_action",
        value=workflow_item_straggler_emit_tflop,
    )
