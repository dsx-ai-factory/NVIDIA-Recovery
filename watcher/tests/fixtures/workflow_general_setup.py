# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

import pytest_asyncio
from watcher.lib.utils.data import convert_datetime_to_timestamp_ns, generate_uuid7
from watcher.models.workflow import LogRecord, WorkflowItem


@pytest_asyncio.fixture
def generate_events_workflow_item(request):
    return [
        WorkflowItem(
            record=LogRecord(
                body=event,
                timestamp=convert_datetime_to_timestamp_ns(datetime.now()),
                attributes={
                    "job_id": "1366321",
                    "log.file.name": "56B_hybrid_20T_phase1_1366321_date_25-01-08_time_05-58-28.log",
                    "log.file.path": "/tmp/56b_hybrid-logs-phase1/56B_hybrid_20T_phase1_1366321_date_25-01-08_time_05-58-28.log",
                    "uuid": generate_uuid7(),
                },
            ),
            step_name=None,
            step_value=None,
        )
        for event in request.param
    ]
