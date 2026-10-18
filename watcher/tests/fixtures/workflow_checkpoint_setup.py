# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path

import pytest_asyncio
import yaml
from watcher.models.workflow import Checkpoint, LogRecord, WorkflowItem

_CONFIG = Path(__file__).parent / "watcher_config.yaml"


def get_checkpoint_config(workflow_name):
    with open(_CONFIG) as f:
        config = yaml.safe_load(f)
    for rule in config["rules"]:
        if rule["name"] == "local":
            for step in rule["workflow"]:
                if step["name"] == workflow_name:
                    return step


def make_checkpoint_from_event(event_json):
    return Checkpoint(
        key="test",
        value=WorkflowItem(
            record=LogRecord(
                body=event_json["line"],
                timestamp=event_json["timestamp"],
                attributes={
                    "job_id": event_json["fields"]["job_id"],
                    "log.file.name": event_json["fields"]["log_file_name"],
                    "log.file.path": event_json["fields"]["log_file_path"],
                    "uuid": "test-uuid",
                },
            )
        ),
    )


@pytest_asyncio.fixture
def av_checkpoint_events():
    with open("tests/data/events-av-checkpoints.json") as f:
        return json.load(f)
