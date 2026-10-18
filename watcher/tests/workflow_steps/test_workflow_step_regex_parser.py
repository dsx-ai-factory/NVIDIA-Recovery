# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

from recovery_libs.schema.watcher.model import Attributes
from watcher.models.workflow import (
    Checkpoint,
    LogRecord,
    WorkflowItem,
    WorkflowStepRegexConfig,
)


def test_regex_parser_generates_unique_uuids_for_multiple_matches(rules_engine_async):
    """Test that multiple events from same log get unique UUIDs."""
    item = WorkflowItem(
        record=LogRecord(
            body="test",
            timestamp="1730329215704736095",
            attributes=Attributes(job_id="999", log_file_name="t.log", log_file_path="/t.log", uuid="orig"),
        )
    )
    cfg = WorkflowStepRegexConfig(
        name="test",
        type="regex_parser",
        attributes={"publish_events": True, "advanced_regex": [{"event_type": "CRASH", "regexes": [r"(?P<m>.*)"]}]},
    )
    with patch("watcher.workflow.extract_structured_data_from_text", return_value=[{"m": "1"}, {"m": "2"}]):
        rules_engine_async.publisher.publish_message = MagicMock()
        rules_engine_async.workflow_step_regex_parser(cfg, Checkpoint(value=item))
        uuids = [
            eval(c[1]["message"])["attributes"]["uuid"]
            for c in rules_engine_async.publisher.publish_message.call_args_list
        ]
        assert len(uuids) == 2 and uuids[0] != uuids[1] and "orig" not in uuids


def test_regex_parser_dedup_skips_duplicate_events(rules_engine_async):
    """Test that dedup skips events with identical body content."""
    attrs = Attributes(job_id="123", log_file_name="r.log", log_file_path="/r.log", uuid="u1", slurm_job_attempt=0)
    items = [
        WorkflowItem(record=LogRecord(body="same content", timestamp="1", attributes=attrs)),
        WorkflowItem(record=LogRecord(body="same content", timestamp="2", attributes=attrs)),
    ]
    cfg = WorkflowStepRegexConfig(
        name="test",
        type="regex_parser",
        attributes={
            "publish_events": True,
            "dedup": True,
            "advanced_regex": [{"event_type": "CRASH", "regexes": [r"(?P<m>.*)"]}],
        },
    )
    rules_engine_async.publisher.publish_message = MagicMock()
    with patch("watcher.workflow.extract_structured_data_from_text", return_value=[{"m": "x"}]):
        for item in items:
            rules_engine_async.workflow_step_regex_parser(cfg, Checkpoint(value=item))

    assert rules_engine_async.publisher.publish_message.call_count == 1


def test_regex_parser_dedup_disabled_publishes_all(rules_engine_async):
    """Test that without dedup, all events are published."""
    attrs = Attributes(job_id="123", log_file_name="r.log", log_file_path="/r.log", uuid="u1", slurm_job_attempt=0)
    items = [
        WorkflowItem(record=LogRecord(body="same", timestamp="1", attributes=attrs)),
        WorkflowItem(record=LogRecord(body="same", timestamp="2", attributes=attrs)),
    ]
    cfg = WorkflowStepRegexConfig(
        name="test",
        type="regex_parser",
        attributes={
            "publish_events": True,
            "dedup": False,
            "advanced_regex": [{"event_type": "CRASH", "regexes": [r"(?P<m>.*)"]}],
        },
    )
    rules_engine_async.publisher.publish_message = MagicMock()
    with patch("watcher.workflow.extract_structured_data_from_text", return_value=[{"m": "x"}]):
        for item in items:
            rules_engine_async.workflow_step_regex_parser(cfg, Checkpoint(value=item))

    assert rules_engine_async.publisher.publish_message.call_count == 2
