# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from watcher.lib.utils.constants import JOB_LEVEL_DEDUP_CACHE_PREFIX
from watcher.lib.utils.data import generate_sha256_signature
from watcher.models.workflow import WorkflowStepRegexConfig


def test_nested_restarter_config_schema(rules_engine):
    # mock publish_message method and `unique` variable inside RulesEngine()
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.publisher.publish_message.unique.side_effect = True

    workflow_config_rule = None
    for rule in rules_engine.config.rules:
        if rule.name == "local":
            workflow_config_rule = rule

    workflow_config_step_dict = None
    for step in workflow_config_rule.workflow:
        if step.name == "nested_restarter":
            for child in step.children:
                if child.name == "nested_restarter_regex_parse":
                    workflow_config_step_dict = child.to_json()

    # assert WorkflowStepRegexConfig is instantiated with all expected fields reading from YAML config
    workflow_config_step = WorkflowStepRegexConfig(**workflow_config_step_dict)
    assert workflow_config_step.attributes.advanced_regex is not None
    assert "NESTED_RESTARTER" in [regex.event_type for regex in workflow_config_step.attributes.advanced_regex]


def test_nested_restarter_init_job_signature_in_action_cache(
    rules_engine_async,
    workflow_step_nested_restarter_config_dict,
    workflow_item_nested_restarter_init,
    checkpoint_nested_restarter_init,
):
    # setup
    job_id = workflow_item_nested_restarter_init.record.attributes.job_id
    expected_job_signature_in_cache = generate_sha256_signature(data=f"{JOB_LEVEL_DEDUP_CACHE_PREFIX}{job_id}")
    workflow_config_step = WorkflowStepRegexConfig(**workflow_step_nested_restarter_config_dict)

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 804973
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nested_restarter_init

    # mock method `publish_message` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # trigger workflow_step_regex_parser workflow run
    rules_engine_async.workflow_step_regex_parser(
        workflow_step=workflow_config_step,
        checkpoint=checkpoint_nested_restarter_init,
    )

    # assert job_id has been persisted in action_cache since not tracking unique events
    assert expected_job_signature_in_cache not in rules_engine_async.action_cache


@pytest.mark.asyncio
async def test_nested_restarter_init_workflow_sends_notification(
    rules_engine_async,
    workflow_step_nested_restarter_config_dict,
    workflow_item_nested_restarter_init,
    checkpoint_nested_restarter_init,
):
    # setup
    workflow_config_step = WorkflowStepRegexConfig(**workflow_step_nested_restarter_config_dict)

    # assert fixture data is configured as expected
    job_id = workflow_item_nested_restarter_init.record.attributes.job_id
    assert job_id == "804973"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 804973
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nested_restarter_init

    # mock method `publish_message` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # trigger workflow_step_regex_parser workflow run
    rules_engine_async.workflow_step_regex_parser(
        workflow_step=workflow_config_step,
        checkpoint=checkpoint_nested_restarter_init,
    )

    # assert message has been published to RabbitMQ
    assert rules_engine_async.publisher.publish_message.call_count == 1
    call_args = rules_engine_async.publisher.publish_message.call_args
    assert call_args.kwargs["routing_key"] == "anomaly.detected.NESTED_RESTARTER"
    msg = json.loads(call_args.kwargs["message"])
    assert msg["type"] == "NESTED_RESTARTER"
    assert msg["attributes"]["job_id"] == "804973"
    assert msg["attributes"]["uuid"] != "0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee", "UUID should be regenerated"
