# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import asdict
from unittest.mock import AsyncMock, MagicMock

import pytest
from recovery_libs.schema.watcher.model import Attributes
from watcher.lib.utils.constants import JOB_ATTRIBUTES_NOOUTPUT_PREFIX
from watcher.lib.utils.data import generate_unique_cache_key
from watcher.models.workflow import (
    Checkpoint,
    LogRecord,
    WorkflowItem,
    WorkflowStep,
    WorkflowStepNooutputJobPhase,
)

EVENT_CKP_LOAD = "1622536:[04-14 12:41:30|CRITICAL|projects/cosmos/diffusion/v2/checkpointer/dcp.py:355:load] Loaded checkpoint from s3://checkpoints-us-east-1/cosmos_diffusion_v2/ablation_2B_0321_tokenizer_data/"
EVENT_CKP_SAVE = "329293:  0: [04-11 10:00:34|CRITICAL|projects/cosmos/diffusion/v2/checkpointer/dcp.py:423:save_state_dict_worker] Saved checkpoint to s3://checkpoints-us-east-1/cosmos_diffusion_v2/ablation_2B_0321_tokenizer_data/ablation_2B_0321_tokenizer_data_213pt2_wan2pt1tokenizer_resizeT6_Res480V250322-241219_14B/checkpoints/iter_000060000"
EVENT_ITERATION_TRAINING_LOOP = "[04-10 16:04:48|CRITICAL|projects/cosmos/diffusion/v2/callbacks/val_video_sampling.py:124:on_train_start] [RANK 469]The rank recieved task to sample sora_upsampled_val_v0_s3 from 0 to 1. DataLength, WorldSize, Rank:(32, 640, 469)"


def get_cosmos_config_schema(config, workflow_name: str, child_workflow_name: str | None = None):
    rule = next(r for r in config.rules if r.name == "local")
    step = next(s for s in rule.workflow if s.name == workflow_name)
    if not child_workflow_name:
        return asdict(step)
    child = next(c for c in step.children if c.name == child_workflow_name)
    return asdict(child)


def generate_loki_log(event):
    return LogRecord(
        body=event,
        timestamp="1730329215704736095",
        attributes=Attributes(
            job_id="804973",
            detection_time=None,
            log_file_name="gpt_15b_804973_date_24-10-30_time_14-29-33.log",
            log_file_path="/home/test-service-user/otelfilelog_paths/prod/cosmos-gpt3-15b-8t-bs-1152/gpt_15b_804973_date_24-10-30_time_14-29-33.log",
            uuid="0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee",
        ),
    )


def generate_workflow_item(event):
    return WorkflowItem(
        record=generate_loki_log(event=event),
        step_name=None,
        step_value=None,
    )


def generate_workflow_item_list(event):
    return [
        WorkflowItem(
            record=generate_loki_log(event=event),
            step_name=None,
            step_value=None,
        )
    ]


def generate_checkpoint(event):
    return Checkpoint(
        key="action",
        value=generate_workflow_item(event=event),
    )


def generate_checkpoint_list(event):
    return Checkpoint(
        key="action",
        value=[generate_workflow_item(event=event)],
    )


@pytest.mark.asyncio
async def test_workflow_cosmos_ckp_load_notification(
    rules_engine_async,
):
    # setup
    workflow_item = generate_workflow_item(event=EVENT_CKP_LOAD)
    checkpoint = generate_checkpoint(event=EVENT_CKP_LOAD)

    workflow_config_step = WorkflowStep(
        **get_cosmos_config_schema(
            rules_engine_async.config, workflow_name="chkpt_load", child_workflow_name="chkpt_load_action"
        )
    )

    # assert fixture data is configured as expected
    job_id = workflow_item.record.attributes.job_id
    assert job_id == "804973"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 804973
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item

    # mock method `publish_message` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # trigger workflow_step_regex_parser workflow run
    rules_engine_async.workflow_step_simple_action(
        workflow_step=workflow_config_step,
        checkpoint=checkpoint,
    )

    # assert message has been published to RabbitMQ
    assert rules_engine_async.publisher.publish_message.call_count == 1
    call_args = rules_engine_async.publisher.publish_message.call_args
    assert call_args.kwargs["routing_key"] == "anomaly.detected.CHECKPOINTLOAD"
    msg = json.loads(call_args.kwargs["message"])
    assert msg["type"] == "CHECKPOINTLOAD"
    assert msg["attributes"]["job_id"] == "804973"
    assert msg["attributes"]["uuid"] != "0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee", "UUID should be regenerated"


@pytest.mark.asyncio
async def test_workflow_cosmos_ckp_save_notification(
    rules_engine_async,
):
    # setup
    workflow_item = generate_workflow_item(event=EVENT_CKP_SAVE)
    checkpoint = generate_checkpoint(event=EVENT_CKP_SAVE)

    workflow_config_step = WorkflowStep(
        **get_cosmos_config_schema(
            rules_engine_async.config, workflow_name="chkpt_save", child_workflow_name="chkpt_save_action"
        )
    )

    # assert fixture data is configured as expected
    job_id = workflow_item.record.attributes.job_id
    assert job_id == "804973"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 804973
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item

    # mock method `publish_message` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # trigger workflow_step_regex_parser workflow run
    rules_engine_async.workflow_step_simple_action(
        workflow_step=workflow_config_step,
        checkpoint=checkpoint,
    )

    # assert message has been published to RabbitMQ
    assert rules_engine_async.publisher.publish_message.call_count == 1
    call_args = rules_engine_async.publisher.publish_message.call_args
    assert call_args.kwargs["routing_key"] == "anomaly.detected.CHECKPOINTSAVE"
    msg = json.loads(call_args.kwargs["message"])
    assert msg["type"] == "CHECKPOINTSAVE"
    assert msg["attributes"]["job_id"] == "804973"
    assert msg["attributes"]["uuid"] != "0192dfe1-7a96-7c8f-b84b-0da6cd4cdcee", "UUID should be regenerated"


@pytest.mark.asyncio
async def test_workflow_cosmos_nooutput_workflow_state_cache(
    mocker,
    rules_engine_async,
    jm_job_status_running,
):
    # setup
    workflow_item = generate_workflow_item(event=EVENT_ITERATION_TRAINING_LOOP)
    checkpoint = generate_checkpoint_list(event=EVENT_ITERATION_TRAINING_LOOP)
    workflow_config_step = WorkflowStep(
        **get_cosmos_config_schema(rules_engine_async.config, workflow_name="noprogress")
    )
    workflow_config_step.attributes["interval"]["PRE_TRAINING"] = 2
    workflow_config_step.attributes["interval"]["TRAINING_LOOP"] = 2

    # generate job key ID
    key = rules_engine_async._get_bucket_id(
        key_name=generate_unique_cache_key(key_suffix=workflow_config_step.attributes["cache_key"]),
        bucket_keys=workflow_config_step.bucket_by,
        workflow_item=workflow_item,
    )

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 612685
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = generate_workflow_item_list(event=EVENT_ITERATION_TRAINING_LOOP)

    # mock method `publish_message` and variable `unique` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # mock get_async/get_job_state (Job Manager) inside module watcher.workflow
    status = json.loads(jm_job_status_running)
    assert status["state"] == "RUNNING"
    mock_get_async = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock)
    mock_get_async.return_value = status

    # trigger NOOUTPUT workflow run
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_config_step,
        checkpoint=checkpoint,
        bucket_keys=workflow_config_step.bucket_by,
    )

    # assert job_phase is set to WorkflowStepNooutputJobPhase.TRAINING_LOOP in state cache
    assert (
        rules_engine_async.kvstore[f"{JOB_ATTRIBUTES_NOOUTPUT_PREFIX}{key}"].job_phase
        == WorkflowStepNooutputJobPhase.TRAINING_LOOP
    )
