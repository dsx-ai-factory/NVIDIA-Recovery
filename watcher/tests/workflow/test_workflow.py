# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from opentelemetry.trace import StatusCode
from watcher.lib.utils.constants import (
    JOB_ATTRIBUTES_NOOUTPUT_PREFIX,
    METRICS_CLEANUP_KEY,
)
from watcher.lib.utils.data import generate_unique_cache_key
from watcher.models.config import WatcherEventSource, WorkflowNativeQueries
from watcher.models.workflow import WorkflowStepNooutputJobPhase


def test_is_last_iteration_true(rules_engine):
    event = "3071:  [2024-06-21 11:39:47] iteration  3973600/ 3973642 | consumed samples..."
    last_iteration_regex = r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|"
    last_iteration_operation_schema = "{current} > 0.99 * {total}"
    result = rules_engine.is_last_iteration(event, "", last_iteration_operation_schema, last_iteration_regex)
    assert result is True


def test_is_last_iteration_false(rules_engine):
    event = "3071:  [2024-06-21 11:39:47] iteration  3973600/ 3973642 | consumed samples..."
    last_iteration_regex = r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|"
    last_iteration_operation_schema = "{current} > 0.9999999 * {total}"
    result = rules_engine.is_last_iteration(event, "", last_iteration_operation_schema, last_iteration_regex)
    assert result is False


def test_is_last_iteration_no_match(rules_engine):
    event = "3071:  [2024-06-21 11:39:47] iteration  ABCDE/ FGHIJ | consumed samples..."
    last_iteration_regex = r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|"
    last_iteration_operation_schema = "{current} > 0.9999999 * {total}"
    result = rules_engine.is_last_iteration(event, "", last_iteration_operation_schema, last_iteration_regex)
    assert result is False


def test_unbound_crash_replay_is_not_suppressed(rules_engine, workflow_step_crash, checkpoint_crash, mocker):
    """Missing registration must not consume the next retry's notification slot."""
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=None)
    rules_engine.publisher.publish_message = MagicMock()
    for _ in range(2):
        rules_engine.workflow_step_simple_action(workflow_step=workflow_step_crash, checkpoint=checkpoint_crash)
    assert rules_engine.publisher.publish_message.call_count == 2


@pytest.mark.asyncio
async def test_straggler_throughput_metrics_cleanup_cache_tracking(
    rules_engine_async, workflow_step_straggler_emit_tflop, checkpoint_straggler_emit_tflop
):
    # setup
    job_id = checkpoint_straggler_emit_tflop.value.record.attributes.job_id
    metric_name = workflow_step_straggler_emit_tflop.attributes["metric_name"]
    rules_engine_async.config.metrics_cleanup_threshold_secs = 2

    # mock publish_message and send_ping methods inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()
    rules_engine_async.publisher.send_ping = MagicMock()

    # run emit_metric workflow step using STRAGGLER event
    rules_engine_async.workflow_step_emit_metric(
        workflow_step=workflow_step_straggler_emit_tflop, checkpoint=checkpoint_straggler_emit_tflop
    )

    # assert job_id has been persisted in cache
    assert METRICS_CLEANUP_KEY in rules_engine_async.kvstore
    assert metric_name in rules_engine_async.kvstore[METRICS_CLEANUP_KEY]
    assert job_id in rules_engine_async.kvstore[METRICS_CLEANUP_KEY][metric_name]

    # wait 3 seconds (watcher cache timeout set to 2 secs) to ensure event expires in cache and trigger workflow run
    await asyncio.sleep(3)

    # run maintenance tests to trigger cache entry removal
    rules_engine_async.run_maintenance_tasks()

    # assert job_id has been deleted from cache
    assert job_id not in rules_engine_async.kvstore[METRICS_CLEANUP_KEY][metric_name]


@pytest.mark.asyncio
async def test_nooutput_workflow_sends_notification(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    jm_job_status_running,
):
    # assert fixture data is configured as expected
    job_id = workflow_item_nooutput_training_loop_phase_async[0].record.attributes.job_id
    assert job_id == "612685"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 612685
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nooutput_training_loop_phase_async

    # mock method `publish_message` and variable `unique` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # mock get_async/get_job_state (Job Manager) inside module watcher.workflow
    status = json.loads(jm_job_status_running)
    assert status["state"] == "RUNNING"
    status["start_time"] = "2024-10-30T00:00:00+00:00"
    status["job_attempt_index"] = 0
    mock_get_async = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock)
    mock_get_async.return_value = status

    mock_tracer = mocker.patch("watcher.workflow.tracer", autospec=True)
    mock_populator = mocker.patch("watcher.workflow.populate_headers_with_span_context", autospec=True)

    mock_span = MagicMock()
    mock_tracer.start_span.return_value = mock_span

    trace_headers = {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
    mock_populator.return_value = trace_headers

    # trigger NOOUTPUT workflow run
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    # set `run_log_query` return value to empty list (no events)
    rules_engine_async.run_log_query.return_value = []

    # wait 3 seconds (watcher cache timeout set to 2 secs) to ensure event expires in cache and trigger workflow run
    await asyncio.sleep(3)

    # mock rules_engine_async.timed_counter_add to avoid running into thread sync issues in pytest async
    rules_engine_async.timed_counter_add = MagicMock()

    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    # assert message has been published to RabbitMQ
    rules_engine_async.publisher.publish_message.assert_called_once()
    assert rules_engine_async.publisher.publish_message.call_args.kwargs["properties"].headers == trace_headers
    mock_populator.assert_called_once_with(mock_span)

    message = rules_engine_async.publisher.publish_message.call_args.kwargs["message"]
    message_dict = json.loads(message)

    # assert a trace span has been created for the workflow step
    mock_tracer.start_span.assert_called_once()
    mock_span.set_attribute.assert_has_calls(
        [
            call("job_anomaly.type", "HANGNOOUTPUT"),
            call("job_anomaly.uuid", message_dict["attributes"]["uuid"]),
            call("job_event.uuid", message_dict["attributes"]["uuid"]),
        ],
        any_order=True,
    )
    mock_span.end.assert_called_once()
    mock_span.set_status.assert_called_once_with(StatusCode.OK)


@pytest.mark.asyncio
async def test_nooutput_workflow_last_iter_disabled_sends_notification(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_last_iter_disabled_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    jm_job_status_running,
):
    # assert fixture data is configured as expected
    job_id = workflow_item_nooutput_training_loop_phase_async[0].record.attributes.job_id
    assert job_id == "612685"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 612685
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nooutput_training_loop_phase_async

    # mock method `publish_message` and variable `unique` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # mock get_async/get_job_state (Job Manager) inside module watcher.workflow
    status = json.loads(jm_job_status_running)
    assert status["state"] == "RUNNING"
    status["start_time"] = "2024-10-30T00:00:00+00:00"
    status["job_attempt_index"] = 0
    mock_get_async = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock)
    mock_get_async.return_value = status

    # trigger NOOUTPUT workflow run
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_last_iter_disabled_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_last_iter_disabled_training_loop_phase_async.bucket_by,
    )

    # set `run_log_query` return value to empty list (no events)
    rules_engine_async.run_log_query.return_value = []

    # wait 3 seconds (watcher cache timeout set to 2 secs) to ensure event expires in cache and trigger workflow run
    await asyncio.sleep(3)

    # mock rules_engine_async.timed_counter_add to avoid running into thread sync issues in pytest async
    rules_engine_async.timed_counter_add = MagicMock()

    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_last_iter_disabled_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_last_iter_disabled_training_loop_phase_async.bucket_by,
    )

    # assert message has been published to RabbitMQ
    rules_engine_async.publisher.publish_message.assert_called_once()


@pytest.mark.asyncio
async def test_nooutput_workflow_skip_notification(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    jm_job_status_failed,
):
    # assert fixture data is configured as expected
    job_id = workflow_item_nooutput_training_loop_phase_async[0].record.attributes.job_id
    assert job_id == "612685"

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 612685
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nooutput_training_loop_phase_async

    # mock method `publish_message` and variable `unique` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()
    rules_engine_async.publisher.publish_message.unique.side_effect = False

    # mock get_async/get_job_state (Job Manager) inside module watcher.workflow
    status = json.loads(jm_job_status_failed)
    assert status["state"] == "FAILED"
    mock_get_async = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock)
    mock_get_async.return_value = status

    # trigger NOOUTPUT workflow run
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    # set `run_log_query` return value to empty list (no events)
    rules_engine_async.run_log_query.return_value = []

    # wait 3 seconds (watcher cache timeout set to 2 secs) and trigger workflow run
    await asyncio.sleep(3)
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    # assert message has been published to RabbitMQ
    rules_engine_async.publisher.publish_message.assert_not_called()


@pytest.mark.asyncio
async def test_nooutput_workflow_state_cache(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_all_phases_async,
    workflow_item_nooutput_pre_training_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    jm_job_status_running,
):
    # generate job key ID
    key = rules_engine_async._get_bucket_id(
        key_name=generate_unique_cache_key(key_suffix=workflow_step_nooutput_all_phases_async.attributes["cache_key"]),
        bucket_keys=workflow_step_nooutput_all_phases_async.bucket_by,
        workflow_item=workflow_item_nooutput_pre_training_phase_async[0],
    )

    # mock `run_log_query` and set `run_log_query` return value to 1 event for job 612685
    rules_engine_async.run_log_query = AsyncMock()
    # nice trick - `side_effect` will return an item from list() every time it's called
    rules_engine_async.run_log_query.side_effect = [
        workflow_item_nooutput_pre_training_phase_async,
        workflow_item_nooutput_training_loop_phase_async,
    ]

    # mock method `publish_message` and variable `unique` inside RulesEngine()
    rules_engine_async.publisher.publish_message = MagicMock()

    # test
    assert rules_engine_async.event_source == WatcherEventSource.LOG_QUERY

    # mock get_async/get_job_state (Job Manager) inside module watcher.workflow
    status = json.loads(jm_job_status_running)
    assert status["state"] == "RUNNING"
    status["start_time"] = "2024-10-30T00:00:00+00:00"
    status["job_attempt_index"] = 0
    mock_get_async = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock)
    mock_get_async.return_value = status

    # trigger NOOUTPUT workflow run
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_all_phases_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_all_phases_async.bucket_by,
    )

    # Verify counter key exists
    assert rules_engine_async.timed_counter_does_key_exist(key) is True

    # assert there are 3 events in state cache
    assert rules_engine_async.timed_counter_len(key) == 3

    # assert job_phase is set to WorkflowStepNooutputJobPhase.TRAINING_LOOP in state cache
    assert f"{JOB_ATTRIBUTES_NOOUTPUT_PREFIX}{key}" in rules_engine_async.kvstore
    assert (
        rules_engine_async.kvstore[f"{JOB_ATTRIBUTES_NOOUTPUT_PREFIX}{key}"].job_phase
        == WorkflowStepNooutputJobPhase.TRAINING_LOOP
    )


## Test Job Phase Transitions
##
def test_valid_transition(rules_engine):
    """
    Test a valid transition from PRE_TRAINING to TRAINING_LOOP.
    """
    current_phase = WorkflowStepNooutputJobPhase.PRE_TRAINING
    transition_to = WorkflowStepNooutputJobPhase.TRAINING_LOOP

    result = rules_engine.is_job_phase_transition_valid(
        current_phase=current_phase,
        transition_to=transition_to,
    )
    assert result is True


def test_invalid_transition_to_phase_not_allowed(rules_engine):
    """
    Test an invalid transition from TRAINING_LOOP to PRE_TRAINING.
    """
    current_phase = WorkflowStepNooutputJobPhase.TRAINING_LOOP
    transition_to = WorkflowStepNooutputJobPhase.PRE_TRAINING

    result = rules_engine.is_job_phase_transition_valid(
        current_phase=current_phase,
        transition_to=transition_to,
    )
    assert result is False


@pytest.mark.asyncio
async def test_execute_workflow_propagates_time_bounds_to_log_query(rules_engine_async):
    """Option A: start/end on execute_workflow reach run_log_query (chunk bounds aren't dropped)."""
    rules_engine_async.run_log_query = AsyncMock(return_value=[])
    start, end = object(), object()
    rules_engine_async.config.datasources.log_query.selected_provider.queries.workflows["s"] = WorkflowNativeQueries(
        query=['{x="y"}']
    )
    step = {"name": "s", "type": "log_query", "attributes": {}}
    await rules_engine_async.execute_workflow(workflow=[step], start=start, end=end)
    kwargs = rules_engine_async.run_log_query.call_args.kwargs
    assert (kwargs["start"], kwargs["end"]) == (start, end)
