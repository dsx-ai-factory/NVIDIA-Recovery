# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from watcher.lib.utils.loki_stats import LOKI_DEGRADED
from watcher.lib.utils.metrics import (
    WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED,
    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND,
    WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND,
)
from watcher.models.config import WatcherEventSource
from watcher.models.workflow import WorkflowStep, WorkflowStepType


@pytest.fixture(autouse=True)
def _reset_loki_degraded():
    """Ensure LOKI_DEGRADED gauge is reset to 0 before and after each test."""
    LOKI_DEGRADED.set(0)
    yield
    LOKI_DEGRADED.set(0)


def _counter_value(counter, job_id="612685"):
    return counter.labels(job_id=job_id)._value.get()


@pytest.mark.parametrize(
    "template,expected",
    [
        ("|= `{job_id}`", "|= `959935`"),
        ('| job_id = "{job_id}"', '| job_id = "959935"'),
    ],
)
def test_nooutput_validation_query(rules_engine, template, expected):
    """Render an explicit provider validation template without a workflow fallback."""
    query = rules_engine._render_nooutput_validation_query(template, job_id="959935")
    assert query == expected


def test_moving_avg_config():
    """Test moving_avg workflow step config attributes are properly set."""
    attrs = {
        "cache_key": "test",
        "window_size": 100,
        "base_percentile_threshold": 35,
        "anomaly_percent_threshold": 10,
    }
    step = WorkflowStep(name="test_step", type=WorkflowStepType.moving_avg, attributes=attrs)
    assert step.attributes.get("window_size", 100) == 100


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start_time,expected", [("2024-10-30T00:00:00+00:00", 1), ("2025-01-01T00:00:00+00:00", 0), (None, 0)]
)
async def test_rabbitmq_nooutput_timeout_does_not_require_provider_queries(
    start_time,
    expected,
    mocker,
    rules_engine_async,
    workflow_step_nooutput_last_iter_disabled_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
):
    """The event-bus path observes regex activity and never queries a log backend."""
    engine = rules_engine_async
    engine.event_source = WatcherEventSource.RABBITMQ
    engine.config.event_source = WatcherEventSource.RABBITMQ
    engine.config.datasources.log_query = None
    engine.run_log_query = AsyncMock()
    engine.publisher.publish_message = MagicMock()
    status = {
        "job_id": "612685",
        "state": "RUNNING",
        "job_attempt_index": 0,
        "start_time": "2024-10-30T00:00:00+00:00",
        "log_file_paths": ["/logs/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log"],
    }
    status["start_time"] = start_time
    mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock, return_value=status)
    mocker.patch("watcher.workflow.tracer", autospec=True)
    mocker.patch("watcher.workflow.populate_headers_with_span_context", return_value={})
    step = workflow_step_nooutput_last_iter_disabled_training_loop_phase_async
    checkpoint_nooutput_async.value = workflow_item_nooutput_training_loop_phase_async

    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)
    engine.publisher.publish_message.assert_not_called()

    # Simulate the existing activity counter expiring without sleeping for wall time.
    checkpoint_nooutput_async.value = []
    engine.timed_counter_len = MagicMock(return_value=0)
    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)

    assert engine.publisher.publish_message.call_count == expected
    engine.run_log_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config_validation_failures,log_file_paths,should_skip",
    [
        (["LOG_FILE_PATH_NOT_FOUND"], [], True),
        ([], ["/some/other/path.log"], True),
        ([], ["/path/to/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log"], False),
    ],
    ids=["log_path_not_found_failure", "log_file_not_in_paths", "valid_log_file_path"],
)
async def test_nooutput_workflow_log_path_validation(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    config_validation_failures,
    log_file_paths,
    should_skip,
):
    """Test that nooutput workflow validates log paths in job_info response."""
    rules_engine_async.run_log_query = AsyncMock()
    rules_engine_async.run_log_query.return_value = workflow_item_nooutput_training_loop_phase_async
    rules_engine_async.publisher.publish_message = MagicMock()

    status = {
        "job_id": "612685",
        "state": "RUNNING",
        "job_attempt_index": 0,
        "start_time": "2024-10-30T00:00:00+00:00",
        "config_validation_failure_types": config_validation_failures,
        "log_file_paths": log_file_paths,
    }
    mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock, return_value=status)
    mocker.patch("watcher.workflow.tracer", autospec=True)
    mocker.patch("watcher.workflow.populate_headers_with_span_context", return_value={})

    # First call - registers job in cache
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    # Wait for cache timeout (set to 2 secs in fixture)
    rules_engine_async.run_log_query.return_value = []
    await asyncio.sleep(3)
    rules_engine_async.timed_counter_add = MagicMock()

    # Second call - triggers validation
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=workflow_step_nooutput_training_loop_phase_async,
        checkpoint=checkpoint_nooutput_async,
        bucket_keys=workflow_step_nooutput_training_loop_phase_async.bucket_by,
    )

    if should_skip:
        rules_engine_async.publisher.publish_message.assert_not_called()
    else:
        rules_engine_async.publisher.publish_message.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "validation_returns_events,loki_degraded,should_fire_anomaly,found_delta,not_found_delta,skipped_delta",
    [
        (True, False, False, 1, 0, 0),
        (False, False, True, 0, 1, 0),
        (False, True, False, 0, 0, 1),
    ],
    ids=["validation_finds_events", "validation_empty_loki_healthy", "validation_empty_loki_degraded"],
)
async def test_nooutput_full_flow_with_loki_degradation_check(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    validation_returns_events,
    loki_degraded,
    should_fire_anomaly,
    found_delta,
    not_found_delta,
    skipped_delta,
):
    """Test the full nooutput flow: Loki query -> cache timeout -> validation -> degradation check -> anomaly."""
    rules_engine_async.run_log_query = AsyncMock(return_value=workflow_item_nooutput_training_loop_phase_async)
    rules_engine_async.publisher.publish_message = MagicMock()

    status = {
        "job_id": "612685",
        "state": "RUNNING",
        "job_attempt_index": 0,
        "start_time": "2024-10-30T00:00:00+00:00",
        "config_validation_failure_types": [],
        "log_file_paths": [
            "/home/test-service-user/otelfilelog_paths/prod/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log"
        ],
    }
    mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock, return_value=status)
    mocker.patch("watcher.workflow.tracer", autospec=True)
    mocker.patch("watcher.workflow.populate_headers_with_span_context", return_value={})

    step = workflow_step_nooutput_training_loop_phase_async
    ckpt = checkpoint_nooutput_async

    # First call — registers job in cache (events returned)
    await rules_engine_async.workflow_step_nooutput(workflow_step=step, checkpoint=ckpt, bucket_keys=step.bucket_by)

    # Wait for cache timeout (interval=2s in fixture)
    await asyncio.sleep(3)

    # Set up second call: initial query returns empty (triggers validation)
    validation_data = workflow_item_nooutput_training_loop_phase_async if validation_returns_events else []
    rules_engine_async.run_log_query = AsyncMock(return_value=validation_data)
    rules_engine_async.timed_counter_add = MagicMock()
    before = {
        "found": _counter_value(WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND),
        "not_found": _counter_value(WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND),
        "skipped": _counter_value(WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED),
    }

    if loki_degraded:
        LOKI_DEGRADED.set(1)

    await rules_engine_async.workflow_step_nooutput(workflow_step=step, checkpoint=ckpt, bucket_keys=step.bucket_by)

    if should_fire_anomaly:
        rules_engine_async.publisher.publish_message.assert_called_once()
    else:
        rules_engine_async.publisher.publish_message.assert_not_called()
    assert _counter_value(WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_FOUND) == before["found"] + found_delta
    assert _counter_value(WORKFLOW_HANGNOOUTPUT_VALIDATION_EVENTS_NOT_FOUND) == before["not_found"] + not_found_delta
    assert _counter_value(WORKFLOW_HANGNOOUTPUT_SKIPPED_LOKI_DEGRADED) == before["skipped"] + skipped_delta


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "aps_spec,expected_timeout",
    [
        (None, 2),
        ({"anomaly_detection_config": {}}, 840),
        ({"anomaly_detection_config": {"log_based_timeout_threshold_trainingloop": 99}}, 99),
    ],
    ids=["no_aps_spec_uses_step_default", "spec_defaults_apply", "spec_override_applies"],
)
async def test_nooutput_per_job_timeout_override(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    aps_spec,
    expected_timeout,
):
    """workflow_step_nooutput honors per-job aps_spec threshold when scheduling the timer."""
    rules_engine_async.run_log_query = AsyncMock(return_value=workflow_item_nooutput_training_loop_phase_async)
    spy = mocker.spy(rules_engine_async, "timed_counter_add")
    status = {"job_id": "612685", "state": "RUNNING", "aps_spec": aps_spec}
    mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock, return_value=status)
    mocker.patch("watcher.workflow.tracer", autospec=True)
    mocker.patch("watcher.workflow.populate_headers_with_span_context", return_value={})

    step = workflow_step_nooutput_training_loop_phase_async
    await rules_engine_async.workflow_step_nooutput(
        workflow_step=step, checkpoint=checkpoint_nooutput_async, bucket_keys=step.bucket_by
    )

    assert {c.kwargs.get("timeout") for c in spy.call_args_list} == {expected_timeout}


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", [True, False], ids=["late_registration", "still_missing"])
@pytest.mark.parametrize("more_progress", [False, True], ids=["silent", "more_progress"])
@pytest.mark.parametrize("grace_secs", [None, 60], ids=["default_grace", "configured_grace"])
async def test_nooutput_retries_missing_job_without_more_progress(
    mocker,
    rules_engine_async,
    workflow_step_nooutput_training_loop_phase_async,
    workflow_item_nooutput_training_loop_phase_async,
    checkpoint_nooutput_async,
    registered,
    more_progress,
    grace_secs,
):
    """Late registration uses the override; a missing job gets one bounded fallback.

    Neither the retry nor timer creation requires another progress event, and
    resolving configuration does not restart the elapsed silence window.
    """
    engine = rules_engine_async
    step = workflow_step_nooutput_training_loop_phase_async
    step.attributes["interval"] = {"PRE_TRAINING": 960, "TRAINING_LOOP": 840}
    if grace_secs is not None:
        step.attributes["job_discovery_grace_period_secs"] = grace_secs
    retry_delay = grace_secs if grace_secs is not None else 31
    engine.run_log_query = AsyncMock(return_value=workflow_item_nooutput_training_loop_phase_async)
    clock = mocker.patch("watcher.workflow.time")
    clock.monotonic.return_value = 100
    lookup = mocker.patch("watcher.workflow.get_job_info", new_callable=AsyncMock, return_value=None)
    timer = mocker.spy(engine, "timed_counter_add")

    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)
    lookup.assert_awaited_once()
    timer.assert_not_called()

    engine.run_log_query.return_value = workflow_item_nooutput_training_loop_phase_async if more_progress else []
    clock.monotonic.return_value = 100 + retry_delay - 1
    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)
    lookup.assert_awaited_once()
    timer.assert_not_called()

    if registered:
        lookup.return_value = {
            "job_id": "612685",
            "aps_spec": {"anomaly_detection_config": {"log_based_timeout_threshold_trainingloop": 120}},
        }
    engine.run_log_query.return_value = []
    clock.monotonic.return_value = 100 + retry_delay
    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)
    assert lookup.await_count == 2
    timer.assert_called_once()
    expected_remaining = (120 if registered else 840) - (1 if more_progress else retry_delay)
    assert timer.call_args.kwargs["timeout"] == expected_remaining

    clock.monotonic.return_value = 101 + retry_delay
    await engine.workflow_step_nooutput(step, checkpoint_nooutput_async, bucket_keys=step.bucket_by)
    assert lookup.await_count == 2
    timer.assert_called_once()
