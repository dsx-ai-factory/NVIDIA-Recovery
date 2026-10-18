# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from recovery_libs.schema.aps_spec.spec import AnomalyDetectionConfig, ApsSlurmJobSpec
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Attributes, Event
from watcher.models.workflow import LogRecord, WorkflowItem


def _event(event_type=EventType.CRASH, body="payload", job_id="12345", job_attempt_index=None):
    attributes = Attributes(uuid="uuid-1", job_id=job_id)
    if job_attempt_index is not None:
        attributes.slurm_job_attempt = job_attempt_index
    return Event(type=event_type, body=body, attributes=attributes)


def _job_status(aps_spec=None, job_id="12345"):
    return SlurmJobExecutionStatus(
        job_id=job_id,
        job_attempt_index=0,
        workflow_id="workflow-test",
        heimdall_job_id="018f89d8-7550-7df1-a59b-5a5a20d80ef6",
        aps_spec=aps_spec,
    )


def _spec(**flags):
    return ApsSlurmJobSpec(anomaly_detection_config=AnomalyDetectionConfig(**flags))


@pytest.mark.parametrize(
    "event_type,routing,flag_field",
    [
        (EventType.CRASH, "CRASH", "log_based_crash_enabled"),
        (EventType.HANGNOOUTPUT, "HANGNOOUTPUT", "log_based_timeout_enabled"),
    ],
)
@pytest.mark.parametrize(
    "build_job_info,enabled,expected_count",
    [
        (lambda spec: _job_status(spec), False, 0),
        (lambda spec: _job_status(spec), True, 1),
        (lambda _: _job_status(ApsSlurmJobSpec()), True, 1),
        (lambda _: _job_status(aps_spec=None), True, 1),
        (lambda _: None, True, 1),
    ],
)
def test_detection_check_per_event_type(
    rules_engine, mocker, caplog, event_type, routing, flag_field, build_job_info, enabled, expected_count
):
    rules_engine.publisher.publish_message = MagicMock()
    aps_spec = _spec(**{flag_field: enabled})
    mocker.patch(
        "watcher.workflow.get_job_attempts_sync",
        return_value=[status] if (status := build_job_info(aps_spec)) else None,
    )
    with caplog.at_level(logging.INFO):
        rules_engine.send_message_eventbus(events=[_event(event_type=event_type)], routing_suffix=routing)
    assert rules_engine.publisher.publish_message.call_count == expected_count
    if expected_count == 0:
        assert any("Dropping event" in m and "detection disabled" in m for m in caplog.messages)


def test_non_detection_event_skips_check(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    mock_get = mocker.patch("watcher.workflow.get_job_attempts_sync")
    rules_engine.send_message_eventbus(events=[_event(event_type=EventType.STRAGGLER)], routing_suffix="STRAGGLER")
    mock_get.assert_not_called()
    assert rules_engine.publisher.publish_message.call_count == 1


def test_non_detection_anomaly_with_attempt_adds_audit_without_job_manager_lookup(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    mock_get = mocker.patch("watcher.workflow.get_job_attempts_sync")

    rules_engine.send_message_eventbus(
        events=[_event(event_type=EventType.STRAGGLER, job_attempt_index="2")],
        routing_suffix="STRAGGLER",
    )

    mock_get.assert_not_called()
    audit_event = rules_engine.audit_event_buffer.add.call_args.args[0]
    assert audit_event.heimdall_job_id == str(generate_heimdall_job_id("12345", 2))
    payload = audit_event.model_dump(mode="json")["payload"]
    assert payload["job_id"] == "12345"
    assert payload["attempt_index"] == 2


def test_event_type_does_not_gate_audit_generation(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    mock_get = mocker.patch("watcher.workflow.get_job_attempts_sync")

    rules_engine.send_message_eventbus(
        events=[_event(event_type=EventType.CHECKPOINTSAVE, job_attempt_index="2")],
        routing_suffix="CHECKPOINTSAVE",
    )

    mock_get.assert_not_called()
    audit_event = rules_engine.audit_event_buffer.add.call_args.args[0]
    assert audit_event.payload.anomaly_type is EventType.CHECKPOINTSAVE


def test_non_detection_anomaly_without_attempt_skips_only_audit(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    mock_get = mocker.patch("watcher.workflow.get_job_attempts_sync")

    rules_engine.send_message_eventbus(
        events=[_event(event_type=EventType.STRAGGLER)],
        routing_suffix="STRAGGLER",
    )

    mock_get.assert_not_called()
    rules_engine.audit_event_buffer.add.assert_not_called()
    rules_engine.publisher.publish_message.assert_called_once()


def test_detection_check_caches_job_info_per_job_id(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    mock_get = mocker.patch(
        "watcher.workflow.get_job_attempts_sync",
        return_value=[_job_status(ApsSlurmJobSpec(), job_id="999")],
    )
    rules_engine.send_message_eventbus(events=[_event(job_id="999"), _event(job_id="999")], routing_suffix="CRASH")
    mock_get.assert_called_once()
    assert rules_engine.publisher.publish_message.call_count == 2


def test_audit_event_uses_unix_microseconds(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])
    mocker.patch("watcher.workflow.time.time_ns", return_value=1_700_000_000_123_456_789)

    event = _event(job_attempt_index=0)
    event.timestamp = "1699999999123456789"
    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH")

    audit_event = rules_engine.audit_event_buffer.add.call_args.args[0]
    assert audit_event.event_produced_at == 1_700_000_000_123_456
    payload = audit_event.model_dump(mode="json")["payload"]
    assert payload["job_id"] == "12345"
    assert payload["attempt_index"] == 0
    assert payload["event_occurred_at"] == 1_699_999_999_123_456


def test_audit_event_without_source_time_uses_detection_time(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])
    mocker.patch("watcher.workflow.time.time_ns", return_value=1_700_000_000_123_456_789)

    rules_engine.send_message_eventbus(events=[_event(job_attempt_index=0)], routing_suffix="CRASH")

    audit_event = rules_engine.audit_event_buffer.add.call_args.args[0]
    assert audit_event.payload.event_occurred_at == audit_event.event_produced_at


def test_replayed_anomaly_uses_same_audit_idempotency_key(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])

    event = _event(job_attempt_index=0)
    event.timestamp = "1699999999123456789"
    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH")
    first = rules_engine.audit_event_buffer.add.call_args.args[0]

    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH")
    second = rules_engine.audit_event_buffer.add.call_args.args[0]

    assert first.idempotency_key == second.idempotency_key


def test_timestamp_less_occurrences_remain_distinct(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync")
    workflow_item = WorkflowItem(
        record=LogRecord(
            body="checkpoint saved",
            attributes=Attributes(uuid="source-uuid", job_id="12345", slurm_job_attempt=0),
        )
    )

    first_event = rules_engine._get_event(workflow_item, EventType.CHECKPOINTSAVE)
    second_event = rules_engine._get_event(workflow_item, EventType.CHECKPOINTSAVE)
    rules_engine.send_message_eventbus(events=[first_event, second_event], routing_suffix="CHECKPOINTSAVE")

    first_audit, second_audit = (call.args[0] for call in rules_engine.audit_event_buffer.add.call_args_list)
    assert first_event.attributes.uuid != second_event.attributes.uuid
    assert first_audit.idempotency_key != second_audit.idempotency_key


def test_distinct_anomalies_use_different_audit_idempotency_keys(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])

    first_event = _event(job_attempt_index=0)
    first_event.attributes.uuid = "anomaly-1"
    second_event = _event(job_attempt_index=0)
    second_event.attributes.uuid = "anomaly-2"
    rules_engine.send_message_eventbus(events=[first_event, second_event], routing_suffix="CRASH")

    first, second = (call.args[0] for call in rules_engine.audit_event_buffer.add.call_args_list)
    assert first.idempotency_key != second.idempotency_key


def test_same_anomaly_in_different_workflow_steps_has_distinct_audit_keys(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])
    event = _event(job_attempt_index=0)

    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH", workflow_step_name="first_action")
    first = rules_engine.audit_event_buffer.add.call_args.args[0]
    rules_engine.send_message_eventbus(events=[event], routing_suffix="CRASH", workflow_step_name="second_action")
    second = rules_engine.audit_event_buffer.add.call_args.args[0]

    assert first.idempotency_key != second.idempotency_key


def test_multiple_checkpoint_saves_remain_distinct_while_replay_is_deduplicated(rules_engine, mocker):
    rules_engine.audit_event_buffer = MagicMock()
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[_job_status(ApsSlurmJobSpec())])

    first_checkpoint = _event(event_type=EventType.CHECKPOINTSAVE, body="checkpoint saved", job_attempt_index=0)
    first_checkpoint.timestamp = "1699999999123456789"
    second_checkpoint = _event(event_type=EventType.CHECKPOINTSAVE, body="checkpoint saved", job_attempt_index=0)
    second_checkpoint.timestamp = "1700000059123456789"

    rules_engine.send_message_eventbus(events=[first_checkpoint], routing_suffix="CHECKPOINTSAVE")
    first = rules_engine.audit_event_buffer.add.call_args.args[0]
    rules_engine.send_message_eventbus(events=[second_checkpoint], routing_suffix="CHECKPOINTSAVE")
    second = rules_engine.audit_event_buffer.add.call_args.args[0]
    rules_engine.send_message_eventbus(events=[first_checkpoint], routing_suffix="CHECKPOINTSAVE")
    first_replay = rules_engine.audit_event_buffer.add.call_args.args[0]

    assert first.payload.anomaly_uuid != second.payload.anomaly_uuid
    assert first.idempotency_key != second.idempotency_key
    assert first_replay.idempotency_key == first.idempotency_key


@pytest.mark.parametrize("event_type", [EventType.CRASH, EventType.HANGPUSHRECV, EventType.STRAGGLER])
def test_rapid_shared_log_retry_and_restart_replay(rules_engine, mocker, event_type):
    """One attempt cannot consume the next attempt's five-minute dedup slot."""
    now = datetime.now(UTC)
    status = _job_status(ApsSlurmJobSpec())
    status.start_time = now - timedelta(seconds=60)
    history = [status]
    mocker.patch("watcher.workflow.get_job_attempts_sync", side_effect=lambda **_: history)
    rules_engine.publisher.publish_message = MagicMock()
    old_event = _event(event_type=event_type, body="RuntimeError: failed")
    old_event.timestamp = str(int((now - timedelta(seconds=30)).timestamp() * 1e9))
    old_event.attributes.log_file_name = "shared.log"
    rules_engine.send_message_eventbus([old_event], routing_suffix=event_type.value, unique=True)
    first = json.loads(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    rules_engine.send_message_eventbus([old_event], routing_suffix=event_type.value, unique=True)
    assert rules_engine.publisher.publish_message.call_count == 1

    history.append(status.model_copy(deep=True))
    status.job_attempt_index = 1
    status.heimdall_job_id = None
    status.start_time = now - timedelta(seconds=10)
    new_event = old_event.model_copy(deep=True)
    new_event.timestamp = str(int(now.timestamp() * 1e9))
    rules_engine.send_message_eventbus([new_event], routing_suffix=event_type.value, unique=True)
    second = json.loads(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert rules_engine.publisher.publish_message.call_count == 2
    assert [first["attributes"]["slurm_job_attempt"], second["attributes"]["slurm_job_attempt"]] == [0, 1]
    assert first["attributes"]["uuid"] != second["attributes"]["uuid"]

    # Historical audit resolution must not add an attempt label to a delayed delivery.
    from watcher.lib.utils.data import ActionCache

    rules_engine.action_cache = ActionCache()
    rules_engine.send_message_eventbus([old_event], routing_suffix=event_type.value, unique=True)
    replay = json.loads(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert "slurm_job_attempt" not in replay["attributes"]
    assert replay["attributes"]["uuid"] == first["attributes"]["uuid"]


def test_unregistered_event_does_not_poison_retry_dedup(rules_engine, mocker):
    rules_engine.publisher.publish_message = MagicMock()
    now = datetime.now(UTC)
    status = _job_status()
    status.start_time = now - timedelta(seconds=10)
    mocker.patch("watcher.workflow.get_job_attempts_sync", side_effect=[None, [status]])
    event = _event()
    event.timestamp = str(int(now.timestamp() * 1e9))
    rules_engine.send_message_eventbus([event], routing_suffix="CRASH", unique=True)
    rules_engine.send_message_eventbus([event], routing_suffix="CRASH", unique=True)
    assert rules_engine.publisher.publish_message.call_count == 2
    messages = [json.loads(call.kwargs["message"]) for call in rules_engine.publisher.publish_message.call_args_list]
    assert messages[0]["attributes"]["uuid"] == messages[1]["attributes"]["uuid"]
    assert messages[1]["attributes"]["slurm_job_attempt"] == 0


@pytest.mark.parametrize("latest_enabled", [False, True])
@pytest.mark.parametrize("unique,dedup", [(True, False), (False, True), (True, True)])
def test_historical_audit_preserves_delivery_and_latest_detection_settings(
    rules_engine, mocker, latest_enabled, unique, dedup
):
    """Historical attribution must not suppress or label delayed deliveries."""
    older = _job_status(_spec(log_based_crash_enabled=not latest_enabled))
    older.start_time = datetime.fromtimestamp(100, UTC)
    older.end_time = datetime.fromtimestamp(200, UTC)
    newer = _job_status(_spec(log_based_crash_enabled=latest_enabled))
    newer.job_attempt_index = 1
    newer.start_time = datetime.fromtimestamp(300, UTC)
    history = mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[newer, older])
    rules_engine.audit_event_buffer = MagicMock()
    rules_engine.publisher.publish_message = MagicMock()
    event = _event()
    event.timestamp = "150000000000"

    rules_engine.send_message_eventbus(
        [event, event.model_copy(deep=True)], routing_suffix="CRASH", unique=unique, dedup=dedup
    )

    history.assert_called_once()
    assert rules_engine.publisher.publish_message.call_count == 2 * int(latest_enabled)
    assert rules_engine.audit_event_buffer.add.call_count == 2 * int(latest_enabled)
    if latest_enabled:
        audits = [call.args[0] for call in rules_engine.audit_event_buffer.add.call_args_list]
        assert all(audit.payload.attempt_index == 0 for audit in audits)
        assert all(audit.heimdall_job_id == str(generate_heimdall_job_id("12345", 0)) for audit in audits)
        assert audits[0].idempotency_key == audits[1].idempotency_key
        for call in rules_engine.publisher.publish_message.call_args_list:
            published = json.loads(call.kwargs["message"])
            assert "slurm_job_attempt" not in published["attributes"]
    assert "slurm_job_attempt" not in event.attributes.model_extra


@pytest.mark.parametrize("history_kind", ["failed", "empty", "outside", "overlap"])
def test_unresolved_audit_preserves_latest_attempt_delivery(rules_engine, mocker, history_kind):
    """Audit ambiguity cannot change existing latest-attempt delivery decisions."""
    older = _job_status()
    older.start_time = datetime.fromtimestamp(100, UTC)
    older.end_time = datetime.fromtimestamp(200, UTC)
    newer = older.model_copy(update={"job_attempt_index": 1})
    histories = {"failed": None, "empty": [], "outside": [older], "overlap": [newer, older]}
    mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=histories[history_kind])
    rules_engine.audit_event_buffer = MagicMock()
    rules_engine.publisher.publish_message = MagicMock()
    event = _event()
    event.timestamp = "250000000000" if history_kind == "outside" else "150000000000"

    rules_engine.send_message_eventbus(
        [event, event.model_copy(deep=True)], routing_suffix="CRASH", unique=True, dedup=True
    )

    assert rules_engine.publisher.publish_message.call_count == (1 if history_kind == "overlap" else 2)
    rules_engine.audit_event_buffer.add.assert_not_called()
    for call in rules_engine.publisher.publish_message.call_args_list:
        attributes = json.loads(call.kwargs["message"])["attributes"]
        if history_kind == "overlap":
            assert attributes["slurm_job_attempt"] == 1
        else:
            assert "slurm_job_attempt" not in attributes


def test_audit_identity_does_not_leak_between_jobs_in_batch(rules_engine, mocker):
    """Missing history for the next job must not reuse the previous job's identity."""
    registered_attempt = _job_status(_spec(log_based_crash_enabled=True))
    registered_attempt.start_time = datetime.fromtimestamp(100, UTC)
    registered_attempt.end_time = datetime.fromtimestamp(200, UTC)
    mocker.patch("watcher.workflow.get_job_attempts_sync", side_effect=[[registered_attempt], None])
    rules_engine.audit_event_buffer = MagicMock()
    rules_engine.publisher.publish_message = MagicMock()
    registered_event = _event(job_id="12345")
    registered_event.timestamp = "150000000000"
    unregistered_event = _event(job_id="999")
    unregistered_event.timestamp = "150000000000"

    rules_engine.send_message_eventbus([registered_event, unregistered_event], routing_suffix="CRASH")

    assert rules_engine.publisher.publish_message.call_count == 2
    rules_engine.audit_event_buffer.add.assert_called_once()
    audit = rules_engine.audit_event_buffer.add.call_args.args[0]
    assert audit.payload.job_id == "12345"
    assert audit.heimdall_job_id == str(generate_heimdall_job_id("12345", 0))
    second_message = json.loads(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert second_message["attributes"]["job_id"] == "999"
    assert "slurm_job_attempt" not in second_message["attributes"]


@pytest.mark.parametrize(
    "attempt_label,expected_attempt_index",
    [(0, 0), ("2", 2), ("abc", None), (-1, None), (True, None), (1.5, None)],
)
def test_only_valid_attempt_labels_bypass_history_lookup(rules_engine, mocker, attempt_label, expected_attempt_index):
    """Label presence alone cannot establish an identity or bypass the lookup."""
    status = _job_status()
    status.start_time = datetime.fromtimestamp(100, UTC)
    status.end_time = datetime.fromtimestamp(200, UTC)
    history = mocker.patch("watcher.workflow.get_job_attempts_sync", return_value=[status])
    rules_engine.audit_event_buffer = MagicMock()
    rules_engine.publisher.publish_message = MagicMock()
    event = _event(event_type=EventType.STRAGGLER, job_attempt_index=attempt_label)
    event.timestamp = "150000000000"

    rules_engine.send_message_eventbus(
        [event, event.model_copy(deep=True)], routing_suffix="STRAGGLER", unique=True, dedup=True
    )

    if expected_attempt_index is None:
        history.assert_called_once()
        rules_engine.audit_event_buffer.add.assert_not_called()
        assert rules_engine.publisher.publish_message.call_count == 2
        for call in rules_engine.publisher.publish_message.call_args_list:
            published = json.loads(call.kwargs["message"])
            assert published["attributes"]["slurm_job_attempt"] == attempt_label
    else:
        history.assert_not_called()
        rules_engine.audit_event_buffer.add.assert_called_once()
        audit = rules_engine.audit_event_buffer.add.call_args.args[0]
        assert audit.payload.attempt_index == expected_attempt_index
        rules_engine.publisher.publish_message.assert_called_once()
