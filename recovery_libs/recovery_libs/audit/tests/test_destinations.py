# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import threading
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from recovery_libs.audit import AuditEventLevel, DeliveryError, LoggingSender, NotificationSender, ProducerEvent

_DEFAULT_RESPONSE_BODY = object()


def _event(idempotency_key: str = "event-1") -> ProducerEvent:
    return ProducerEvent(
        heimdall_job_id="job-1",
        event_producer="job-manager",
        event_type="job-requeued",
        event_level=AuditEventLevel.OPERATIONAL,
        payload={"reason": "recovery"},
        event_produced_at=1_700_000_000_123_456,
        idempotency_key=idempotency_key,
    )


def _response(status_code: int = 202, body: object = _DEFAULT_RESPONSE_BODY) -> MagicMock:
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = {} if body is _DEFAULT_RESPONSE_BODY else body
    return response


def _never_stopping_event() -> MagicMock:
    """A stop_event double whose wait() reports the deadline never fired."""
    event = MagicMock(spec=threading.Event)
    event.wait.return_value = False
    return event


def test_logging_sender_logs_event_metadata(caplog: pytest.LogCaptureFixture) -> None:
    event_logger = logging.getLogger("test.audit.destination")
    sender = LoggingSender(event_logger)

    with caplog.at_level(logging.INFO, logger=event_logger.name):
        delivered_count = sender.send([_event()], threading.Event())

    assert delivered_count == 1
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == "sent audit event"
    assert record.heimdall_job_id == "job-1"
    assert record.event_producer == "job-manager"
    assert record.event_type == "job-requeued"
    assert record.event_level is AuditEventLevel.OPERATIONAL
    assert record.event_produced_at == 1_700_000_000_123_456
    assert record.idempotency_key == "event-1"
    assert not hasattr(record, "payload")


def test_notification_sender_posts_batch(caplog: pytest.LogCaptureFixture) -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response()
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )
    event = _event()
    assert event.model_dump(mode="json")["event_level"] == "operational"

    with caplog.at_level(logging.INFO, logger="recovery_libs.audit.destinations"):
        delivered_count = sender.send([event], threading.Event())

    assert delivered_count == 1
    session.post.assert_called_once_with(
        "http://heimdall-notifications:8080/v1/audit/events",
        json={"items": [event.model_dump(mode="json")]},
        timeout=10,
    )
    record = next(record for record in caplog.records if record.getMessage() == "sent audit event batch")
    assert record.event_count == 1


def test_notification_sender_retries_with_exponential_randomized_delay() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.side_effect = [_response(429), _response(503), _response()]
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    stop_event = _never_stopping_event()
    with patch(
        "recovery_libs.audit.destinations._random_delay_up_to",
        side_effect=lambda delay: delay / 2,
    ) as random_delay:
        delivered_count = sender.send([_event()], stop_event)

    assert delivered_count == 1
    assert session.post.call_count == 3
    assert random_delay.call_args_list == [call(0.1), call(0.2)]
    assert stop_event.wait.call_args_list == [call(0.05), call(0.1)]


def test_notification_sender_stops_after_five_attempts() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response(503)
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    stop_event = _never_stopping_event()
    with (
        patch("recovery_libs.audit.destinations._random_delay_up_to", return_value=0) as random_delay,
        pytest.raises(DeliveryError, match="HTTP status 503") as error,
    ):
        sender.send([_event()], stop_event)

    assert error.value.delivered_count == 0
    assert session.post.call_count == 5
    assert random_delay.call_args_list == [call(0.1), call(0.2), call(0.4), call(0.8)]
    assert stop_event.wait.call_count == 4


def test_notification_sender_interrupts_backoff_when_stop_event_fires() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response(503)
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )
    stop_event = MagicMock(spec=threading.Event)
    stop_event.wait.return_value = True

    with (
        patch("recovery_libs.audit.destinations._random_delay_up_to", return_value=0),
        pytest.raises(DeliveryError, match="stop deadline exceeded during retry backoff") as error,
    ):
        sender.send([_event()], stop_event)

    assert error.value.delivered_count == 0
    assert session.post.call_count == 1
    stop_event.wait.assert_called_once()


def test_notification_sender_does_not_retry_partial_acceptance() -> None:
    events = [_event("event-1"), _event("event-2")]
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response(body={"partial_success": {"rejected_events": 1}})
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    with pytest.raises(DeliveryError, match="rejected 1 of 2 events") as error:
        sender.send(events, threading.Event())

    assert error.value.delivered_count == 1
    expected_payload = {"items": [event.model_dump(mode="json") for event in events]}
    session.post.assert_called_once_with(
        "http://heimdall-notifications:8080/v1/audit/events",
        json=expected_payload,
        timeout=10,
    )


def test_notification_sender_resets_retry_delay_for_each_send() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.side_effect = [_response(503), _response(), _response(503), _response()]
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    stop_event = _never_stopping_event()
    with patch(
        "recovery_libs.audit.destinations._random_delay_up_to",
        side_effect=lambda delay: delay,
    ) as random_delay:
        sender.send([_event("event-1")], stop_event)
        sender.send([_event("event-2")], stop_event)

    assert random_delay.call_args_list == [call(0.1), call(0.1)]
    assert stop_event.wait.call_args_list == [call(0.1), call(0.1)]


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([], "invalid acceptance response"),
        ({"partial_success": []}, "invalid partial acceptance response"),
        ({"partial_success": {}}, "invalid rejected event count"),
        ({"partial_success": {"rejected_events": True}}, "invalid rejected event count"),
        ({"partial_success": {"rejected_events": 0}}, "invalid rejected event count"),
        ({"partial_success": {"rejected_events": 2}}, "invalid rejected event count"),
        ({"partial_success": {"rejected_events": "1"}}, "invalid rejected event count"),
    ],
)
def test_notification_sender_rejects_invalid_acceptance_response(body: object, message: str) -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response(body=body)
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    with pytest.raises(DeliveryError, match=message) as error:
        sender.send([_event()], threading.Event())

    assert error.value.delivered_count == 0
    session.post.assert_called_once()


def test_notification_sender_rejects_malformed_acceptance_response() -> None:
    response = _response()
    response.json.side_effect = ValueError("invalid JSON")
    session = MagicMock(spec=requests.Session)
    session.post.return_value = response
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    with pytest.raises(DeliveryError, match="malformed acceptance response") as error:
        sender.send([_event()], threading.Event())

    assert error.value.delivered_count == 0
    session.post.assert_called_once()


def test_notification_sender_does_not_retry_permanent_failure() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.return_value = _response(400)
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    with pytest.raises(DeliveryError, match="HTTP status 400") as error:
        sender.send([_event()], threading.Event())

    assert error.value.delivered_count == 0
    session.post.assert_called_once()


def test_notification_sender_reports_request_failure() -> None:
    session = MagicMock(spec=requests.Session)
    session.post.side_effect = [requests.ConnectionError("connection refused"), _response()]
    sender = NotificationSender(
        "http://heimdall-notifications:8080/v1/audit/events",
        session=session,
    )

    with patch("recovery_libs.audit.destinations._random_delay_up_to", return_value=0):
        delivered_count = sender.send([_event()], _never_stopping_event())

    assert delivered_count == 1
    assert session.post.call_count == 2
