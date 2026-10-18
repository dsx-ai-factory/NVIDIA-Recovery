# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import random
import threading
from collections.abc import Sequence
from http import HTTPStatus
from typing import Any

import requests

from recovery_libs.audit.buffered import DeliveryError
from recovery_libs.audit.events import ProducerEvent

logger = logging.getLogger(__name__)

_MAX_DELIVERY_ATTEMPTS = 5
_INITIAL_RETRY_DELAY_SECONDS = 0.1
_MAX_RETRY_DELAY_SECONDS = 2.0


class _RetryableDeliveryError(DeliveryError):
    """Mark a delivery failure that may succeed when the batch is replayed."""


class LoggingSender:
    """Write audit event metadata to an application logger."""

    _logger: logging.Logger

    def __init__(self, event_logger: logging.Logger) -> None:
        self._logger = event_logger

    def send(self, events: Sequence[ProducerEvent], stop_event: threading.Event) -> int:
        del stop_event  # Logging is synchronous and fast; no backoff to interrupt.
        for event in events:
            self._logger.info(
                "sent audit event",
                extra={
                    "heimdall_job_id": event.heimdall_job_id,
                    "event_producer": event.event_producer,
                    "event_type": event.event_type,
                    "event_level": event.event_level,
                    "event_produced_at": event.event_produced_at,
                    "idempotency_key": event.idempotency_key,
                },
            )

        return len(events)


class NotificationSender:
    """Send audit event batches to the Notifications ingestion endpoint."""

    _endpoint: str
    _session: requests.Session
    _timeout_seconds: float

    def __init__(
        self,
        endpoint: str,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 10,
    ) -> None:
        self._endpoint = endpoint
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds

    def send(self, events: Sequence[ProducerEvent], stop_event: threading.Event) -> int:
        """Deliver a batch, retrying transient failures.

        Retry state belongs to this invocation so later batches always begin
        with the initial backoff. stop_event lets a caller's stop deadline
        interrupt a queued retry immediately instead of sleeping it out.
        """
        payload = {"items": [event.model_dump(mode="json") for event in events]}
        event_count = len(events)

        for attempt in range(_MAX_DELIVERY_ATTEMPTS):
            try:
                self._deliver_once(payload, event_count)
            except DeliveryError as error:
                if not isinstance(error, _RetryableDeliveryError) or attempt + 1 >= _MAX_DELIVERY_ATTEMPTS:
                    raise

                if stop_event.wait(_retry_delay(attempt)):
                    raise DeliveryError(
                        "audit buffer stop deadline exceeded during retry backoff",
                        delivered_count=0,
                    ) from None
                continue

            logger.info("sent audit event batch", extra={"event_count": event_count})
            return event_count

        raise AssertionError("delivery retry loop exhausted without returning")

    def _deliver_once(self, payload: dict[str, Any], event_count: int) -> None:
        try:
            response = self._session.post(
                self._endpoint,
                json=payload,
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as error:
            raise _RetryableDeliveryError("send audit event batch", delivered_count=0) from error

        if response.status_code != HTTPStatus.ACCEPTED:
            error_type = _RetryableDeliveryError if _is_retryable_status(response.status_code) else DeliveryError
            raise error_type(
                f"audit ingestion returned HTTP status {response.status_code}",
                delivered_count=0,
            )

        try:
            acceptance = response.json()
        except ValueError as error:
            raise DeliveryError(
                "audit ingestion returned malformed acceptance response",
                delivered_count=0,
            ) from error

        if not isinstance(acceptance, dict):
            raise DeliveryError(
                "audit ingestion returned invalid acceptance response",
                delivered_count=0,
            )

        partial_success = acceptance.get("partial_success")
        if partial_success is None:
            return
        if not isinstance(partial_success, dict):
            raise DeliveryError(
                "audit ingestion returned invalid partial acceptance response",
                delivered_count=0,
            )

        rejected_count = partial_success.get("rejected_events")
        if (
            isinstance(rejected_count, bool)
            or not isinstance(rejected_count, int)
            or rejected_count <= 0
            or rejected_count > event_count
        ):
            raise DeliveryError(
                "audit ingestion returned invalid rejected event count",
                delivered_count=0,
            )

        raise DeliveryError(
            f"audit ingestion rejected {rejected_count} of {event_count} events",
            delivered_count=event_count - rejected_count,
        )


def _is_retryable_status(status_code: int) -> bool:
    return (
        status_code
        in {
            HTTPStatus.REQUEST_TIMEOUT,
            HTTPStatus.TOO_EARLY,
            HTTPStatus.TOO_MANY_REQUESTS,
        }
        or HTTPStatus.INTERNAL_SERVER_ERROR <= status_code <= 599
    )


def _retry_delay(attempt: int) -> float:
    delay = min(_INITIAL_RETRY_DELAY_SECONDS * 2**attempt, _MAX_RETRY_DELAY_SECONDS)
    return _random_delay_up_to(delay)


def _random_delay_up_to(max_delay_seconds: float) -> float:
    """Spread retries uniformly between zero and the calculated delay cap."""
    return random.uniform(0, max_delay_seconds)
