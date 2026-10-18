# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from contextlib import asynccontextmanager

import jq
from sqlmodel import update
from taskiq import InMemoryBroker

from notifications.dependencies import HttpClientDep, SessionDep
from notifications.models.notifications import (
    NotificationOrm,
    NotificationPublic,
    NotificationStatus,
)
from notifications.prometheus.metrics import (
    NOTIFICATION_DELIVERY_FAILURES,
    NOTIFICATION_SEND_LATENCY,
    NOTIFICATION_SENT_COUNTER,
)
from notifications.utils.config import Config
from notifications.utils.logger import logger

broker = InMemoryBroker()


@asynccontextmanager
async def lifespan(app):
    """Handle the taskiq broker lifespan."""
    await broker.startup()
    try:
        yield
    finally:
        await broker.shutdown()


async def send_notifications(notification: NotificationPublic):
    """Send a notification via the configured channels."""
    logger.debug(f"Starting notification send for ID {notification.id}, recipient: {notification.recipient}")
    notification_json = notification.model_dump_json()

    if notification.via_callback == NotificationStatus.PENDING:
        logger.debug(f"Queuing callback notification for ID {notification.id}")
        await send_via_callback.kiq(notification_json)  # type: ignore[call-overload]


@broker.task()
async def send_via_callback(notification_json: str, session: SessionDep, http_client: HttpClientDep):
    notification = NotificationPublic.model_validate_json(notification_json)
    logger.debug(f"Processing callback notification for ID {notification.id}, recipient: {notification.recipient}")

    start_time = time.perf_counter_ns()
    new_status = NotificationStatus.FAILTOSEND
    error_reason = "unknown_error"

    try:
        method = Config.get("CALLBACK_METHOD") or "POST"
        jq_program = Config.get("CALLBACK_PAYLOAD_JQ") or "."
        callback_url = Config.get("CALLBACK_URL")
        if not callback_url:
            raise ValueError("CALLBACK_ENABLED is TRUE but CALLBACK_URL is not set")

        logger.debug(f"Preparing callback {method} request to {callback_url} for notification ID {notification.id}")
        logger.debug(f"Using JQ transform: {jq_program} for notification ID {notification.id}")

        payload = jq.compile(jq_program).input_value(notification.model_dump(mode="json")).first()
        logger.debug(f"Generated callback payload for notification ID {notification.id}")

        res = await http_client.request(method=method, url=callback_url, json=payload)
        logger.debug(f"Callback request completed with status {res.status_code} for notification ID {notification.id}")
        res.raise_for_status()
        new_status = NotificationStatus.SENT
        logger.debug(f"Successfully sent callback notification for ID {notification.id}")

        # Record successful delivery
        NOTIFICATION_SENT_COUNTER.labels(channel="callback", status="success").inc()
    except Exception as e:
        # Record delivery failure
        error_reason = type(e).__name__
        logger.debug(f"Failed to send callback notification for ID {notification.id}: {error_reason} - {e!s}")
        NOTIFICATION_DELIVERY_FAILURES.labels(channel="callback", reason=error_reason).inc()
        NOTIFICATION_SENT_COUNTER.labels(channel="callback", status="failure").inc()
        raise
    finally:
        # Record latency
        latency_ns = time.perf_counter_ns() - start_time
        latency_ms = latency_ns / 1_000_000
        logger.debug(f"Callback notification processing took {latency_ms:.2f}ms for ID {notification.id}")
        NOTIFICATION_SEND_LATENCY.labels(channel="callback").observe(latency_ns)

        logger.debug(f"Updating callback status to {new_status} for notification ID {notification.id}")
        session.execute(
            update(NotificationOrm)
            .where(NotificationOrm.id == notification.id)  # type: ignore[arg-type]
            .values(via_callback=new_status)
        )
        session.commit()
        logger.debug(f"Callback database update completed for notification ID {notification.id}")
