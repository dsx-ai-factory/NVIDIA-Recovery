# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from queue import Queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from recovery_libs.audit import AuditEventBuffer
from recovery_libs.event_bus.event_bus_config import EventBusPayload
from watcher.models.config import RabbitMQSubscriberConfig
from watcher.models.workflow import LogRecord, WorkflowItem
from watcher.service import WatcherService


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    return MagicMock()


@pytest.fixture
def queue():
    """Create a queue for testing."""
    return Queue(maxsize=1024)


@pytest.fixture
def mock_publisher():
    """Create a mock publisher."""
    return MagicMock()


@pytest.fixture
def mock_subscriber():
    """Create a mock subscriber."""
    return MagicMock()


@pytest.fixture
def service(get_watcher_config, mock_engine, queue, mock_publisher, mock_subscriber):
    """Create a WatcherService instance for testing."""
    return WatcherService(
        config=get_watcher_config, engine=mock_engine, queue=queue, publisher=mock_publisher, subscriber=mock_subscriber
    )


def test_initialization(service, get_watcher_config, queue, mock_engine, mock_publisher, mock_subscriber):
    """Test that the service initializes correctly."""
    assert service.config == get_watcher_config
    assert service.queue == queue
    assert service.engine == mock_engine
    assert service.publisher == mock_publisher
    assert service.subscriber == mock_subscriber
    assert not service.interrupted
    assert service.loop is None
    assert service.last_maintenance_time == 0


def test_make_passes_audit_buffer_to_rules_engine(get_watcher_config):
    audit_event_buffer = MagicMock(spec=AuditEventBuffer)

    with (
        patch("watcher.service.EventBusListener"),
        patch("watcher.service.EventBusPublisher"),
        patch("watcher.service.RulesEngine") as rules_engine,
    ):
        WatcherService.make(get_watcher_config, audit_event_buffer=audit_event_buffer)

    assert rules_engine.call_args.kwargs["audit_event_buffer"] is audit_event_buffer


@pytest.mark.parametrize(
    "settings, expected_payload, expected_exchange_type, expected_timeout, expected_arguments",
    [
        ({}, EventBusPayload.JSON, "direct", 15, {}),
        ({"is_payload_protobuf": True}, EventBusPayload.PROTOBUF_LOGS, "direct", 15, {}),
        (
            {"exchange_type": "topic", "queue_full_timeout_sec": 0, "queue_max_size_mb": 2},
            EventBusPayload.JSON,
            "topic",
            0,
            {"x-max-length-bytes": 2 * 1024 * 1024, "x-overflow": "reject-publish"},
        ),
    ],
)
def test_rabbitmq_startup_and_message_delivery(
    get_watcher_config, settings, expected_payload, expected_exchange_type, expected_timeout, expected_arguments
):
    """Legacy input settings must initialize the real listener and enqueue deliveries."""
    subscriber = RabbitMQSubscriberConfig.from_json(
        {
            "host": "localhost",
            "username": "guest",
            "password": "guest",
            "vhost": "/",
            "exchange_name": "input",
            "queue_name": "input-queue",
            "routing_key": "telemetry",
            **settings,
        }
    )
    get_watcher_config.datasources.rabbitmq.subscriber = subscriber
    channel = MagicMock()
    with (
        patch("recovery_libs.event_bus.rabbit_eventing.connect", return_value=channel),
        patch("watcher.service.EventBusPublisher"),
    ):
        service = WatcherService.make(get_watcher_config)

    channel.exchange_declare.assert_called_once_with(
        exchange="input", exchange_type=expected_exchange_type, durable=True
    )
    channel.queue_declare.assert_called_once_with(queue="input-queue", durable=True, arguments=expected_arguments)
    channel.queue_bind.assert_called_once_with(queue="input-queue", exchange="input", routing_key="telemetry")
    assert service.subscriber.config.queue_full_timeout_sec == expected_timeout

    body = b"telemetry payload"
    service.subscriber.callback_func(MagicMock(routing_key="telemetry", delivery_tag=42), MagicMock(), body)

    assert service.queue.get_nowait() == ("telemetry", expected_payload, body)
    channel.basic_ack.assert_called_once_with(42)


@pytest.mark.asyncio
async def test_run_maintenance_tasks(service, mock_engine):
    """Test that maintenance tasks are executed correctly."""
    # Run maintenance tasks
    await service.run_maintenance_tasks()

    # Verify engine's run_maintenance_tasks was called
    mock_engine.run_maintenance_tasks.assert_called_once()


@pytest.mark.asyncio
async def test_process_workflow_item(service, mock_engine, get_watcher_config):
    """Test processing of a workflow item."""
    # Create test data
    loki_log = LogRecord(
        body="test message",
        timestamp="2024-01-01T00:00:00Z",
        attributes={"uuid": "123e4567-e89b-12d3-a456-426614174000", "job_id": "123456"},
    )
    item = WorkflowItem(record=loki_log, routing_suffix="test", step_name=None, step_value=None)
    # Get the first rule's workflow list
    workflows = [{"name": wf.name, "type": wf.type} for wf in get_watcher_config.rules[0].workflow]

    # Mock workflow execution
    mock_engine.execute_workflow = AsyncMock()

    # Process workflow item
    await service.process_workflow_item(item, workflows)

    # Verify engine's execute_workflow was called at least once
    mock_engine.execute_workflow.assert_called()
