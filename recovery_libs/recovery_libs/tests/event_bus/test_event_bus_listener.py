# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from queue import Queue
from unittest.mock import MagicMock, patch

import pytest
from recovery_libs.event_bus.event_bus_config import EventBusConfig, EventBusPayload
from recovery_libs.event_bus.event_bus_listener import EventBusListener

_ROUTING_KEY = "test-routing-key"


@pytest.fixture
def mock_errors_counter():
    return MagicMock()


@pytest.fixture
def mock_routing_sub_keys():
    return ["routing-1", "routing-2"]


@pytest.fixture
def mock_messages() -> list[str]:
    return [
        "{test: 'test-1'}",
        "{test: 'test-2'}",
    ]


@pytest.fixture
def mock_channel(mock_messages):
    channel = MagicMock()
    channel.is_closed = False
    method = MagicMock()
    method.routing_key = _ROUTING_KEY
    properties = MagicMock()
    channel.consume.return_value = [(method, properties, m.encode("utf-8")) for m in mock_messages] + [
        (None, None, None)
    ]
    return channel


@pytest.fixture
def event_bus_config() -> EventBusConfig:
    return EventBusConfig(
        host="test-host",
        username="test-user",
        password="test-pass",
        vhost="/",
        exchange_name="test-exchange",
        exchange_type="direct",
        queue_name="test-queue",
        routing_key="test-routing-key",
        queue_full_timeout_sec=10,
        queue_max_size_mb=2048,
        payload=EventBusPayload.JSON,
    )


@patch("recovery_libs.event_bus.event_bus_listener.rabbitmq")
def test_listener(
    mock_rabbitmq, event_bus_config, mock_channel, mock_messages, mock_errors_counter, mock_routing_sub_keys
):
    """Test that EventBusListener initializes correctly with provided config and mocks, and consumes all messages passed in."""
    queue = Queue()
    mock_rabbitmq.connect.return_value = mock_channel

    listener = EventBusListener(
        config=event_bus_config,
        queue=queue,
        routing_sub_keys=mock_routing_sub_keys,
        errors_counter=mock_errors_counter,
    )

    assert listener.client_queue == queue
    assert listener.errors_counter == mock_errors_counter
    assert listener.config == event_bus_config
    assert listener.routing_keys == [event_bus_config.routing_key + sub_key for sub_key in mock_routing_sub_keys]
    assert listener.channel == mock_rabbitmq.connect.return_value

    assert mock_rabbitmq.declare_exchange.call_count == 1
    assert mock_rabbitmq.declare_queue.call_count == 1
    assert mock_rabbitmq.bind_queue.call_count == len(mock_routing_sub_keys)

    listener.start_consuming()
    assert listener.is_healthy()

    for m in mock_messages:
        routing_key, payload_type, data = queue.get(timeout=1)
        json = data.decode()
        print(f"routing_key: {routing_key}, payload_type: {payload_type}, json: {json}")
        assert routing_key == _ROUTING_KEY
        assert payload_type == EventBusPayload.JSON
        assert json == m

    listener.stop_consuming()
    assert listener.errors_counter.inc.call_count == 0
