# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from watcher.lib.plugins.rabbitmq import EventBusError, EventBusPublisher


@pytest.fixture
def publisher():
    return EventBusPublisher(
        hostname="localhost",
        port=5672,
        username="guest",
        password="guest",
        vhost="/",
        exchange="test_exchange",
        queues=["anomaly_detected_job_manager"],
        routing_keys={
            "anomaly_detected": [
                "anomaly.detected.CHECKPOINTLOAD",
                "anomaly.detected.CHECKPOINTSAVE",
                "anomaly.detected.HANG",
            ],
        },
    )


def test_declare_queue(publisher):
    publisher.channel = MagicMock()
    publisher.declare_queue("test_queue")
    publisher.channel.queue_declare.assert_called_once_with(queue="test_queue", durable=True, arguments={})


def test_declare_exchange(publisher):
    publisher.channel = MagicMock()
    publisher.declare_exchange()
    publisher.channel.exchange_declare.assert_called_once_with(
        exchange="test_exchange", exchange_type="direct", durable=True
    )


@patch("watcher.lib.plugins.rabbitmq.validate_json")
def test_publish_message_invalid_json(validate_json_mock, publisher):
    validate_json_mock.return_value = False
    with pytest.raises(EventBusError) as excinfo:
        publisher.publish_message("test_routing_key", "invalid json")
    assert str(excinfo.value) == "Invalid JSON message body"


def test_setup_all_queues(mocker, publisher):
    # Mock the declare_queue and bind_queue methods
    mocker.patch.object(publisher, "declare_queue")
    mocker.patch.object(publisher, "bind_queue")

    # Call the method to test
    publisher.setup_all_queues()

    # Check that declare_queue was called for each queue
    assert publisher.declare_queue.call_count == len(publisher.queues)
    for queue in publisher.queues:
        publisher.declare_queue.assert_any_call(queue=queue)

    # Check that bind_queue was called for each combination of queue and routing key
    tot_expected_calls = 0
    for routing_prefix, routing_keys in publisher.routing_keys.items():
        for queue in publisher.queues:
            if queue.startswith(routing_prefix):
                tot_expected_calls += len(routing_keys)
                for routing_key in routing_keys:
                    publisher.bind_queue.assert_any_call(queue=queue, routing_key=routing_key)
    assert publisher.bind_queue.call_count == tot_expected_calls
