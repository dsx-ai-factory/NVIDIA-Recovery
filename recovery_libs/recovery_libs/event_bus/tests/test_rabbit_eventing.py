# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock, call, patch

import pika
from recovery_libs.event_bus import rabbit_eventing as re


class TestRabbitEventing(unittest.TestCase):
    def test_bounded_queue_rejects_new_publishes(self):
        """Bounded queues must request rejection rather than dropping older messages."""
        channel = MagicMock()

        re.declare_queue(channel, "bounded", queue_max_size_mb=2)

        channel.queue_declare.assert_called_once_with(
            queue="bounded",
            durable=True,
            arguments={"x-max-length-bytes": 2 * 1024 * 1024, "x-overflow": "reject-publish"},
        )

    def test_unlimited_queue_has_no_limit_arguments(self):
        """Default declarations remain compatible with existing unlimited queues."""
        channel = MagicMock()

        re.declare_queue(channel, "unlimited")

        channel.queue_declare.assert_called_once_with(queue="unlimited", durable=True, arguments={})

    def test_validate_json_valid(self):
        message_body = '{"key": "value"}'
        self.assertTrue(re.validate_json(message_body))

    def test_validate_json_invalid(self):
        message_body = "not-json"
        self.assertFalse(re.validate_json(message_body))

    @patch("pika.BlockingConnection")
    def test_connect_success(self, mock_connection):
        mock_connection.return_value.channel.return_value = MagicMock()  # Mock the channel
        re.connect()
        mock_connection.assert_called_once()  # Ensure BlockingConnection was called
        mock_connection.return_value.channel.assert_called_once()

    @patch("pika.BlockingConnection")
    def test_connect_failure(self, mock_connection):
        mock_connection.side_effect = pika.exceptions.AMQPConnectionError("Test Error")
        with self.assertRaises(re.EventBusError) as cm:
            re.connect()

        self.assertEqual(cm.exception.error_type, "ConnectionError")

    @patch("pika.BlockingConnection")
    def test_publish_message_valid_json(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value = mock_channel
        re.publish_message(mock_channel, "exchange", "routing_key", '{"key": "value"}')
        mock_channel.basic_publish.assert_called_once()

    @patch("pika.BlockingConnection")
    def test_publish_message_invalid_json(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value = mock_channel

        with self.assertRaises(re.EventBusError) as cm:
            re.publish_message(mock_channel, "exchange", "routing_key", "not-json")

        self.assertEqual(cm.exception.error_type, "InvalidJSON")

    @patch("pika.BlockingConnection")
    def test_publish_message_with_response_success(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value = mock_channel

        corr_id = re.publish_message_with_response(
            mock_channel, "exchange", "routing_key", '{"key": "value"}', "response_queue"
        )

        mock_channel.basic_publish.assert_called_once()
        self.assertIsNotNone(corr_id)

    @patch("pika.BlockingConnection")
    def test_publish_message_with_response_invalid_json(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value = mock_channel

        with self.assertRaises(re.EventBusError) as cm:
            re.publish_message_with_response(mock_channel, "exchange", "routing_key", "not-json", "response_queue")

        self.assertEqual(cm.exception.error_type, "InvalidJSON")

    @patch("pika.BlockingConnection")
    def test_consume_messages_multiple_queues(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value = mock_channel

        def callback1(ch, method, properties, body):
            self.assertEqual(body, b"Message for queue1")

        def callback2(ch, method, properties, body):
            self.assertEqual(body, b"Message for queue2")

        queue_callbacks = {"queue1": callback1, "queue2": callback2}

        # Mock the `start_consuming` method to prevent blocking
        mock_channel.start_consuming = MagicMock()

        re.consume_multiple_messages(mock_channel, queue_callbacks)

        # Assertions
        mock_channel.basic_consume.assert_has_calls(
            [
                call(queue="queue1", on_message_callback=callback1, auto_ack=True),
                call(queue="queue2", on_message_callback=callback2, auto_ack=True),
            ],
            any_order=True,
        )  # Allow calls in any order

        mock_channel.start_consuming.assert_called_once()


if __name__ == "__main__":
    unittest.main()
