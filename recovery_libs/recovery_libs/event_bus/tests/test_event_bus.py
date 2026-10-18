# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from threading import Event
from unittest.mock import MagicMock, patch

from recovery_libs.event_bus.event_bus_utils import (
    RabbitMQConsumer,
    RabbitMQConsumerConfig,
    RabbitMQPublisher,
    RabbitMQPublisherConfig,
    RabbitMQQueueConfig,
    get_dead_letter_timer_name,
)


class TestRabbitMQConsumer(unittest.TestCase):
    def setUp(self):
        self.message_callback = MagicMock()
        self.consumer_config = RabbitMQConsumerConfig(
            host="test_host",
            username="test_user",
            password="test_password",
            vhost="test_vhost",
            exchange_name="test_exchange",
            exchange_type="test_exchange_type",
            queue_name="test_queue",
            routing_keys=["test_routing_key"],
            callback_func=self.message_callback,
            thread_count=1,
            delay_secs=10,
            dead_letter_routing_key="test_dead_letter_routing_key",
            dead_letter_redelivery_routing_key="test_dead_letter_redelivery_routing_key",
        )

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_consumer_creation(self, counter_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock
        channel_mock.basic_consume.return_value = "test_consumer_tag"
        consumer = RabbitMQConsumer(
            self.consumer_config, Event(), unexpected_error_counter_mock, retryable_error_counter_mock
        )
        self.assertEqual(len(consumer.consumer_thread_trackings), 1)
        self.assertEqual(consumer.consumer_thread_trackings[0].consumer_tag, "test_consumer_tag")

        rabbitmq_mock.connect.assert_called_once()
        rabbitmq_mock.declare_exchange.assert_called_once()
        rabbitmq_mock.declare_queue.assert_called_once()
        self.assertEqual(rabbitmq_mock.bind_queue.call_count, 3)

        # Evaluate the arguments of the bind_queue calls.
        self.assertEqual(rabbitmq_mock.bind_queue.call_args_list[0][1]["queue_name"], self.consumer_config.queue_name)
        self.assertEqual(
            rabbitmq_mock.bind_queue.call_args_list[0][1]["routing_key"], self.consumer_config.routing_keys[0]
        )
        self.assertEqual(
            rabbitmq_mock.bind_queue.call_args_list[1][1]["queue_name"],
            get_dead_letter_timer_name("test_queue", self.consumer_config.delay_secs),
        )
        self.assertEqual(
            rabbitmq_mock.bind_queue.call_args_list[1][1]["routing_key"], self.consumer_config.dead_letter_routing_key
        )
        self.assertEqual(rabbitmq_mock.bind_queue.call_args_list[2][1]["queue_name"], self.consumer_config.queue_name)
        self.assertEqual(
            rabbitmq_mock.bind_queue.call_args_list[2][1]["routing_key"],
            self.consumer_config.dead_letter_redelivery_routing_key,
        )
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_start_consuming(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock
        consumer = RabbitMQConsumer(
            self.consumer_config, Event(), retryable_error_counter_mock, unexpected_error_counter_mock
        )
        consumer.start_consuming()

        self.assertEqual(thread_mock.call_count, 1)
        thread_mock.return_value.start.assert_called_once()
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_stop_consuming(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock
        consumer = RabbitMQConsumer(
            self.consumer_config, Event(), retryable_error_counter_mock, unexpected_error_counter_mock
        )
        consumer.start_consuming()
        consumer.stop_consuming()

        self.assertEqual(thread_mock.call_count, 1)
        thread_mock.return_value.start.assert_called_once()
        channel_mock.basic_cancel.assert_called_once()
        channel_mock.stop_consuming.assert_called_once()
        thread_mock.return_value.join.assert_called_once()
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_consumer_healthy(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock
        consumer = RabbitMQConsumer(
            self.consumer_config, Event(), retryable_error_counter_mock, unexpected_error_counter_mock
        )
        consumer.start_consuming()
        thread_mock.is_alive.return_value = True
        self.assertTrue(consumer.is_healthy())
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_consume_messages(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        channel_mock.is_closed = False
        # Set the event to break the 'while True' loop in the consume_messages method.
        event = Event()
        consumer = RabbitMQConsumer(
            self.consumer_config, event, retryable_error_counter_mock, unexpected_error_counter_mock
        )
        event.set()

        consumer.consume_messages(channel=channel_mock, thread_index=0)
        channel_mock.start_consuming.assert_called_once()
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_consume_messages_with_closed_connection(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = counter_mock.return_value
        unexpected_error_counter_mock = counter_mock.return_value
        channel_mock = MagicMock()
        channel_mock.is_closed = True
        channel_mock.basic_consume.return_value = "updated_test_consumer_tag"

        rabbitmq_mock.connect.return_value = channel_mock
        # Set the event to break the 'while True' loop in the consume_messages method.
        event = Event()
        consumer = RabbitMQConsumer(
            self.consumer_config, event, retryable_error_counter_mock, unexpected_error_counter_mock
        )
        event.set()

        consumer.consume_messages(channel=channel_mock, thread_index=0)
        # One made from the constructor and one from the consume_messages in the reconnection block.
        self.assertEqual(channel_mock.basic_consume.call_count, 2)
        consumer.consumer_thread_trackings[0].consumer_tag = "updated_test_consumer_tag"
        consumer.consumer_thread_trackings[0].channel = channel_mock
        channel_mock.start_consuming.assert_called_once()
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    @patch("recovery_libs.event_bus.event_bus_utils.Thread", autospec=True)
    @patch("prometheus_client.Counter", autospec=True)
    def test_consume_messages_with_exception(self, counter_mock, thread_mock, rabbitmq_mock):
        retryable_error_counter_mock = MagicMock()
        unexpected_error_counter_mock = MagicMock()
        channel_mock = MagicMock()
        channel_mock.is_closed = False
        channel_mock.start_consuming.side_effect = Exception("Test exception")
        # Set the event to break the 'while True' loop in the consume_messages method.
        event = Event()
        consumer = RabbitMQConsumer(
            self.consumer_config, event, retryable_error_counter_mock, unexpected_error_counter_mock
        )
        event.set()

        consumer.consume_messages(channel=channel_mock, thread_index=0)
        channel_mock.start_consuming.assert_called_once()
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_called_once_with(1)

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_get_queue_length(self, rabbitmq_mock):
        retryable_error_counter_mock = MagicMock()
        unexpected_error_counter_mock = MagicMock()
        rabbitmq_mock.connect.return_value = MagicMock()
        queue_mock = MagicMock()
        rabbitmq_mock.declare_queue.return_value = queue_mock
        queue_mock.method.message_count = 10

        consumer = RabbitMQConsumer(
            self.consumer_config, Event(), unexpected_error_counter_mock, retryable_error_counter_mock
        )

        self.assertEqual(10, consumer.get_queue_length())
        retryable_error_counter_mock.inc.assert_not_called()
        unexpected_error_counter_mock.inc.assert_not_called()


class TestRabbitMQPublisher(unittest.TestCase):
    def setUp(self):
        self.config = RabbitMQPublisherConfig(
            host="test_host",
            username="test_user",
            password="test_password",
            vhost="test_vhost",
            exchange_name="test_exchange",
            exchange_type="direct",
            queue_routing_key_mapping={
                "test_queue": RabbitMQQueueConfig(
                    queue_name="test_queue", routing_keys=["test_routing_key_1", "test_routing_key_2"]
                )
            },
        )

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_publisher_creation(self, rabbitmq_mock):
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        publisher = RabbitMQPublisher(publisher_config=self.config)

        rabbitmq_mock.connect.assert_called_once()
        rabbitmq_mock.declare_exchange.assert_called_once_with(
            channel=channel_mock,
            exchange_name=self.config.exchange_name,
            exchange_type=self.config.exchange_type,
        )
        rabbitmq_mock.declare_queue.assert_called_once_with(
            channel=channel_mock,
            queue_name="test_queue",
        )
        self.assertEqual(rabbitmq_mock.bind_queue.call_count, 2)
        self.assertEqual(publisher.channel, channel_mock)

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_message_publish(self, rabbitmq_mock):
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        publisher = RabbitMQPublisher(publisher_config=self.config)

        publisher.publish_message(message="test_message", routing_key="test_routing_key_1")

        rabbitmq_mock.publish_message.assert_called_once_with(
            channel=channel_mock,
            exchange_name=self.config.exchange_name,
            routing_key="test_routing_key_1",
            message_body="test_message",
            properties=None,
        )

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_message_publish_with_reconnection(self, rabbitmq_mock):
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        publisher = RabbitMQPublisher(publisher_config=self.config)
        publisher.channel.is_closed = True

        publisher.publish_message(message="test_message", routing_key="test_routing_key_1")

        rabbitmq_mock.publish_message.assert_called_once_with(
            channel=channel_mock,
            exchange_name=self.config.exchange_name,
            routing_key="test_routing_key_1",
            message_body="test_message",
            properties=None,
        )
        self.assertEqual(rabbitmq_mock.connect.call_count, 2)

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_message_publish_with_delay(self, rabbitmq_mock):
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        # Set the delay_secs for the test_queue to trigger the other code path.
        self.config.queue_routing_key_mapping["test_queue"].delay_secs = 10
        self.config.queue_routing_key_mapping["test_queue"].dead_letter_redelivery_routing_key = "test_routing_key_1"

        publisher = RabbitMQPublisher(publisher_config=self.config)
        publisher.publish_message(message="test_message", routing_key="test_routing_key_1", delay_secs=10)

        channel_mock.queue_declare.assert_called_once_with(
            queue=get_dead_letter_timer_name("test_queue", 10),
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.config.exchange_name,
                "x-dead-letter-routing-key": self.config.queue_routing_key_mapping[
                    "test_queue"
                ].dead_letter_redelivery_routing_key,
                "x-message-ttl": 10000,
            },
        )

        self.assertEqual(rabbitmq_mock.bind_queue.call_count, 3)
        kwargs = rabbitmq_mock.bind_queue.call_args_list[0].kwargs
        self.assertEqual(
            kwargs,
            {
                "queue_name": get_dead_letter_timer_name("test_queue", 10),
                "exchange_name": self.config.exchange_name,
                "channel": channel_mock,
                "routing_key": get_dead_letter_timer_name("test_routing_key_1", 10),
            },
        )

        rabbitmq_mock.publish_message.assert_called_once_with(
            channel=channel_mock,
            exchange_name=self.config.exchange_name,
            routing_key=get_dead_letter_timer_name("test_routing_key_1", 10),
            message_body="test_message",
            properties=None,
        )

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_publisher_heartbeat_thread_created(self, rabbitmq_mock):
        """Test that RabbitMQPublisher creates and starts a heartbeat thread"""
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        publisher = RabbitMQPublisher(publisher_config=self.config)

        # Verify heartbeat thread was created and started
        self.assertIsNotNone(publisher._heartbeat_thread)
        self.assertTrue(publisher._heartbeat_thread.is_alive())
        self.assertTrue(publisher._heartbeat_thread.daemon)
        self.assertIn("RabbitMQPublisher-Heartbeat", publisher._heartbeat_thread.name)

        # Cleanup
        publisher.cleanup()

    @patch("recovery_libs.event_bus.event_bus_utils.rabbitmq", autospec=True)
    def test_publisher_cleanup_stops_heartbeat(self, rabbitmq_mock):
        """Test that cleanup() properly stops the heartbeat thread"""
        channel_mock = MagicMock()
        rabbitmq_mock.connect.return_value = channel_mock

        publisher = RabbitMQPublisher(publisher_config=self.config)

        # Verify thread is running
        self.assertTrue(publisher._heartbeat_thread.is_alive())

        # Call cleanup
        publisher.cleanup()

        # Verify stop event was set
        self.assertTrue(publisher._stop_event.is_set())

        # Give thread a moment to stop
        publisher._heartbeat_thread.join(timeout=2)

        # Verify thread stopped
        self.assertFalse(publisher._heartbeat_thread.is_alive())
