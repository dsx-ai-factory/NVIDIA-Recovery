# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from queue import Queue
from unittest.mock import MagicMock, call, patch

import pytest
from recovery_libs.event_bus.event_bus_listener import EventBusListener
from watcher.lib.plugins.rabbitmq import EventBusPublisher
from watcher.models.config import (
    DatasourcesConfig,
    RabbitMQPublisherConfig,
    RabbitMQSubscriberConfig,
    WatcherConfig,
    WatcherEventSource,
)
from watcher.service import WatcherService
from watcher.workflow import RulesEngine


class TestRabbitMQConnectionRetry:
    """Test cases for RabbitMQ connection retry mechanism."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock WatcherConfig for testing."""
        publisher_config = RabbitMQPublisherConfig(
            host="localhost", username="guest", password="guest", vhost="/", exchange="test_exchange"
        )

        subscriber_config = RabbitMQSubscriberConfig(
            host="localhost",
            username="guest",
            password="guest",
            vhost="/",
            exchange_name="test_exchange",
            queue_name="test_queue",
            routing_key="test.key",
        )

        datasources_config = DatasourcesConfig(
            rabbitmq={"publisher": publisher_config, "subscriber": subscriber_config}
        )

        config = WatcherConfig(datasources=datasources_config, rules=[], event_source=WatcherEventSource.RABBITMQ)
        return config

    @pytest.fixture
    def mock_publisher(self):
        """Create a mock EventBusPublisher."""
        publisher = MagicMock(spec=EventBusPublisher)
        publisher.connection = MagicMock()
        publisher.connection.is_open = True
        publisher.channel = MagicMock()
        publisher.channel.is_open = True
        return publisher

    @pytest.fixture
    def mock_subscriber(self):
        """Create a mock EventBusListener."""
        subscriber = MagicMock(spec=EventBusListener)
        subscriber.is_healthy.return_value = True
        return subscriber

    @pytest.fixture
    def watcher_service(self, mock_config, mock_publisher, mock_subscriber):
        """Create a WatcherService instance with mocked dependencies."""
        queue = Queue()
        engine = MagicMock()

        service = WatcherService(
            config=mock_config, engine=engine, queue=queue, publisher=mock_publisher, subscriber=mock_subscriber
        )
        return service

    @pytest.fixture
    def rules_engine(self, mock_config, mock_publisher):
        """Create a RulesEngine instance with mocked dependencies."""
        kvstore = {}
        action_cache = MagicMock()

        engine = RulesEngine(config=mock_config, publisher=mock_publisher, kvstore=kvstore, action_cache=action_cache)
        return engine

    def test_setup_rabbitmq_connections_success_first_try(self, watcher_service):
        """Test successful RabbitMQ setup on first attempt."""
        result = watcher_service.setup_rabbitmq_connections(max_retries=3)

        assert result is True
        watcher_service.publisher.connect.assert_called_once()
        watcher_service.publisher.setup_channel.assert_called_once()
        watcher_service.publisher.declare_exchange.assert_called_once()
        watcher_service.publisher.setup_all_queues.assert_called_once()
        watcher_service.publisher.configure_delivery_qos.assert_called_once()
        watcher_service.subscriber.start_consuming.assert_called_once()

    def test_setup_rabbitmq_connections_retry_then_success(self, watcher_service):
        """Test RabbitMQ setup with failure then success."""
        # Make first two calls fail, third succeed
        watcher_service.publisher.connect.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            None,  # Success on third try
        ]

        with patch("time.sleep") as mock_sleep:
            result = watcher_service.setup_rabbitmq_connections(max_retries=3, base_delay=1)

        assert result is True
        assert watcher_service.publisher.connect.call_count == 3
        # Verify exponential backoff delays
        expected_calls = [call(2), call(4)]  # base_delay * 2^(attempt+1)
        mock_sleep.assert_has_calls(expected_calls)

    def test_setup_rabbitmq_connections_all_retries_fail(self, watcher_service):
        """Test RabbitMQ setup when all retry attempts fail."""
        watcher_service.publisher.connect.side_effect = Exception("Connection failed")

        with patch("time.sleep") as mock_sleep:
            result = watcher_service.setup_rabbitmq_connections(max_retries=3, base_delay=1)

        assert result is False
        assert watcher_service.publisher.connect.call_count == 3
        assert mock_sleep.call_count == 2  # 2 retries after first failure

    def test_setup_rabbitmq_connections_subscriber_failure(self, watcher_service):
        """Test RabbitMQ setup when subscriber fails."""
        watcher_service.subscriber.start_consuming.side_effect = Exception("Subscriber failed")

        result = watcher_service.setup_rabbitmq_connections(max_retries=2)

        assert result is False
        assert watcher_service.subscriber.start_consuming.call_count == 2

    @pytest.mark.asyncio
    async def test_check_subscriber_health_healthy(self, watcher_service):
        """Test subscriber health check when subscriber is healthy."""
        watcher_service.subscriber.is_healthy.return_value = True

        await watcher_service.check_subscriber_health()

        watcher_service.subscriber.is_healthy.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_subscriber_health_unhealthy(self, watcher_service):
        """Test subscriber health check when subscriber is unhealthy."""
        watcher_service.subscriber.is_healthy.return_value = False

        await watcher_service.check_subscriber_health()

        watcher_service.subscriber.is_healthy.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_maintenance_tasks_calls_subscriber_check_and_engine(self, watcher_service):
        """Test that run_maintenance_tasks calls subscriber health check and engine maintenance."""

        # Create an async mock for check_subscriber_health
        async def mock_check_subscriber_health():
            pass

        watcher_service.check_subscriber_health = MagicMock(side_effect=mock_check_subscriber_health)

        await watcher_service.run_maintenance_tasks()

        watcher_service.check_subscriber_health.assert_called_once()
        watcher_service.engine.run_maintenance_tasks.assert_called_once()

    def test_rules_engine_publisher_health_check_healthy(self, rules_engine):
        """Test RulesEngine publisher health check when publisher is healthy."""
        rules_engine.check_and_maintain_publisher_connection()

        # Should call send_ping for healthy connections
        rules_engine.publisher.send_ping.assert_called_once()
        rules_engine.publisher.reset_connection_and_channel.assert_not_called()

    def test_rules_engine_publisher_health_check_unhealthy_connection(self, rules_engine):
        """Test RulesEngine publisher health check when connection is None."""
        rules_engine.publisher.connection = None

        rules_engine.check_and_maintain_publisher_connection()

        # Should attempt to reset connection
        rules_engine.publisher.reset_connection_and_channel.assert_called_once()
        rules_engine.publisher.send_ping.assert_not_called()

    def test_rules_engine_publisher_health_check_unhealthy_channel(self, rules_engine):
        """Test RulesEngine publisher health check when channel is closed."""
        rules_engine.publisher.channel.is_open = False

        rules_engine.check_and_maintain_publisher_connection()

        # Should attempt to reset connection
        rules_engine.publisher.reset_connection_and_channel.assert_called_once()
        rules_engine.publisher.send_ping.assert_not_called()

    def test_rules_engine_run_maintenance_tasks_calls_publisher_check(self, rules_engine):
        """Test that RulesEngine run_maintenance_tasks calls publisher health check."""
        rules_engine.check_and_maintain_publisher_connection = MagicMock()

        rules_engine.run_maintenance_tasks()

        rules_engine.check_and_maintain_publisher_connection.assert_called_once()

    @patch("watcher.service.logger")
    def test_run_with_connection_failure(self, mock_logger, watcher_service):
        """Test service run method when initial connection setup fails."""
        watcher_service.setup_rabbitmq_connections = MagicMock(return_value=False)

        watcher_service.run()

        # Service should exit gracefully after logging error
        mock_logger.error.assert_called_with("Failed to establish RabbitMQ connections. Service will exit.")

    def test_run_with_connection_success(self, watcher_service):
        """Test service run method when connection setup succeeds."""
        watcher_service.setup_rabbitmq_connections = MagicMock(return_value=True)
        watcher_service.interrupted = True  # Prevent infinite loop in test

        # Mock the queue to avoid blocking
        watcher_service.queue.get = MagicMock(side_effect=KeyboardInterrupt())

        watcher_service.run()

        # Should have set up connections and entered main loop
        watcher_service.setup_rabbitmq_connections.assert_called_once()
