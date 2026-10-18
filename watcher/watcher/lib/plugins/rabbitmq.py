# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import pika
from pika.exceptions import (
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
    ConnectionWrongStateError,
    UnroutableError,
)
from recovery_libs.event_bus import rabbit_eventing
from recovery_libs.utils.retry import exponential_backoff_retry

from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import (
    RABBITMQ_HEALTHCHECK_FAILED,
    RABBITMQ_MESSAGES_EXCEPTIONS,
)
from watcher.models.exceptions import EventBusError


def validate_json(message_body):
    try:
        json.loads(message_body)
        return True
    except ValueError:
        return False


class EventBusPublisher:
    """
    Publishes messages to RabbitMQ.

    Args:
        hostname (str): RabbitMQ server hostname. Default is "localhost".
        port (int): RabbitMQ port number. Default is 5672.
        username (str): Username for authentication. Default is "guest".
        password (str): Password for authentication. Default is "guest".
        vhost (str): The virtual host to connect to. Default is "/".
        exchange (str): Exchange name. Default is None.
        queues (list[str]): A list of queues. Default is None.
        routing_keys (dict[str, list[str]]): A dict of routing keys indexed by queue prefix

    """

    def __init__(
        self,
        hostname: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        vhost: str = "/",
        exchange: str | None = None,
        queues: list[str] | None = None,
        routing_keys: dict[str, list[str]] | None = None,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.exchange = exchange
        self.queues = queues or []
        self.routing_keys = routing_keys or {}
        self.channel = None
        self.connection = None
        logger.debug(f"Queues={self.queues}")
        logger.debug(f"Routing keys={self.routing_keys}")

    def declare_queue(self, queue: str) -> None:
        """
        Declares a durable queue.

        Args:
            queue (str): Queue name
        """
        rabbit_eventing.declare_queue(channel=self.channel, queue_name=queue)

    def setup_all_queues(self) -> None:
        """
        Declares and binds to all RabbitMQ queues.
        """
        for queue in self.queues:
            self.declare_queue(queue=queue)

        for routing_prefix, routing_keys in self.routing_keys.items():
            for queue in self.queues:
                if not queue.startswith(routing_prefix):
                    continue
                for routing_key in routing_keys:
                    logger.debug(f"Binding queue={queue} to routing key={routing_key}")
                    self.bind_queue(queue=queue, routing_key=routing_key)

    def declare_exchange(self) -> None:
        """
        Declares an exchange.
        """
        rabbit_eventing.declare_exchange(channel=self.channel, exchange_name=self.exchange)  # type: ignore[arg-type]

    def send_ping(self) -> None:
        """
        Issues a process_data_events() call. Useful to maintain long lived connections.

        Notes:
            If your application maintains a long-lived publisher connection, this method
            should be called periodically in order to respond to heartbeats and other

            Source: https://github.com/pika/pika/blob/ce7226e1aac88db151a0c1c4803ff55af7c2aa5a/pika/adapters/blocking_connection.py#L821  # noqa
        """
        try:
            self.connection.process_data_events(time_limit=1)  # type: ignore[attr-defined]
        except (
            ChannelClosedByBroker,
            ConnectionClosedByBroker,
            ConnectionWrongStateError,
            UnroutableError,
            ValueError,
        ) as err:
            logger.warning(f"[RMQ] Failed to send ping. Reseting connection: {err}")
            RABBITMQ_HEALTHCHECK_FAILED.inc()
            self.reset_connection_and_channel()

    def reset_connection_and_channel(self) -> None:
        """
        Resets RabbitMQ connection and channel
        """
        logger.info("[RMQ] Resetting up connection and channel")
        if self.connection and self.connection.is_open:
            logger.info("[RMQ] Connection is still open - closing before reset")
            self.connection.close()

        self.connect()
        self.setup_channel()
        self.declare_exchange()
        self.setup_all_queues()
        self.configure_delivery_qos()

    def bind_queue(self, queue: str, routing_key: str) -> None:
        """
        Binds a queue to an exchange with a routing key.

        Args:
            queue (str): Queue name.
            exchange (str): Exchange name.
            routing_key (str): The routing key to use for binding.
        """
        rabbit_eventing.bind_queue(
            channel=self.channel,
            queue_name=queue,
            exchange_name=self.exchange,  # type: ignore[arg-type]
            routing_key=routing_key,
        )

    @RABBITMQ_MESSAGES_EXCEPTIONS.count_exceptions()
    @exponential_backoff_retry(
        max_retries=5,
        base_delay=2,
        exceptions=(
            ConnectionClosedByBroker,
            ConnectionWrongStateError,
            ChannelClosedByBroker,
            UnroutableError,
            rabbit_eventing.EventBusError,
        ),
    )
    def publish_message(
        self, routing_key: str, message: str, properties: pika.spec.BasicProperties | None = None
    ) -> None:
        """
        Publishes a message with a routing key.

        Args:
            routing_key (str): Routing key to use for publishing.
            message (str): Message to publish.
            properties (Optional[pika.spec.BasicProperties]): Optional message properties.

        Raises:
            EventBusError: If the message is not valid JSON or if publishing
                           fails.
        """
        if not validate_json(message):
            raise EventBusError("InvalidJSON", "Invalid JSON message body")

        if not self.connection or self.connection.is_closed:
            self.reset_connection_and_channel()

        rabbit_eventing.publish_message(
            channel=self.channel,
            exchange_name=self.exchange,  # type: ignore[arg-type]
            routing_key=routing_key,
            message_body=message,
            properties=properties,
        )

    def connect(self) -> None:
        """
        Establishes a connection to RabbitMQ.
        """
        logger.info("[RMQ] Creating connection")
        self.connection = rabbit_eventing.create_connection(
            hostname=self.hostname,
            vhost=self.vhost,
            credentials=pika.PlainCredentials(self.username, self.password),
        )

    def setup_channel(self) -> None:
        """
        Sets up RabbitMQ channel.
        """
        logger.info("[RMQ] Setting up channel")
        self.channel = self.connection.channel()  # type: ignore[attr-defined]

    def configure_delivery_qos(self) -> None:
        """
        Configures RabbitMQ channel QoS.
        """
        self.channel.basic_qos(prefetch_count=1)  # type: ignore[attr-defined]
        self.enable_delivery_confirmations()

    def enable_delivery_confirmations(self) -> None:
        """
        Sends the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.
        """
        logger.info("[RMQ] Issuing Confirm.Select RPC command")
        self.channel.confirm_delivery()  # type: ignore[attr-defined]
