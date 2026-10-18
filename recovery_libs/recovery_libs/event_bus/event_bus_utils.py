# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Thread

import pika
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
    StreamLostError,
)
from prometheus_client import Counter

import recovery_libs.event_bus.rabbit_eventing as rabbitmq
from recovery_libs.event_bus.rabbit_eventing import EventBusError
from recovery_libs.utils.retry import exponential_backoff_retry

logger = logging.getLogger(__name__)


# This function is used to compose both the dead letter queue name and the dead letter routing key name with their base
# name as prefix.
def get_dead_letter_timer_name(prefix: str, delay_secs: int) -> str:
    return f"{prefix}_requeue_{delay_secs}" if delay_secs > 0 else prefix


@dataclass
class RabbitMQConsumerConfig:
    host: str
    username: str
    password: str = field(repr=False)
    vhost: str
    exchange_name: str
    queue_name: str
    routing_keys: list[str]
    callback_func: Callable[
        [
            pika.channel.Channel,
            pika.spec.Basic.Deliver,
            pika.spec.BasicProperties,
            bytes,
        ],
        None,
    ]
    delay_secs: int = 0
    dead_letter_routing_key: str | None = None
    dead_letter_redelivery_routing_key: str | None = None
    exchange_type: str = "direct"
    thread_count: int = 1


@dataclass
class RabbitMQQueueConfig:
    queue_name: str
    routing_keys: list[str]
    delay_secs: int = 0
    dead_letter_redelivery_routing_key: str | None = None


@dataclass
class RabbitMQPublisherConfig:
    host: str
    username: str
    password: str = field(repr=False)
    vhost: str
    exchange_name: str
    exchange_type: str = "direct"
    queue_routing_key_mapping: dict[str, RabbitMQQueueConfig] | None = None


@dataclass
class RabbitMQConsumerTracking:
    consumer_tag: str
    thread: Thread
    channel: pika.adapters.blocking_connection.BlockingChannel


class RabbitMQConsumer:
    """
    This class is used to spin up multiple threads to consume messages from one RabbitMQ queue and gracefully
    stop consuming messages when a stop event is received.
    """

    def __init__(
        self,
        consumer_config: RabbitMQConsumerConfig,
        thread_event: Event,
        EVENT_BUS_RETRYABLE_ERROR_COUNTER: Counter,
        EVENT_BUS_UNEXPECTED_ERROR_COUNTER: Counter,
    ):
        self.consumer_config = consumer_config
        self.consumer_thread_trackings: list[RabbitMQConsumerTracking] = []
        self.thread_event = thread_event
        self.event_bus_retryable_error_counter = EVENT_BUS_RETRYABLE_ERROR_COUNTER
        self.event_bus_unexpected_error_counter = EVENT_BUS_UNEXPECTED_ERROR_COUNTER

        for i in range(self.consumer_config.thread_count):
            channel = rabbitmq.connect(
                hostname=self.consumer_config.host,
                credentials=pika.PlainCredentials(
                    self.consumer_config.username,
                    self.consumer_config.password,
                ),
                vhost=self.consumer_config.vhost,
            )

            logger.info(f"Declaring exchange: {self.consumer_config.exchange_name}.")
            rabbitmq.declare_exchange(
                channel=channel,
                exchange_name=self.consumer_config.exchange_name,
                exchange_type=self.consumer_config.exchange_type,
            )
            logger.info(f"Declaring queue: {self.consumer_config.queue_name}.")
            rabbitmq.declare_queue(channel=channel, queue_name=self.consumer_config.queue_name)

            logger.info(
                f"Binding routing key(s) {self.consumer_config.routing_keys} to queue {self.consumer_config.queue_name}."
            )
            for routing_key in self.consumer_config.routing_keys:
                rabbitmq.bind_queue(
                    channel=channel,
                    queue_name=self.consumer_config.queue_name,
                    exchange_name=self.consumer_config.exchange_name,
                    routing_key=routing_key,
                )

            if (
                self.consumer_config.delay_secs > 0
                and self.consumer_config.dead_letter_routing_key
                and self.consumer_config.dead_letter_redelivery_routing_key
            ):
                dead_letter_queue_name = get_dead_letter_timer_name(
                    prefix=self.consumer_config.queue_name, delay_secs=self.consumer_config.delay_secs
                )
                logger.info(
                    f"Setting up the dead letter queue routing with queue '{dead_letter_queue_name}', "
                    f"routing key '{self.consumer_config.dead_letter_routing_key}', "
                    f"dead letter redelivery routing key '{self.consumer_config.dead_letter_redelivery_routing_key}'."
                )

                channel.queue_declare(
                    queue=dead_letter_queue_name,
                    durable=True,
                    arguments={
                        # Route the msg via the dead-letter-exchange
                        "x-dead-letter-exchange": self.consumer_config.exchange_name,
                        # Route the message back to this key once the message expired, this key is bound to the original
                        # queue to re-deliver the message.
                        "x-dead-letter-routing-key": self.consumer_config.dead_letter_redelivery_routing_key,
                        # TTL must be msec
                        "x-message-ttl": self.consumer_config.delay_secs * 1000,
                    },
                )

                # Bind the dead letter routing to the dead letter queue to route the dead letter messsage properly.
                # properly.
                rabbitmq.bind_queue(
                    channel=channel,
                    queue_name=dead_letter_queue_name,
                    exchange_name=self.consumer_config.exchange_name,
                    routing_key=self.consumer_config.dead_letter_routing_key,
                )

                # Bind the dead letter redelivery routing key to the consumer queue to route the expried dead letter
                # messages (i.e. redelivery of them) back to the consumer queue.
                rabbitmq.bind_queue(
                    channel=channel,
                    queue_name=self.consumer_config.queue_name,
                    exchange_name=self.consumer_config.exchange_name,
                    routing_key=self.consumer_config.dead_letter_redelivery_routing_key,
                )

            consumer_tag = channel.basic_consume(
                queue=self.consumer_config.queue_name,
                on_message_callback=self.consumer_config.callback_func,
                auto_ack=True,
            )

            self.consumer_thread_trackings.append(
                RabbitMQConsumerTracking(
                    consumer_tag=consumer_tag,
                    thread=Thread(
                        target=self.consume_messages,
                        kwargs={"channel": channel, "thread_index": i},
                    ),
                    channel=channel,
                )
            )

    # This should always be used as the entry point of a thread to start consuming.
    def consume_messages(self, channel: pika.adapters.blocking_connection.BlockingChannel, thread_index: int) -> None:
        while True:
            try:
                if channel is None or channel.is_closed:
                    logger.info(f"Reconnecting to the RabbitMQ from thread {thread_index}.")
                    channel = rabbitmq.connect(
                        hostname=self.consumer_config.host,
                        credentials=pika.PlainCredentials(
                            self.consumer_config.username,
                            self.consumer_config.password,
                        ),
                        vhost=self.consumer_config.vhost,
                    )
                    consumer_tag = channel.basic_consume(
                        queue=self.consumer_config.queue_name,
                        on_message_callback=self.consumer_config.callback_func,
                        auto_ack=True,
                    )

                    self.consumer_thread_trackings[thread_index].consumer_tag = consumer_tag
                    self.consumer_thread_trackings[thread_index].channel = channel

                channel.start_consuming()
            except (
                ConnectionClosedByBroker,
                EventBusError,
                StreamLostError,
                AMQPConnectionError,
                ChannelClosedByBroker,
            ) as rabbit_ex:
                self.event_bus_retryable_error_counter.inc(1)
                logger.debug(f"Consumer config: {self.consumer_config}.")
                logger.error(f"Encountered a retryable RabbitMQ error: {rabbit_ex}.")
            except Exception as ex:
                self.event_bus_unexpected_error_counter.inc(1)
                logger.exception("Encountered unexpected error.", extra={"error": str(ex)})

            if self.thread_event.is_set():
                logger.info("Thread shutdown event is set, exiting without further retries.")
                break
            else:
                logger.info("Sleeping for 5 seconds before reconnecting to RabbitMQ.")
                time.sleep(5)

    def start_consuming(self) -> None:
        for consumer_tracking in self.consumer_thread_trackings:
            consumer_tracking.thread.start()

    def stop_consuming(self) -> None:
        for consumer_tracking in self.consumer_thread_trackings:
            consumer_tracking.channel.basic_cancel(consumer_tracking.consumer_tag)
            consumer_tracking.channel.stop_consuming()
            logger.info(f"Message consumption with consumer tag '{consumer_tracking.consumer_tag}' stopped.")
            consumer_tracking.thread.join()
            logger.info("Event bus consumer thread stopped.")

    def is_healthy(self) -> bool:
        return all(tracking.thread.is_alive() for tracking in self.consumer_thread_trackings)

    def get_queue_length(self) -> int:
        """
        Return the number of pending messages in the queue.
        """
        channel = rabbitmq.connect(
            hostname=self.consumer_config.host,
            credentials=pika.PlainCredentials(
                self.consumer_config.username,
                self.consumer_config.password,
            ),
            vhost=self.consumer_config.vhost,
        )
        queue = rabbitmq.declare_queue(channel=channel, queue_name=self.consumer_config.queue_name)
        return queue.method.message_count


class RabbitMQPublisher:
    def __init__(self, publisher_config: RabbitMQPublisherConfig):
        logger.info(f"Publisher config: {publisher_config}")
        self.publisher_config = publisher_config

        self.channel = rabbitmq.connect(
            hostname=self.publisher_config.host,
            credentials=pika.PlainCredentials(
                self.publisher_config.username,
                self.publisher_config.password,
            ),
            vhost=self.publisher_config.vhost,
        )

        logger.info(f"Declaring exchange: {self.publisher_config.exchange_name}.")
        rabbitmq.declare_exchange(
            channel=self.channel,
            exchange_name=self.publisher_config.exchange_name,
            exchange_type=publisher_config.exchange_type,
        )

        # Initialize heartbeat support but don't start thread yet
        self._stop_event = Event()
        self._heartbeat_thread = None
        # Thread lock to protect channel access from publish and heartbeat threads
        self._channel_lock = threading.Lock()

        if self.publisher_config.queue_routing_key_mapping:
            for queue_name, queue_config in self.publisher_config.queue_routing_key_mapping.items():
                logger.info(f"Declaring queue: {queue_name}.")
                rabbitmq.declare_queue(channel=self.channel, queue_name=queue_name)
                # When delay_secs is set, we need to create a dead letter queue to delay the message delivery.
                # For futurn extension, we acn create a dead letter queue for each routing key for fan-out use cases if
                # needed.
                if queue_config.delay_secs > 0:
                    dead_letter_queue_name = get_dead_letter_timer_name(
                        prefix=queue_name, delay_secs=queue_config.delay_secs
                    )
                    logger.info(
                        f"Declaring dead letter queue '{dead_letter_queue_name}' to delay message delivery by "
                        f"{queue_config.delay_secs} seconds."
                    )
                    self.channel.queue_declare(
                        queue=dead_letter_queue_name,
                        durable=True,
                        arguments={
                            # Route the msg via the dead-letter-exchange
                            "x-dead-letter-exchange": self.publisher_config.exchange_name,
                            # Route the message back to this key once the message expired, this key is bound to the original
                            # queue to re-deliver the message.
                            "x-dead-letter-routing-key": queue_config.dead_letter_redelivery_routing_key,
                            # TTL must be msec
                            "x-message-ttl": queue_config.delay_secs * 1000,
                        },
                    )

                    dead_letter_routing_key = get_dead_letter_timer_name(
                        prefix=queue_config.routing_keys[0], delay_secs=queue_config.delay_secs
                    )
                    logger.info(f"Binding dead letter routing key '{dead_letter_routing_key}' to queue {queue_name}.")
                    rabbitmq.bind_queue(
                        channel=self.channel,
                        queue_name=dead_letter_queue_name,
                        exchange_name=self.publisher_config.exchange_name,
                        routing_key=dead_letter_routing_key,
                    )

                logger.info(f"Binding routing key(s) {queue_config.routing_keys} to queue {queue_name}.")
                for routing_key in queue_config.routing_keys:
                    rabbitmq.bind_queue(
                        channel=self.channel,
                        queue_name=queue_name,
                        exchange_name=self.publisher_config.exchange_name,
                        routing_key=routing_key,
                    )

        # Start heartbeat thread AFTER all initialization is complete
        self._heartbeat_thread = Thread(
            target=self._heartbeat_loop, name=f"RabbitMQPublisher-Heartbeat-{id(self)}", daemon=True
        )
        self._heartbeat_thread.start()
        logger.info("Started publisher heartbeat thread")

    # Only retry on connection related errors to avoid publishing the same message multiple times.
    @exponential_backoff_retry(
        max_retries=3, base_delay=2, exceptions=(StreamLostError, ConnectionClosedByBroker, ChannelClosedByBroker)
    )
    def publish_message(
        self, message: str, routing_key: str, delay_secs: int = 0, properties: pika.BasicProperties | None = None
    ) -> None:
        with self._channel_lock:
            if self.channel is None or self.channel.is_closed:
                logger.info("Reconnecting to the RabbitMQ before publishing the message.")
                self.channel = rabbitmq.connect(
                    hostname=self.publisher_config.host,
                    credentials=pika.PlainCredentials(
                        self.publisher_config.username,
                        self.publisher_config.password,
                    ),
                    vhost=self.publisher_config.vhost,
                )

            if delay_secs > 0:
                dead_letter_routing_key = get_dead_letter_timer_name(prefix=routing_key, delay_secs=delay_secs)
                rabbitmq.publish_message(
                    channel=self.channel,
                    exchange_name=self.publisher_config.exchange_name,
                    routing_key=dead_letter_routing_key,
                    message_body=message,
                    properties=properties,
                )
            else:
                rabbitmq.publish_message(
                    channel=self.channel,
                    exchange_name=self.publisher_config.exchange_name,
                    routing_key=routing_key,
                    message_body=message,
                    properties=properties,
                )

    def _heartbeat_loop(self):
        """Sends heartbeats every 30 seconds to keep connection alive"""
        while not self._stop_event.is_set():
            try:
                with self._channel_lock:
                    if self.channel and not self.channel.is_closed:
                        self.channel.connection.process_data_events(time_limit=1)
            except Exception as e:
                logger.debug("RabbitMQ heartbeat failed", exc_info=True, extra={"error": str(e)})

            # Use Event.wait() for interruptible sleep - returns True if event is set
            if self._stop_event.wait(timeout=30):
                break  # Stop event was set, exit immediately

    def cleanup(self) -> None:
        # Signal the heartbeat thread to stop (immediate interruption)
        self._stop_event.set()

        # Wait for heartbeat thread to stop if it was started
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2)

        # Protect channel close operation from concurrent access
        with self._channel_lock:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
                logger.info("RabbitMQ channel closed.")
            else:
                logger.info("RabbitMQ channel is already closed.")
