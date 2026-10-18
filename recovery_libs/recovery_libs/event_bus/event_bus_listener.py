# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from queue import Full, Queue
from threading import Thread

import pika
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
    StreamLostError,
)
from prometheus_client import Counter

import recovery_libs.event_bus.rabbit_eventing as rabbitmq
from recovery_libs.event_bus.event_bus_config import EventBusConfig
from recovery_libs.event_bus.rabbit_eventing import EventBusError
from recovery_libs.utils.logger import get_logger

logger = get_logger(__name__)


class EventBusListener:
    """
    This class spins s thread for listening to event bus events,
    and forward the data received to the queue passed in the constructor,
    until the shutdown event is set.
    """

    def __init__(
        self,
        config: EventBusConfig,
        queue: Queue,
        routing_sub_keys: list[str] | None = None,
        errors_counter: Counter | None = None,
    ):
        self.config = config
        self.client_queue = queue
        self._is_interrupted = False
        self.routing_keys = (
            [config.routing_key + sub_key for sub_key in routing_sub_keys] if routing_sub_keys else [config.routing_key]
        )
        self.errors_counter = (
            errors_counter
            if errors_counter
            else Counter(
                name="event_bus_listener_errors", documentation="The number of errors in the event bus listener"
            )
        )
        self.channel = rabbitmq.connect(
            hostname=self.config.host,
            credentials=pika.PlainCredentials(
                self.config.username,
                self.config.password,
            ),
            vhost=self.config.vhost,
        )

        logger.info(f"Declaring exchange: {self.config.exchange_name}")
        rabbitmq.declare_exchange(
            channel=self.channel,
            exchange_name=self.config.exchange_name,
            exchange_type=self.config.exchange_type,
        )
        logger.info(f"Declaring queue: {self.config.queue_name} with max size {self.config.queue_max_size_mb} MB")
        rabbitmq.declare_queue(
            channel=self.channel, queue_name=self.config.queue_name, queue_max_size_mb=self.config.queue_max_size_mb
        )

        for routing_key in self.routing_keys:
            logger.info(f"Binding routing key {routing_key} to queue {self.config.queue_name}")
            rabbitmq.bind_queue(
                channel=self.channel,
                queue_name=self.config.queue_name,
                exchange_name=self.config.exchange_name,
                routing_key=routing_key,
            )
        self.consumer_thread: Thread = Thread(target=self.consume_messages, daemon=True)

    def interrupt(self):
        self._is_interrupted = True

    def consume_messages(self) -> None:
        while not self._is_interrupted:
            try:
                if self.channel.is_closed:
                    logger.info("Reconnecting to RabbitMQ")
                    self.channel = rabbitmq.connect(
                        hostname=self.config.host,
                        credentials=pika.PlainCredentials(
                            self.config.username,
                            self.config.password,
                        ),
                        vhost=self.config.vhost,
                    )
                # https://pika.readthedocs.io/en/stable/examples/blocking_consumer_generator.html
                for message in self.channel.consume(self.config.queue_name, inactivity_timeout=1):
                    if self._is_interrupted:
                        logger.info("Interrupted, exiting RMQ thread")
                        break
                    if not all(message):
                        continue  # we get (None, None, None) when the inactivity timeout is reached
                    method, properties, body = message
                    self.callback_func(method, properties, body)  # needs to ack the message

                self.channel.cancel()
            except (
                ConnectionClosedByBroker,
                EventBusError,
                StreamLostError,
                AMQPConnectionError,
                ChannelClosedByBroker,
            ) as rabbit_ex:
                logger.error(f"Encountered a retryable RabbitMQ error: {rabbit_ex}.")
                self.errors_counter.inc()
            except Exception as ex:
                logger.exception("Encountered unexpected error", extra={"error": str(ex)})
                self.errors_counter.inc()

            if self._is_interrupted:
                break
            else:
                logger.info("Closing channel and sleeping for 5 seconds before reconnecting to RabbitMQ.")
                self.channel.close()
                time.sleep(5)

    def callback_func(
        self,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            routing_key = method.routing_key
            logger.debug("Received %d bytes on key %s", len(body), routing_key)
            payload_type = self.config.payload
            self.client_queue.put((routing_key, payload_type, body), timeout=self.config.queue_full_timeout_sec)
        except Full:
            # FIXME - use dead letter queue
            logger.error("Queue is full, message will be discarded: %s", body)
            self.errors_counter.inc()
        except Exception as e:
            # FIXME - use dead letter queue
            logger.exception(
                "Failed to pass notification from event bus thread",
                extra={"body": body, "error": str(e)},
            )
            self.errors_counter.inc()
        finally:
            self.channel.basic_ack(method.delivery_tag)

    def start_consuming(self) -> None:
        self.consumer_thread.start()

    def stop_consuming(self) -> None:
        logger.info("Event bus consumer cancelling the consumer in RMQ")
        self.interrupt()
        logger.info("Event bus consumer waiting up to 10 seconds for consumer thread to stop")
        try:
            self.consumer_thread.join(timeout=10)
        except BaseException as e:
            logger.exception("Failed to wait for RMQ thread to stop", extra={"error": str(e)})
        logger.info("Event bus consumer thread stopped.")

    def is_healthy(self) -> bool:
        return self.consumer_thread.is_alive()

    def get_queue_length(self) -> int:
        """
        Return the number of pending messages in the queue.
        """
        queue = rabbitmq.declare_queue(channel=self.channel, queue_name=self.config.queue_name)
        return queue.method.message_count
