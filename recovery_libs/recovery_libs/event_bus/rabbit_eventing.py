# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from collections.abc import Callable

import pika

from .event_bus_error import EventBusError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)


def connect(
    hostname: str = "localhost",
    port: int = 5672,
    credentials: pika.PlainCredentials | None = None,
    vhost: str = "/",
    *,
    heartbeat: int = 600,
    blocked_connection_timeout: int = 300,
    retry_delay: int = 1,
    connection_attempts: int = 10,
) -> pika.adapters.blocking_connection.BlockingChannel:
    """Establishes a connection to RabbitMQ.

    Parameters
    ----------
    hostname : str
        Hostname of the RabbitMQ server. Defaults to "localhost".
    port : int
        Port of the RabbitMQ server. Defaults to 5672
    credentials : pika.PlainCredentials | None
        Credential to connect to RabbitMQ server. Defaults to guest:guest
    vhost : str
        Virtual hostname of the RabbitMQ server. Defaults to ""
    heartbeat : int
        Heartbeat interval in seconds. Defaults to 600.
    blocked_connection_timeout: int
        Timeout for blocked connections in seconds. Defaults to 300.
    retry_delay : int
        Delay between connection attempts in seconds. Defaults to 1.
    connection_attempts : int
        Number of connection attempts. Defaults to 10.

    Returns
    -------
    pika.adapters.blocking_connection.BlockingChannel
        The channel to use for the connection.

    Raises
    ------
        EventBusError: If a connection error occurs.
    """

    try:
        params = pika.ConnectionParameters(
            host=hostname,
            port=port,
            virtual_host=vhost,
            credentials=credentials or pika.PlainCredentials("guest", "guest"),
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_connection_timeout,
            retry_delay=retry_delay,
            connection_attempts=connection_attempts,
        )
        connection = pika.BlockingConnection(params)
        return connection.channel()
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection failed: {e}")
        raise EventBusError("ConnectionError", f"Failed to connect to RabbitMQ: {e}")


def create_connection(
    hostname: str = "localhost",
    port: int = 5672,
    credentials: pika.PlainCredentials | None = None,
    vhost: str = "/",
    *,
    heartbeat: int = 600,
    blocked_connection_timeout: int = 300,
    retry_delay: int = 1,
    connection_attempts: int = 10,
) -> pika.adapters.BlockingConnection:
    """Establishes a connection to RabbitMQ.

    Parameters
    ----------
    hostname : str
        Hostname of the RabbitMQ server. Defaults to "localhost".
    port : int
        Port of the RabbitMQ server. Defaults to 5672
    credentials : pika.PlainCredentials | None
        Credential to connect to RabbitMQ server. Defaults to guest:guest
    vhost : str
        Virtual hostname of the RabbitMQ server. Defaults to ""
    heartbeat : int
        Heartbeat interval in seconds. Defaults to 600.
    blocked_connection_timeout: int
        Timeout for blocked connections in seconds. Defaults to 300.
    retry_delay : int
        Delay between connection attempts in seconds. Defaults to 1.
    connection_attempts : int
        Number of connection attempts. Defaults to 10.

    Returns
    -------
    pika.adapters.BlockingConnection
        The connection to rabbitMQ.

    Raises
    ------
        EventBusError: If a connection error occurs.
    """

    try:
        params = pika.ConnectionParameters(
            host=hostname,
            port=port,
            virtual_host=vhost,
            credentials=credentials or pika.PlainCredentials("guest", "guest"),
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_connection_timeout,
            retry_delay=retry_delay,
            connection_attempts=connection_attempts,
        )
        return pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection failed: {e}")
        raise EventBusError("ConnectionError", f"Failed to connect to RabbitMQ: {e}")


def declare_queue(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    durable=True,
    queue_max_size_mb: int = 0,
):
    """Declares a queue.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for the declaration.
        queue_name (str): The name of the queue.
        durable (bool, optional): Whether the queue should survive a server restart. Defaults to True.
        queue_max_size_mb (int, optional): The maximum size of the queue in MB. Defaults to 0, no limit.
    """

    arguments: dict[str, int | str] = {}
    if queue_max_size_mb > 0:
        arguments["x-max-length-bytes"] = queue_max_size_mb * 1024 * 1024
        arguments["x-overflow"] = "reject-publish"

    return channel.queue_declare(
        queue=queue_name,
        durable=durable,
        arguments=arguments,
    )


def declare_exchange(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    exchange_name: str,
    exchange_type: str = "direct",
    durable=True,
):
    """Declares an exchange.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for the declaration.
        exchange_name (str): The name of the exchange.
        exchange_type (str, optional): The type of exchange. Defaults to "direct".
        durable (bool, optional): Whether the queue should survive a server restart. Defaults to True.

    """
    channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=durable)


def bind_queue(
    channel: pika.adapters.blocking_connection.BlockingChannel, queue_name: str, exchange_name: str, routing_key: str
):
    """Binds a queue to an exchange.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for the binding.
        queue_name (str): The name of the queue.
        exchange_name (str): The name of the exchange.
        routing_key (str): The routing key to use for the binding.
    """
    channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)


def publish_message(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    exchange_name: str,
    routing_key: str,
    message_body: str,
    properties: pika.spec.BasicProperties = None,
):
    """Publishes a message to an exchange.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for publishing.
        exchange_name (str): The name of the exchange.
        routing_key (str): The routing key to use for publishing.
        message_body (str): The message body in JSON format.
        properties (pika.BasicProperties, optional): Properties for the message. Defaults to None.

    Raises:
        EventBusError: If the message body is not valid JSON or there's a publishing error.
    """

    if not validate_json(message_body):
        raise EventBusError("InvalidJSON", "Invalid JSON message body")

    if properties is None:
        properties = pika.BasicProperties(content_type="application/json")

    try:
        channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message_body, properties=properties)

    except pika.exceptions.NackError as e:
        raise EventBusError("PublishError", f"Message publishing failed: {e}")


def publish_message_with_response(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    exchange_name: str,
    routing_key: str,
    message_body: str,
    response_key: str,
    properties: pika.spec.BasicProperties = None,
):
    """Publishes a message specifiying a response queue and returns the request identifying correlation id.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for publishing and consuming.
        exchange_name (str): The exchange to publish to.
        routing_key (str): The routing key for the message.
        message_body (str): The message body in JSON format.
        response_key (str): The routing key to be used for the response.
        properties (pika.BasicProperties, optional): Properties for the message. Defaults to None.

    Returns:
        corr_id (str): The uuid for the message queued that can be used to identify response.

    Raises:
        EventBusError: If the message body is not valid JSON or there's a publishing error.
    """

    if not validate_json(message_body):
        raise EventBusError("InvalidJSON", "Invalid JSON message body")

    corr_id = str(uuid.uuid4())

    if properties is None:
        properties = pika.BasicProperties(content_type="application/json")

    try:
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=response_key,
                correlation_id=corr_id,
            ),
            body=message_body,
        )
    except pika.exceptions.NackError as e:
        raise EventBusError("PublishError", f"Message publishing failed: {e}")

    return corr_id


def consume_messages(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    callback_function: Callable[
        [pika.adapters.blocking_connection.BlockingChannel, pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes],
        None,
    ],
    auto_ack: bool = True,
) -> None:
    """Consumes messages from a queue.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for consuming.
        queue_name (str): The name of the queue.
        callback_function (function): The function to call when a message is received.
        auto_ack (bool): Whether to auto-acknowledge the messages after fetching.
    """

    channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=auto_ack)
    channel.start_consuming()


def consume_multiple_messages(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue_callbacks: dict[
        str, Callable[[pika.channel.Channel, pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes], None]
    ],
    auto_ack: bool = True,
) -> None:
    """Consumes messages from multiple queues with specified callbacks.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to use for consuming.
        queue_callbacks (dict): A dictionary mapping queue names to their callback functions.
            - key: name of the queue
            - value: callback function
        auto_ack (bool): Whether to auto-acknowledge the messages after fetching.

    """

    for queue_name, callback_function in queue_callbacks.items():
        channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=auto_ack)

    logger.info(f"Consuming messages from queues: {list(queue_callbacks.keys())}")
    channel.start_consuming()


def close_consumer(channel: pika.adapters.blocking_connection.BlockingChannel):
    """Stops consuming messages and closes the RabbitMQ channel.

    Args:
        channel (pika.adapters.blocking_connection.BlockingChannel): The channel to close.
    """
    if channel.is_open:  # Check if already closed
        try:
            channel.stop_consuming()
        except Exception as e:
            # Log the error if stopping consumption fails
            logger.exception("Error stopping consumer", extra={"error": str(e)})
        finally:
            channel.close()
            logger.debug("RabbitMQ channel closed.")


def validate_json(message_body):
    try:
        json.loads(message_body)
        return True
    except ValueError:
        return False
