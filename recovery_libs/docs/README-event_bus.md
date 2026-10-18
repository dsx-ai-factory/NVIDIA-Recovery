# RabbitMQ Eventing Library

A Python library for simplifying event-driven communication using RabbitMQ.

## Features

* **Publishing Events:** Easily publish events to RabbitMQ exchanges.
* **Consuming Events:** Consume messages from RabbitMQ queues with callback functions.
* **Request-Response (RPC):**  Implement request-response patterns for synchronous communication.
* **Logging:**  Integrated logging for debugging and monitoring.

## Basic Usage
1. Make sure RabbitMQ is installed and running on the server:
https://www.rabbitmq.com/docs/download
2. Establish Connection

In RabbitMQ, which follows the Advanced Message Queuing Protocol (AMQP), a channel is a virtual connection inside a real communication connection like TCP between your application (client) and the message broker (server).

```Python
import rabbit_eventing as re

channel = re.connect("hostname.subdomain.domain")
```
3. Publish an Event

 An exchange is a routing mechanism that receives messages and routes them to queues based on certain rules known as bindings. Here the routing is keyed by `order.new`.

To publish an event, we need an exchange (`my_events`) and a routing key (`order.new`).

```Python
re.declare_exchange(channel, "my_events")
re.publish_message(channel, "my_events", "order.new", json.dumps({"event_type": "order_placed", "order_id": 12345}))
```
4. Consume an Event

A queue named `order_queue` is declared, which stores messages until they are consumed by a consuming application. Next, a binding between the `order_queue` and the `my_events` exchange with the routing key `order.new` . This allows messages published to this exchange and key to be routed to the `order_queue`

```Python
def process_order(ch, method, properties, body):
    # Process order event
    pass


re.declare_queue(channel, "order_queue")
re.declare_exchange(channel, "my_events")
re.bind_queue(channel, "order_queue", "my_events", "order.new")
re.consume_messages(channel, "order_queue", process_order)
```
### Request-Response Example
```Python
response = re.publish_message_with_response(
    channel, "rpc_requests", "get_status", json.dumps({"order_id": 12345}), "rpc_response"
)
print(response)
```
