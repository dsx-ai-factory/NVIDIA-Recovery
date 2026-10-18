#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import time
import uuid

import pika
import recovery_libs.event_bus.rabbit_eventing as rabbitmq
import yaml
from recovery_libs.schema.watcher.model import Attributes, Event, event_type

# Script to publish an anomaly event to the event bus.
# Check if the script is being executed with the expected number of arguments
if len(sys.argv) != 2:
    print("Usage: python3 checkpoint_event_bus_publish.py <job_id>")
    sys.exit(1)

connection_spec = None
with open("./configs/event_bus_connection_spec.yaml", "r") as config:
    connection_spec = yaml.safe_load(config)["anomaly"]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=connection_spec["host"],
        port=connection_spec["port"],
        virtual_host=connection_spec["virtual_host"],
        credentials=pika.PlainCredentials(
            connection_spec["username"],
            connection_spec["password"],
        ),
    )
)
channel = connection.channel()
rabbitmq.declare_exchange(channel=channel, exchange_name=connection_spec["exchange"])
rabbitmq.declare_queue(channel=channel, queue_name=connection_spec["queue"])
rabbitmq.bind_queue(
    channel=channel,
    queue_name=connection_spec["queue"],
    exchange_name=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
)

event = Event(
    type=event_type.EventType.CHECKPOINTSAVE,
    timestamp=str(time.time_ns() - 10 * 60 * 1_000_000_000),
    attributes=Attributes(job_id=sys.argv[1], uuid=str(uuid.uuid4())),  # type: ignore[call-arg]
)

print("Publishing an event to the exchange.")
print(f"Checkpoint: {event.model_dump_json()}")

channel.basic_publish(
    exchange=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
    body=event.model_dump_json(),
)
