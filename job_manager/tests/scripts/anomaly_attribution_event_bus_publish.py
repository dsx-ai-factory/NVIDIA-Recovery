#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import sys
from datetime import UTC, datetime

import pika
import recovery_libs.event_bus.rabbit_eventing as rabbitmq
import yaml
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)

# Script to publish an anomaly attribution result message to rabbitmq instance running locally

# Check if the script is being executed with the expected number of arguments
if len(sys.argv) != 4:
    print("Usage: python3 event_bus_publish.py <job_id> <job_attempt_index> <anomaly_uuid>")
    sys.exit(1)

connection_spec = None
with open("./configs/event_bus_connection_spec.yaml", "r") as config:
    connection_spec = yaml.safe_load(config)["anomaly_attribution"]

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

print("Publishing an event to the exchange.")

anomaly_attribution_result = AnomalyAttributionResult(  # type: ignore[call-arg]
    job_id=sys.argv[1],
    job_attempt_index=int(sys.argv[2]),
    anomaly_type=EventType.CRASH,
    anomaly_uuid=sys.argv[3],
    error_attributions={
        JobExecutionError.create(
            code=123,
            message="Test error message",
            description="Test description",
            signature="test_signature",
        ): [
            NodeEvent.create(
                node=f"test_node_{i}",
                job_id=sys.argv[1],
                job_attempt_index=int(sys.argv[2]),
                timestamp=datetime.now(UTC),
                job_anomaly_id=sys.argv[3],  # type: ignore[arg-type]
                error_attribution=ErrorAttribution.APPLICATION,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.FACT,
                error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS, ErrorAttributionSource.SYSLOGS],
                exclusion_type=ExclusionType.TEMPORARY,
                fact_status=FactStatus.OK,
            )
            for i in range(20)
        ]
    },
)

print(f"Attribution result: {anomaly_attribution_result.to_json()}")

channel.basic_publish(
    exchange=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
    body=anomaly_attribution_result.to_json(),
)
