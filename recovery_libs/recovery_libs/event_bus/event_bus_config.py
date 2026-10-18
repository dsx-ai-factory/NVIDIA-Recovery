# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from dataclasses import dataclass, field
from enum import Enum


class EventBusPayload(Enum):
    # JSON payloads, these are normally sent by the watcher and other Heimdall services
    JSON = "json"
    # Logs encoded as protobuf, these are normally sent by the Open Telemetry Collector
    PROTOBUF_LOGS = "protobuf_logs"


@dataclass
class EventBusConfig:
    host: str
    username: str
    password: str = field(repr=False)
    vhost: str
    exchange_name: str
    exchange_type: str
    queue_name: str
    routing_key: str
    queue_full_timeout_sec: int
    queue_max_size_mb: int
    payload: EventBusPayload = EventBusPayload.JSON

    def __post_init__(self):
        if os.environ.get("RABBITMQ_PASSWORD"):
            self.password = os.environ.get("RABBITMQ_PASSWORD")
        if not self.username or not self.password:
            raise ValueError("Missing RMQ credentials")
        if isinstance(self.payload, str):
            self.payload = EventBusPayload(self.payload)
