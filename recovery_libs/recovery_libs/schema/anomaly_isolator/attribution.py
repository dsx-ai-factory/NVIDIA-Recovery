# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import dataclass

from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import ErrorAttributionType, NodeEvent


@dataclass
class AnomalyAttributionResult:
    """
    This class is the event bus message schema for anomaly attribution results. It's used to pass the results among
    Heimdall services. It's built based on the two SQLAlchemy dataclasses that also define schema for the database
    tables to simplify the data model conversion process.

    Node and global attributions are separated for simpler consumption - consumers can directly access
    the type they need without type checking.
    """

    job_id: str
    job_attempt_index: int
    anomaly_type: EventType
    anomaly_uuid: str
    attribution_type: ErrorAttributionType
    node_attributions: dict[JobExecutionError, list[NodeEvent]]
    global_attributions: dict[JobExecutionError, list[GlobalEvent]]

    def to_json(self) -> str:
        return json.dumps(
            {
                "job_id": self.job_id,
                "anomaly_type": self.anomaly_type.value,
                "job_attempt_index": self.job_attempt_index,
                "anomaly_uuid": self.anomaly_uuid,
                "attribution_type": self.attribution_type.value,
                "node_attributions": {
                    json.dumps(error.to_json()): [event.to_json() for event in events]
                    for error, events in self.node_attributions.items()
                },
                "global_attributions": {
                    json.dumps(error.to_json()): [event.to_json() for event in events]
                    for error, events in self.global_attributions.items()
                },
            }
        )

    @classmethod
    def from_json(cls, data: dict) -> "AnomalyAttributionResult":
        return cls(
            job_id=data["job_id"],
            job_attempt_index=data["job_attempt_index"],
            anomaly_type=EventType(data["anomaly_type"]),
            anomaly_uuid=data["anomaly_uuid"],
            attribution_type=ErrorAttributionType(data["attribution_type"]),
            node_attributions={
                JobExecutionError.from_json(json.loads(error)): [NodeEvent.from_json(event) for event in events]
                for error, events in data.get("node_attributions", {}).items()
            },
            global_attributions={
                JobExecutionError.from_json(json.loads(error)): [GlobalEvent.from_json(event) for event in events]
                for error, events in data.get("global_attributions", {}).items()
            },
        )
