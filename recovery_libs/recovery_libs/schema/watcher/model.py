# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .. import event_type


class Attributes(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
    )

    job_id: str
    detection_time: str | None = None
    log_file_name: str | None = Field(None, alias="log.file.name")
    log_file_path: str | None = Field(None, alias="log.file.path")
    uuid: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Attributes:
        return cls.model_validate(data)


class Event(BaseModel):
    body: str | None = None
    type: event_type.EventType
    timestamp: str | None = None
    attributes: Attributes | None = None

    @property
    def job_id(self):
        return self.attributes.job_id

    @property
    def log_file_name(self):
        return self.attributes.log_file_name

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> Event:
        return cls(
            body=(str(json["body"]) if json["body"] else None),
            type=event_type.EventType(json["type"]),
            timestamp=(json["timestamp"] if json["timestamp"] else None),
            attributes=(json["attributes"] if json["attributes"] else None),
        )
