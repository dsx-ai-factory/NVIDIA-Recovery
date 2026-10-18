# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from enum import Enum
from typing import Annotated

import pydantic
import sqlalchemy as sa
from recovery_libs.storage.type_decorators import TZDateTimeMicrosecondsType
from sqlmodel import Field

from notifications.models.base import BaseModel, JsonModelField
from notifications.models.database_types import utc_now
from notifications.models.notification_types import NotificationType
from notifications.templates.base import TemplateData


class JsonTemplateData(JsonModelField):
    """Coerce TemplateData to JSON-compatible data.

    Without this, sqlmodel/sqlalchemy will try (and fail) to put the pydantic object into the database without coercing
    it first.
    """

    pydantic_model = TemplateData


class NotificationStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILTOSEND = "FAILTOSEND"
    DISABLED = "DISABLED"


class NotificationBase(BaseModel):
    """Base schema for the notifications."""

    recipient: Annotated[str, Field(index=True)]
    """The recipient of the notification."""  # TODO: What format will this be in?

    job_id: Annotated[str | None, Field(index=True)] = None
    """The job id associated with the notification (if applicable.)"""


class NotificationCreate(NotificationBase):
    """Payload for creating a schema."""

    title: str | None = None
    """The title of the notification."""

    message: str | None = None
    """The notification's message body."""

    template_data: TemplateData | None = None
    """The template id and data used to generate this notification."""

    type: NotificationType | None = None
    """The type of notification."""

    @pydantic.model_validator(mode="after")
    def cannot_use_title_and_message_with_template_data(self):
        using_template_data = self.template_data is not None
        if not using_template_data:
            if not (self.title and self.message):
                raise ValueError("Must supply either 'template_data' or 'title' and 'message'.")
        elif self.title or self.message:
            raise ValueError("'title' and 'message' cannot be used with 'template_data'.")
        return self


class NotificationPublic(NotificationBase):
    """A fully formed notification object."""

    id: Annotated[uuid.UUID, Field(default_factory=uuid.uuid4, primary_key=True)]
    """The unique id of the notification."""

    title: str
    """The title of the notification."""

    message: Annotated[str, Field(sa_type=sa.String(10000))]
    """The notification's message body."""

    template_data: Annotated[TemplateData | None, Field(sa_type=JsonTemplateData)] = None
    """The template id and data used to generate this notification."""

    type: NotificationType
    """The type of notification."""

    created_time: Annotated[
        datetime,
        Field(default_factory=utc_now, sa_column=sa.Column(TZDateTimeMicrosecondsType(), index=True, nullable=False)),
    ]
    """When was the notification created."""

    via_callback: NotificationStatus = NotificationStatus.DISABLED
    """The current status of this notification being sent via http request callback."""


class NotificationOrm(NotificationPublic, table=True):
    __tablename__ = "Notifications"
