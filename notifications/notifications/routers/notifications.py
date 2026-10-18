# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import ValidationInfo, field_validator
from sqlalchemy import func
from sqlmodel import desc, select

from notifications import sending
from notifications.dependencies import SessionDep
from notifications.models.base import BaseModel
from notifications.models.notification_types import NotificationType
from notifications.models.notifications import (
    NotificationCreate,
    NotificationOrm,
    NotificationPublic,
    NotificationStatus,
)
from notifications.models.preferences import UserPreferencesOrm, UserPreferencesPut
from notifications.models.wrappers import Items
from notifications.prometheus.metrics import (
    CREATE_NOTIFICATION_API_COUNTER,
    GET_NOTIFICATION_API_COUNTER,
)
from notifications.utils.config import Config

router = APIRouter(tags=["notifications"])


class NotificationsFilterParams(BaseModel):
    id: str | None = None
    """The id of the notification."""

    recipient: str | None = None
    """The recipient the notifications were sent to."""

    job_id: str | None = None
    """The job_id associated with the job."""

    limit: int = 50
    """The maximum number of records to return."""

    offset: int = 0
    """The offset to start the records from."""

    created_after: datetime | None = None
    """Only show notifications created after this time."""

    created_before: datetime | None = None
    """Only show notifications created before this time."""

    @field_validator("created_after", "created_before", mode="after")
    @classmethod
    def validate_timestamps_are_utc(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        if value and value.strftime("%Z") != "UTC":
            raise ValueError(f"{info.field_name!r} must be in UTC, got tz={value.strftime('%Z')!r}")
        return value

    @field_validator("created_before", mode="after")
    @classmethod
    def validate_created_before_and_created_after_order(
        cls, value: datetime | None, info: ValidationInfo
    ) -> datetime | None:
        created_after = info.data.get("created_after")
        has_both = bool(value and created_after)
        if has_both and value is not None and created_after is not None and value < created_after:
            raise ValueError("'created_before' must come after 'created_after'.")
        return value


@router.post(
    "/notification",
    summary="Create a new notification",
    status_code=201,
)
async def notification_create(
    session: SessionDep, notification_create: NotificationCreate, background_tasks: BackgroundTasks
) -> NotificationPublic:
    CREATE_NOTIFICATION_API_COUNTER.inc()

    notification_dict = notification_create.model_dump()
    if notification_create.template_data:
        notification_dict["title"] = notification_create.template_data.render_title()
        notification_dict["message"] = notification_create.template_data.render_message()

    if notification_create.type is None:
        if notification_create.template_data:
            notification_dict["type"] = notification_create.template_data.root.DEFAULT_TYPE
        else:
            notification_dict["type"] = NotificationType.info

    notification = NotificationOrm.model_validate(notification_dict)

    preferences = session.get(UserPreferencesOrm, notification.recipient) or UserPreferencesPut()
    type_enabled = getattr(preferences.notification_types, notification.type)

    if type_enabled and Config.CALLBACK_ENABLED.upper() == "TRUE":
        notification.via_callback = NotificationStatus.PENDING

    session.add(notification)
    session.commit()
    session.refresh(notification)
    background_tasks.add_task(sending.send_notifications, notification)
    return notification


@router.get(
    "/notifications",
    summary="Get notifications",
)
async def notifications_get(
    session: SessionDep,
    filter_query: Annotated[NotificationsFilterParams, Query()],
) -> Items[NotificationPublic]:
    GET_NOTIFICATION_API_COUNTER.inc()

    def _modify_sql_query(query):
        if filter_query.id:
            query = query.where(NotificationOrm.id == uuid.UUID(filter_query.id))
        if filter_query.recipient:
            query = query.where(NotificationOrm.recipient == filter_query.recipient)
        if filter_query.job_id:
            query = query.where(NotificationOrm.job_id == filter_query.job_id)
        if filter_query.created_before:
            query = query.where(NotificationOrm.created_time < filter_query.created_before)
        if filter_query.created_after:
            query = query.where(NotificationOrm.created_time >= filter_query.created_after)
        return query

    count_query = select(func.count()).select_from(NotificationOrm)
    count_query = _modify_sql_query(count_query)
    total_items = session.scalar(count_query) or 0

    select_query = select(NotificationOrm).limit(filter_query.limit).offset(filter_query.offset)
    select_query = _modify_sql_query(select_query)
    select_query = select_query.order_by(desc(NotificationOrm.created_time))
    return Items[NotificationPublic](items=session.scalars(select_query).all(), total_items=total_items)
