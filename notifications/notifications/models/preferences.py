# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

import sqlalchemy as sa
from sqlmodel import Field

from .base import BaseModel, JsonModelField


class NotificationTypePreferences(BaseModel):
    optional: bool = False
    info: bool = True
    performance_alert: bool = True
    warning: bool = True


class JsonNotificationTypePreferences(JsonModelField):
    pydantic_model = NotificationTypePreferences


class UserPreferencesBase(BaseModel):
    """Base schema for user preferences."""

    notification_types: Annotated[
        NotificationTypePreferences,
        Field(
            sa_type=JsonNotificationTypePreferences,
            # Make sure the current defaults are always reflected in the DB's defaults when generating migrations.
            sa_column_kwargs={
                # MySQL hack: MySQL's default JSON values must be expressions. Put our literal into an expression to
                # make MySQL happy.
                "server_default": sa.text("(:json_literal)").bindparams(
                    # Make sure there's a space after every ":", so the migration doesn't treat JSON values as params.
                    json_literal=NotificationTypePreferences().model_dump_json().replace(":", ": ")
                )
            },
        ),
    ]


class UserPreferencesPut(UserPreferencesBase):
    """Payload for creating/updating user preferences."""

    notification_types: NotificationTypePreferences = NotificationTypePreferences()


class UserPreferencesPublic(UserPreferencesBase):
    """A user's notification preferences."""


class UserPreferencesOrm(UserPreferencesPublic, table=True):
    """The ORM object for user preferences."""

    __tablename__ = "UserPreferences"
    user: Annotated[str, Field(primary_key=True)]
