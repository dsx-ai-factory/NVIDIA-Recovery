# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Heimdall OSS notifications baseline

Revision ID: 000000000002
Revises:
Create Date: 2026-08-07 16:17:44.217589

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = "000000000002"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "Notifications",
        sa.Column("recipient", sa.String(length=255), nullable=False),
        sa.Column("job_id", sa.String(length=255), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("message", sa.String(length=10000), nullable=False),
        sa.Column("template_data", sa.JSON(), nullable=True),
        sa.Column(
            "type", sa.Enum("optional", "info", "performance_alert", "warning", name="notificationtype"), nullable=False
        ),
        sa.Column("created_time", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column(
            "via_callback",
            sa.Enum("PENDING", "SENT", "FAILTOSEND", "DISABLED", name="notificationstatus"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_Notifications_created_time"), "Notifications", ["created_time"], unique=False)
    op.create_index(op.f("ix_Notifications_job_id"), "Notifications", ["job_id"], unique=False)
    op.create_index(op.f("ix_Notifications_recipient"), "Notifications", ["recipient"], unique=False)
    op.create_table(
        "UserPreferences",
        sa.Column(
            "notification_types",
            sa.JSON(),
            server_default=sa.text('(\'{"optional": false,"info": true,"performance_alert": true,"warning": true}\')'),
            nullable=False,
        ),
        sa.Column("user", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("user"),
    )


def downgrade() -> None:
    op.drop_table("UserPreferences")
    op.drop_table("Notifications")
