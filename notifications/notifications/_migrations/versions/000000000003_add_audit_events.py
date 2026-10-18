# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add Notifications audit events.

Revision ID: 000000000003
Revises: 000000000002
Create Date: 2026-08-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "000000000003"
down_revision: str | None = "000000000002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "AuditEvents",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("heimdall_job_id", sa.String(length=255), nullable=False),
        sa.Column("event_producer", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=255), nullable=False),
        sa.Column(
            "event_level",
            sa.Enum("recovery", "operational", name="audit_event_level"),
            nullable=False,
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("recorded_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("event_produced_at", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_AuditEvents_idempotency_key"),
    )
    op.create_index(op.f("ix_AuditEvents_event_level"), "AuditEvents", ["event_level"], unique=False)
    op.create_index(op.f("ix_AuditEvents_event_produced_at"), "AuditEvents", ["event_produced_at"], unique=False)
    op.create_index(op.f("ix_AuditEvents_event_producer"), "AuditEvents", ["event_producer"], unique=False)
    op.create_index(op.f("ix_AuditEvents_heimdall_job_id"), "AuditEvents", ["heimdall_job_id"], unique=False)
    op.create_index(op.f("ix_AuditEvents_recorded_at"), "AuditEvents", ["recorded_at"], unique=False)


def downgrade() -> None:
    op.drop_table("AuditEvents")
