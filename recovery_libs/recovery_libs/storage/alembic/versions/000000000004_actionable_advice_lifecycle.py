# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add Advice generations, freshness, and status history.

Revision ID: 000000000004
Revises: 000000000003
Create Date: 2026-09-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "000000000004"
down_revision: str | None = "000000000003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("advice", sa.Column("generation", sa.Integer(), nullable=False, server_default="1"))
    op.add_column("advice", sa.Column("desired_state_changed_at", mysql.TIMESTAMP(fsp=6), nullable=True))
    op.execute("UPDATE advice SET desired_state_changed_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP(6))")
    op.alter_column(
        "advice",
        "desired_state_changed_at",
        existing_type=mysql.TIMESTAMP(fsp=6),
        nullable=False,
    )
    op.create_table(
        "advice_reconciliation_status",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("advice_id", sa.Uuid(), nullable=False),
        sa.Column("observed_generation", sa.Integer(), nullable=False),
        sa.Column("disposition", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("observed_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("transitioned_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("diagnostic_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["advice_id"], ["advice.heimdall_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "advice_id",
            "observed_generation",
            name="uq_advice_status_generation",
        ),
    )

    # Remove the compatibility default after existing rows have been backfilled.
    op.alter_column("advice", "generation", server_default=None)


def downgrade() -> None:
    op.drop_table("advice_reconciliation_status")
    op.drop_column("advice", "desired_state_changed_at")
    op.drop_column("advice", "generation")
