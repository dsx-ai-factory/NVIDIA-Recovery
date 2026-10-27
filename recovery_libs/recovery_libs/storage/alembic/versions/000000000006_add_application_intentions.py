# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add durable application intentions.

Revision ID: 000000000006
Revises: 000000000005
Create Date: 2026-10-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "000000000006"
down_revision: str | None = "000000000005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the job-level application-intention store."""
    op.create_table(
        "application_intentions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("desired_state", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="uq_application_intention_job_id"),
    )
    op.create_index(
        "idx_application_intention_created_at",
        "application_intentions",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Remove application intentions without modifying job or Advice history."""
    op.drop_index("idx_application_intention_created_at", table_name="application_intentions")
    op.drop_table("application_intentions")
