# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add durable DAG dependency evaluation explanations.

Revision ID: 000000000002
Revises: 000000000001
Create Date: 2026-08-18

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "000000000002"
down_revision: str | None = "000000000001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("dag_dependency", sa.Column("reason_code", sa.String(length=64), nullable=True))
    op.add_column("dag_dependency", sa.Column("reason_message", sa.Text(), nullable=True))
    op.add_column("dag_dependency", sa.Column("reason_details", sa.JSON(), nullable=True))
    op.add_column("dag_dependency", sa.Column("last_evaluated_at", sa.TIMESTAMP(), nullable=True))


def downgrade() -> None:
    op.drop_column("dag_dependency", "last_evaluated_at")
    op.drop_column("dag_dependency", "reason_details")
    op.drop_column("dag_dependency", "reason_message")
    op.drop_column("dag_dependency", "reason_code")
