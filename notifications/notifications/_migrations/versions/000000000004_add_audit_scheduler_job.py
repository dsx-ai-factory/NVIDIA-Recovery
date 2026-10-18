# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add the internal scheduler-job projection without rewriting audit history.

Revision ID: 000000000004
Revises: 000000000003
Create Date: 2026-10-06
"""

import sqlalchemy as sa
from alembic import op

revision: str = "000000000004"
down_revision: str = "000000000003"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    """Expand the schema; old writers may continue omitting the nullable column."""
    op.add_column(
        "AuditEvents",
        sa.Column("scheduler_job_id", sa.String(255, collation="utf8mb4_0900_bin"), nullable=True),
    )
    op.create_index(
        "ix_AuditEvents_scheduler_job_time_id", "AuditEvents", ["scheduler_job_id", "event_produced_at", "id"]
    )


def downgrade() -> None:
    """Remove only the projection; operational rollback keeps this schema in place."""
    op.drop_index("ix_AuditEvents_scheduler_job_time_id", table_name="AuditEvents")
    op.drop_column("AuditEvents", "scheduler_job_id")
