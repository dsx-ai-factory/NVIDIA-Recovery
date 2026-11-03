# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bind application intentions to verified requesters and ownership decisions."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "000000000007"
down_revision = "000000000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Leave legacy requests pending and non-actionable until identity is checked."""
    op.add_column(
        "application_intentions",
        sa.Column("requested_by", mysql.VARCHAR(256, collation="utf8mb4_bin"), nullable=True),
    )
    op.add_column(
        "application_intentions",
        sa.Column("authorization_status", sa.String(16), nullable=False, server_default="PENDING"),
    )
    op.add_column("application_intentions", sa.Column("reason_code", sa.String(64), nullable=True))
    op.drop_constraint("uq_application_intention_job_id", "application_intentions", type_="unique")
    op.create_unique_constraint(
        "uq_application_intention_job_requester", "application_intentions", ["job_id", "requested_by"]
    )


def downgrade() -> None:
    """Refuse a lossy downgrade when several principals requested the same job."""
    op.create_unique_constraint("uq_application_intention_job_id", "application_intentions", ["job_id"])
    op.drop_constraint("uq_application_intention_job_requester", "application_intentions", type_="unique")
    op.drop_column("application_intentions", "reason_code")
    op.drop_column("application_intentions", "authorization_status")
    op.drop_column("application_intentions", "requested_by")
