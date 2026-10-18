# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Remove tracking for retired on-demand job update requests.

Revision ID: 000000000005
Revises: 000000000004
Create Date: 2026-09-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "000000000005"
down_revision: str | None = "000000000004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Discard obsolete request tracking while retaining job execution records."""
    op.drop_column("job_execution_records", "pending_on_demand_job_update_response")
    op.drop_column("job_execution_records", "on_demand_job_update_requested_at")


def downgrade() -> None:
    """Restore nullable tracking columns; discarded request state is not recoverable."""
    op.add_column(
        "job_execution_records", sa.Column("pending_on_demand_job_update_response", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "job_execution_records", sa.Column("on_demand_job_update_requested_at", sa.TIMESTAMP(), nullable=True)
    )
