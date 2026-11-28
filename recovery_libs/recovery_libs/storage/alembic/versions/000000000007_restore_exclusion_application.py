# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Restore attribution exclusion application outcomes.

Revision ID: 000000000007
Revises: 000000000006
Create Date: 2026-11-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "000000000007"
down_revision: str | None = "000000000006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Keep existing attribution outcomes unknown until fresh confirmation."""
    op.add_column(
        "node_events",
        sa.Column("exclusion_applied", sa.String(length=32), nullable=False, server_default="UNKNOWN"),
    )


def downgrade() -> None:
    """Remove application outcomes without changing attribution diagnoses."""
    op.drop_column("node_events", "exclusion_applied")
