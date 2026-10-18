# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add durable recovery sequences, action milestones, and Advice context.

Revision ID: 000000000003
Revises: 000000000002
Create Date: 2026-08-18
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
        "recovery_sequences",
        sa.Column("sequence_id", sa.Uuid(), nullable=False),
        sa.Column("scheduler_system", sa.String(length=32), nullable=False),
        sa.Column("scheduler_scope", sa.String(length=256), nullable=False),
        sa.Column("job_id", sa.String(length=256), nullable=False),
        sa.Column("source_attempt_index", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("traceparent", sa.String(length=128), nullable=False),
        sa.Column("tracestate", sa.String(length=512), nullable=True),
        sa.Column("trigger_type", sa.String(length=32), nullable=False),
        sa.Column("trigger_identifier", sa.String(length=256), nullable=True),
        sa.Column("triggered_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("completed_at", mysql.TIMESTAMP(fsp=6), nullable=True),
        sa.Column("created_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("updated_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.PrimaryKeyConstraint("sequence_id"),
        sa.UniqueConstraint(
            "scheduler_system",
            "scheduler_scope",
            "job_id",
            "source_attempt_index",
            "operation",
            name="uq_recovery_sequence_natural_key",
        ),
    )
    op.create_index(
        "idx_recovery_sequence_job_attempt",
        "recovery_sequences",
        ["job_id", "source_attempt_index"],
        unique=False,
    )
    op.create_index(
        "idx_recovery_sequence_status_triggered",
        "recovery_sequences",
        ["status", "triggered_at"],
        unique=False,
    )

    op.create_table(
        "recovery_milestones",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sequence_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("completes_sequence", sa.Boolean(), nullable=False),
        sa.Column("completed_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.Column("created_at", mysql.TIMESTAMP(fsp=6), nullable=False),
        sa.ForeignKeyConstraint(
            ["sequence_id"],
            ["recovery_sequences.sequence_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sequence_id", "idempotency_key", name="uq_recovery_milestone_occurrence"),
    )
    op.create_index(
        "idx_recovery_milestone_sequence_completed",
        "recovery_milestones",
        ["sequence_id", "completed_at"],
        unique=False,
    )

    op.add_column("advice", sa.Column("recovery_sequence_id", sa.Uuid(), nullable=True))
    op.add_column("advice", sa.Column("recovery_traceparent", sa.String(length=128), nullable=True))
    op.add_column("advice", sa.Column("recovery_tracestate", sa.String(length=512), nullable=True))
    op.add_column("advice", sa.Column("recovery_triggered_at", mysql.TIMESTAMP(fsp=6), nullable=True))
    op.add_column("advice", sa.Column("recovery_operation", sa.String(length=32), nullable=True))
    op.create_foreign_key(
        "fk_advice_recovery_sequence",
        "advice",
        "recovery_sequences",
        ["recovery_sequence_id"],
        ["sequence_id"],
        ondelete="SET NULL",
    )
    op.create_index("idx_advice_recovery_sequence", "advice", ["recovery_sequence_id"], unique=False)


def downgrade() -> None:
    op.drop_constraint("fk_advice_recovery_sequence", "advice", type_="foreignkey")
    op.drop_index("idx_advice_recovery_sequence", table_name="advice")
    op.drop_column("advice", "recovery_operation")
    op.drop_column("advice", "recovery_triggered_at")
    op.drop_column("advice", "recovery_tracestate")
    op.drop_column("advice", "recovery_traceparent")
    op.drop_column("advice", "recovery_sequence_id")
    op.drop_table("recovery_milestones")
    op.drop_table("recovery_sequences")
