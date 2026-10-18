# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Heimdall OSS storage baseline

Revision ID: 000000000001
Revises:
Create Date: 2026-08-07 16:17:36.486716

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = "000000000001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dag_dependency",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("dependent_job_id", sa.String(length=64), nullable=False),
        sa.Column("dependent_job_action", sa.String(length=32), nullable=False),
        sa.Column("dependency_job_identifier", sa.String(length=256), nullable=False),
        sa.Column("dependency_job_identifier_type", sa.String(length=16), nullable=False),
        sa.Column("dependency_trigger_condition", sa.String(length=32), nullable=False),
        sa.Column("action_status", sa.String(length=32), nullable=False),
        sa.Column("action_time", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "dependent_job_id",
            "dependency_job_identifier",
            "dependency_job_identifier_type",
            name="uq_dag_dep_job_identifier",
        ),
    )
    op.create_index("idx_dag_dep_action_status", "dag_dependency", ["action_status"], unique=False)
    op.create_index("idx_dag_dep_dependent_job_id", "dag_dependency", ["dependent_job_id"], unique=False)
    op.create_index("idx_dag_dep_identifier", "dag_dependency", ["dependency_job_identifier"], unique=False)
    op.create_table(
        "job_execution_errors",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.Integer(), nullable=False),
        sa.Column("message", sa.String(length=1024), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=False),
        sa.Column("signature", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("code_index", "job_execution_errors", ["code"], unique=False)
    op.create_index("signature_index", "job_execution_errors", ["signature"], unique=False)
    op.create_table(
        "job_execution_records",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("heimdall_job_id", sa.Uuid(), nullable=True),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("job_attempt_index", sa.Integer(), nullable=False),
        sa.Column("job_group_id", sa.String(length=64), nullable=True),
        sa.Column(
            "workflow_id", mysql.VARCHAR(charset="utf8mb4", collation="utf8mb4_0900_bin", length=256), nullable=False
        ),
        sa.Column("job_name", sa.String(length=256), nullable=True),
        sa.Column("state", sa.String(length=32), nullable=True),
        sa.Column("state_reason", sa.String(length=64), nullable=True),
        sa.Column("submit_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("start_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("end_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("cluster", sa.String(length=64), nullable=True),
        sa.Column("partition", sa.String(length=64), nullable=True),
        sa.Column("reservation", sa.String(length=64), nullable=True),
        sa.Column("namespace", sa.String(length=256), nullable=True),
        sa.Column("worker_cluster_name", sa.String(length=64), nullable=True),
        sa.Column("user", sa.String(length=32), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("command", sa.Text(), nullable=True),
        sa.Column("dependency", sa.String(length=64), nullable=True),
        sa.Column("total_task_count", sa.Integer(), nullable=True),
        sa.Column("node_count", sa.Integer(), nullable=True),
        sa.Column("allocated_node_list", sa.JSON(), nullable=True),
        sa.Column("inclusion_node_list", sa.JSON(), nullable=True),
        sa.Column("exclusion_node_list", sa.JSON(), nullable=True),
        sa.Column("sticky_exclusion_node_list", sa.JSON(), nullable=True),
        sa.Column("failed_node_list", sa.JSON(), nullable=True),
        sa.Column("total_cpu_count", sa.Integer(), nullable=True),
        sa.Column("total_memory_mb", sa.Integer(), nullable=True),
        sa.Column("total_gpu_count", sa.Integer(), nullable=True),
        sa.Column("scheduler_metadata", sa.JSON(), nullable=True),
        sa.Column("anomalies", sa.JSON(), nullable=True),
        sa.Column("checkpoints", sa.JSON(), nullable=True),
        sa.Column("aps_spec", sa.JSON(), nullable=True),
        sa.Column("config_validation_failure_types", sa.JSON(), nullable=True),
        sa.Column("last_state_transition_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("last_updated_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("pending_on_demand_job_update_response", sa.Boolean(), nullable=True),
        sa.Column("on_demand_job_update_requested_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("attribution_request_emitted", sa.Boolean(), nullable=False),
        sa.Column("retry_of", sa.String(length=64), nullable=True),
        sa.Column("log_file_paths", sa.JSON(), nullable=True),
        sa.CheckConstraint("CHAR_LENGTH(workflow_id) > 0", name="ck_job_execution_records_workflow_id_nonempty"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("heimdall_job_id"),
    )
    op.create_index("end_time_index", "job_execution_records", ["end_time"], unique=False)
    op.create_index("heimdall_job_id_index", "job_execution_records", ["heimdall_job_id"], unique=False)
    op.create_index("ix_job_execution_records_workflow_id", "job_execution_records", ["workflow_id"], unique=False)
    op.create_index("job_group_id_index", "job_execution_records", ["job_group_id"], unique=False)
    op.create_index(
        "job_id_attempt_unique_index", "job_execution_records", ["job_id", "job_attempt_index"], unique=True
    )
    op.create_index("job_name_index", "job_execution_records", ["job_name"], unique=False)
    op.create_index("job_state_index", "job_execution_records", ["state"], unique=False)
    op.create_index("start_time_index", "job_execution_records", ["start_time"], unique=False)
    op.create_index("submit_time_index", "job_execution_records", ["submit_time"], unique=False)
    op.create_index("user_index", "job_execution_records", ["user"], unique=False)
    op.create_table(
        "key_value_cache",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("key", sa.String(length=256), nullable=False),
        sa.Column("value", sa.String(length=2048), nullable=False),
        sa.Column("timestamp", sa.TIMESTAMP(), nullable=False),
        sa.Column("ttl_secs", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("key_index", "key_value_cache", ["key"], unique=False)
    op.create_index("timestamp_index", "key_value_cache", ["timestamp"], unique=False)
    op.create_table(
        "advice",
        sa.Column("heimdall_id", sa.Uuid(), nullable=False),
        sa.Column("visible", sa.Boolean(), nullable=False),
        sa.Column("job_id", sa.String(length=256), nullable=False),
        sa.Column("scheduler_metadata", sa.JSON(), nullable=True),
        sa.Column("attempt_index", sa.Integer(), nullable=False),
        sa.Column("advice_state", sa.String(length=32), nullable=True),
        sa.Column("node_exclusions", sa.JSON(), nullable=True),
        sa.Column("node_inclusions", sa.JSON(), nullable=True),
        sa.Column("issues", sa.JSON(), nullable=True),
        sa.Column("fact_attribution", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(
            ["heimdall_id"], ["job_execution_records.heimdall_job_id"], name="fk_advice_heimdall_job_id"
        ),
        sa.PrimaryKeyConstraint("heimdall_id"),
        sa.UniqueConstraint("job_id", "attempt_index", name="uq_advice_job_attempt"),
    )
    op.create_index("idx_advice_created_at", "advice", ["created_at"], unique=False)
    op.create_index("idx_advice_job_id", "advice", ["job_id"], unique=False)
    op.create_index("idx_advice_visible", "advice", ["visible"], unique=False)
    op.create_table(
        "global_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_execution_error_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.TIMESTAMP(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("job_attempt_index", sa.Integer(), nullable=False),
        sa.Column("job_anomaly_id", sa.Uuid(), nullable=True),
        sa.Column("error_attribution", sa.Integer(), nullable=False),
        sa.Column("error_attribution_type", sa.Integer(), nullable=False),
        sa.Column("error_attribution_origin", sa.Integer(), nullable=False),
        sa.Column("error_attribution_sources", sa.JSON(), nullable=False),
        sa.Column("fact_status", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_execution_error_id"],
            ["job_execution_errors.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("global_event_job_anomaly_id_index", "global_events", ["job_anomaly_id"], unique=False)
    op.create_index(
        "global_event_job_execution_error_id_index", "global_events", ["job_execution_error_id"], unique=False
    )
    op.create_index("global_event_job_id_attempt_index", "global_events", ["job_id", "job_attempt_index"], unique=False)
    op.create_index("global_event_timestamp_index", "global_events", ["timestamp"], unique=False)
    op.create_table(
        "job_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_record_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("job_attempt_index", sa.Integer(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("time", sa.TIMESTAMP(), nullable=False),
        sa.Column("log_file_name", sa.String(length=256), nullable=True),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_record_id"],
            ["job_execution_records.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("event_time_index", "job_events", ["time"], unique=False)
    op.create_index("event_type_index", "job_events", ["type"], unique=False)
    op.create_index("job_id_attempt_index", "job_events", ["job_id", "job_attempt_index"], unique=False)
    op.create_table(
        "node_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_execution_error_id", sa.Integer(), nullable=False),
        sa.Column("node", sa.String(length=255), nullable=False),
        sa.Column("timestamp", sa.TIMESTAMP(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("job_attempt_index", sa.Integer(), nullable=False),
        sa.Column("job_start_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("job_end_time", sa.TIMESTAMP(), nullable=True),
        sa.Column("job_anomaly_id", sa.Uuid(), nullable=True),
        sa.Column("cluster", sa.String(length=64), nullable=False),
        sa.Column("error_attribution", sa.Integer(), nullable=False),
        sa.Column("error_attribution_type", sa.Integer(), nullable=False),
        sa.Column("error_attribution_origin", sa.Integer(), nullable=False),
        sa.Column("error_attribution_sources", sa.JSON(), nullable=False),
        sa.Column("exclusion_type", sa.Integer(), nullable=False),
        sa.Column("original_exclusion_type", sa.Integer(), nullable=False),
        sa.Column("fact_status", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_execution_error_id"],
            ["job_execution_errors.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("job_anomaly_id_index", "node_events", ["job_anomaly_id"], unique=False)
    op.create_index("job_execution_error_id_index", "node_events", ["job_execution_error_id"], unique=False)
    op.create_index("job_id_attempt_index", "node_events", ["job_id", "job_attempt_index"], unique=False)
    op.create_index("node_index", "node_events", ["node"], unique=False)
    op.create_index("timestamp_index", "node_events", ["timestamp"], unique=False)


def downgrade() -> None:
    op.drop_table("node_events")
    op.drop_table("job_events")
    op.drop_table("global_events")
    op.drop_table("advice")
    op.drop_table("key_value_cache")
    op.drop_table("job_execution_records")
    op.drop_table("job_execution_errors")
    op.drop_table("dag_dependency")
