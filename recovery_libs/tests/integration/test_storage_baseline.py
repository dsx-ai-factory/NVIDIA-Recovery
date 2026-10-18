# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from recovery_libs.storage import dataclass as storage_dataclass
from recovery_libs.storage.dataclass import (
    DagDependency,
    DagDependencyActionStatus,
    DagDependencyReasonCode,
    SlurmJobExecutionRecord,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL, Connection
from sqlalchemy.exc import SQLAlchemyError

BASELINE_REVISION = "000000000001"
DAG_OBSERVABILITY_REVISION = "000000000002"
ADVICE_LIFECYCLE_REVISION = "000000000004"
HEAD_REVISION = "000000000005"
ALEMBIC_DIRECTORY = Path(storage_dataclass.__file__).parent / "alembic"
APPLICATION_TABLES = {
    "advice",
    "advice_reconciliation_status",
    "dag_dependency",
    "global_events",
    "job_events",
    "job_execution_errors",
    "job_execution_records",
    "key_value_cache",
    "node_events",
    "recovery_milestones",
    "recovery_sequences",
}


def _database_url(db_config: dict[str, Any], database_name: str) -> URL:
    return URL.create(
        "mysql+pymysql",
        username=db_config["username"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
        database=database_name,
    )


@pytest.fixture
def migration_connection(db_config: dict[str, Any]):
    """Provide a unique database for auto-committing MySQL DDL tests."""
    database_name = f"storage_baseline_{uuid.uuid4().hex}"
    admin_engine = create_engine(_database_url(db_config, db_config["database_name"]))
    database_created = False
    database_engine = None

    try:
        try:
            with admin_engine.begin() as connection:
                connection.execute(
                    text(f"CREATE DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
                )
            database_created = True
        except SQLAlchemyError as error:
            raise RuntimeError(
                "Storage baseline tests require credentials that can create and drop a unique disposable database; "
                "the configured database was not modified"
            ) from error

        database_engine = create_engine(_database_url(db_config, database_name))
        with database_engine.connect() as connection:
            try:
                yield connection
            finally:
                connection.rollback()
    finally:
        if database_engine is not None:
            database_engine.dispose()
        if database_created:
            try:
                with admin_engine.begin() as connection:
                    connection.execute(text(f"DROP DATABASE `{database_name}`"))
            except SQLAlchemyError as error:
                raise RuntimeError(f"Failed to drop disposable migration database {database_name}") from error
        admin_engine.dispose()


def _alembic_config(connection: Connection) -> Config:
    config = Config(str(ALEMBIC_DIRECTORY / "alembic.ini"))
    config.set_main_option("script_location", str(ALEMBIC_DIRECTORY))
    config.set_main_option(
        "sqlalchemy.url",
        connection.engine.url.render_as_string(hide_password=False).replace("%", "%%"),
    )
    config.attributes["connection"] = connection
    config.attributes["configure_logger"] = False
    return config


def _upgrade(connection: Connection) -> None:
    command.upgrade(_alembic_config(connection), "head")
    connection.commit()


def test_storage_baseline_matches_metadata_and_round_trips(migration_connection: Connection) -> None:
    connection = migration_connection
    config = _alembic_config(connection)

    _upgrade(connection)

    assert set(inspect(connection).get_table_names()) == APPLICATION_TABLES | {"alembic_version"}
    assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == HEAD_REVISION
    command.check(config)
    command.current(config, check_heads=True)

    command.downgrade(config, "base")
    connection.commit()
    assert set(inspect(connection).get_table_names()) == {"alembic_version"}

    _upgrade(connection)
    assert set(inspect(connection).get_table_names()) == APPLICATION_TABLES | {"alembic_version"}


def test_dag_dependency_observability_migrates_from_baseline(migration_connection: Connection) -> None:
    connection = migration_connection
    config = _alembic_config(connection)
    observability_columns = {"reason_code", "reason_message", "reason_details", "last_evaluated_at"}

    command.upgrade(config, BASELINE_REVISION)
    connection.commit()
    baseline_columns = {column["name"] for column in inspect(connection).get_columns("dag_dependency")}
    assert observability_columns.isdisjoint(baseline_columns)

    command.upgrade(config, DAG_OBSERVABILITY_REVISION)
    connection.commit()
    upgraded_columns = {column["name"] for column in inspect(connection).get_columns("dag_dependency")}
    assert observability_columns <= upgraded_columns

    command.downgrade(config, BASELINE_REVISION)
    connection.commit()
    downgraded_columns = {column["name"] for column in inspect(connection).get_columns("dag_dependency")}
    assert observability_columns.isdisjoint(downgraded_columns)


def test_retire_on_demand_tracking_preserves_job_data(migration_connection: Connection) -> None:
    """Removing request state preserves job history, including freshness timestamps."""
    connection = migration_connection
    config = _alembic_config(connection)
    retired_columns = {"pending_on_demand_job_update_response", "on_demand_job_update_requested_at"}
    command.upgrade(config, ADVICE_LIFECYCLE_REVISION)
    connection.execute(
        text(
            "INSERT INTO job_execution_records "
            "(job_id, job_attempt_index, workflow_id, state, last_updated_at, "
            "last_state_transition_time, pending_on_demand_job_update_response, "
            "on_demand_job_update_requested_at, attribution_request_emitted) "
            "VALUES ('stale-job', 2, 'workflow-42', 'RUNNING', '2026-09-01 12:00:00', "
            "'2026-09-01 11:00:00', TRUE, '2026-09-01 12:30:00', FALSE)"
        )
    )
    connection.commit()
    original = dict(connection.execute(text("SELECT * FROM job_execution_records")).mappings().one())
    expected = {key: value for key, value in original.items() if key not in retired_columns}

    _upgrade(connection)
    assert retired_columns.isdisjoint(
        column["name"] for column in inspect(connection).get_columns("job_execution_records")
    )
    assert dict(connection.execute(text("SELECT * FROM job_execution_records")).mappings().one()) == expected

    command.downgrade(config, ADVICE_LIFECYCLE_REVISION)
    connection.commit()
    restored = dict(connection.execute(text("SELECT * FROM job_execution_records")).mappings().one())
    assert restored == expected | {column: None for column in retired_columns}
    restored_columns = {column["name"]: column for column in inspect(connection).get_columns("job_execution_records")}
    assert all(restored_columns[column]["nullable"] for column in retired_columns)

    _upgrade(connection)
    assert dict(connection.execute(text("SELECT * FROM job_execution_records")).mappings().one()) == expected
    command.check(config)


def test_dag_dependency_observability_values_round_trip_through_storage(
    migration_connection: Connection,
    db_config: dict[str, Any],
) -> None:
    connection = migration_connection
    _upgrade(connection)
    database_name = connection.engine.url.database
    assert database_name is not None

    storage_connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=database_name,
    )
    storage = MySQLStorage(storage_connection)
    evaluated_at = datetime(2026, 8, 18, 18, 42, tzinfo=UTC)
    reason_details = {
        "dependent_job_id": "child-200",
        "dependency_job_identifier": "parent-100",
        "dependency_job_identifier_type": "id",
    }

    try:
        dependency, created = storage.upsert_dag_dependency(
            DagDependency(
                dependent_job_id="child-200",
                dependent_job_action="release",
                dependency_job_identifier="parent-100",
                dependency_job_identifier_type="id",
                dependency_trigger_condition="completed",
            )
        )
        assert created is True
        assert dependency.id is not None

        assert storage.update_dag_dependency_status(
            id=dependency.id,
            action_status=DagDependencyActionStatus.NOT_ELIGIBLE.value,
            reason_code=DagDependencyReasonCode.WAITING_FOR_PARENT_COMPLETION.value,
            reason_message="Waiting for parent job parent-100 to complete",
            reason_details=reason_details,
            last_evaluated_at=evaluated_at,
        )

        dependencies, total = storage.read_dag_dependencies(dependent_job_id="child-200")
        assert total == 1
        assert len(dependencies) == 1
        stored = dependencies[0]
        assert stored.action_status == DagDependencyActionStatus.NOT_ELIGIBLE.value
        assert stored.reason_code == DagDependencyReasonCode.WAITING_FOR_PARENT_COMPLETION.value
        assert stored.reason_message == "Waiting for parent job parent-100 to complete"
        assert stored.reason_details == reason_details
        assert stored.last_evaluated_at == evaluated_at
    finally:
        storage_connection.engine.dispose()


def test_job_manager_storage_uses_exact_canonical_workflow_membership(
    migration_connection: Connection,
    db_config: dict[str, Any],
) -> None:
    connection = migration_connection
    _upgrade(connection)
    database_name = connection.engine.url.database
    assert database_name is not None

    storage_connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=database_name,
    )
    storage = MySQLStorage(storage_connection)
    updated_at = datetime(2026, 7, 7, tzinfo=UTC)

    try:
        records = (
            SlurmJobExecutionRecord(
                job_id="member-job",
                job_attempt_index=0,
                job_name="train",
                workflow_id="experiment-42",
                last_updated_at=updated_at,
            ),
            SlurmJobExecutionRecord(
                job_id="decoy-job",
                job_attempt_index=0,
                job_name="experiment-42",
                workflow_id="other",
                last_updated_at=updated_at,
            ),
            SlurmJobExecutionRecord(
                job_id="prefix-neighbor-job",
                job_attempt_index=0,
                job_name="neighbor",
                workflow_id="experiment-42-extra",
                last_updated_at=updated_at,
            ),
        )
        for record in records:
            assert storage.upsert_job_record(record)[1] is True

        workflows, workflow_count = storage.read_workflows(limit=100)
        assert workflow_count == 3
        assert {workflow.id for workflow in workflows} == {"experiment-42", "experiment-42-extra", "other"}

        exact_workflows, exact_workflow_count = storage.read_workflows(
            workflow_id="experiment-42", workflow_id_match="exact", limit=100
        )
        assert exact_workflow_count == 1
        assert [workflow.id for workflow in exact_workflows] == ["experiment-42"]

        members, member_count = storage.read_job_records(workflow_id="experiment-42", limit=100)
        assert member_count == 1
        assert [(record.job_id, record.job_name, record.workflow_id) for record in members] == [
            ("member-job", "train", "experiment-42")
        ]
    finally:
        storage_connection.engine.dispose()
