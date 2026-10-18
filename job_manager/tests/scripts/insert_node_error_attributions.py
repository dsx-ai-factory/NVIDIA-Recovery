#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from collections.abc import Sequence
from datetime import UTC, datetime

import fire
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)
from recovery_libs.storage.node_storage import NodeStorage

_LOG_LEVEL = logging.getLevelNamesMapping()[os.environ.get("LOG_LEVEL", "INFO")]
logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s - [thread_id: %(thread)d] - %(name)s - %(levelname)s - %(message)s",
    force=True,
)


def get_dummy_attributions() -> tuple[JobExecutionError, Sequence[NodeEvent]]:
    """
    Get a list of dummy error attributions. This is temporary to allow the developemnt of the Web UI.
    In the next MR this function will be moved to the unit tests.
    """
    error = JobExecutionError.create(
        code=1234,
        message="error message",
        description="error description",
        signature="error signature",
    )
    node_events = []
    for i in range(10):
        node_events.append(
            NodeEvent.create(
                node=f"node{i}",
                timestamp=datetime.now(UTC),
                job_id=f"{i + 1}",
                job_attempt_index=0,
                job_start_time=datetime.now(UTC),
                job_end_time=datetime.now(UTC),
                cluster="test cluster",
                error_attribution=ErrorAttribution.HARDWARE,
                error_attribution_type=ErrorAttributionType.FAST,
                error_attribution_origin=ErrorAttributionOrigin.FACT,
                error_attribution_sources=[ErrorAttributionSource.SYSLOGS, ErrorAttributionSource.APPLICATION_LOGS],
                exclusion_type=ExclusionType.TEMPORARY,
                fact_status=FactStatus.OK,
            )
        )
    return error, node_events


def insert_node_attributions(
    host: str = "localhost",
    port: int = 3306,
    username: str = "aps_test",
    password: str = "aps_test",
    database_name: str = "aps_workflow_db",
):
    """Insert node attributions into the database"""
    connection = MySQLConnection(
        host=host,
        port=port,
        username=username,
        password=password,
        database_name=database_name,
        echo=True,
    )
    storage = NodeStorage(connection)
    storage.bootstrap()

    error, node_events = get_dummy_attributions()
    storage.store_attribution(error, node_events)  # type: ignore[arg-type]
    num_inserted = len(node_events)
    job_ids = [node_event.job_id for node_event in node_events]
    node_names = [node_event.node for node_event in node_events]
    print(f"Inserted {num_inserted} dummy node attributions")

    attributions, _ = storage.read_attributions(job_ids=job_ids)
    print(f"Read {len(attributions)} attributions matching job ids")
    assert len(attributions) == num_inserted

    attributions, _ = storage.read_attributions(node_names=node_names)
    print(f"Read {len(attributions)} attributions matching node names")
    assert len(attributions) == num_inserted

    attributions, _ = storage.read_attributions(job_ids=job_ids, node_names=node_names)
    print(f"Read {len(attributions)} attributions matching job ids and node names")
    assert len(attributions) == num_inserted


if __name__ == "__main__":
    fire.Fire(insert_node_attributions)
