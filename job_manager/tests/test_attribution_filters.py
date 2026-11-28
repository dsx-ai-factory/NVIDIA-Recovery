# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise attribution HTTP filters against the production storage query code."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from job_manager.main import app
from recovery_libs.storage.attribution_storage import AttributionStorage
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import NodeEvent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@pytest.fixture
def attribution_client():
    """Provide node and global history for two jobs through real storage and HTTP."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    for model in (JobExecutionError, NodeEvent, GlobalEvent):
        model.__table__.create(engine)
    factory = sessionmaker(engine)
    storage = AttributionStorage(SimpleNamespace(session=factory))
    timestamp = datetime(2026, 11, 28, tzinfo=UTC)
    error = JobExecutionError(code=1, message="GPU lost", description="GPU failure", signature="gpu-lost")
    with factory.begin() as session:
        for event in [
            NodeEvent(node="n1", job_id="job-a", timestamp=timestamp),
            NodeEvent(node="n2", job_id="job-b", timestamp=timestamp),
            GlobalEvent(job_id="job-a", timestamp=timestamp),
            GlobalEvent(job_id="job-b", timestamp=timestamp),
        ]:
            event.job_execution_error = error
            session.add(event)
    try:
        with (
            patch("job_manager.main.job_manager", SimpleNamespace(attribution_storage=storage)),
            TestClient(app) as client,
        ):
            yield client
    finally:
        engine.dispose()


def test_node_only_filter_returns_matching_nodes_without_global_history(attribution_client):
    """A node selector must not fail or expose unrelated job-global events."""
    response = attribution_client.get("/error_attributions", params={"node_names": "n1"})

    assert response.status_code == 200
    result = response.json()
    assert [(event["node"], event["job_id"]) for event in result["events"]] == [("n1", "job-a")]
    assert result["global_events"] == []
    assert result["item_count"] == 1


def test_unknown_node_returns_empty_result(attribution_client):
    """No matching node is an empty success, not a failed or unfiltered global query."""
    response = attribution_client.get("/error_attributions", params={"node_names": "missing"})

    assert response.status_code == 200
    result = response.json()
    assert result["events"] == []
    assert result["global_events"] == []
    assert result["item_count"] == 0


def test_node_and_job_filters_retain_scoped_global_events(attribution_client):
    """A shared job selector still scopes global history independently of node names."""
    response = attribution_client.get("/error_attributions", params={"node_names": "n1", "job_ids": "job-a"})

    assert response.status_code == 200
    result = response.json()
    assert [event["node"] for event in result["events"]] == ["n1"]
    assert [event["job_id"] for event in result["global_events"]] == ["job-a"]
    assert result["item_count"] == 2
