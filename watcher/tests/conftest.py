# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from log_query_fakes import fake_backend, log_query_config
from recovery_libs.log_query import BackendName
from watcher.lib.plugins.log_query import LogQueryBatcher
from watcher.lib.plugins.rabbitmq import EventBusPublisher
from watcher.lib.utils.config import load_typed_config
from watcher.lib.utils.data import ActionCache
from watcher.lib.utils.logging import logger
from watcher.lib.utils.loki_stats import LOKI_DEGRADED
from watcher.models.config import WatcherConfig
from watcher.workflow import RulesEngine

WATCHER_FIXTURE_CONFIG = Path(__file__).parent / "fixtures" / "watcher_config.yaml"


@pytest.fixture
def watcher_config_path() -> Path:
    """Path to the shared watcher fixture config."""
    return WATCHER_FIXTURE_CONFIG


# Import fixtures from sub-modules
pytest_plugins = [
    "tests.fixtures.models_setup",
    "tests.fixtures.workflow_checkpoint_setup",
    "tests.fixtures.workflow_crash_setup",
    "tests.fixtures.workflow_general_setup",
    "tests.fixtures.workflow_nested_restarter_setup",
    "tests.fixtures.workflow_nooutput_setup",
    "tests.fixtures.workflow_straggler_setup",
]


# Enable log propagation for tests to support pytest's caplog fixture
@pytest.fixture(autouse=True)
def enable_log_propagation():
    """Enable log propagation for all tests to support pytest's caplog fixture."""
    original_propagate = logger.propagate
    logger.propagate = True
    yield
    logger.propagate = original_propagate


@pytest.fixture(autouse=True)
def reset_loki_degraded():
    """Reset the shared Loki degraded gauge between tests."""
    LOKI_DEGRADED.set(0)
    yield
    LOKI_DEGRADED.set(0)


# Global mock for EventBusPublisher to prevent any accidental real connections
@pytest_asyncio.fixture(autouse=True)
def mock_eventbus_publisher():
    """Automatically mock EventBusPublisher for all tests to prevent real RabbitMQ connections."""

    def create_mock_publisher(*args, **kwargs):
        mock_publisher = MagicMock(spec=EventBusPublisher)

        # Mock all the common methods
        mock_publisher.publish_message = MagicMock()
        mock_publisher.send_ping = MagicMock()
        mock_publisher.setup_connection = MagicMock()
        mock_publisher.close_connection = MagicMock()

        # Mock connection health status
        mock_publisher.connection = MagicMock()
        mock_publisher.connection.is_open = True
        mock_publisher.channel = MagicMock()
        mock_publisher.channel.is_open = True

        return mock_publisher

    with patch("watcher.lib.plugins.rabbitmq.EventBusPublisher", side_effect=create_mock_publisher):
        yield


# Config file
@pytest_asyncio.fixture
def get_watcher_config() -> WatcherConfig:
    return load_typed_config(str(WATCHER_FIXTURE_CONFIG))


@pytest_asyncio.fixture
def get_watcher_config_async() -> WatcherConfig:
    return load_typed_config(str(WATCHER_FIXTURE_CONFIG))


# Log-query consumer fixtures use the production provider configuration.
@pytest_asyncio.fixture
def log_query_client():
    return LogQueryBatcher(
        backend_factory=fake_backend,
        log_query_config=log_query_config(BackendName.LOKI),
        state_store={},
        max_lookback=60,
        query_delay=45,
        query_timeout=15,
        query_limit=1000,
    )


@pytest_asyncio.fixture
def log_query_client_async() -> AsyncGenerator[str, None]:
    return LogQueryBatcher(
        backend_factory=fake_backend,
        log_query_config=log_query_config(BackendName.LOKI),
        state_store={},
        max_lookback=60,
        query_delay=45,
        query_timeout=15,
        query_limit=1000,
    )


## RabbitMQ Client setup (Mocked for testing)
##
@pytest_asyncio.fixture
def rabbitmq_publisher():
    """Create a mock RabbitMQ publisher to avoid real connections during testing."""
    mock_publisher = MagicMock(spec=EventBusPublisher)

    # Mock all the common methods that tests use
    mock_publisher.publish_message = MagicMock()
    mock_publisher.send_ping = MagicMock()
    mock_publisher.setup_connection = MagicMock()
    mock_publisher.close_connection = MagicMock()

    # Mock connection health status
    mock_publisher.connection = MagicMock()
    mock_publisher.connection.is_open = True
    mock_publisher.channel = MagicMock()
    mock_publisher.channel.is_open = True

    return mock_publisher


@pytest_asyncio.fixture
def rabbitmq_publisher_async() -> AsyncGenerator[str, None]:
    """Create a mock async RabbitMQ publisher to avoid real connections during testing."""
    mock_publisher = MagicMock(spec=EventBusPublisher)

    # Mock all the common methods that tests use
    mock_publisher.publish_message = MagicMock()
    mock_publisher.send_ping = MagicMock()
    mock_publisher.setup_connection = MagicMock()
    mock_publisher.close_connection = MagicMock()

    # Mock connection health status
    mock_publisher.connection = MagicMock()
    mock_publisher.connection.is_open = True
    mock_publisher.channel = MagicMock()
    mock_publisher.channel.is_open = True

    return mock_publisher


# Workflow Rules Engine instance setup
@pytest_asyncio.fixture
def rules_engine(log_query_client, rabbitmq_publisher, get_watcher_config):
    action_cache = ActionCache()
    config = get_watcher_config
    kvstore = {}
    return RulesEngine(
        log_query_batcher=log_query_client,
        publisher=rabbitmq_publisher,
        kvstore=kvstore,
        config=config,
        action_cache=action_cache,
    )


@pytest_asyncio.fixture
def rules_engine_async(
    log_query_client_async, rabbitmq_publisher, get_watcher_config_async
) -> AsyncGenerator[str, None]:
    action_cache = ActionCache()
    config = get_watcher_config_async
    kvstore: dict[str, Any] = {}
    return RulesEngine(  # type: ignore[return-value]
        log_query_batcher=log_query_client_async,
        publisher=rabbitmq_publisher,
        kvstore=kvstore,
        config=config,
        action_cache=action_cache,
    )


# Job Manager
@pytest_asyncio.fixture
def jm_job_status_failed():
    return json.dumps(
        {
            "job_id": "608070",
            "job_attempt_index": 0,
            "job_name": "8B_hybrid_15T_phase2_final",
            "state": "FAILED",
            "state_reason": "NonZeroExitCode",
            "submit_time": "2024-11-02T04:47:36Z",
            "start_time": "2024-11-02T08:39:28Z",
            "end_time": "2024-11-02T09:05:52Z",
            "cluster": "test-cluster-03",
            "partition": "batch_large_long",
            "reservation": "sla_res_adlr_2k",
            "user": "user1",
            "command": "../8b_hybrid/scripts/8B_hybrid_15T_phase2_final.sh",
            "dependency": "",
            "node_count": 256,
            "total_gpu_count": 2048,
            "total_cpu_count": 2048,
            "total_memory_mb": 515000000,
            "allocated_node_list": ["test-node-069"],
            "anomalies": [
                {
                    "anomaly_type": "HANGNOOUTPUT",
                    "timestamp": "2024-11-02T09:10:40.909916Z",
                    "uuid": "0192ec20-2fb4-7482-b0db-0c39371a5b05",
                    "processed": False,
                    "isolation_result_json": None,
                }
            ],
            "checkpoints": [
                {"timestamp": "2024-11-02T08:44:52.559116Z", "uuid": "0192ec0d-a041-7de9-8693-9913f5394353"}
            ],
            "aps_spec": None,
            "retry_of": None,
            "last_updated_at": "2024-11-02T09:11:03Z",
        }
    )


@pytest_asyncio.fixture
def jm_job_status_completed() -> AsyncGenerator[str, None]:
    return json.dumps(  # type: ignore[return-value]
        {
            "job_id": "608070",
            "job_attempt_index": 0,
            "job_name": "8B_hybrid_15T_phase2_final",
            "state": "COMPLETED",
            "state_reason": "NonZeroExitCode",
            "submit_time": "2024-11-02T04:47:36Z",
            "start_time": "2024-11-02T08:39:28Z",
            "end_time": "2024-11-02T09:05:52Z",
            "cluster": "test-cluster-03",
            "partition": "batch_large_long",
            "reservation": "sla_res_adlr_2k",
            "user": "user1",
            "command": "../8b_hybrid/scripts/8B_hybrid_15T_phase2_final.sh",
            "dependency": "",
            "node_count": 256,
            "total_gpu_count": 2048,
            "total_cpu_count": 2048,
            "total_memory_mb": 515000000,
            "allocated_node_list": ["test-node-069"],
            "anomalies": [
                {
                    "anomaly_type": "HANGNOOUTPUT",
                    "timestamp": "2024-11-02T09:10:40.909916Z",
                    "uuid": "0192ec20-2fb4-7482-b0db-0c39371a5b05",
                    "processed": False,
                    "isolation_result_json": None,
                }
            ],
            "checkpoints": [
                {"timestamp": "2024-11-02T08:44:52.559116Z", "uuid": "0192ec0d-a041-7de9-8693-9913f5394353"}
            ],
            "aps_spec": None,
            "retry_of": None,
            "last_updated_at": "2024-11-02T09:11:03Z",
        }
    )


@pytest_asyncio.fixture
def jm_job_status_running() -> AsyncGenerator[str, None]:
    return json.dumps(  # type: ignore[return-value]
        {
            "job_id": "608070",
            "job_attempt_index": 0,
            "job_name": "8B_hybrid_15T_phase2_final",
            "state": "RUNNING",
            "state_reason": "NonZeroExitCode",
            "submit_time": "2024-11-02T04:47:36Z",
            "start_time": "2024-11-02T08:39:28Z",
            "end_time": "2024-11-02T09:05:52Z",
            "cluster": "test-cluster-03",
            "partition": "batch_large_long",
            "reservation": "sla_res_adlr_2k",
            "user": "user1",
            "command": "../8b_hybrid/scripts/8B_hybrid_15T_phase2_final.sh",
            "dependency": "",
            "node_count": 256,
            "total_gpu_count": 2048,
            "total_cpu_count": 2048,
            "total_memory_mb": 515000000,
            "allocated_node_list": ["test-node-069"],
            "anomalies": [
                {
                    "anomaly_type": "HANGNOOUTPUT",
                    "timestamp": "2024-11-02T09:10:40.909916Z",
                    "uuid": "0192ec20-2fb4-7482-b0db-0c39371a5b05",
                    "processed": False,
                    "isolation_result_json": None,
                }
            ],
            "checkpoints": [
                {"timestamp": "2024-11-02T08:44:52.559116Z", "uuid": "0192ec0d-a041-7de9-8693-9913f5394353"}
            ],
            "aps_spec": None,
            "retry_of": None,
            "last_updated_at": "2024-11-02T09:11:03Z",
            "log_file_paths": [
                "/home/test-service-user/otelfilelog_paths/prod/8B_hybrid_15T_phase2_final_612685_date_24-11-05_time_14-57-42.log"
            ],
            "config_validation_failure_types": [],
        }
    )
