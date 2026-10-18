# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for main.py - LogQueryPoller integration."""

import asyncio
import os
import threading
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from recovery_libs.audit import AuditBuffer, AuditEventBuffer

os.environ.setdefault("CONFIG_PATH", str(Path(__file__).parent / "fixtures" / "watcher_config.yaml"))
import watcher.main as main_module
from watcher.main import (
    _prepare_log_query_poller_runtime,
    _shutdown_log_query_poller,
    _stop_log_query_poller_on_owner,
    poller_workflow_callback,
    start_log_query_poller,
)
from watcher.models.config import (
    DatasourcesConfig,
    LogQueryConfig,
    LogQueryPolicyConfig,
    RabbitMQConfig,
    RabbitMQPublisherConfig,
    RuleAttributesConfig,
    RuleConfig,
    RuleWorkflowConfig,
    WatcherConfig,
)
from watcher.models.workflow import WorkflowStepType
from watcher.workflow import RulesEngine


@pytest.fixture
def mock_config() -> WatcherConfig:
    return WatcherConfig(
        datasources=DatasourcesConfig(
            log_query_policy=LogQueryPolicyConfig(query_max_lookback=15, query_limit=1000),
            log_query=LogQueryConfig.from_json(
                {
                    "providers": {
                        "loki": {
                            "url": "http://localhost:3100",
                            "queries": {"workflows": {"test_query": {"query": ['{job="test"}']}}},
                        }
                    }
                }
            ),
            rabbitmq=RabbitMQConfig(
                publisher=RabbitMQPublisherConfig(
                    host="localhost", username="user", password="pass", vhost="/", exchange="test"
                ),
            ),
        ),
        rules=[
            RuleConfig(
                name="test",
                workflow=[
                    RuleWorkflowConfig(
                        name="test_query",
                        type=WorkflowStepType.log_query,
                        attributes=RuleAttributesConfig(),
                    )
                ],
            )
        ],
    )


@pytest.mark.asyncio
async def test_poller_workflow_callback(mock_config):
    """Test poller_workflow_callback executes workflow."""
    with (
        patch.object(RulesEngine, "execute_workflow", new_callable=AsyncMock) as mock_exec,
        patch("watcher.main.ActionCache"),
    ):
        await poller_workflow_callback(
            {"name": "test", "type": "log_query"},
            datetime(2024, 1, 1),
            datetime(2024, 1, 1, 0, 1),
            MagicMock(),
            MagicMock(),
            {},
            MagicMock(),
            mock_config,
        )
        mock_exec.assert_called_once()


def test_start_log_query_poller_creates_poller(mock_config):
    """Test start_log_query_poller creates LogQueryPoller."""
    mock_loop = MagicMock(run_forever=MagicMock(side_effect=KeyboardInterrupt))
    mock_poller = MagicMock()
    with (
        patch.multiple(
            "watcher.main",
            create_log_query_client=MagicMock(return_value=MagicMock()),
            _create_rabbitmq_publisher=MagicMock(return_value=MagicMock()),
            LogQueryPoller=MagicMock(return_value=mock_poller),
            log_query_state_store={},
            typed_config=mock_config,
            rabbitmq_publisher_config=MagicMock(),
        ),
        patch("asyncio.new_event_loop", return_value=mock_loop),
        patch("asyncio.set_event_loop"),
    ):
        start_log_query_poller()
        # Verify poller created with correct interval
        from watcher.main import LogQueryPoller

        assert LogQueryPoller.call_args[1]["poll_interval_secs"] == 15


def test_start_log_query_poller_handles_rabbitmq_failure(mock_config):
    """Test start_log_query_poller handles RabbitMQ failure."""
    log_query_client = MagicMock()
    log_query_client.close = AsyncMock()
    with patch.multiple(
        "watcher.main",
        create_log_query_client=MagicMock(return_value=log_query_client),
        _create_rabbitmq_publisher=MagicMock(side_effect=Exception("Fail")),
        log_query_state_store={},
        typed_config=mock_config,
        rabbitmq_publisher_config=MagicMock(),
    ):
        start_log_query_poller()  # Should not raise

    log_query_client.close.assert_awaited_once_with()


def test_start_log_query_poller_closes_backend_when_loop_setup_fails(mock_config):
    """An owned backend is closed if setup fails after publisher creation."""

    log_query_client = MagicMock()
    log_query_client.close = AsyncMock()
    publisher = MagicMock()
    publisher.connection.is_closed = False
    with (
        patch.multiple(
            "watcher.main",
            create_log_query_client=MagicMock(return_value=log_query_client),
            _create_rabbitmq_publisher=MagicMock(return_value=publisher),
            log_query_state_store={},
            typed_config=mock_config,
            rabbitmq_publisher_config=MagicMock(),
        ),
        patch("asyncio.new_event_loop", side_effect=RuntimeError("loop setup failed")),
        pytest.raises(RuntimeError, match="loop setup failed"),
    ):
        start_log_query_poller()

    log_query_client.close.assert_awaited_once_with()
    publisher.connection.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_poller_stop_failure_still_attempts_backend_cleanup():
    """A failed poller stop cannot bypass cleanup of its owned backend."""

    poller = MagicMock()
    poller.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
    poller.log_query_batcher.close = AsyncMock()

    with pytest.raises(RuntimeError, match="stop failed"):
        await _stop_log_query_poller_on_owner(poller)

    poller.log_query_batcher.close.assert_awaited_once_with()


def test_shutdown_stops_poller_on_owning_loop_and_joins_thread(mock_config):
    """Cross-thread shutdown stays on the worker loop and closes its backend."""

    started = threading.Event()
    log_query_client = MagicMock()
    log_query_client.close = AsyncMock()

    class LoopBoundPoller:
        def __init__(self) -> None:
            self.running = False
            self.log_query_batcher = log_query_client
            self.owner_loop = None
            self.stop_loop = None
            self.stop_thread_id = None
            self.stop_calls = 0

        async def start(self) -> None:
            self.running = True
            self.owner_loop = asyncio.get_running_loop()
            started.set()

        async def stop(self) -> None:
            if not self.running:
                return
            self.stop_calls += 1
            await asyncio.sleep(0.05)
            self.stop_loop = asyncio.get_running_loop()
            self.stop_thread_id = threading.get_ident()
            self.running = False
            await log_query_client.close()

    poller = LoopBoundPoller()
    publisher = MagicMock()
    publisher.connection.is_closed = False
    with patch.multiple(
        "watcher.main",
        create_log_query_client=MagicMock(return_value=log_query_client),
        _create_rabbitmq_publisher=MagicMock(return_value=publisher),
        LogQueryPoller=MagicMock(return_value=poller),
        log_query_state_store={},
        typed_config=mock_config,
        rabbitmq_publisher_config=MagicMock(),
    ):
        poller_thread = threading.Thread(target=start_log_query_poller, daemon=True)
        _prepare_log_query_poller_runtime()
        poller_thread.start()
        assert started.wait(timeout=5)

        shutdown_threads = [
            threading.Thread(target=_shutdown_log_query_poller, args=(poller_thread, 5)) for _ in range(2)
        ]
        for shutdown_thread in shutdown_threads:
            shutdown_thread.start()
        for shutdown_thread in shutdown_threads:
            shutdown_thread.join(timeout=5)

    assert not poller_thread.is_alive()
    assert all(not shutdown_thread.is_alive() for shutdown_thread in shutdown_threads)
    assert poller.stop_calls == 1
    assert poller.owner_loop is poller.stop_loop
    assert poller.stop_thread_id == poller_thread.ident
    log_query_client.close.assert_awaited_once_with()
    publisher.connection.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_poller_workflow_callback_passes_audit_buffer(mock_config):
    audit_event_buffer = MagicMock(spec=AuditEventBuffer)
    engine = MagicMock()
    engine.execute_workflow = AsyncMock()

    with patch("watcher.main.RulesEngine", return_value=engine) as rules_engine:
        await poller_workflow_callback(
            {"name": "test", "type": "loki_query"},
            datetime(2024, 1, 1),
            datetime(2024, 1, 1, 0, 1),
            MagicMock(),
            MagicMock(),
            {},
            MagicMock(),
            mock_config,
            audit_event_buffer,
        )

    assert rules_engine.call_args.kwargs["audit_event_buffer"] is audit_event_buffer


def test_main_starts_passes_and_stops_audit_buffer() -> None:
    audit_buffer = MagicMock(spec=AuditBuffer)
    poller_thread = MagicMock()

    with (
        patch("watcher.main.set_up_trace_provider"),
        patch("watcher.main.create_audit_buffer", return_value=audit_buffer) as create_buffer,
        patch("watcher.main.emit_app_info_metrics"),
        patch("watcher.main.threading.Thread", return_value=poller_thread),
        patch("watcher.main.time.sleep", side_effect=KeyboardInterrupt),
        patch("watcher.main._shutdown_log_query_poller"),
    ):
        main_module.main()

    audit_buffer.start.assert_called_once_with()
    create_buffer.assert_called_once_with(
        main_module.typed_config.service.audit_trail,
        logger=main_module.logger.getChild("audit"),
    )
    poller_thread.start.assert_called()
    audit_buffer.stop.assert_called_once_with()
