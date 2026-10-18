# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query failures must survive the production callback to prevent cursor commits."""

from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from log_query_fakes import fake_backend, log_query_config
from recovery_libs.log_query import BackendName, QueryFailure, QueryFailureKind
from watcher.lib.plugins.log_query import LogQueryBatcher, LogQueryPoller
from watcher.models.config import DatasourcesConfig, WatcherConfig
from watcher.workflow import RulesEngine


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", list(BackendName))
@pytest.mark.parametrize("kind", [QueryFailureKind.INCOMPLETE, QueryFailureKind.UNAVAILABLE])
async def test_production_callback_preserves_failed_window_for_retry(backend, kind, monkeypatch):
    """A failed callback keeps its existing cursor; recovery retries identical bounds."""
    monkeypatch.setenv("CONFIG_PATH", str(Path(__file__).parent / "fixtures" / "watcher_config.yaml"))
    from watcher.main import poller_workflow_callback

    provider_config = log_query_config(backend)
    batcher = LogQueryBatcher(
        backend_factory=partial(fake_backend, backend),
        log_query_config=provider_config,
        state_store={},
        query_delay=0,
    )
    step = {"name": "crash", "type": "log_query", "attributes": {}}
    cursor_query = batcher.cursor_query_identity(step, "crash")
    start = datetime(2026, 9, 8, tzinfo=UTC)
    end = start + timedelta(seconds=10)
    batcher._store_query_last_execution_time(query=cursor_query, query_id="crash", end_time=start)
    initial_state = dict(batcher.kvstore)
    batcher.batch_query = AsyncMock(
        side_effect=QueryFailure(kind, backend=backend, query_id="crash", message="failure", retryable=True)
    )
    config = WatcherConfig(datasources=DatasourcesConfig(log_query=provider_config), rules=[])
    callback = partial(
        poller_workflow_callback,
        log_query_client=batcher,
        eventbus_publisher=MagicMock(),
        rules_engine_kvstore={},
        rules_engine_action_cache=MagicMock(),
        watcher_config=config,
    )
    poller = LogQueryPoller(batcher, callback, [step], chunk_size_secs=10)

    with patch.object(batcher, "_query_now", return_value=end), patch.object(RulesEngine, "run_maintenance_tasks"):
        await poller._process_query_chunks(step, "crash")

        assert batcher.kvstore == initial_state
        assert poller.query_backoff["crash"] > 0
        failed_bounds = batcher.batch_query.await_args.kwargs
        assert failed_bounds["start"] == start
        assert failed_bounds["end"] == end

        batcher.batch_query.side_effect = None
        batcher.batch_query.return_value = []
        await poller._process_query_chunks(step, "crash")

    assert batcher.batch_query.await_count == 2
    assert batcher.batch_query.await_args.kwargs == failed_bounds
    assert (
        batcher._start_after_cursor(batcher._get_query_last_execution_time(query=cursor_query, query_id="crash")) == end
    )
    assert poller.query_backoff["crash"] == 0
