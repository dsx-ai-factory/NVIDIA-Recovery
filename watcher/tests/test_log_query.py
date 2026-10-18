# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Focused tests for Watcher's backend-neutral log-query integration."""

import asyncio
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from log_query_fakes import fake_backend, log_query_config
from recovery_libs.log_query import (
    BackendName,
    LogRecord,
    LokiBackendConfig,
    QueryFailure,
    QueryFailureKind,
    QueryPage,
    QueryStats,
    VictoriaLogsBackendConfig,
)
from watcher.config import LokiConnectionSettings, VictoriaLogsConnectionSettings
from watcher.lib.plugins.log_query import LogQueryBatcher, LokiBackendFactory, create_log_query_client
from watcher.lib.plugins.log_query.batcher import LOG_QUERY_COMPATIBILITY_MAX_PAGES, _datetime_to_epoch_ns
from watcher.lib.utils.constants import DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS
from watcher.lib.utils.loki_stats import LOKI_STATS_BYTES_PROCESSED, _observations, record_query_stats
from watcher.lib.utils.metrics import (
    LOG_QUERY_API_EXCEPTIONS,
    LOG_QUERY_API_LATENCY,
    LOG_QUERY_EMPTY_RESPONSE,
    LOG_QUERY_ERRORS,
    LOG_QUERY_PROCESSING_TIME,
    LOG_QUERY_RECORDS,
    LOG_QUERY_REQUESTS,
)
from watcher.models.config import (
    DatasourcesConfig,
    LogQueryConfig,
    LogQueryPolicyConfig,
    RuleConfig,
    RuleWorkflowConfig,
    WatcherConfig,
)
from watcher.models.workflow import WorkflowStepType
from watcher.workflow import RulesEngine


def _batcher(backend_name: BackendName, *, config: LogQueryConfig | None = None, **kwargs) -> LogQueryBatcher:
    backend = fake_backend(backend_name)
    return LogQueryBatcher(
        backend_factory=lambda: backend,
        log_query_config=config or log_query_config(backend_name),
        state_store={},
        **kwargs,
    )


class _HeldBackend:
    """Shared backend whose page request waits until the test releases it."""

    name = BackendName.LOKI
    dialect = "logql"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    async def query_page(self, request):
        self.started.set()
        await self.release.wait()
        return QueryPage(
            records=(),
            next_page_token=None,
            complete=True,
            stats=QueryStats(duration_seconds=0.0, attempts=1, response_bytes=0, returned_records=0),
        )

    async def close(self) -> None:
        self.closed = True


def _batcher_over(*backends) -> LogQueryBatcher:
    return LogQueryBatcher(
        backend_factory=MagicMock(side_effect=list(backends)),
        log_query_config=log_query_config(BackendName.LOKI),
        state_store={},
    )


@pytest.mark.regression
@pytest.mark.asyncio
async def test_restart_keeps_the_replaced_backend_open_until_its_query_finishes():
    """A restart used to close the previous backend while a concurrent query was still awaiting it."""
    previous_backend, replacement = _HeldBackend(), _HeldBackend()
    batcher = _batcher_over(previous_backend, replacement)
    end = datetime(2026, 1, 1, tzinfo=UTC)
    in_flight = asyncio.create_task(
        batcher.query_records(query="q", query_id="q", start=end - timedelta(minutes=1), end=end)
    )
    await previous_backend.started.wait()

    batcher.restart_client()
    await asyncio.sleep(0)
    assert batcher.backend is replacement
    assert not previous_backend.closed

    previous_backend.release.set()
    await in_flight
    await asyncio.sleep(0)
    assert previous_backend.closed
    assert not replacement.closed


@pytest.mark.asyncio
async def test_restart_closes_an_idle_backend_immediately():
    previous_backend, replacement = _HeldBackend(), _HeldBackend()
    batcher = _batcher_over(previous_backend, replacement)

    batcher.restart_client()
    await asyncio.sleep(0)

    assert batcher.backend is replacement
    assert previous_backend.closed


def test_create_log_query_client_selects_shared_victorialogs_backend():
    backend = fake_backend(BackendName.VICTORIALOGS)
    with patch("watcher.lib.plugins.log_query.backends.create_log_query_backend", return_value=backend) as factory:
        client = create_log_query_client(
            connection_settings=LokiConnectionSettings(url="http://legacy-loki:3100"),
            state_store={},
            log_query_policy=LogQueryPolicyConfig(),
            log_query_config=log_query_config(BackendName.VICTORIALOGS),
        )

    assert client.backend is backend
    backend_name, provider_config = factory.call_args.args
    assert backend_name == BackendName.VICTORIALOGS
    assert isinstance(provider_config, VictoriaLogsBackendConfig)
    assert factory.call_args.kwargs == {"mode": "async"}


def test_batcher_rejects_and_closes_a_backend_for_a_different_provider_than_its_queries():
    backend = fake_backend(BackendName.LOKI)
    with pytest.raises(ValueError, match="built a loki backend, but log_query_config selects victorialogs"):
        LogQueryBatcher(
            backend_factory=lambda: backend,
            log_query_config=log_query_config(BackendName.VICTORIALOGS),
            state_store={},
        )

    backend.close.assert_awaited_once()


def test_victorialogs_environment_settings_reject_tenant_ids_above_uint32():
    with pytest.raises(ValueError, match="less than or equal to 4294967295"):
        VictoriaLogsConnectionSettings(url="http://victorialogs:9428", account_id=2**32)


@pytest.mark.parametrize(
    ("backend", "variable"),
    [
        (BackendName.LOKI, "LOKI_URL"),
        (BackendName.LOKI, "LOKI_TENANTS"),
        (BackendName.LOKI, "LOKI_X_SCOPE_ORG_ID"),
        (BackendName.VICTORIALOGS, "VICTORIALOGS_URL"),
        (BackendName.VICTORIALOGS, "VICTORIALOGS_ACCOUNT_ID"),
        (BackendName.VICTORIALOGS, "VICTORIALOGS_PROJECT_ID"),
        (BackendName.VICTORIALOGS, "VICTORIALOGS_REQUIRE_TENANT"),
    ],
)
def test_shared_backend_rejects_legacy_environment_identity(monkeypatch, backend, variable):
    monkeypatch.setenv(variable, "1")
    with pytest.raises(ValueError, match=f"Move {variable} to datasources.log_query.providers.{backend}"):
        create_log_query_client({}, LogQueryPolicyConfig(), log_query_config(backend))


def test_loki_identity_comes_from_provider_and_lepton_only_refreshes_token(monkeypatch):
    config = log_query_config(BackendName.LOKI)
    config.selected_provider.tenants = "application-user"
    config.selected_provider.org_id = "application-tenant"
    monkeypatch.setenv("USE_LEPTON", "true")
    with (
        patch(
            "watcher.lib.plugins.log_query.backends.read_api_token_from_file", side_effect=OSError("unused file token")
        ),
        patch.object(
            LokiBackendFactory, "_get_loki_config_from_lepton", return_value=("http://discovered:3100", "fresh-token")
        ),
        patch(
            "watcher.lib.plugins.log_query.backends.create_log_query_backend",
            return_value=fake_backend(BackendName.LOKI),
        ) as factory,
    ):
        create_log_query_client({}, LogQueryPolicyConfig(), config)
    provider = factory.call_args.args[1]
    assert provider.url == "http://loki:3100"
    assert provider.tenants == "application-user"
    assert provider.org_id == "application-tenant"
    assert provider.auth_token == "Bearer fresh-token"


def test_datetime_to_epoch_ns_treats_naive_as_utc_and_normalizes_aware_offsets():
    expected_ns = 123_456_000

    assert _datetime_to_epoch_ns(datetime(1970, 1, 1, 0, 0, 0, 123456)) == expected_ns
    assert (
        _datetime_to_epoch_ns(datetime(1969, 12, 31, 19, 0, 0, 123456, tzinfo=timezone(-timedelta(hours=5))))
        == expected_ns
    )


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="requires POSIX timezone support")
@pytest.mark.asyncio
async def test_shared_poll_bounds_are_utc_on_non_utc_host(monkeypatch):
    batcher = _batcher(BackendName.VICTORIALOGS)
    backend = batcher.backend
    backend.query_page = AsyncMock(
        return_value=QueryPage(
            records=(),
            next_page_token=None,
            complete=True,
            stats=QueryStats(
                duration_seconds=0.01,
                attempts=1,
                response_bytes=0,
                returned_records=0,
            ),
        )
    )

    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", "EST5EDT")
            time.tzset()

            assert batcher._query_now().utcoffset() == timedelta(0)
            assert batcher._normalize_query_datetime(datetime(2026, 1, 1)) == datetime(2026, 1, 1, tzinfo=UTC)
            assert _datetime_to_epoch_ns(datetime(1970, 1, 1)) == 0
            before = time.time()
            await batcher.batch_query(query="_msg:*", query_id="non_utc_poll")
            after = time.time()
            request = backend.query_page.await_args.args[0]
            assert before <= request.end_ns / 1_000_000_000 <= after
            assert request.end_ns - request.start_ns == 15 * 1_000_000_000
    finally:
        # ``monkeypatch.context`` restores TZ before this finally block.
        time.tzset()


@pytest.mark.parametrize(
    ("backend_name", "config_type"),
    [(BackendName.LOKI, LokiBackendConfig), (BackendName.VICTORIALOGS, VictoriaLogsBackendConfig)],
)
@pytest.mark.parametrize(
    ("query_timeout", "expected_connect"), [(None, DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS), (45, None)]
)
def test_shared_factory_preserves_timeout_compatibility(backend_name, config_type, query_timeout, expected_connect):
    with patch(
        "watcher.lib.plugins.log_query.backends.create_log_query_backend", return_value=fake_backend(backend_name)
    ) as factory:
        create_log_query_client({}, LogQueryPolicyConfig(query_timeout=query_timeout), log_query_config(backend_name))

    backend_config = factory.call_args.args[1]
    assert isinstance(backend_config, config_type)
    assert backend_config.connect_timeout_seconds == expected_connect


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend_name", "dialect"), [(BackendName.LOKI, "logql"), (BackendName.VICTORIALOGS, "logsql")]
)
async def test_shared_backend_page_uses_native_query_and_existing_consumer_normalization(backend_name, dialect):
    record_stats = MagicMock()
    batcher = _batcher(backend_name, query_timeout=(10, 60), stats_recorder=record_stats)
    backend = batcher.backend
    backend.query_page = AsyncMock(
        return_value=QueryPage(
            records=(
                LogRecord(
                    timestamp_ns=1_725_000_000_123_456_000,
                    body='{"message":"segfault"}',
                    fields={"job_id": "42", "filename": "/logs/job-42.out"},
                ),
            ),
            next_page_token=None,
            complete=True,
            stats=QueryStats(
                duration_seconds=0.01,
                attempts=1,
                response_bytes=100,
                returned_records=1,
            ),
        )
    )

    end = datetime(2026, 1, 1, 0, 1, 0, 123456, tzinfo=UTC)
    requests_before = LOG_QUERY_REQUESTS.labels(
        backend=backend_name, query_id="crash_0", outcome="success"
    )._value.get()
    records_before = LOG_QUERY_RECORDS.labels(backend=backend_name, query_id="crash_0")._value.get()
    result = await batcher.batch_query(
        query='job_id:="42"',
        query_id="crash_0",
        start=end - timedelta(minutes=1),
        end=end,
    )

    request = backend.query_page.await_args.args[0]
    assert request.query.dialect == dialect
    assert request.query.query_id == "crash_0"
    assert request.query.text == 'job_id:="42"'
    assert request.start_ns == 1_767_225_600_123_456_000
    assert request.end_ns == 1_767_225_660_123_456_000
    assert request.end_ns - request.start_ns == 60 * 1_000_000_000
    assert request.order == "forward"
    assert request.page_size == 1000
    assert request.timeout_seconds == 60
    assert result[0]["body"] == "segfault"
    assert result[0]["timestamp"] == "1725000000123456000"
    assert result[0]["attributes"]["job_id"] == "42"
    assert result[0]["attributes"]["log.file.name"] == "job-42.out"
    assert (
        LOG_QUERY_REQUESTS.labels(backend=backend_name, query_id="crash_0", outcome="success")._value.get()
        - requests_before
        == 1
    )
    assert LOG_QUERY_RECORDS.labels(backend=backend_name, query_id="crash_0")._value.get() - records_before == 1
    record_stats.assert_called_once_with([backend.query_page.return_value.stats], True, "crash_0")


@pytest.mark.parametrize(
    ("backend_name", "restarts_on_auth_failure", "stats_recorder"),
    [(BackendName.LOKI, True, record_query_stats), (BackendName.VICTORIALOGS, False, None)],
)
def test_create_log_query_client_wires_provider_specific_behavior(
    backend_name, restarts_on_auth_failure, stats_recorder
):
    with patch(
        "watcher.lib.plugins.log_query.backends.create_log_query_backend", return_value=fake_backend(backend_name)
    ):
        client = create_log_query_client({}, LogQueryPolicyConfig(), log_query_config(backend_name))

    assert client.backend_name == backend_name
    assert client.restart_on_auth_failure is restarts_on_auth_failure
    assert client.stats_recorder is stats_recorder


@pytest.mark.asyncio
async def test_shared_loki_frozen_provider_stats_feed_compatibility_metrics():
    query_id = "frozen_stats_contract"
    batcher = _batcher(BackendName.LOKI, stats_recorder=record_query_stats)
    stats = QueryStats(
        duration_seconds=0.01,
        attempts=1,
        response_bytes=100,
        returned_records=0,
        provider={
            "summary": {"totalBytesProcessed": 37, "totalLinesProcessed": 2},
            "ingester": {"totalChunksMatched": 1},
            "store": {"totalChunksDownloaded": 0},
        },
    )
    assert isinstance(stats.provider["summary"], Mapping)
    assert not isinstance(stats.provider["summary"], dict)
    batcher.backend.query_page = AsyncMock(
        return_value=QueryPage(records=(), next_page_token=None, complete=True, stats=stats)
    )
    histogram = LOKI_STATS_BYTES_PROCESSED.labels(query=query_id)
    before_sum = histogram._sum.get()

    await batcher.query_records(
        query='{app="watcher"}',
        query_id=query_id,
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert histogram._sum.get() == before_sum + 37
    assert _observations[-1].query_label == query_id
    assert _observations[-1].bytes_processed == 37


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", list(BackendName))
async def test_backends_use_neutral_metrics_without_raw_query_labels(provider):
    raw_query = '_msg:"unique-victoria-raw-query"'
    batcher = _batcher(provider)
    batcher.backend.query_page = AsyncMock(
        return_value=QueryPage(
            records=(),
            next_page_token=None,
            complete=True,
            stats=QueryStats(
                duration_seconds=0.01,
                attempts=1,
                response_bytes=0,
                returned_records=0,
            ),
        )
    )
    api_latency = LOG_QUERY_API_LATENCY.labels(query=raw_query)
    empty = LOG_QUERY_EMPTY_RESPONSE.labels(query=raw_query)
    processing = LOG_QUERY_PROCESSING_TIME.labels(query=raw_query)
    errors = LOG_QUERY_ERRORS.labels(query=raw_query)
    raw_query_before = (
        api_latency._sum.get(),
        empty._value.get(),
        processing._count.get(),
        errors._value.get(),
        LOG_QUERY_API_EXCEPTIONS._value.get(),
    )
    neutral_before = LOG_QUERY_REQUESTS.labels(backend=provider, query_id="anonymous", outcome="empty")._value.get()
    empty_before = LOG_QUERY_EMPTY_RESPONSE.labels(query="anonymous")._value.get()
    processing_before = LOG_QUERY_PROCESSING_TIME.labels(query="anonymous")._count.get()

    await batcher.batch_query(
        query=raw_query,
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert (
        api_latency._sum.get(),
        empty._value.get(),
        processing._count.get(),
        errors._value.get(),
        LOG_QUERY_API_EXCEPTIONS._value.get(),
    ) == raw_query_before
    assert LOG_QUERY_EMPTY_RESPONSE.labels(query="anonymous")._value.get() == empty_before + 1
    assert LOG_QUERY_PROCESSING_TIME.labels(query="anonymous")._count.get() == processing_before + 1
    assert (
        LOG_QUERY_REQUESTS.labels(backend=provider, query_id="anonymous", outcome="empty")._value.get() - neutral_before
        == 1
    )


@pytest.mark.asyncio
async def test_shared_backend_rejects_results_beyond_legacy_page_compatibility_limit():
    batcher = _batcher(BackendName.VICTORIALOGS)
    backend = batcher.backend
    backend.query_page = AsyncMock(
        side_effect=[
            QueryPage(
                records=(),
                next_page_token=f"page-{page}",
                complete=False,
                stats=QueryStats(
                    duration_seconds=0.01,
                    attempts=1,
                    response_bytes=10,
                    returned_records=0,
                ),
            )
            for page in range(1, LOG_QUERY_COMPATIBILITY_MAX_PAGES + 1)
        ]
    )
    end = datetime(2026, 1, 1, 0, 1)
    failures_before = LOG_QUERY_REQUESTS.labels(
        backend=BackendName.VICTORIALOGS, query_id="query", outcome="failure"
    )._value.get()
    exceptions_before = LOG_QUERY_API_EXCEPTIONS._value.get()
    errors_before = LOG_QUERY_ERRORS.labels(query="query")._value.get()

    with pytest.raises(QueryFailure) as failure:
        await batcher.batch_query(
            query="native-query",
            query_id="query",
            start=end - timedelta(minutes=1),
            end=end,
        )

    assert failure.value.kind == QueryFailureKind.INCOMPLETE
    assert backend.query_page.await_count == LOG_QUERY_COMPATIBILITY_MAX_PAGES
    assert LOG_QUERY_API_EXCEPTIONS._value.get() == exceptions_before + 1
    assert LOG_QUERY_ERRORS.labels(query="query")._value.get() == errors_before + 1
    assert (
        LOG_QUERY_REQUESTS.labels(backend=BackendName.VICTORIALOGS, query_id="query", outcome="failure")._value.get()
        - failures_before
        == 1
    )


@pytest.mark.asyncio
async def test_rules_engine_selects_active_provider_query_variant():
    provider_config = log_query_config(BackendName.VICTORIALOGS)
    provider_config.providers.loki = log_query_config(BackendName.LOKI).selected_provider
    config = WatcherConfig(
        datasources=DatasourcesConfig(
            log_query_policy=LogQueryPolicyConfig(),
            log_query=provider_config,
        ),
        rules=[
            RuleConfig(
                name="rule",
                workflow=[
                    RuleWorkflowConfig(
                        name="crash",
                        type=WorkflowStepType.log_query,
                    )
                ],
            )
        ],
    )
    engine = RulesEngine(
        publisher=MagicMock(),
        kvstore={},
        config=config,
        action_cache=MagicMock(),
        log_query_batcher=MagicMock(),
    )
    engine.run_maintenance_tasks = MagicMock()
    engine.run_log_query = AsyncMock(return_value=[])

    await engine.execute_workflow([config.rules[0].workflow[0].to_json()])

    selected_queries = provider_config.providers.victorialogs.queries
    assert engine.run_log_query.await_args.kwargs["query"] == selected_queries.workflows["crash"].query
    assert engine.run_log_query.await_args.kwargs["query_prefix"] == selected_queries.query_prefix
    assert selected_queries.query_prefix != provider_config.providers.loki.queries.query_prefix


def test_victorialogs_does_not_evaluate_loki_provider_health():
    config = WatcherConfig(
        datasources=DatasourcesConfig(log_query=log_query_config(BackendName.VICTORIALOGS)),
        rules=[],
    )
    engine = RulesEngine(
        publisher=MagicMock(),
        kvstore={},
        config=config,
        action_cache=MagicMock(),
        log_query_batcher=MagicMock(),
    )
    engine.check_and_maintain_publisher_connection = MagicMock()
    engine.cleanup_metric_gauges = MagicMock()
    engine.loki_perf_monitor.evaluate_if_due = MagicMock()

    with (
        patch("watcher.workflow.update_certificate_expiration_metrics"),
        patch("watcher.workflow.update_deduplicator_capacity_metrics"),
    ):
        engine.run_maintenance_tasks()

    engine.loki_perf_monitor.evaluate_if_due.assert_not_called()


def test_victorialogs_nooutput_template_preserves_logsql_stream_braces():
    config = WatcherConfig(
        datasources=DatasourcesConfig(log_query=log_query_config(BackendName.VICTORIALOGS)),
        rules=[],
    )
    engine = RulesEngine(
        publisher=MagicMock(),
        kvstore={},
        config=config,
        action_cache=MagicMock(),
        log_query_batcher=MagicMock(),
    )
    template = '_stream:{cluster="prod",appName="slurm-app-log"} job_id:="{job_id}" | _msg:*'

    rendered = engine._render_nooutput_validation_query(template, job_id="612685")

    assert rendered == '_stream:{cluster="prod",appName="slurm-app-log"} job_id:="612685" | _msg:*'


def test_cursor_identity_is_namespaced_by_backend_and_native_query_revision():
    loki_batcher = _batcher(BackendName.LOKI)
    victoria_batcher = _batcher(BackendName.VICTORIALOGS)

    loki_key = loki_batcher._query_cursor_signature(query="native-query", query_id="crash")
    victoria_key = victoria_batcher._query_cursor_signature(query="native-query", query_id="crash")
    changed_query_key = victoria_batcher._query_cursor_signature(query="changed-query", query_id="crash")

    assert loki_key != victoria_key
    assert victoria_key != changed_query_key

    cursor_end = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    loki_batcher._store_query_last_execution_time(
        query="native-query",
        query_id="crash",
        end_time=cursor_end,
    )
    assert loki_batcher.kvstore[loki_key] == f"v1:ns:{_datetime_to_epoch_ns(cursor_end)}"
    stored_cursor = loki_batcher._get_query_last_execution_time(query="native-query", query_id="crash")
    assert isinstance(stored_cursor, int)
    assert loki_batcher._start_after_cursor(stored_cursor) == cursor_end


@pytest.mark.parametrize("backend", list(BackendName))
@pytest.mark.parametrize("changed_field", ["query_prefix", "query", "nooutput_validation_query"])
def test_cursor_identity_changes_with_provider_templates(backend, changed_field):
    config = log_query_config(backend)
    batcher = _batcher(backend, config=config)
    workflow = {"name": "crash", "type": "log_query"}
    original = batcher.cursor_query_identity(workflow, "crash")
    original_key = batcher._query_cursor_signature(query=original, query_id="crash")

    if changed_field == "query_prefix":
        config.selected_provider.queries.query_prefix = "changed-prefix"
    elif changed_field == "query":
        config.selected_provider.queries.workflows["crash"].query = ["changed-query"]
    else:
        config.selected_provider.queries.workflows["crash"].nooutput_validation_query = "job={job_id}"

    changed = batcher.cursor_query_identity(workflow, "crash")
    assert changed != original
    assert batcher._query_cursor_signature(query=changed, query_id="crash") != original_key


def test_cursor_identity_preserves_native_query_order():
    config = log_query_config(BackendName.LOKI)
    batcher = _batcher(BackendName.LOKI, config=config)
    native = config.selected_provider.queries.workflows["crash"]
    native.query = ["first", "second"]
    forward = batcher.cursor_query_identity({"name": "crash"}, "crash")
    native.query.reverse()
    assert batcher.cursor_query_identity({"name": "crash"}, "crash") != forward
