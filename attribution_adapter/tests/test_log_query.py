# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from attribution_adapter.log_query import (
    ADILeptonLogQueryFetcher,
    ADILogQueryFetcher,
    create_attribution_log_fetcher,
    serialize_records_for_fact,
)
from recovery_libs.log_query import (
    STATIC_BACKENDS,
    BackendName,
    LogRecord,
    LokiBackend,
    LokiBackendConfig,
    QueryDialect,
    QueryFailure,
    QueryFailureKind,
    QueryOrder,
    QueryPage,
    QueryStats,
)


def _created_as_requested(*backends: MagicMock):
    """Backend-factory side effect giving each mock the identity of the backend the fetcher requested."""
    remaining = list(backends)

    def create(backend_name, config, *, mode):
        backend = remaining.pop(0)
        backend.name = BackendName(backend_name)
        backend.dialect = STATIC_BACKENDS[backend.name][2].dialect
        return backend

    return create


def _settings(backend: BackendName = BackendName.LOKI) -> SimpleNamespace:
    return SimpleNamespace(
        log_query_backend=backend,
        loki_app_url="https://app-loki.example",
        loki_sys_url="https://sys-loki.example",
        loki_app_tenants="tenant-user",
        loki_sys_tenants="tenant-user",
        loki_app_org_id="tenant-org",
        loki_sys_org_id="tenant-org",
        loki_http_timeout_seconds=31,
        victorialogs_app_url="https://app-victorialogs.example",
        victorialogs_sys_url="https://sys-victorialogs.example",
        victorialogs_app_account_id=17,
        victorialogs_app_project_id=21,
        victorialogs_sys_account_id=27,
        victorialogs_sys_project_id=31,
        victorialogs_app_require_tenant=True,
        victorialogs_sys_require_tenant=True,
        victorialogs_http_timeout_seconds=47,
        is_lepton_enabled=False,
    )


def _page(*records: LogRecord, token: str | None = None) -> QueryPage:
    return QueryPage(
        records=records,
        next_page_token=token,
        complete=token is None,
        stats=QueryStats(
            duration_seconds=0.1,
            attempts=1,
            response_bytes=10,
            returned_records=len(records),
        ),
    )


class _FakeClock:
    """Deterministic monotonic clock whose sleeps consume simulated budget."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_loki_remains_the_default_backend(monkeypatch):
    """The neutral facade creates independent Loki clients with explicit domain endpoints and tenancy."""
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    app_backend = MagicMock()
    sys_backend = MagicMock()
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, sys_backend),
    ) as factory:
        fetcher = ADILogQueryFetcher(_settings())

    assert fetcher.backend_name is BackendName.LOKI
    assert fetcher.app_log_backend is app_backend
    assert fetcher.sys_log_backend is sys_backend
    assert [item.args[0] for item in factory.call_args_list] == [BackendName.LOKI, BackendName.LOKI]
    app_config = factory.call_args_list[0].args[1]
    sys_config = factory.call_args_list[1].args[1]
    assert app_config.url == "https://app-loki.example"
    assert sys_config.url == "https://sys-loki.example"
    assert app_config.tenants == sys_config.tenants == "tenant-user"
    assert app_config.org_id == sys_config.org_id == "tenant-org"
    assert app_config.auth_token == sys_config.auth_token == "shared-token"
    assert factory.call_args_list == [
        call(BackendName.LOKI, app_config, mode="sync"),
        call(BackendName.LOKI, sys_config, mode="sync"),
    ]


def test_loki_domain_token_files_override_the_shared_token(monkeypatch, tmp_path):
    """The neutral Loki facade preserves independent application/system credential precedence."""
    app_token = tmp_path / "app-token"
    sys_token = tmp_path / "sys-token"
    app_token.write_text("app-secret\n")
    sys_token.write_text("sys-secret\n")
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    monkeypatch.setenv("APP_LOKI_API_TOKEN_FILE", str(app_token))
    monkeypatch.setenv("SYS_LOKI_API_TOKEN_FILE", str(sys_token))

    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(_settings())

    assert factory.call_args_list[0].args[1].auth_token == "app-secret"
    assert factory.call_args_list[1].args[1].auth_token == "sys-secret"


def test_loki_without_configured_credentials(monkeypatch):
    """An unauthenticated Loki installation does not acquire implicit credentials."""
    for name in ("LOKI_API_TOKEN", "APP_LOKI_API_TOKEN_FILE", "SYS_LOKI_API_TOKEN_FILE"):
        monkeypatch.delenv(name, raising=False)
    settings = _settings()
    settings.loki_app_tenants = None
    settings.loki_sys_tenants = None
    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(settings)
    assert all(item.args[1].auth_token is None for item in factory.call_args_list)


def test_loki_domain_tenant_and_org_settings_are_independent(monkeypatch):
    """Application and system Loki clients may use independent auth and org scopes."""
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    settings = _settings()
    settings.loki_app_tenants = "app-user"
    settings.loki_sys_tenants = "sys-user"
    settings.loki_app_org_id = "app-org"
    settings.loki_sys_org_id = "sys-org"

    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(settings)

    app_config = factory.call_args_list[0].args[1]
    sys_config = factory.call_args_list[1].args[1]
    assert app_config.tenants == "app-user"
    assert sys_config.tenants == "sys-user"
    assert app_config.org_id == "app-org"
    assert sys_config.org_id == "sys-org"


def test_loki_system_does_not_inherit_application_identity(monkeypatch):
    """An anonymous system domain never inherits the application domain's tenant."""
    settings = _settings()
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    settings.loki_sys_tenants = None
    settings.loki_sys_org_id = None
    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(settings)
    assert factory.call_args_list[0].args[1].tenants == "tenant-user"
    assert factory.call_args_list[1].args[1].tenants is None
    assert factory.call_args_list[1].args[1].org_id is None


@pytest.mark.parametrize("backend", list(BackendName))
@pytest.mark.parametrize("domain", ["app", "sys"])
def test_missing_domain_url_has_no_shared_fallback(backend, domain, monkeypatch):
    """Each domain needs its configured provider connection even when its peer has one."""
    settings = _settings(backend)
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    setattr(settings, f"{backend}_{domain}_url", None)
    with (
        patch("attribution_adapter.log_query.create_log_query_backend"),
        pytest.raises(ValueError, match="log-query URL is not configured"),
    ):
        ADILogQueryFetcher(settings)


def test_close_is_idempotent_and_deduplicates_aliased_backends(monkeypatch):
    """One shared domain client is not double-closed during process shutdown."""
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    backend = MagicMock()
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=[backend, backend],
    ):
        fetcher = ADILogQueryFetcher(_settings())

    fetcher.close()
    fetcher.close()

    backend.close.assert_called_once_with()


def test_partial_backend_construction_closes_the_first_client(monkeypatch):
    """A failed system-client construction does not leak its application peer."""
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    app_backend = MagicMock()
    with (
        patch(
            "attribution_adapter.log_query.create_log_query_backend",
            side_effect=[app_backend, RuntimeError("system construction failed")],
        ),
        pytest.raises(RuntimeError, match="system construction failed"),
    ):
        ADILogQueryFetcher(_settings())

    app_backend.close.assert_called_once_with()


def test_victorialogs_selection_uses_separate_domains_and_native_query(monkeypatch):
    """VictoriaLogs selection keeps app/sys clients distinct and dispatches LogsQL unchanged."""
    monkeypatch.setenv("VICTORIALOGS_API_TOKEN", "shared-token")
    app_backend = MagicMock()
    sys_backend = MagicMock()
    app_backend.query_page.return_value = _page(
        LogRecord(timestamp_ns=1_704_110_400_123_456_000, body="body", fields={"job_id": "42"}),
        token="opaque-next-page",
    )
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, sys_backend),
    ) as factory:
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))

    app_config = factory.call_args_list[0].args[1]
    sys_config = factory.call_args_list[1].args[1]
    assert app_config.url == "https://app-victorialogs.example"
    assert sys_config.url == "https://sys-victorialogs.example"
    assert app_config.account_id == 17
    assert app_config.project_id == 21
    assert app_config.require_tenant is True
    assert sys_config.account_id == 27
    assert sys_config.project_id == 31
    assert sys_config.require_tenant is True
    assert app_config.bearer_token == sys_config.bearer_token == "shared-token"

    with (
        patch("attribution_adapter.log_query.LOG_QUERY_REQUESTS") as request_metric,
        patch("attribution_adapter.log_query.LOG_QUERY_RECORDS") as record_metric,
    ):
        result, token = fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query='_stream:{appName="slurm-app-log"} AND job_id:=42',
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            limit=700,
            direction=QueryOrder.BACKWARD,
        )

    request = app_backend.query_page.call_args.args[0]
    assert request.query.query_id == "attribution-application"
    assert request.query.dialect is QueryDialect.LOGSQL
    assert request.query.text == '_stream:{appName="slurm-app-log"} AND job_id:=42'
    assert request.start_ns == 1_704_067_200_000_000_000
    assert request.end_ns == 1_704_153_600_000_000_000
    assert request.order is QueryOrder.BACKWARD
    assert request.page_size == 700
    assert 0 < request.timeout_seconds <= 47.0
    assert token == "opaque-next-page"
    assert result == '[{"stream": {"job_id": "42"}, "values": [["1704110400123456000", "body"]]}]'
    request_metric.labels.assert_called_once_with(
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        outcome="success",
    )
    request_metric.labels.return_value.inc.assert_called_once_with()
    record_metric.labels.assert_called_once_with(
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
    )
    record_metric.labels.return_value.inc.assert_called_once_with(1)


def test_victorialogs_allows_explicit_tenantless_domain_configs():
    """Each domain passes its opt-in tenancy requirement to the shared driver."""
    settings = _settings(BackendName.VICTORIALOGS)
    settings.victorialogs_app_account_id = None
    settings.victorialogs_app_project_id = None
    settings.victorialogs_sys_account_id = None
    settings.victorialogs_sys_project_id = None
    settings.victorialogs_app_require_tenant = False
    settings.victorialogs_sys_require_tenant = False

    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(settings)

    app_config = factory.call_args_list[0].args[1]
    sys_config = factory.call_args_list[1].args[1]
    assert app_config.account_id is None
    assert app_config.project_id is None
    assert app_config.require_tenant is False
    assert sys_config.account_id is None
    assert sys_config.project_id is None
    assert sys_config.require_tenant is False


def test_victorialogs_basic_auth_uses_domain_overrides_and_shared_fallback(monkeypatch):
    """Application and system proxy Basic credentials resolve independently."""
    monkeypatch.setenv("VICTORIALOGS_USERNAME", "shared-user")
    monkeypatch.setenv("VICTORIALOGS_PASSWORD", "shared-password")
    monkeypatch.setenv("APP_VICTORIALOGS_USERNAME", "app-user")
    monkeypatch.setenv("APP_VICTORIALOGS_PASSWORD", "app-password")

    with patch("attribution_adapter.log_query.create_log_query_backend") as factory:
        ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))

    app_config = factory.call_args_list[0].args[1]
    sys_config = factory.call_args_list[1].args[1]
    assert app_config.username == "app-user"
    assert app_config.password == "app-password"
    assert sys_config.username == "shared-user"
    assert sys_config.password == "shared-password"


def test_victorialogs_rejects_partial_basic_auth(monkeypatch):
    """The shared provider config rejects an effective username without a password."""
    monkeypatch.setenv("APP_VICTORIALOGS_USERNAME", "app-user")

    with pytest.raises(ValueError, match="username and password must be configured together"):
        ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))


def test_victorialogs_rejects_basic_and_bearer_auth_together(monkeypatch):
    """The shared provider config rejects ambiguous proxy authentication modes."""
    monkeypatch.setenv("VICTORIALOGS_USERNAME", "shared-user")
    monkeypatch.setenv("VICTORIALOGS_PASSWORD", "shared-password")
    monkeypatch.setenv("VICTORIALOGS_API_TOKEN", "shared-token")

    with pytest.raises(ValueError, match="basic and bearer authentication are mutually exclusive"):
        ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))


def test_clean_empty_page_uses_distinct_neutral_metric_outcome():
    """An exhausted empty query is observable as empty rather than failure or data success."""
    app_backend = MagicMock()
    app_backend.query_page.return_value = _page()
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))

    with patch("attribution_adapter.log_query.LOG_QUERY_REQUESTS") as request_metric:
        result, token = fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="_stream:slurm",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert result == "[]"
    assert token is None
    request_metric.labels.assert_called_once_with(
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        outcome="empty",
    )


def test_fact_serializer_groups_fields_and_preserves_duplicate_occurrences():
    """FACT compatibility output keeps Loki stream/value shape without deduplicating records."""
    records = (
        LogRecord(timestamp_ns=10, body="same", fields={"cluster": "slurm", "job_id": "42"}),
        LogRecord(timestamp_ns=10, body="same", fields={"job_id": "42", "cluster": "slurm"}),
        LogRecord(timestamp_ns=11, body="other", fields={"cluster": "slurm", "job_id": "43"}),
    )

    assert serialize_records_for_fact(records) == (
        '[{"stream": {"cluster": "slurm", "job_id": "42"}, '
        '"values": [["10", "same"], ["10", "same"]]}, '
        '{"stream": {"cluster": "slurm", "job_id": "43"}, "values": [["11", "other"]]}]'
    )


def test_nonretryable_query_failure_is_not_retried(monkeypatch):
    """Invalid native queries fail immediately according to the shared failure contract."""
    app_backend = MagicMock()
    app_backend.query_page.side_effect = QueryFailure(
        QueryFailureKind.INVALID_QUERY,
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        message="invalid query",
        retryable=False,
        http_status=400,
    )
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))
    sleep = MagicMock()
    monkeypatch.setattr("attribution_adapter.log_query.time.sleep", sleep)

    with pytest.raises(QueryFailure, match="invalid_query"):
        fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="bad LogsQL",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert app_backend.query_page.call_count == 1
    sleep.assert_not_called()


def test_retryable_query_failure_honors_retry_after_and_is_bounded(monkeypatch):
    """Transient failures use retry hints but stop after the established three retries."""
    app_backend = MagicMock()
    failure = QueryFailure(
        QueryFailureKind.RATE_LIMITED,
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        message="rate limited",
        retryable=True,
        http_status=429,
        retry_after_seconds=0.25,
    )
    app_backend.query_page.side_effect = failure
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))
    sleep = MagicMock()
    monkeypatch.setattr("attribution_adapter.log_query.time.sleep", sleep)

    with pytest.raises(QueryFailure, match="rate_limited"):
        fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="valid LogsQL",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert app_backend.query_page.call_count == 4
    assert sleep.call_args_list == [call(0.25), call(0.25), call(0.25)]


def test_retryable_query_failure_preserves_legacy_backoff_schedule(monkeypatch):
    """Transient failures without Retry-After use the prior 0.5 backoff factor."""
    app_backend = MagicMock()
    app_backend.query_page.side_effect = QueryFailure(
        QueryFailureKind.UNAVAILABLE,
        backend=BackendName.LOKI,
        query_id="attribution-application",
        message="temporarily unavailable",
        retryable=True,
        http_status=503,
    )
    monkeypatch.setenv("LOKI_API_TOKEN", "shared-token")
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings())
    sleep = MagicMock()
    monkeypatch.setattr("attribution_adapter.log_query.time.sleep", sleep)

    with pytest.raises(QueryFailure, match="unavailable"):
        fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query='{job_id="42"}',
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert app_backend.query_page.call_count == 4
    assert sleep.call_args_list == [call(0.5), call(1.0), call(2.0)]


def test_retry_attempts_receive_only_remaining_operation_budget(monkeypatch):
    """Retries and their delays share one absolute monotonic page-fetch deadline."""
    clock = _FakeClock()
    retryable = QueryFailure(
        QueryFailureKind.UNAVAILABLE,
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        message="temporarily unavailable",
        retryable=True,
        http_status=503,
        retry_after_seconds=0.5,
    )
    app_backend = MagicMock()
    app_backend.query_page.side_effect = [retryable, _page()]
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))
    monkeypatch.setattr("attribution_adapter.log_query.time.monotonic", clock.monotonic)
    monkeypatch.setattr("attribution_adapter.log_query.time.sleep", clock.sleep)

    fetcher.get_app_telemetry_logs(
        job_id="42",
        log_query="valid LogsQL",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        timeout_seconds=2.0,
    )

    attempts = [item.args[0] for item in app_backend.query_page.call_args_list]
    assert [attempt.timeout_seconds for attempt in attempts] == [2.0, 1.5]


def test_retry_delay_that_exhausts_budget_raises_timeout_before_sleep(monkeypatch):
    """Retry-After cannot extend an operation beyond its caller-provided budget."""
    clock = _FakeClock()
    app_backend = MagicMock()
    app_backend.query_page.side_effect = QueryFailure(
        QueryFailureKind.RATE_LIMITED,
        backend=BackendName.VICTORIALOGS,
        query_id="attribution-application",
        message="rate limited",
        retryable=True,
        http_status=429,
        retry_after_seconds=2.0,
    )
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))
    sleep = MagicMock()
    monkeypatch.setattr("attribution_adapter.log_query.time.monotonic", clock.monotonic)
    monkeypatch.setattr("attribution_adapter.log_query.time.sleep", sleep)

    with pytest.raises(QueryFailure) as failure:
        fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="valid LogsQL",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            timeout_seconds=1.0,
        )

    assert failure.value.kind == QueryFailureKind.TIMEOUT
    assert app_backend.query_page.call_count == 1
    sleep.assert_not_called()


def test_response_returning_after_deadline_is_discarded_as_timeout(monkeypatch):
    """Elapsed synchronous responses cannot escape the observed operation budget."""
    clock = _FakeClock()
    app_backend = MagicMock()

    def slow_response(_request):
        clock.now = 3.0
        return _page(LogRecord(timestamp_ns=10, body="too late"))

    app_backend.query_page.side_effect = slow_response
    with patch(
        "attribution_adapter.log_query.create_log_query_backend",
        side_effect=_created_as_requested(app_backend, MagicMock()),
    ):
        fetcher = ADILogQueryFetcher(_settings(BackendName.VICTORIALOGS))
    monkeypatch.setattr("attribution_adapter.log_query.time.monotonic", clock.monotonic)

    with pytest.raises(QueryFailure) as failure:
        fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="valid LogsQL",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            timeout_seconds=2.0,
        )

    assert failure.value.kind == QueryFailureKind.TIMEOUT


def test_lepton_factory_uses_the_neutral_loki_facade():
    """Lepton selection no longer bypasses the shared Loki backend contract."""
    settings = _settings()
    settings.is_lepton_enabled = True
    neutral_fetcher = MagicMock()

    with patch(
        "attribution_adapter.log_query.ADILeptonLogQueryFetcher",
        return_value=neutral_fetcher,
    ) as lepton_type:
        result = create_attribution_log_fetcher(settings, 700)

    assert result is neutral_fetcher
    lepton_type.assert_called_once_with(settings, 700)


@pytest.mark.parametrize("domain", ["app", "sys"])
def test_lepton_fact_normalization_preserves_legacy_record_behavior(monkeypatch, domain):
    """Both production domain methods preserve bodies and canonical node names for FACT."""
    monkeypatch.setenv("LEPTON_WORKER_CLUSTER_NAME", "worker-a")
    monkeypatch.setenv("LEPTON_BASE_URL", "https://lepton.example")
    records = (
        LogRecord(1, '{"message":"extracted body"}', {"host": "host-a"}),
        LogRecord(2, "  plain text\n", {"node": "node-a"}),
        LogRecord(3, '{"message":"unclosed}', {"node": "node-a"}),
        LogRecord(4, '{"level":"INFO"}', {"node": "node-a"}),
        LogRecord(5, "unchanged", {"node_name": "canonical", "host": "alias"}),
    )
    backend = MagicMock()
    backend.query_page.return_value = _page(*records)
    discovery = {"status": {"properties": {"loki_url": "https://loki.example", "loki_bearer_token": "token"}}}
    with (
        patch.object(ADILeptonLogQueryFetcher, "get_worker_cluster", return_value=discovery),
        patch(
            "attribution_adapter.log_query.create_log_query_backend",
            side_effect=_created_as_requested(backend, backend),
        ),
    ):
        fetcher = ADILeptonLogQueryFetcher(_settings())
        result, token = getattr(fetcher, f"get_{domain}_telemetry_logs")(
            job_id="42",
            log_query="{job_id='42'}",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            direction=QueryOrder.BACKWARD,
        )

    assert token is None
    request = backend.query_page.call_args.args[0]
    assert request.order is QueryOrder.BACKWARD
    assert request.query.text == '{job_id="42"}'
    assert json.loads(result) == [
        {"stream": {"node_name": "host-a"}, "values": [["1", "extracted body"]]},
        {
            "stream": {"node_name": "node-a"},
            "values": [["2", "  plain text\n"], ["3", '{"message":"unclosed}'], ["4", '{"level":"INFO"}']],
        },
        {"stream": {"node_name": "canonical", "host": "alias"}, "values": [["5", "unchanged"]]},
    ]


def test_neutral_lepton_fetcher_refreshes_once_and_normalizes_for_fact(monkeypatch):
    """A Loki 401 refreshes Lepton discovery and preserves its FACT-only normalization."""
    monkeypatch.setenv("LEPTON_WORKER_CLUSTER_NAME", "worker-a")
    monkeypatch.setenv("LEPTON_BASE_URL", "https://lepton.example")
    settings = _settings()
    old_app = MagicMock()
    old_sys = MagicMock()
    unauthenticated = QueryFailure(
        QueryFailureKind.UNAUTHENTICATED,
        backend=BackendName.LOKI,
        query_id="attribution-application",
        message="expired worker token",
        retryable=False,
        http_status=401,
    )
    old_app.query_page.side_effect = [
        unauthenticated,
        _page(
            LogRecord(
                timestamp_ns=10,
                body='{"message":"preserved body","level":"INFO"}',
                fields={"host": "node-a", "cluster": "slurm"},
            )
        ),
    ]
    discovered = [
        {"status": {"properties": {"loki_url": "https://old-loki", "loki_bearer_token": "old"}}},
        {"status": {"properties": {"loki_url": "https://old-loki", "loki_bearer_token": "new"}}},
    ]

    with (
        patch.object(ADILeptonLogQueryFetcher, "get_worker_cluster", side_effect=discovered) as discovery,
        patch(
            "attribution_adapter.log_query.create_log_query_backend",
            side_effect=_created_as_requested(old_app, old_sys),
        ) as backend_factory,
    ):
        fetcher = ADILeptonLogQueryFetcher(settings)
        result, token = fetcher.get_app_telemetry_logs(
            job_id="42",
            log_query="{job_id='42'}",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert discovery.call_count == 2
    assert backend_factory.call_count == 2
    assert old_app.query_page.call_count == 2
    old_app.close.assert_not_called()
    old_sys.close.assert_not_called()
    refreshed_app_config = old_app.replace_config.call_args.args[0]
    refreshed_sys_config = old_sys.replace_config.call_args.args[0]
    assert refreshed_app_config.url == "https://app-loki.example"
    assert refreshed_sys_config.url == "https://sys-loki.example"
    assert refreshed_app_config.auth_token == "Bearer new"
    assert refreshed_app_config.org_id == "tenant-org"
    assert refreshed_sys_config.org_id == "tenant-org"
    assert refreshed_sys_config.auth_token == "Bearer new"
    assert token is None
    assert result == ('[{"stream": {"cluster": "slurm", "node_name": "node-a"}, "values": [["10", "preserved body"]]}]')


def test_lepton_second_page_401_preserves_signed_continuation(monkeypatch):
    """Credential refresh retains the signer and neither repeats nor omits page records."""
    monkeypatch.setenv("LEPTON_WORKER_CLUSTER_NAME", "worker-a")
    monkeypatch.setenv("LEPTON_BASE_URL", "https://lepton.example")
    settings = _settings()
    settings.loki_app_tenants = None
    settings.loki_sys_tenants = None
    first_timestamp = 1_704_067_200_000_000_001
    boundary_timestamp = 1_704_067_200_000_000_002
    final_timestamp = 1_704_067_200_000_000_003
    old_token_requests = 0

    def response(values: list[list[str]]) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [{"stream": {"cluster": "slurm"}, "values": values}],
                    "stats": {},
                },
            },
        )

    def app_handler(request: httpx.Request) -> httpx.Response:
        nonlocal old_token_requests
        if request.headers["Authorization"] == "Bearer old":
            old_token_requests += 1
            if old_token_requests == 1:
                return response(
                    [
                        [str(first_timestamp), "first"],
                        [str(boundary_timestamp), "boundary"],
                    ]
                )
            return httpx.Response(401)

        assert request.headers["Authorization"] == "Bearer new"
        assert request.url.params["start"] == str(boundary_timestamp)
        assert request.url.params["limit"] == "3"
        return response(
            [
                [str(boundary_timestamp), "boundary"],
                [str(final_timestamp), "final"],
            ]
        )

    app_client = httpx.Client(transport=httpx.MockTransport(app_handler))
    sys_client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    created: list[LokiBackend] = []

    def backend_factory(backend_name: str, config: LokiBackendConfig, *, mode: str) -> LokiBackend:
        assert backend_name == BackendName.LOKI
        assert mode == "sync"
        client = app_client if not created else sys_client
        backend = LokiBackend(config, client=client)
        created.append(backend)
        return backend

    original_replace_config = LokiBackend.replace_config

    def replace_config(backend: LokiBackend, config: LokiBackendConfig) -> None:
        client = app_client if backend is created[0] else sys_client
        original_replace_config(backend, config, client=client)

    discovered = [
        {"status": {"properties": {"loki_url": "https://loki.example", "loki_bearer_token": "old"}}},
        {"status": {"properties": {"loki_url": "https://loki.example", "loki_bearer_token": "new"}}},
    ]

    try:
        with (
            patch.object(ADILeptonLogQueryFetcher, "get_worker_cluster", side_effect=discovered),
            patch("attribution_adapter.log_query.create_log_query_backend", side_effect=backend_factory),
            patch.object(LokiBackend, "replace_config", autospec=True, side_effect=replace_config),
        ):
            fetcher = ADILeptonLogQueryFetcher(settings)
            first_result, page_token = fetcher.get_app_telemetry_logs(
                job_id="42",
                log_query='{job_id="42"}',
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
                limit=2,
            )
            second_result, final_token = fetcher.get_app_telemetry_logs(
                job_id="42",
                log_query='{job_id="42"}',
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
                limit=2,
                page_token=page_token,
            )
    finally:
        app_client.close()
        sys_client.close()

    assert page_token is not None
    assert first_result == (
        '[{"stream": {"cluster": "slurm"}, "values": '
        f'[["{first_timestamp}", "first"], ["{boundary_timestamp}", "boundary"]]}}]'
    )
    assert second_result == (f'[{{"stream": {{"cluster": "slurm"}}, "values": [["{final_timestamp}", "final"]]}}]')
    assert final_token is None
