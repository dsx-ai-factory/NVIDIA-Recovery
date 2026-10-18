# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os

import pytest
from attribution_adapter.settings.settings import Settings


def provider_queries(backend, placeholder=None):
    """Encode the same provider query projection emitted by the Helm chart."""
    if placeholder is None:
        placeholder = "{job_id}" if backend == "victorialogs" else "{}"
    return json.dumps(
        {
            backend: {
                "queries": {
                    "application": "app query " + placeholder,
                    "system": "system query",
                    "epilogue": "epilogue query " + placeholder,
                }
            }
        }
    )


@pytest.fixture
def env_vars():
    """Fixture to set up environment variables for testing."""
    # Store original environment
    original_env = dict(os.environ)

    # Clear all environment variables first
    os.environ.clear()

    # Remove any existing CONFIG_FILE_PATH
    original_env.pop("CONFIG_FILE_PATH", None)

    # Set required environment variables for attribution-adapter
    os.environ.update(
        {
            "loki_app_url": "http://test-loki:3100",
            "loki_sys_url": "http://test-loki:3100",
            "loki_cluster": "test-cluster",
            "prometheus_url": "http://test-prometheus:9090",
            "prometheus_cluster": "test-prometheus-cluster",
            "prometheus_checks_enabled": "true",
            "prometheus_hostname_label": "host",
            "syslog_formatter": "test-formatter",
            "kernel_logs_only": "true",
            "syslogindex_enabled": "true",
            "ib_metrics_query": "test-ib-query",
            "eth_metrics_query": "test-eth-query",
            "health_check_query": "test-health-query",
            "log_query_providers": provider_queries("loki"),
            "logql_lookback_timewindow_duration_minutes": "30",
            "logql_applog_grace_time_seconds": "60",
        }
    )

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


def test_settings_from_env_vars(env_vars):
    """Test that Settings class correctly loads values from environment variables."""
    settings = Settings()

    # Loki settings
    assert settings.loki_cluster == "test-cluster"
    assert settings.loki_app_url == "http://test-loki:3100"
    assert settings.loki_sys_url == "http://test-loki:3100"
    assert settings.loki_app_tenants is None
    assert settings.loki_sys_tenants is None
    assert settings.loki_app_org_id is None
    assert settings.loki_sys_org_id is None

    # Prometheus settings
    assert settings.prometheus_url == "http://test-prometheus:9090"
    assert settings.prometheus_cluster == "test-prometheus-cluster"
    assert settings.prometheus_checks_enabled is True
    assert settings.prometheus_hostname_label == "host"
    assert settings.prometheus_tenants is None

    # Syslog / kernel log
    assert settings.syslog_formatter == "test-formatter"
    assert settings.kernel_logs_only is True
    assert settings.syslogindex_enabled is True

    # Provider-native templates are loaded from one nested JSON setting.
    queries = settings.log_query_providers["loki"].queries
    assert queries.application == "app query {}"
    assert queries.system == "system query"
    assert queries.epilogue == "epilogue query {}"
    assert settings.ib_metrics_query == "test-ib-query"
    assert settings.eth_metrics_query == "test-eth-query"
    assert settings.health_check_query == "test-health-query"

    # Log-fetch policy remains separate from query text.
    assert settings.logql_lookback_timewindow_duration_minutes == 30
    assert settings.logql_applog_grace_time_seconds == 60


def test_settings_http_timeout_defaults(env_vars):
    """Verify HTTP timeout settings have correct defaults."""
    settings = Settings()
    assert settings.fact_http_timeout_seconds == 30
    assert settings.loki_http_timeout_seconds == 30
    assert settings.victorialogs_http_timeout_seconds == 30


def test_settings_log_fetch_defaults(env_vars):
    """Test that log-fetch optimization fields have correct defaults."""
    settings = Settings()
    assert settings.log_fetch_optimization_enabled is False
    assert settings.log_fetch_direction == "backward"
    assert settings.log_fetch_timeout_syslogs_seconds == 60
    assert settings.log_fetch_timeout_applogs_seconds == 60
    assert settings.log_fetch_timeout_slow_signals_seconds == 300


def test_settings_optional_fields_default_none(env_vars):
    """Test that optional fields default to None or False as specified."""
    settings = Settings()
    assert settings.fact_url is None
    assert settings.loki_app_url == "http://test-loki:3100"
    assert settings.loki_sys_url == "http://test-loki:3100"
    assert settings.prometheus_tenants is None
    assert settings.is_lepton_enabled is False
    assert settings.log_query_backend == "loki"
    assert settings.victorialogs_app_url is None
    assert settings.victorialogs_sys_url is None
    assert settings.victorialogs_app_account_id is None
    assert settings.victorialogs_app_project_id is None
    assert settings.victorialogs_sys_account_id is None
    assert settings.victorialogs_sys_project_id is None
    assert settings.victorialogs_app_require_tenant is False
    assert settings.victorialogs_sys_require_tenant is False


def test_settings_straggler_defaults(env_vars):
    """Test straggler detection default values."""
    settings = Settings()
    assert settings.nvrx_straggler_threshold == 0.7
    assert settings.nvrx_straggler_score_types == [0, 1]
    assert settings.megatron_straggler_threshold == 0.85
    assert settings.megatron_straggler_score_types == [0]
    assert settings.megatron_straggler_infer_threshold_from_data is True


def test_settings_fact_url_from_env(env_vars):
    """Test that fact_url can be set via environment variable."""
    os.environ["fact_url"] = "http://test-fact:8080"
    settings = Settings()
    assert settings.fact_url == "http://test-fact:8080"


@pytest.mark.parametrize(
    "retired",
    [
        "LOKI_URL",
        "loki_tenants",
        "use_x_scope_org_id",
        "x_scope_org_id",
        "victorialogs_url",
        "victorialogs_account_id",
        "victorialogs_project_id",
    ],
)
def test_settings_rejects_retired_connection_and_tenant_env(env_vars, retired):
    """Old environment names fail explicitly instead of losing configured tenant isolation."""
    os.environ[retired] = "legacy-value"
    with pytest.raises(ValueError, match="Retired log-query settings"):
        Settings()


@pytest.mark.parametrize("domain", ["app", "sys"])
@pytest.mark.parametrize("is_lepton_enabled", [False, True])
def test_settings_requires_each_loki_domain_url(env_vars, domain, is_lepton_enabled):
    """Neither the other domain nor Lepton discovery supplies a missing connection."""
    del os.environ[f"loki_{domain}_url"]
    with pytest.raises(ValueError, match=f"loki_{domain}_url"):
        Settings(is_lepton_enabled=is_lepton_enabled)


def test_settings_rejects_retired_connection_in_dotenv(env_vars, tmp_path):
    """A stale mounted settings file must report the provider-block migration."""
    config = tmp_path / "settings.env"
    config.write_text("loki_tenants=old-tenant\n")
    with pytest.raises(ValueError, match="Retired log-query settings: loki_tenants"):
        Settings(_env_file=config)


def test_settings_domain_specific_loki_tenancy(env_vars):
    """Settings retain independent application/system Loki auth and org scopes."""
    os.environ.update(
        {
            "loki_app_tenants": "app-user",
            "loki_sys_tenants": "sys-user",
            "loki_app_org_id": "app-org",
            "loki_sys_org_id": "sys-org",
        }
    )

    settings = Settings()

    assert settings.loki_app_tenants == "app-user"
    assert settings.loki_sys_tenants == "sys-user"
    assert settings.loki_app_org_id == "app-org"
    assert settings.loki_sys_org_id == "sys-org"


def test_settings_invalid_boolean(env_vars):
    """Test that invalid boolean value raises validation error."""
    os.environ["prometheus_checks_enabled"] = "not-a-bool"
    with pytest.raises(ValueError):
        Settings()


def test_settings_accepts_complete_victorialogs_backend(env_vars):
    """VictoriaLogs is selectable when its connection, tenant, and native queries are configured."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "7",
            "victorialogs_app_project_id": "11",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )

    settings = Settings()

    assert settings.log_query_backend == "victorialogs"
    assert settings.victorialogs_app_url == "http://victorialogs:9428"
    assert settings.victorialogs_app_account_id == 7
    assert settings.victorialogs_app_project_id == 11


def test_settings_accepts_victorialogs_without_loki_only_fields(env_vars):
    """A VictoriaLogs-only deployment does not need legacy Loki or LogQL configuration."""
    for field_name in (
        "loki_app_url",
        "loki_sys_url",
        "loki_cluster",
        "logql_lookback_timewindow_duration_minutes",
        "logql_applog_grace_time_seconds",
        "syslog_formatter",
        "kernel_logs_only",
        "syslogindex_enabled",
    ):
        os.environ.pop(field_name)
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )

    settings = Settings()

    assert settings.log_query_backend == "victorialogs"
    assert settings.loki_app_url is None
    assert settings.loki_sys_url is None
    assert settings.loki_cluster is None
    assert settings.syslog_formatter is None


def test_settings_rejects_incomplete_victorialogs_backend(env_vars):
    """Selecting VictoriaLogs must fail closed instead of falling back to Loki or tenant zero."""
    os.environ["log_query_backend"] = "victorialogs"

    with pytest.raises(ValueError, match="missing required settings"):
        Settings()


def test_settings_rejects_victorialogs_job_queries_without_literal_placeholder(env_vars):
    """LogsQL job queries use a literal token so provider syntax is never passed to str.format."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "7",
            "victorialogs_app_project_id": "11",
            "log_query_providers": provider_queries("victorialogs", placeholder="{}"),
        }
    )

    with pytest.raises(ValueError, match=r"must contain \{job_id\}"):
        Settings()


def test_settings_accepts_domain_specific_victorialogs_tenants(env_vars):
    """Application and system queries may use distinct effective VictoriaLogs tenants."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "17",
            "victorialogs_app_project_id": "21",
            "victorialogs_sys_account_id": "27",
            "victorialogs_sys_project_id": "31",
            "victorialogs_app_require_tenant": "true",
            "victorialogs_sys_require_tenant": "true",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )

    settings = Settings()

    assert settings.victorialogs_app_account_id == 17
    assert settings.victorialogs_app_project_id == 21
    assert settings.victorialogs_sys_account_id == 27
    assert settings.victorialogs_sys_project_id == 31
    assert settings.victorialogs_app_require_tenant is True
    assert settings.victorialogs_sys_require_tenant is True


def test_settings_rejects_half_configured_victorialogs_tenant(env_vars):
    """A partial tenant pair is invalid even when tenant routing is optional."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "7",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )

    with pytest.raises(ValueError, match="account_id and project_id must be configured together"):
        Settings()


def test_settings_requires_effective_tenant_only_when_domain_flag_is_enabled(env_vars):
    """Tenantless VictoriaLogs is explicit and each protected domain fails closed."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_require_tenant": "true",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )

    with pytest.raises(ValueError, match="effective VictoriaLogs application account_id/project_id"):
        Settings()


def test_settings_does_not_inherit_victorialogs_application_tenant(env_vars):
    """Application tenancy does not satisfy an independently protected system domain."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "7",
            "victorialogs_app_project_id": "11",
            "victorialogs_sys_require_tenant": "true",
            "log_query_providers": provider_queries("victorialogs"),
        }
    )
    with pytest.raises(ValueError, match="effective VictoriaLogs system account_id/project_id"):
        Settings()


def test_settings_rejects_victorialogs_for_lepton(env_vars):
    """Lepton's credential-refresh integration remains Loki-specific."""
    os.environ.update(
        {
            "log_query_backend": "victorialogs",
            "victorialogs_app_url": "http://victorialogs:9428",
            "victorialogs_sys_url": "http://victorialogs:9428",
            "victorialogs_app_account_id": "7",
            "victorialogs_app_project_id": "11",
            "log_query_providers": provider_queries("victorialogs"),
            "is_lepton_enabled": "true",
        }
    )

    with pytest.raises(ValueError, match="require log_query_backend=loki"):
        Settings()


@pytest.mark.parametrize("url", ["http://provider:8000", "https://provider.example.com/api", "http://localhost"])
def test_provider_endpoint_validation_preserves_configured_url(url):
    """Service names and URL prefixes remain accepted without rewriting the URL."""
    settings = Settings.model_construct(fact_url=url)
    settings.validate_provider_url()
    assert settings.fact_url == url
