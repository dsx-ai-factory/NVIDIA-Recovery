# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator
from pydantic_settings import BaseSettings
from recovery_libs.log_query import BackendName, QueryOrder


class AttributionQueries(BaseModel):
    """Native row queries for one provider; formatting remains dialect-specific."""

    model_config = ConfigDict(extra="forbid")
    application: str = Field(min_length=1, description="Application-log template scoped by job ID.")
    system: str = Field(min_length=1, description="System-log query.")
    epilogue: str = Field(min_length=1, description="Epilogue/slow-signal template.")


class LogQueryProviderSettings(BaseModel):
    """Attribution's projection of one global log-query provider."""

    model_config = ConfigDict(extra="forbid")
    queries: AttributionQueries = Field(description="Native templates used only by Attribution.")


class Settings(BaseSettings):
    """Configuration for the attribution-adapter.

    Owns all attribution-side settings per design §9: FACT, log-query backends, Prometheus,
    provider-native query templates, log-fetch optimization, and Lepton/tenant-scoping.
    RabbitMQ, Job Manager, rack caps, and slow-signal scheduling
    remain in the Policy Engine (anomaly_detection_isolator).
    """

    # FACT client
    fact_url: str | None = None
    fact_http_timeout_seconds: int = 30
    # Default GPUs (ranks) per node, used only when node_count/total_gpu_count are unavailable.
    fact_default_ranks_per_node: int = 8

    # NVRX Straggler Detection Configuration
    nvrx_straggler_threshold: float = 0.7
    nvrx_straggler_score_types: list[int] = [0, 1]  # 0=RELATIVE, 1=INDIVIDUAL

    # Megatron Straggler Detection Configuration
    megatron_straggler_threshold: float = 0.85
    megatron_straggler_score_types: list[int] = [0]  # 0=RELATIVE
    megatron_straggler_infer_threshold_from_data: bool = True

    # Domain-specific provider settings rendered from global.log_query.providers.
    loki_app_url: str | None = None
    loki_sys_url: str | None = None
    loki_cluster: str | None = None
    loki_app_tenants: str | None = None
    loki_sys_tenants: str | None = None
    loki_app_org_id: str | None = None
    loki_sys_org_id: str | None = None
    loki_http_timeout_seconds: int = 30

    # Only the selected provider's domain connections are required.
    log_query_backend: BackendName = BackendName.LOKI
    log_query_providers: dict[BackendName, LogQueryProviderSettings] = Field(
        description="Provider-owned query templates projected from the global Helm catalog."
    )
    victorialogs_app_url: str | None = None
    victorialogs_sys_url: str | None = None
    victorialogs_app_account_id: int | None = None
    victorialogs_app_project_id: int | None = None
    victorialogs_sys_account_id: int | None = None
    victorialogs_sys_project_id: int | None = None
    victorialogs_app_require_tenant: bool = False
    victorialogs_sys_require_tenant: bool = False
    victorialogs_http_timeout_seconds: int = 30

    # Prometheus metrics source
    prometheus_url: str
    prometheus_cluster: str
    prometheus_tenants: str | None = None
    prometheus_checks_enabled: bool
    prometheus_hostname_label: str

    # Log-fetch policy (independent from provider-native query templates).
    logql_lookback_timewindow_duration_minutes: int | None = None
    logql_applog_grace_time_seconds: int | None = None
    # Prometheus query templates are not log-query provider configuration.
    ib_metrics_query: str
    eth_metrics_query: str
    health_check_query: str

    # Syslog / kernel log handling
    syslog_formatter: str | None = None
    kernel_logs_only: bool | None = None
    syslogindex_enabled: bool | None = None

    # Log fetch optimization feature flag — single toggle to enable/disable all changes
    log_fetch_optimization_enabled: bool = False  # disabled by default for safe rollout

    # Log fetching configuration (only effective when log_fetch_optimization_enabled=True)
    log_fetch_direction: QueryOrder = QueryOrder.BACKWARD
    log_fetch_timeout_syslogs_seconds: int = 60
    log_fetch_timeout_applogs_seconds: int = 60
    log_fetch_timeout_slow_signals_seconds: int = 300

    # Lepton worker-cluster support
    is_lepton_enabled: bool | None = False

    @model_validator(mode="before")
    @classmethod
    def reject_shared_log_query_settings(cls, values: Any) -> Any:
        """Reject retired connection settings instead of silently ignoring tenant scope."""
        retired = {
            "loki_url",
            "loki_tenants",
            "use_x_scope_org_id",
            "x_scope_org_id",
            "victorialogs_url",
            "victorialogs_account_id",
            "victorialogs_project_id",
        }
        # BaseSettings ignores unknown environment names, unlike dotenv/kwargs.
        supplied = {key.lower() for key, value in os.environ.items() if value}
        if isinstance(values, dict):
            supplied.update(key.lower() for key, value in values.items() if value is not None and value != "")
        invalid = sorted(retired & supplied)
        if invalid:
            raise ValueError(
                "Retired log-query settings: "
                + ", ".join(invalid)
                + "; configure application and system connections and tenants in "
                "global.log_query.providers.<backend> (domain-specific app/sys environment fields)"
            )
        retired_queries = {
            "applog_query",
            "syslog_query",
            "epilogue_query",
            "victorialogs_applog_query",
            "victorialogs_syslog_query",
            "victorialogs_epilogue_query",
            "logql_json_filter",
            "logql_syslog_index_filter",
            "logql_kernel_filter",
            "logql_applog_exclude_filter",
            "logql_prolog_exclude_filter",
            "logql_epilog_exclude_filter",
            "logql_applog_format",
        }
        # Retired query names indicate an unmigrated deployment, never an
        # alternate source of provider defaults.
        supplied_query_keys = {key.lower() for key in os.environ}
        if isinstance(values, dict):
            supplied_query_keys.update(key.lower() for key in values)
        invalid_queries = sorted(retired_queries & supplied_query_keys)
        if invalid_queries:
            raise ValueError(
                "Retired log-query templates or unused filters: "
                + ", ".join(invalid_queries)
                + "; configure global.log_query.providers.<backend>.queries.attribution "
                "(log_query_providers.<backend>.queries in runtime settings)"
            )
        return values

    @model_validator(mode="after")
    def validate_selected_log_query_backend(self) -> "Settings":
        """Validate only the provider configuration selected for log queries."""
        if self.log_query_backend not in self.log_query_providers:
            raise ValueError(f"{self.log_query_backend} backend is missing required settings: log_query_providers")
        queries = self.log_query_providers[self.log_query_backend].queries
        if self.log_query_backend is BackendName.VICTORIALOGS:
            for name in ("application", "epilogue"):
                if "{job_id}" not in getattr(queries, name):
                    raise ValueError(f"log_query_providers.victorialogs.queries.{name} must contain {{job_id}}")
        if self.log_query_backend is BackendName.LOKI:
            loki_missing: list[str] = []
            if not self.loki_app_url:
                loki_missing.append("loki_app_url")
            if not self.loki_sys_url:
                loki_missing.append("loki_sys_url")
            if not self.loki_cluster:
                loki_missing.append("loki_cluster")
            for field_name in (
                "logql_lookback_timewindow_duration_minutes",
                "logql_applog_grace_time_seconds",
                "syslog_formatter",
                "kernel_logs_only",
                "syslogindex_enabled",
            ):
                if getattr(self, field_name) is None:
                    loki_missing.append(field_name)
            if loki_missing:
                raise ValueError("Loki log-query backend is missing required settings: " + ", ".join(loki_missing))
            return self

        victoria_missing: list[str] = []
        if not self.victorialogs_app_url:
            victoria_missing.append("victorialogs_app_url")
        if not self.victorialogs_sys_url:
            victoria_missing.append("victorialogs_sys_url")
        for pair_name, account_id, project_id in (
            (
                "VictoriaLogs application tenant override",
                self.victorialogs_app_account_id,
                self.victorialogs_app_project_id,
            ),
            (
                "VictoriaLogs system tenant override",
                self.victorialogs_sys_account_id,
                self.victorialogs_sys_project_id,
            ),
        ):
            if (account_id is None) != (project_id is None):
                raise ValueError(f"{pair_name} account_id and project_id must be configured together")

        app_account_id = self.victorialogs_app_account_id
        app_project_id = self.victorialogs_app_project_id
        sys_account_id = self.victorialogs_sys_account_id
        sys_project_id = self.victorialogs_sys_project_id
        if self.victorialogs_app_require_tenant and (app_account_id is None or app_project_id is None):
            victoria_missing.append("effective VictoriaLogs application account_id/project_id")
        if self.victorialogs_sys_require_tenant and (sys_account_id is None or sys_project_id is None):
            victoria_missing.append("effective VictoriaLogs system account_id/project_id")
        for field_name in (
            "victorialogs_app_account_id",
            "victorialogs_app_project_id",
            "victorialogs_sys_account_id",
            "victorialogs_sys_project_id",
        ):
            value = getattr(self, field_name)
            if value is not None and not 0 <= value <= 2**32 - 1:
                raise ValueError(f"{field_name} must be an unsigned 32-bit integer")

        if self.is_lepton_enabled:
            raise ValueError("Lepton log queries currently require log_query_backend=loki")
        if victoria_missing:
            raise ValueError(
                "VictoriaLogs log-query backend is missing required settings: " + ", ".join(victoria_missing)
            )
        return self

    def validate_provider_url(self) -> None:
        """Reject missing or invalid provider endpoints before the workload starts."""
        try:
            url = HttpUrl(self.fact_url or "")
            if not url.host:
                raise ValueError("missing host")
        except ValueError:
            raise ValueError("fact_url must be a nonempty HTTP(S) URL when Attribution is enabled") from None

    class Config:
        env_file = os.environ.get("CONFIG_FILE_PATH")
