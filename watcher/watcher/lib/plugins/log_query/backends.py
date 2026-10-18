# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime
from typing import Any, assert_never

import requests
from recovery_libs.log_query import (
    AsyncLogQueryBackend,
    BackendName,
    LokiBackendConfig,
    VictoriaLogsBackendConfig,
    create_log_query_backend,
)
from recovery_libs.utils.retry import exponential_backoff_retry

from watcher.config import LokiConnectionSettings, VictoriaLogsConnectionSettings
from watcher.lib.plugins.log_query.batcher import LogQueryBatcher, StatsRecorder
from watcher.lib.utils.constants import (
    DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS,
    DEFAULT_LOG_QUERY_TIMEOUT_SECS,
)
from watcher.lib.utils.data import (
    LabelRemapper,
    read_api_token_from_file,
)
from watcher.lib.utils.logging import logger
from watcher.lib.utils.loki_stats import record_query_stats
from watcher.models.config import LogQueryConfig, LogQueryPolicyConfig, LogQueryProviderConfig


def create_label_remapper(log_query_policy: LogQueryPolicyConfig) -> LabelRemapper | None:
    """
    Create a LabelRemapper from config if label remapping is enabled.

    Args:
        log_query_policy: Backend-neutral polling and record-mapping policy.

    Returns:
        LabelRemapper if enabled and configured, None otherwise.
    """
    if not log_query_policy.label_remapping_enabled:
        return None

    remapping = log_query_policy.label_remapping
    if not remapping:
        return None

    simple = remapping.simple or {}
    composite_raw = remapping.composite or []
    composite = [{"attribute": c.attribute, "template": c.template, "labels": c.labels} for c in composite_raw]

    if not simple and not composite:
        return None

    logger.info(
        "[LogQuery] Label remapping enabled: %d simple mappings, %d composite mappings",
        len(simple),
        len(composite),
    )
    return LabelRemapper(simple=simple, composite=composite)


def create_log_query_client(
    state_store: dict,
    log_query_policy: LogQueryPolicyConfig,
    log_query_config: LogQueryConfig | None = None,
    connection_settings: LokiConnectionSettings | None = None,
) -> "LogQueryBatcher":
    """
    Create and return a LogQueryBatcher client.

    Args:
        state_store: Durable query cursor state.
        log_query_policy: Backend-neutral polling and record-mapping policy.
        log_query_config: Selected backend and provider settings.
        connection_settings: Optional Loki TLS settings; connection identity comes from the provider.

    Returns:
        LogQueryBatcher: Configured LogQueryBatcher client.
    """
    if log_query_config is None or (provider := log_query_config.selected_provider) is None:
        raise ValueError("datasources.log_query with an explicit provider connection is required")

    start_override = log_query_policy.start_override
    end_override = log_query_policy.end_override
    if start_override and end_override:
        start_override = datetime.fromisoformat(start_override)  # type: ignore[assignment]
        end_override = datetime.fromisoformat(end_override)  # type: ignore[assignment]

    query_timeout = (
        log_query_policy.query_timeout
        if log_query_policy.query_timeout is not None
        else (DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS, DEFAULT_LOG_QUERY_TIMEOUT_SECS)
    )
    backend_factory: LokiBackendFactory | VictoriaLogsBackendFactory
    stats_recorder: StatsRecorder | None
    match log_query_config.backend:
        case BackendName.LOKI:
            backend_factory = LokiBackendFactory(
                provider,
                connection_settings or LokiConnectionSettings(url=provider.url),
                query_timeout,
            )
            stats_recorder = record_query_stats
        case BackendName.VICTORIALOGS:
            backend_factory = VictoriaLogsBackendFactory(provider, query_timeout)
            stats_recorder = None
        case _:
            assert_never(log_query_config.backend)

    return LogQueryBatcher(
        backend_factory=backend_factory,
        log_query_config=log_query_config,
        state_store=state_store,
        max_lookback=(log_query_policy.query_max_lookback if log_query_policy.query_max_lookback is not None else 15),
        start_override=start_override,  # type: ignore[arg-type]
        end_override=end_override,  # type: ignore[arg-type]
        query_delay=log_query_policy.query_delay,
        query_timeout=query_timeout,
        query_limit=log_query_policy.query_limit or 1000,
        label_remapper=create_label_remapper(log_query_policy),
        restart_on_auth_failure=backend_factory.restarts_on_auth_failure,
        stats_recorder=stats_recorder,
    )


def _reject_legacy_connection_env(backend_name: BackendName, names: tuple[str, ...]) -> None:
    """Fail fast when a deployment still sets connection env vars the provider config replaced."""
    for name in names:
        if os.environ.get(name):
            raise ValueError(f"Move {name} to datasources.log_query.providers.{backend_name}")


def _connect_timeout(query_timeout: int | tuple[int, int]) -> float | None:
    return query_timeout[0] if isinstance(query_timeout, tuple) else None


class VictoriaLogsBackendFactory:
    """Build a VictoriaLogs backend from its provider config and connection environment."""

    restarts_on_auth_failure = False
    """VictoriaLogs credentials come from static configuration, so reconnecting cannot fix a rejection."""

    def __init__(self, provider: LogQueryProviderConfig, query_timeout: int | tuple[int, int]):
        """
        Args:
            provider: VictoriaLogs connection and tenant settings.
            query_timeout: Batcher query timeout; a tuple's first value is the transport connect timeout.
        """
        self.provider = provider
        self.query_timeout = query_timeout

    def __call__(self) -> AsyncLogQueryBackend:
        """Construct the async VictoriaLogs backend."""
        _reject_legacy_connection_env(
            BackendName.VICTORIALOGS,
            ("VICTORIALOGS_URL", "VICTORIALOGS_ACCOUNT_ID", "VICTORIALOGS_PROJECT_ID", "VICTORIALOGS_REQUIRE_TENANT"),
        )
        connection = VictoriaLogsConnectionSettings(
            url=self.provider.url,
            account_id=self.provider.account_id,
            project_id=self.provider.project_id,
            require_tenant=self.provider.require_tenant,
        )
        backend_config = VictoriaLogsBackendConfig(
            url=self.provider.url,
            account_id=self.provider.account_id,
            project_id=self.provider.project_id,
            require_tenant=self.provider.require_tenant,
            username=connection.username,
            password=connection.password.get_secret_value() if connection.password is not None else None,
            bearer_token=connection.api_token.get_secret_value() if connection.api_token is not None else None,
            cert_file=connection.cert_file,
            key_file=connection.key_file,
            ca_cert=connection.ca_cert,
            connect_timeout_seconds=_connect_timeout(self.query_timeout),
        )
        return create_log_query_backend(BackendName.VICTORIALOGS, backend_config, mode="async")


class LokiBackendFactory:
    """
    Build a Loki backend, reading credentials afresh on every call.

    The batcher calls this again on restart, so a rotated Loki token or refreshed Lepton credential takes
    effect without restarting Watcher.
    """

    restarts_on_auth_failure = True
    """Rebuilding re-reads the token file or Lepton, so an authentication failure may clear on reconnect."""

    def __init__(
        self,
        provider: LogQueryProviderConfig,
        connection_settings: LokiConnectionSettings,
        query_timeout: int | tuple[int, int],
    ):
        """
        Args:
            provider: Loki connection and tenant settings.
            connection_settings: Loki TLS settings; connection identity comes from the provider.
            query_timeout: Batcher query timeout; a tuple's first value is the transport connect timeout.
        """
        self.provider = provider
        self.connection_settings = connection_settings
        self.query_timeout = query_timeout

    def __call__(self) -> AsyncLogQueryBackend:
        """Construct the async Loki backend with freshly read credentials."""
        _reject_legacy_connection_env(BackendName.LOKI, ("LOKI_URL", "LOKI_TENANTS", "LOKI_X_SCOPE_ORG_ID"))
        loki_url, auth_token = self._get_loki_config()
        backend_config = LokiBackendConfig(
            url=loki_url,
            auth_token=auth_token,
            tenants=self.provider.tenants,
            org_id=self.provider.org_id,
            cert_file=self.connection_settings.cert_file,
            key_file=self.connection_settings.key_file,
            ca_cert=self.connection_settings.ca_cert,
            connect_timeout_seconds=_connect_timeout(self.query_timeout),
        )
        return create_log_query_backend(BackendName.LOKI, backend_config, mode="async")

    def _get_loki_url_from_config(self) -> str:
        """Return configured connection identity independently of credential loading."""

        return self.provider.url

    def _get_loki_config_from_config(self) -> tuple[str, str | None]:
        """Get the configured Loki URL and the mounted or environment token."""

        return self._get_loki_url_from_config(), read_api_token_from_file()

    def _get_worker_cluster_info(self, worker_cluster: str, lepton_base_url: str) -> dict[str, Any] | None:
        """
        Get worker cluster info from Lepton API with retries.

        Args:
            worker_cluster (str): The worker cluster name
            lepton_base_url (str): The base URL for Lepton API

        Returns:
            Optional[Dict[str, Any]]: Worker cluster data if successful, None if failed
        """
        endpoint = f"/worker-clusters/{worker_cluster}"
        url = f"{lepton_base_url}{endpoint}"

        @exponential_backoff_retry(max_retries=3, base_delay=1, exceptions=(requests.exceptions.RequestException,))
        def _make_request() -> dict[str, Any]:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            raise requests.exceptions.RequestException(f"Failed to get worker cluster info: {response.status_code}")

        try:
            return _make_request()
        except Exception as e:
            logger.exception("Error getting worker cluster info after retries", extra={"error": str(e)})
            return None

    def _get_loki_config_from_lepton(self) -> tuple[str, str] | None:
        """
        Refresh the bearer token from Lepton while preserving the configured URL.

        Returns:
            Optional[Tuple[str, str]]: A tuple containing (loki_url, bearer_token) if successful,
                None if not using Lepton or if there's an error.
        """
        worker_cluster = os.environ.get("LEPTON_WORKER_CLUSTER_NAME")
        lepton_base_url = os.environ.get("LEPTON_BASE_URL")

        if not (worker_cluster and lepton_base_url):
            return None

        worker_cluster_data = self._get_worker_cluster_info(worker_cluster, lepton_base_url)
        if worker_cluster_data:
            loki_url = self._get_loki_url_from_config()
            bearer_token = worker_cluster_data["status"]["properties"]["loki_bearer_token"]
            return loki_url, bearer_token

        return None

    def _get_loki_config(self) -> tuple[str, str | None]:
        """
        Get Loki URL and token, choosing between Lepton and standard configuration based on environment.

        Returns:
            Tuple[str, Optional[str]]: A tuple containing (loki_url, auth_token)
        """
        if os.environ.get("USE_LEPTON", "").lower() == "true":
            lepton_config = self._get_loki_config_from_lepton()
            if lepton_config:
                _, bearer_token = lepton_config
                # Lepton may refresh credentials, but endpoint identity is
                # explicit provider configuration in every deployment.
                loki_url = self._get_loki_url_from_config()
                return loki_url, f"Bearer {bearer_token}"
            logger.warning("Failed to get Lepton config, falling back to standard config")

        return self._get_loki_config_from_config()
