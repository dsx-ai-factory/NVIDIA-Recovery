# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Backend-neutral log-query facade for the Attribution Adapter.

The shared backend returns normalized records. This module deliberately keeps
the legacy Loki stream/value JSON conversion local because that shape is part
of the existing FACT observation contract, not the storage-backend contract.
"""

import json
import math
import os
import time
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import requests
from recovery_libs.log_query import (
    BackendConfig,
    BackendName,
    LogRecord,
    LokiBackend,
    LokiBackendConfig,
    NativeQuery,
    QueryFailure,
    QueryFailureKind,
    QueryOrder,
    QueryPage,
    QueryRequest,
    SyncLogQueryBackend,
    VictoriaLogsBackendConfig,
    create_log_query_backend,
)
from requests.exceptions import RequestException

from attribution_adapter.log_query_metrics import (
    LOG_QUERY_ERRORS,
    LOG_QUERY_RECORDS,
    LOG_QUERY_REQUESTS,
    LOG_QUERY_TIME,
)
from attribution_adapter.settings.settings import Settings
from attribution_adapter.utils.logger import logger

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _datetime_to_ns(value: datetime) -> int:
    """Convert a datetime to an exact UTC epoch-nanosecond boundary."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    delta = value.astimezone(UTC) - _EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1_000


def serialize_records_for_fact(records: Iterable[LogRecord]) -> str:
    """Serialize normalized records as Loki streams for ``FACTClient.supply_logs``.

    Records with the same field set are grouped into one stream. Occurrences
    are preserved, including records with identical timestamps and bodies.
    """
    streams: OrderedDict[tuple[tuple[str, str], ...], dict[str, object]] = OrderedDict()
    for record in records:
        normalized_fields = {str(key): str(value) for key, value in record.fields.items()}
        stream_key = tuple(sorted(normalized_fields.items()))
        stream = streams.setdefault(
            stream_key,
            {"stream": normalized_fields, "values": []},
        )
        values = stream["values"]
        assert isinstance(values, list)
        values.append([str(record.timestamp_ns), record.body])
    return json.dumps(list(streams.values()))


def _read_token(shared_env: str, file_env: str) -> str | None:
    token = os.environ.get(shared_env)
    token_file = os.environ.get(file_env)
    if token_file:
        with open(token_file, "r") as token_stream:
            token = token_stream.read().strip()
    return token


def _domain_env(domain: str, setting: str) -> str | None:
    """Resolve a domain-specific environment value with a shared fallback."""
    domain_env = f"{domain.upper()}_VICTORIALOGS_{setting}"
    domain_value = os.environ.get(domain_env)
    if domain_value is not None:
        return domain_value
    return os.environ.get(f"VICTORIALOGS_{setting}")


def _selected_url(domain_url: str | None, backend: str, domain: str) -> str:
    if not domain_url:
        raise ValueError(f"{backend} {domain} log-query URL is not configured")
    return domain_url


def _close_backends(backends: Iterable[SyncLogQueryBackend], *, context: str) -> None:
    """Close each distinct backend once, logging cleanup failures."""
    closed: set[int] = set()
    for backend in backends:
        if id(backend) in closed:
            continue
        closed.add(id(backend))
        try:
            backend.close()
        except Exception as error:
            logger.warning("Failed to close %s log-query backend", context, exc_info=True, extra={"error": str(error)})


def _create_backend_pair(
    backend_name: BackendName,
    app_config: BackendConfig,
    sys_config: BackendConfig,
) -> tuple[SyncLogQueryBackend, SyncLogQueryBackend]:
    """Construct a domain pair without leaking the first client if the second fails."""
    app_backend = create_log_query_backend(backend_name, app_config, mode="sync")
    try:
        sys_backend = create_log_query_backend(backend_name, sys_config, mode="sync")
    except Exception:
        _close_backends((app_backend,), context=f"partially constructed {backend_name}")
        raise
    return app_backend, sys_backend


class ADILogQueryFetcher:
    """Query application and system logs through the configured shared backend."""

    def __init__(self, settings: Settings, max_entries_per_request: int = 5000):
        self.settings = settings
        self.backend_name = BackendName(getattr(settings, "log_query_backend", BackendName.LOKI))
        self.max_entries_per_request = max_entries_per_request
        self.http_timeout = self._http_timeout()
        self._closed = False
        self.app_log_backend, self.sys_log_backend = self._create_backends()

    def _http_timeout(self) -> int:
        if self.backend_name is BackendName.VICTORIALOGS:
            return self.settings.victorialogs_http_timeout_seconds
        return self.settings.loki_http_timeout_seconds

    def _create_backends(self):
        if self.backend_name is BackendName.LOKI:
            return self._create_loki_backends()
        if self.backend_name is BackendName.VICTORIALOGS:
            return self._create_victorialogs_backends()
        raise ValueError(f"Unsupported log-query backend: {self.backend_name}")

    def _create_loki_backends(self) -> tuple[SyncLogQueryBackend, SyncLogQueryBackend]:
        app_token = _read_token("LOKI_API_TOKEN", "APP_LOKI_API_TOKEN_FILE")
        sys_token = _read_token("LOKI_API_TOKEN", "SYS_LOKI_API_TOKEN_FILE")

        app_config = LokiBackendConfig(
            url=_selected_url(self.settings.loki_app_url, "Loki", "application"),
            auth_token=app_token,
            tenants=self.settings.loki_app_tenants,
            org_id=self.settings.loki_app_org_id,
            cert_file=os.environ.get("APP_LOKI_CERT_FILE"),
            key_file=os.environ.get("APP_LOKI_KEY_FILE"),
            ca_cert=os.environ.get("APP_LOKI_CA_CERT"),
        )
        sys_config = LokiBackendConfig(
            url=_selected_url(self.settings.loki_sys_url, "Loki", "system"),
            auth_token=sys_token,
            tenants=self.settings.loki_sys_tenants,
            org_id=self.settings.loki_sys_org_id,
            cert_file=os.environ.get("SYS_LOKI_CERT_FILE"),
            key_file=os.environ.get("SYS_LOKI_KEY_FILE"),
            ca_cert=os.environ.get("SYS_LOKI_CA_CERT"),
        )
        return _create_backend_pair(BackendName.LOKI, app_config, sys_config)

    def _create_victorialogs_backends(self) -> tuple[SyncLogQueryBackend, SyncLogQueryBackend]:
        app_config = VictoriaLogsBackendConfig(
            url=_selected_url(
                self.settings.victorialogs_app_url,
                "VictoriaLogs",
                "application",
            ),
            account_id=self.settings.victorialogs_app_account_id,
            project_id=self.settings.victorialogs_app_project_id,
            require_tenant=self.settings.victorialogs_app_require_tenant,
            username=_domain_env("app", "USERNAME"),
            password=_domain_env("app", "PASSWORD"),
            bearer_token=_read_token("VICTORIALOGS_API_TOKEN", "APP_VICTORIALOGS_API_TOKEN_FILE"),
            cert_file=os.environ.get("APP_VICTORIALOGS_CERT_FILE"),
            key_file=os.environ.get("APP_VICTORIALOGS_KEY_FILE"),
            ca_cert=os.environ.get("APP_VICTORIALOGS_CA_CERT"),
        )
        sys_config = VictoriaLogsBackendConfig(
            url=_selected_url(
                self.settings.victorialogs_sys_url,
                "VictoriaLogs",
                "system",
            ),
            account_id=self.settings.victorialogs_sys_account_id,
            project_id=self.settings.victorialogs_sys_project_id,
            require_tenant=self.settings.victorialogs_sys_require_tenant,
            username=_domain_env("sys", "USERNAME"),
            password=_domain_env("sys", "PASSWORD"),
            bearer_token=_read_token("VICTORIALOGS_API_TOKEN", "SYS_VICTORIALOGS_API_TOKEN_FILE"),
            cert_file=os.environ.get("SYS_VICTORIALOGS_CERT_FILE"),
            key_file=os.environ.get("SYS_VICTORIALOGS_KEY_FILE"),
            ca_cert=os.environ.get("SYS_VICTORIALOGS_CA_CERT"),
        )
        return _create_backend_pair(BackendName.VICTORIALOGS, app_config, sys_config)

    def close(self) -> None:
        """Close the owned domain backends once, including aliased instances."""
        if self._closed:
            return
        self._closed = True
        _close_backends(
            (self.app_log_backend, self.sys_log_backend),
            context=self.backend_name,
        )

    def get_sys_telemetry_logs(
        self,
        job_id: str,
        log_query: str,
        end_time: datetime,
        start_time: datetime,
        limit: int = 5000,
        direction: QueryOrder = QueryOrder.FORWARD,
        page_token: str | None = None,
        timeout_seconds: float | None = None,
        deadline_monotonic: float | None = None,
    ) -> tuple[str, str | None]:
        """Fetch one system-log page within one bounded retry operation."""
        return self.get_telemetry_logs(
            backend=self.sys_log_backend,
            domain="system",
            job_id=job_id,
            log_query=log_query,
            end_time=end_time,
            start_time=start_time,
            limit=limit,
            direction=direction,
            page_token=page_token,
            timeout_seconds=timeout_seconds,
            _deadline_monotonic=deadline_monotonic,
        )

    def get_app_telemetry_logs(
        self,
        job_id: str,
        log_query: str,
        end_time: datetime,
        start_time: datetime,
        limit: int = 5000,
        direction: QueryOrder = QueryOrder.FORWARD,
        page_token: str | None = None,
        timeout_seconds: float | None = None,
        deadline_monotonic: float | None = None,
    ) -> tuple[str, str | None]:
        """Fetch one application-log page within one bounded retry operation."""
        return self.get_telemetry_logs(
            backend=self.app_log_backend,
            domain="application",
            job_id=job_id,
            log_query=log_query,
            end_time=end_time,
            start_time=start_time,
            limit=limit,
            direction=direction,
            page_token=page_token,
            timeout_seconds=timeout_seconds,
            _deadline_monotonic=deadline_monotonic,
        )

    def get_telemetry_logs(
        self,
        backend: SyncLogQueryBackend,
        domain: str,
        job_id: str,
        log_query: str,
        end_time: datetime,
        start_time: datetime,
        limit: int = 5000,
        direction: QueryOrder = QueryOrder.FORWARD,
        page_token: str | None = None,
        timeout_seconds: float | None = None,
        _deadline_monotonic: float | None = None,
    ) -> tuple[str, str | None]:
        """Fetch one provider-native page through the shared log-query contract.

        The monotonic deadline covers every retry and retry delay in this
        public fetch operation. A synchronous transport call cannot be
        preempted here, so a response that returns after the deadline is
        discarded as a timeout instead of being presented as timely success.
        """
        del job_id  # Kept for compatibility with the public legacy fetcher methods.
        if self.backend_name is BackendName.LOKI and "'" in log_query:
            log_query = log_query.replace("'", '"')

        native_query = NativeQuery(
            query_id=f"attribution-{domain}",
            dialect=backend.dialect,
            text=log_query,
        )
        deadline = self._new_operation_deadline(
            timeout_seconds,
            native_query.query_id,
            deadline_monotonic=_deadline_monotonic,
        )
        request_timeout = self._remaining_timeout(deadline, native_query.query_id)
        request = QueryRequest(
            query=native_query,
            start_ns=_datetime_to_ns(start_time),
            end_ns=_datetime_to_ns(end_time),
            order=direction,
            page_size=limit,
            page_token=page_token,
            timeout_seconds=request_timeout,
        )

        logger.info(
            "Fetching %s logs from %s from %s to %s with limit %s",
            domain,
            self.backend_name,
            start_time,
            end_time,
            limit,
        )
        clock_start = time.monotonic()
        try:
            page = self._query_page_with_retry(backend, request, deadline)
            duration = time.monotonic() - clock_start
            LOG_QUERY_TIME.labels(backend=self.backend_name, query_id=native_query.query_id).observe(duration)
            LOG_QUERY_REQUESTS.labels(
                backend=self.backend_name,
                query_id=native_query.query_id,
                outcome="success" if page.records else "empty",
            ).inc()
            LOG_QUERY_RECORDS.labels(
                backend=self.backend_name,
                query_id=native_query.query_id,
            ).inc(len(page.records))
        except Exception as error:
            error_kind = error.kind.value if isinstance(error, QueryFailure) else "unexpected"
            LOG_QUERY_REQUESTS.labels(
                backend=self.backend_name,
                query_id=native_query.query_id,
                outcome="failure",
            ).inc()
            LOG_QUERY_ERRORS.labels(
                backend=self.backend_name,
                query_id=native_query.query_id,
                error_kind=error_kind,
            ).inc()
            logger.error("Error fetching logs from %s: %s", self.backend_name, error)
            raise

        return self._serialize_records_for_fact(page.records), page.next_page_token

    def _serialize_records_for_fact(self, records: Iterable[LogRecord]) -> str:
        """Apply component-local normalization and preserve FACT's legacy wire shape."""
        return serialize_records_for_fact(records)

    def _new_operation_deadline(
        self,
        timeout_seconds: float | None,
        query_id: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> float:
        """Resolve the earliest caller or facade deadline for one page fetch."""
        configured_timeout = float(self.http_timeout)
        requested_timeout = configured_timeout if timeout_seconds is None else float(timeout_seconds)
        budget = min(configured_timeout, requested_timeout)
        if not math.isfinite(budget) or budget <= 0:
            raise self._timeout_failure(query_id)
        now = time.monotonic()
        deadline = now + budget
        if deadline_monotonic is not None:
            if not math.isfinite(deadline_monotonic):
                raise self._timeout_failure(query_id)
            deadline = min(deadline, deadline_monotonic)
        if deadline <= now:
            raise self._timeout_failure(query_id)
        return deadline

    def _remaining_timeout(self, deadline: float, query_id: str) -> float:
        """Return positive remaining budget or fail before another operation."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise self._timeout_failure(query_id)
        return remaining

    def _timeout_failure(self, query_id: str) -> QueryFailure:
        """Build the typed failure used when a facade operation exhausts its budget."""
        return QueryFailure(
            QueryFailureKind.TIMEOUT,
            backend=self.backend_name,
            query_id=query_id,
            message="log-query operation deadline exhausted",
            retryable=True,
        )

    def _query_page_with_retry(
        self,
        backend: SyncLogQueryBackend,
        request: QueryRequest,
        deadline: float,
    ) -> QueryPage:
        """Retry transient failures without exceeding the operation deadline.

        ``timeout_seconds`` is reduced before every transport attempt. This
        bounds retries and sleeps. It does not make a synchronous, trickling
        response cancellable while the backend call is in flight; elapsed
        responses are rejected immediately when that call returns.
        """
        max_retries = 3
        for retry in range(max_retries + 1):
            attempt = replace(
                request,
                timeout_seconds=self._remaining_timeout(deadline, request.query.query_id),
            )
            try:
                page = backend.query_page(attempt)
            except QueryFailure as error:
                if not error.retryable or retry == max_retries:
                    raise
                delay = error.retry_after_seconds
                if delay is None:
                    delay = 0.5 * (2**retry)
                remaining = self._remaining_timeout(deadline, request.query.query_id)
                if delay >= remaining:
                    raise self._timeout_failure(request.query.query_id) from error
                logger.warning(
                    "Retrying %s log query after %ss (%s/%s)",
                    self.backend_name,
                    delay,
                    retry + 1,
                    max_retries,
                )
                time.sleep(delay)
            else:
                self._remaining_timeout(deadline, request.query.query_id)
                return page
        raise AssertionError("unreachable retry loop")


class ADILeptonLogQueryFetcher(ADILogQueryFetcher):
    """Loki-backed Lepton adapter with worker-cluster credential refresh."""

    def __init__(self, settings: Settings, max_entries_per_request: int = 5000):
        self.worker_cluster = os.environ.get("LEPTON_WORKER_CLUSTER_NAME")
        self.lepton_base_url = os.environ.get("LEPTON_BASE_URL")
        self.worker_cluster_data = self.get_worker_cluster()
        super().__init__(settings, max_entries_per_request)

    def _loki_backend_configs(self) -> tuple[LokiBackendConfig, LokiBackendConfig]:
        """Build current Lepton application/system Loki configurations."""
        properties = self.worker_cluster_data["status"]["properties"]
        auth_token = f"Bearer {properties['loki_bearer_token']}"
        app_config = LokiBackendConfig(
            url=_selected_url(self.settings.loki_app_url, "Loki", "application"),
            auth_token=auth_token,
            tenants=self.settings.loki_app_tenants,
            org_id=self.settings.loki_app_org_id,
            cert_file=os.environ.get("APP_LOKI_CERT_FILE"),
            key_file=os.environ.get("APP_LOKI_KEY_FILE"),
            ca_cert=os.environ.get("APP_LOKI_CA_CERT"),
        )
        sys_config = LokiBackendConfig(
            url=_selected_url(self.settings.loki_sys_url, "Loki", "system"),
            auth_token=auth_token,
            tenants=self.settings.loki_sys_tenants,
            org_id=self.settings.loki_sys_org_id,
            cert_file=os.environ.get("SYS_LOKI_CERT_FILE"),
            key_file=os.environ.get("SYS_LOKI_KEY_FILE"),
            ca_cert=os.environ.get("SYS_LOKI_CA_CERT"),
        )
        return app_config, sys_config

    def _create_loki_backends(self) -> tuple[SyncLogQueryBackend, SyncLogQueryBackend]:
        app_config, sys_config = self._loki_backend_configs()
        return _create_backend_pair(BackendName.LOKI, app_config, sys_config)

    def get_telemetry_logs(
        self,
        backend: SyncLogQueryBackend,
        domain: str,
        job_id: str,
        log_query: str,
        end_time: datetime,
        start_time: datetime,
        limit: int = 5000,
        direction: QueryOrder = QueryOrder.FORWARD,
        page_token: str | None = None,
        timeout_seconds: float | None = None,
        _deadline_monotonic: float | None = None,
    ) -> tuple[str, str | None]:
        """Refresh Lepton credentials once after an unauthenticated Loki response."""
        deadline = self._new_operation_deadline(
            timeout_seconds,
            f"attribution-{domain}",
            deadline_monotonic=_deadline_monotonic,
        )
        try:
            return super().get_telemetry_logs(
                backend=backend,
                domain=domain,
                job_id=job_id,
                log_query=log_query,
                end_time=end_time,
                start_time=start_time,
                limit=limit,
                direction=direction,
                page_token=page_token,
                timeout_seconds=timeout_seconds,
                _deadline_monotonic=deadline,
            )
        except QueryFailure as error:
            if error.kind != QueryFailureKind.UNAUTHENTICATED:
                raise
            refresh_timeout = self._remaining_timeout(deadline, f"attribution-{domain}")
            self._refresh_loki_backends(timeout_seconds=refresh_timeout)
            refreshed_backend = self.app_log_backend if domain == "application" else self.sys_log_backend
            return super().get_telemetry_logs(
                backend=refreshed_backend,
                domain=domain,
                job_id=job_id,
                log_query=log_query,
                end_time=end_time,
                start_time=start_time,
                limit=limit,
                direction=direction,
                page_token=page_token,
                timeout_seconds=timeout_seconds,
                _deadline_monotonic=deadline,
            )

    def _refresh_loki_backends(self, *, timeout_seconds: float) -> None:
        """Refresh credentials in place so existing signed page tokens remain valid."""
        previous_cluster_data = self.worker_cluster_data
        replacement_cluster_data = self.get_worker_cluster(timeout=timeout_seconds)
        self.worker_cluster_data = replacement_cluster_data
        try:
            app_config, sys_config = self._loki_backend_configs()
            # The static factory guarantees concrete LokiBackend instances for
            # this Loki-only facade. replace_config preserves their page-token
            # signers and rejects any endpoint or tenant-scope change.
            cast(LokiBackend, self.app_log_backend).replace_config(app_config)
            cast(LokiBackend, self.sys_log_backend).replace_config(sys_config)
        except Exception:
            self.worker_cluster_data = previous_cluster_data
            raise

    def get_worker_cluster(self, timeout: float = 10) -> dict:
        """Resolve the Lepton worker cluster's Loki endpoint and bearer token."""
        endpoint = f"/worker-clusters/{self.worker_cluster}"
        try:
            response = requests.get(f"{self.lepton_base_url}{endpoint}", timeout=timeout)
            if response.status_code == 200:
                return response.json()
            try:
                error_data = response.json()
                error_message = error_data.get("message", f"API error: {response.status_code}")
            except json.JSONDecodeError:
                error_message = f"API error: {response.status_code}"
            raise RuntimeError(error_message)
        except RequestException as error:
            raise RuntimeError(f"Connection error: {error}") from error

    def _serialize_records_for_fact(self, records: Iterable[LogRecord]) -> str:
        """Apply Lepton's existing body and node-name normalization before FACT."""
        normalized: list[LogRecord] = []
        for record in records:
            fields = dict(record.fields)
            if "node_name" not in fields:
                if "host" in fields:
                    fields["node_name"] = fields.pop("host")
                elif "node" in fields:
                    fields["node_name"] = fields.pop("node")
            body = record.body
            try:
                message = json.loads(body)
                if isinstance(message, dict) and isinstance(message.get("message"), str):
                    body = message["message"]
            except json.JSONDecodeError:
                pass
            normalized.append(
                LogRecord(
                    timestamp_ns=record.timestamp_ns,
                    body=body,
                    fields=fields,
                    source_id=record.source_id,
                )
            )
        return serialize_records_for_fact(normalized)


def create_attribution_log_fetcher(
    settings: Settings,
    max_entries_per_request: int = 5000,
) -> ADILogQueryFetcher:
    """Create the configured Attribution log-query fetcher."""
    backend_name = BackendName(getattr(settings, "log_query_backend", BackendName.LOKI))
    if settings.is_lepton_enabled:
        if backend_name is not BackendName.LOKI:
            raise ValueError("Lepton log queries currently require log_query_backend=loki")
        return ADILeptonLogQueryFetcher(settings, max_entries_per_request)
    return ADILogQueryFetcher(settings, max_entries_per_request)
