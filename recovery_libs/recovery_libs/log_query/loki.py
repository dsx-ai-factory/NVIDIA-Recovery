# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Loki implementation of the neutral log-query contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from secrets import token_bytes
from time import monotonic
from typing import Any

import httpx

from recovery_libs.log_query._certificates import days_until_expiry
from recovery_libs.log_query._http import build_verify, raise_http_failure, request_timeout, translate_transport_error
from recovery_libs.log_query._pagination import PageContext, finish_page, prepare_page, token_scope
from recovery_libs.log_query.config import LokiBackendConfig
from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind
from recovery_libs.log_query.models import BackendName, LogRecord, QueryDialect, QueryPage, QueryRequest, QueryStats

_BACKEND = BackendName.LOKI
# Stay within Loki's default max_entries_limit_per_query, including overlap.
# Larger logical pages may yield smaller transport pages without losing data.
_MAX_FETCH_LIMIT = 5_000


def _malformed(query_id: str, message: str, cause: Exception | None = None) -> QueryFailure:
    failure = QueryFailure(
        QueryFailureKind.MALFORMED_RESPONSE,
        backend=_BACKEND,
        query_id=query_id,
        message=message,
        retryable=True,
    )
    if cause is not None:
        failure.__cause__ = cause
    return failure


def _parse_timestamp_ns(value: object) -> int:
    """Parse Loki's decimal-string nanosecond timestamp without coercion."""
    if not isinstance(value, str) or not value.isdecimal():
        raise TypeError("stream timestamp must be a decimal nanosecond string")
    return int(value)


def _connection_options(config: LokiBackendConfig) -> tuple[dict[str, str], httpx.Auth | tuple[str, str] | None, Any]:
    headers = {"Accept": "application/json"}
    auth: httpx.Auth | tuple[str, str] | None = None
    if config.tenants and config.auth_token:
        auth = (config.tenants, config.auth_token)
    elif config.auth_token:
        headers["Authorization"] = config.auth_token
    if config.org_id:
        headers["X-Scope-OrgID"] = config.org_id
    verify = build_verify(
        verify_tls=config.verify_tls,
        ca_cert=config.ca_cert,
        cert_file=config.cert_file,
        key_file=config.key_file,
    )
    return headers, auth, verify


def _parse_loki_response(
    response: httpx.Response, request: QueryRequest
) -> tuple[list[LogRecord], Mapping[str, object]]:
    try:
        payload = response.json()
    except ValueError as error:
        raise _malformed(request.query.query_id, "response body is not valid JSON", error)
    try:
        if not isinstance(payload, Mapping) or payload.get("status") != "success":
            raise TypeError("top-level status is not success")
        data = payload["data"]
        if not isinstance(data, Mapping) or data.get("resultType") != "streams":
            raise TypeError("resultType is not streams")
        streams = data["result"]
        if not isinstance(streams, list):
            raise TypeError("result is not a list")
        records: list[LogRecord] = []
        for stream_result in streams:
            if not isinstance(stream_result, Mapping):
                raise TypeError("stream result is not an object")
            labels = stream_result["stream"]
            values = stream_result["values"]
            if not isinstance(labels, Mapping) or not isinstance(values, list):
                raise TypeError("stream labels or values have an invalid type")
            normalized_labels = {str(key): value for key, value in labels.items()}
            if any(not isinstance(value, str) for value in normalized_labels.values()):
                raise TypeError("stream label values must be strings")
            for value in values:
                if (
                    not isinstance(value, Sequence)
                    or isinstance(value, (str, bytes))
                    or len(value) < 2
                    or not isinstance(value[1], str)
                ):
                    raise TypeError("stream value is not a timestamp/body pair")
                timestamp_ns = _parse_timestamp_ns(value[0])
                records.append(LogRecord(timestamp_ns=timestamp_ns, body=value[1], fields=normalized_labels))
        stats = data.get("stats", {})
        if not isinstance(stats, Mapping):
            raise TypeError("stats is not an object")
        return records, stats
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise _malformed(request.query.query_id, "response does not match the Loki streams schema", error)


def _validate_dialect(request: QueryRequest) -> None:
    if request.query.dialect != QueryDialect.LOGQL:
        raise QueryFailure(
            QueryFailureKind.UNSUPPORTED_SEMANTICS,
            backend=_BACKEND,
            query_id=request.query.query_id,
            message="Loki requires a LogQL query variant",
            retryable=False,
        )


def _params(request: QueryRequest, context: PageContext) -> dict[str, str | int]:
    return {
        "query": request.query.text,
        "start": context.start_ns,
        "end": context.end_ns,
        "limit": context.fetch_limit,
        "direction": request.order,
    }


def _build_page(
    *,
    request: QueryRequest,
    context: PageContext,
    records: list[LogRecord],
    provider_stats: Mapping[str, object],
    response_bytes: int,
    duration_seconds: float,
) -> QueryPage:
    in_range = [record for record in records if context.start_ns <= record.timestamp_ns < context.end_ns]
    page_records, token, complete = finish_page(
        request=request,
        backend=_BACKEND,
        context=context,
        records=in_range,
        response_record_count=len(records),
    )
    return QueryPage(
        records=page_records,
        next_page_token=token,
        complete=complete,
        stats=QueryStats(
            duration_seconds=duration_seconds,
            attempts=1,
            response_bytes=response_bytes,
            returned_records=len(page_records),
            provider=provider_stats,
        ),
    )


def _stat(section: object, key: str) -> float | None:
    value = section.get(key) if isinstance(section, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


@dataclass(frozen=True, slots=True)
class LokiQueryStats:
    """Loki's own accounting for one request, read from the response's ``stats`` block.

    Fields are None when Loki omits them; which sections Loki reports varies by version and query path.

    Attributes:
        bytes_processed: ``summary.totalBytesProcessed``.
        lines_processed: ``summary.totalLinesProcessed``.
        entries_returned: ``summary.totalEntriesReturned``.
        exec_time_seconds: ``summary.execTime``.
        queue_time_seconds: ``summary.queueTime``.
        ingester_chunks_matched: ``ingester.totalChunksMatched``.
        ingester_lines_sent: ``ingester.totalLinesSent``.
        ingesters_reached: ``ingester.totalReached``.
        store_chunks_downloaded: ``store.totalChunksDownloaded``.
    """

    bytes_processed: float | None = None
    lines_processed: float | None = None
    entries_returned: float | None = None
    exec_time_seconds: float | None = None
    queue_time_seconds: float | None = None
    ingester_chunks_matched: float | None = None
    ingester_lines_sent: float | None = None
    ingesters_reached: float | None = None
    store_chunks_downloaded: float | None = None

    @classmethod
    def from_query_stats(cls, stats: QueryStats) -> LokiQueryStats:
        """Read the Loki statistics a Loki backend left in ``stats.provider``; non-numeric values become None."""
        summary = stats.provider.get("summary")
        ingester = stats.provider.get("ingester")
        store = stats.provider.get("store")
        return cls(
            bytes_processed=_stat(summary, "totalBytesProcessed"),
            lines_processed=_stat(summary, "totalLinesProcessed"),
            entries_returned=_stat(summary, "totalEntriesReturned"),
            exec_time_seconds=_stat(summary, "execTime"),
            queue_time_seconds=_stat(summary, "queueTime"),
            ingester_chunks_matched=_stat(ingester, "totalChunksMatched"),
            ingester_lines_sent=_stat(ingester, "totalLinesSent"),
            ingesters_reached=_stat(ingester, "totalReached"),
            store_chunks_downloaded=_stat(store, "totalChunksDownloaded"),
        )


class LokiBackend:
    """Synchronous Loki query backend."""

    name: BackendName = BackendName.LOKI
    dialect: QueryDialect = QueryDialect.LOGQL

    def __init__(self, config: LokiBackendConfig, *, client: httpx.Client | None = None) -> None:
        """Create a driver from validated provider configuration."""
        self._config = config
        self._token_scope = token_scope(config.url.rstrip("/"), config.tenants, config.org_id)
        self._page_token_secret = token_bytes(32)
        self._headers, self._auth, verify = _connection_options(config)
        self._owns_client = client is None
        self._client = client or httpx.Client(verify=verify)

    def query_page(self, request: QueryRequest) -> QueryPage:
        """Execute one integer-nanosecond Loki range query."""
        _validate_dialect(request)
        context = prepare_page(
            request, _BACKEND, self._token_scope, self._page_token_secret, max_fetch_limit=_MAX_FETCH_LIMIT
        )
        started = monotonic()
        try:
            response = self._client.get(
                f"{self._config.url.rstrip('/')}/loki/api/v1/query_range",
                params=_params(request, context),
                headers=self._headers,
                auth=self._auth,
                timeout=request_timeout(request.timeout_seconds, self._config.connect_timeout_seconds),
            )
        except httpx.RequestError as error:
            raise translate_transport_error(error, backend=_BACKEND, query_id=request.query.query_id) from error
        duration = monotonic() - started
        raise_http_failure(response, backend=_BACKEND, query_id=request.query.query_id)
        records, provider_stats = _parse_loki_response(response, request)
        return _build_page(
            request=request,
            context=context,
            records=records,
            provider_stats=provider_stats,
            response_bytes=len(response.content),
            duration_seconds=duration,
        )

    def replace_config(self, config: LokiBackendConfig, *, client: httpx.Client | None = None) -> None:
        """Refresh credentials while preserving valid page tokens.

        URL, Basic-auth tenant username, and Loki org ID form the pagination
        security scope and cannot change in place. Callers must create a new
        backend when that scope changes. Credential and TLS changes preserve
        this instance's private page-token signer.

        Args:
            config: Replacement Loki connection and credential configuration.
            client: Optional externally owned HTTP client, primarily for
                embedding or deterministic transport tests.

        Raises:
            QueryFailure: If the replacement changes pagination scope.
        """
        replacement_scope = token_scope(config.url.rstrip("/"), config.tenants, config.org_id)
        if replacement_scope != self._token_scope:
            raise QueryFailure(
                QueryFailureKind.INVALID_REQUEST,
                backend=_BACKEND,
                query_id="<configuration>",
                message="Loki endpoint or tenant scope cannot change during credential refresh",
                retryable=False,
            )

        replacement_headers, replacement_auth, verify = _connection_options(config)
        replacement_owned = client is None
        replacement_client = client or httpx.Client(verify=verify)
        previous_client = self._client
        previous_owned = self._owns_client
        if previous_owned:
            try:
                previous_client.close()
            except Exception:
                if replacement_owned:
                    replacement_client.close()
                raise

        self._config = config
        self._headers = replacement_headers
        self._auth = replacement_auth
        self._client = replacement_client
        self._owns_client = replacement_owned

    def close(self) -> None:
        """Close the internally owned HTTP client."""
        if self._owns_client:
            self._client.close()


class AsyncLokiBackend:
    """Asynchronous Loki query backend that also reports its mutual-TLS certificate expiry."""

    name: BackendName = BackendName.LOKI
    dialect: QueryDialect = QueryDialect.LOGQL

    def __init__(self, config: LokiBackendConfig, *, client: httpx.AsyncClient | None = None) -> None:
        """Create an asynchronous driver from validated provider configuration."""
        self._config = config
        self._token_scope = token_scope(config.url.rstrip("/"), config.tenants, config.org_id)
        self._page_token_secret = token_bytes(32)
        self._headers, self._auth, verify = _connection_options(config)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(verify=verify)

    async def query_page(self, request: QueryRequest) -> QueryPage:
        """Execute one asynchronous integer-nanosecond Loki range query."""
        _validate_dialect(request)
        context = prepare_page(
            request, _BACKEND, self._token_scope, self._page_token_secret, max_fetch_limit=_MAX_FETCH_LIMIT
        )
        started = monotonic()
        try:
            response = await self._client.get(
                f"{self._config.url.rstrip('/')}/loki/api/v1/query_range",
                params=_params(request, context),
                headers=self._headers,
                auth=self._auth,
                timeout=request_timeout(request.timeout_seconds, self._config.connect_timeout_seconds),
            )
        except httpx.RequestError as error:
            raise translate_transport_error(error, backend=_BACKEND, query_id=request.query.query_id) from error
        duration = monotonic() - started
        raise_http_failure(response, backend=_BACKEND, query_id=request.query.query_id)
        records, provider_stats = _parse_loki_response(response, request)
        return _build_page(
            request=request,
            context=context,
            records=records,
            provider_stats=provider_stats,
            response_bytes=len(response.content),
            duration_seconds=duration,
        )

    async def close(self) -> None:
        """Close the internally owned asynchronous HTTP client."""
        if self._owns_client:
            await self._client.aclose()

    def days_until_cert_expires(self) -> int | None:
        """Days until the configured client certificate expires, or None when none is configured or readable."""
        return days_until_expiry(self._config.cert_file)

    def days_until_ca_cert_expires(self) -> int | None:
        """Days until the configured CA certificate expires, or None when none is configured or readable."""
        return days_until_expiry(self._config.ca_cert)
