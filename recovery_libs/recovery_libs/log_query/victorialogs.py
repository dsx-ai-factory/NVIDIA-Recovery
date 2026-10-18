# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""VictoriaLogs implementation of the neutral log-query contract."""

from __future__ import annotations

import json
import re
from calendar import timegm
from collections.abc import Mapping
from datetime import UTC, datetime
from secrets import token_bytes
from time import monotonic
from typing import Any

import httpx

from recovery_libs.log_query._http import (
    build_verify,
    raise_http_failure,
    request_timeout,
    scalar_to_string,
    translate_transport_error,
)
from recovery_libs.log_query._pagination import PageContext, finish_page, prepare_page, token_scope
from recovery_libs.log_query.config import VictoriaLogsBackendConfig
from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind
from recovery_libs.log_query.models import (
    BackendName,
    LogRecord,
    QueryDialect,
    QueryOrder,
    QueryPage,
    QueryRequest,
    QueryStats,
)

_BACKEND = BackendName.VICTORIALOGS
_LABEL_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]*")


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


def _incomplete_stream(query_id: str, cause: httpx.RemoteProtocolError) -> QueryFailure:
    """Normalize a disconnected JSONL body without exposing partial records."""
    failure = QueryFailure(
        QueryFailureKind.INCOMPLETE,
        backend=_BACKEND,
        query_id=query_id,
        message="backend disconnected before the JSONL response completed",
        retryable=True,
    )
    failure.__cause__ = cause
    return failure


def _connection_options(
    config: VictoriaLogsBackendConfig,
) -> tuple[dict[str, str], httpx.Auth | tuple[str, str] | None, Any]:
    headers = {"Accept": "application/stream+json"}
    auth: httpx.Auth | tuple[str, str] | None = None
    if config.username is not None and config.password is not None:
        auth = (config.username, config.password)
    elif config.bearer_token:
        headers["Authorization"] = f"Bearer {config.bearer_token}"
    if config.account_id is not None and config.project_id is not None:
        headers["AccountID"] = str(config.account_id)
        headers["ProjectID"] = str(config.project_id)
    verify = build_verify(
        verify_tls=config.verify_tls,
        ca_cert=config.ca_cert,
        cert_file=config.cert_file,
        key_file=config.key_file,
    )
    return headers, auth, verify


def _rfc3339nano(timestamp_ns: int) -> str:
    seconds, nanoseconds = divmod(timestamp_ns, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{prefix}.{nanoseconds:09d}Z"


def _decode_stream_labelset(value: object) -> dict[str, str]:
    if value is None or value == "" or value == "{}":
        return {}
    if not isinstance(value, str):
        raise TypeError("_stream must be a string")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, Mapping):
        parsed_labels = {str(key): scalar_to_string(item) for key, item in parsed.items()}
        return parsed_labels

    if not value.startswith("{") or not value.endswith("}"):
        raise ValueError("_stream is not a label set")
    content = value[1:-1]
    position = 0
    labels: dict[str, str] = {}
    while position < len(content):
        while position < len(content) and content[position].isspace():
            position += 1
        match = _LABEL_NAME.match(content, position)
        if match is None:
            raise ValueError("_stream contains an invalid label name")
        name = match.group(0)
        position = match.end()
        while position < len(content) and content[position].isspace():
            position += 1
        if position >= len(content) or content[position] != "=":
            raise ValueError("_stream label is missing equals")
        position += 1
        while position < len(content) and content[position].isspace():
            position += 1
        decoder = json.JSONDecoder()
        label_value, consumed = decoder.raw_decode(content[position:])
        if not isinstance(label_value, str):
            raise ValueError("_stream label value must be a string")  # noqa: TRY004
        labels[name] = label_value
        position += consumed
        while position < len(content) and content[position].isspace():
            position += 1
        if position == len(content):
            break
        if content[position] != ",":
            raise ValueError("_stream labels must be comma separated")
        position += 1
    return labels


def _parse_timestamp(value: object) -> int:
    if not isinstance(value, str):
        raise TypeError("_time must be a string")
    if value.isdecimal():
        return int(value)
    if not value.endswith("Z"):
        raise ValueError("_time must be UTC RFC3339")
    date_part, separator, fraction = value[:-1].partition(".")
    if separator and (not fraction.isdecimal() or len(fraction) > 9):
        raise ValueError("_time has invalid fractional precision")
    parsed = datetime.strptime(f"{date_part}Z", "%Y-%m-%dT%H:%M:%SZ")
    seconds = timegm(parsed.timetuple())
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def _parse_victorialogs_response(response: httpx.Response, request: QueryRequest) -> list[LogRecord]:
    records: list[LogRecord] = []
    try:
        for line in response.text.splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            if not isinstance(item, Mapping):
                raise TypeError("JSONL record is not an object")
            timestamp_ns = _parse_timestamp(item.get("_time"))
            body = item.get("_msg")
            if not isinstance(body, str):
                raise TypeError("_msg must be a string")
            fields = _decode_stream_labelset(item.get("_stream"))
            for key, value in item.items():
                if key not in ("_time", "_msg", "_stream"):
                    fields[str(key)] = scalar_to_string(value)
            records.append(LogRecord(timestamp_ns=timestamp_ns, body=body, fields=fields))
        return records
    except (json.JSONDecodeError, TypeError, ValueError, OverflowError) as error:
        raise _malformed(request.query.query_id, "response is not valid VictoriaLogs JSONL", error)


def _validate_dialect(request: QueryRequest) -> None:
    if request.query.dialect != QueryDialect.LOGSQL:
        raise QueryFailure(
            QueryFailureKind.UNSUPPORTED_SEMANTICS,
            backend=_BACKEND,
            query_id=request.query.query_id,
            message="VictoriaLogs requires a LogsQL query variant",
            retryable=False,
        )


def _form_data(request: QueryRequest, context: PageContext) -> dict[str, str]:
    descending = " desc" if request.order is QueryOrder.BACKWARD else ""
    ordered_query = f"{request.query.text} | sort by (_time){descending} limit {context.fetch_limit}"
    return {
        "query": ordered_query,
        "start": _rfc3339nano(context.start_ns),
        "end": _rfc3339nano(context.end_ns),
        "timeout": f"{request.timeout_seconds:g}s",
        "allow_partial_response": "false",
    }


def _provider_stats(response: httpx.Response) -> dict[str, object]:
    return {
        key: response.headers[key]
        for key in ("VL-Request-Duration-Seconds", "AccountID", "ProjectID")
        if key in response.headers
    }


def _validate_tenant_response(
    response: httpx.Response,
    config: VictoriaLogsBackendConfig,
    query_id: str,
) -> None:
    """Require VictoriaLogs to echo the configured tenant without mismatch."""
    if config.account_id is None or config.project_id is None:
        return
    expected_account = str(config.account_id)
    expected_project = str(config.project_id)
    if response.headers.get("AccountID") != expected_account or response.headers.get("ProjectID") != expected_project:
        raise QueryFailure(
            QueryFailureKind.FORBIDDEN_OR_TENANT,
            backend=_BACKEND,
            query_id=query_id,
            message="backend did not confirm the configured tenant",
            retryable=False,
            http_status=response.status_code,
        )


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


class VictoriaLogsBackend:
    """Synchronous VictoriaLogs query backend."""

    name: BackendName = BackendName.VICTORIALOGS
    dialect: QueryDialect = QueryDialect.LOGSQL

    def __init__(self, config: VictoriaLogsBackendConfig, *, client: httpx.Client | None = None) -> None:
        """Create a driver from validated provider configuration."""
        self._config = config
        self._token_scope = token_scope(config.url.rstrip("/"), config.account_id, config.project_id)
        self._page_token_secret = token_bytes(32)
        self._headers, self._auth, verify = _connection_options(config)
        self._owns_client = client is None
        self._client = client or httpx.Client(verify=verify)

    def query_page(self, request: QueryRequest) -> QueryPage:
        """Execute one buffered, ordered LogsQL query page."""
        _validate_dialect(request)
        context = prepare_page(request, _BACKEND, self._token_scope, self._page_token_secret)
        started = monotonic()
        try:
            response = self._client.request(
                "POST",
                f"{self._config.url.rstrip('/')}/select/logsql/query",
                data=_form_data(request, context),
                headers=self._headers,
                auth=self._auth,
                timeout=request_timeout(request.timeout_seconds, self._config.connect_timeout_seconds),
            )
        except httpx.RemoteProtocolError as error:
            raise _incomplete_stream(request.query.query_id, error)
        except httpx.RequestError as error:
            raise translate_transport_error(error, backend=_BACKEND, query_id=request.query.query_id) from error
        duration = monotonic() - started
        raise_http_failure(response, backend=_BACKEND, query_id=request.query.query_id)
        _validate_tenant_response(response, self._config, request.query.query_id)
        records = _parse_victorialogs_response(response, request)
        return _build_page(
            request=request,
            context=context,
            records=records,
            provider_stats=_provider_stats(response),
            response_bytes=len(response.content),
            duration_seconds=duration,
        )

    def close(self) -> None:
        """Close the internally owned HTTP client."""
        if self._owns_client:
            self._client.close()


class AsyncVictoriaLogsBackend:
    """Asynchronous VictoriaLogs query backend."""

    name: BackendName = BackendName.VICTORIALOGS
    dialect: QueryDialect = QueryDialect.LOGSQL

    def __init__(self, config: VictoriaLogsBackendConfig, *, client: httpx.AsyncClient | None = None) -> None:
        """Create an asynchronous driver from validated provider configuration."""
        self._config = config
        self._token_scope = token_scope(config.url.rstrip("/"), config.account_id, config.project_id)
        self._page_token_secret = token_bytes(32)
        self._headers, self._auth, verify = _connection_options(config)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(verify=verify)

    async def query_page(self, request: QueryRequest) -> QueryPage:
        """Execute one asynchronous buffered, ordered LogsQL query page."""
        _validate_dialect(request)
        context = prepare_page(request, _BACKEND, self._token_scope, self._page_token_secret)
        started = monotonic()
        try:
            response = await self._client.request(
                "POST",
                f"{self._config.url.rstrip('/')}/select/logsql/query",
                data=_form_data(request, context),
                headers=self._headers,
                auth=self._auth,
                timeout=request_timeout(request.timeout_seconds, self._config.connect_timeout_seconds),
            )
        except httpx.RemoteProtocolError as error:
            raise _incomplete_stream(request.query.query_id, error)
        except httpx.RequestError as error:
            raise translate_transport_error(error, backend=_BACKEND, query_id=request.query.query_id) from error
        duration = monotonic() - started
        raise_http_failure(response, backend=_BACKEND, query_id=request.query.query_id)
        _validate_tenant_response(response, self._config, request.query.query_id)
        records = _parse_victorialogs_response(response, request)
        return _build_page(
            request=request,
            context=context,
            records=records,
            provider_stats=_provider_stats(response),
            response_bytes=len(response.content),
            duration_seconds=duration,
        )

    async def close(self) -> None:
        """Close the internally owned asynchronous HTTP client."""
        if self._owns_client:
            await self._client.aclose()
