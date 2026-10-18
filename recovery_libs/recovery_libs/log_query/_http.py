# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""HTTP and TLS helpers shared by the built-in log-query drivers."""

from __future__ import annotations

import ssl
from email.utils import parsedate_to_datetime
from time import time
from typing import Any

import httpx

from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind


def request_timeout(request_timeout_seconds: float, connect_timeout_seconds: float | None) -> httpx.Timeout:
    """Apply per-phase transport timeouts with an optional connect override."""
    return httpx.Timeout(
        request_timeout_seconds,
        connect=connect_timeout_seconds or request_timeout_seconds,
    )


def build_verify(
    *,
    verify_tls: bool,
    ca_cert: str | None,
    cert_file: str | None,
    key_file: str | None,
) -> ssl.SSLContext | bool:
    """Build HTTPX verification state, including optional mutual TLS."""
    if not verify_tls:
        return False
    if ca_cert is None and cert_file is None:
        return True
    context = ssl.create_default_context()
    if ca_cert is not None:
        # Custom roots are additive, matching the inherited Loki client's TLS
        # behavior instead of replacing system trust.
        context.load_verify_locations(cafile=ca_cert)
    if cert_file is not None and key_file is not None:
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    return context


def retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse either delta-seconds or an HTTP date from ``Retry-After``."""
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time())
        except (TypeError, ValueError, OverflowError):
            return None


def raise_http_failure(response: httpx.Response, *, backend: str, query_id: str) -> None:
    """Normalize an unsuccessful HTTP response without copying its body."""
    status = response.status_code
    if status == 200:
        return
    if status == 206:
        kind, retryable = QueryFailureKind.INCOMPLETE, True
    elif 200 <= status < 300:
        kind, retryable = QueryFailureKind.MALFORMED_RESPONSE, False
    elif status == 400:
        kind, retryable = QueryFailureKind.INVALID_QUERY, False
    elif status == 401:
        kind, retryable = QueryFailureKind.UNAUTHENTICATED, False
    elif status == 403:
        kind, retryable = QueryFailureKind.FORBIDDEN_OR_TENANT, False
    elif status == 429:
        kind, retryable = QueryFailureKind.RATE_LIMITED, True
    elif status in (408, 504):
        kind, retryable = QueryFailureKind.TIMEOUT, True
    elif status == 499:
        kind, retryable = QueryFailureKind.CANCELLED, False
    elif status >= 500:
        kind, retryable = QueryFailureKind.UNAVAILABLE, True
    else:
        kind, retryable = QueryFailureKind.INVALID_REQUEST, False
    raise QueryFailure(
        kind,
        backend=backend,
        query_id=query_id,
        message="backend returned an unsuccessful HTTP status",
        retryable=retryable,
        http_status=status,
        retry_after_seconds=retry_after_seconds(response) if status == 429 else None,
    )


def translate_transport_error(error: Exception, *, backend: str, query_id: str) -> QueryFailure:
    """Normalize HTTPX transport errors into the public failure taxonomy."""
    if isinstance(error, httpx.TimeoutException):
        return QueryFailure(
            QueryFailureKind.TIMEOUT,
            backend=backend,
            query_id=query_id,
            message="backend request timed out",
            retryable=True,
        )
    return QueryFailure(
        QueryFailureKind.UNAVAILABLE,
        backend=backend,
        query_id=query_id,
        message="backend request failed",
        retryable=True,
    )


def query_failure_from_http_error(error: httpx.HTTPError, *, backend: str, query_id: str) -> QueryFailure:
    """Normalize an HTTPX error raised by a client that does not use the drivers' request path."""
    if isinstance(error, httpx.HTTPStatusError):
        try:
            raise_http_failure(error.response, backend=backend, query_id=query_id)
        except QueryFailure as failure:
            return failure
    if isinstance(error.__cause__, ValueError):
        return QueryFailure(
            QueryFailureKind.MALFORMED_RESPONSE,
            backend=backend,
            query_id=query_id,
            message="response body is not valid JSON",
            retryable=True,
        )
    return translate_transport_error(error, backend=backend, query_id=query_id)


def scalar_to_string(value: Any) -> str:
    """Normalize a JSON scalar while preserving strings exactly."""
    if isinstance(value, str):
        return value
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError("log fields must be JSON scalar values")
