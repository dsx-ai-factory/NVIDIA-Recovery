# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Typed failures shared by log-query backends."""

from __future__ import annotations

from enum import StrEnum


class QueryFailureKind(StrEnum):
    """Stable failure categories on which consumers may base retry policy."""

    INVALID_REQUEST = "invalid_request"
    INVALID_QUERY = "invalid_query"
    UNAUTHENTICATED = "unauthenticated"
    FORBIDDEN_OR_TENANT = "forbidden_or_tenant"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    MALFORMED_RESPONSE = "malformed_response"
    INCOMPLETE = "incomplete"
    UNSUPPORTED_SEMANTICS = "unsupported_semantics"
    CANCELLED = "cancelled"


class QueryFailure(Exception):
    """Backend-neutral query failure with explicit retry semantics.

    Attributes:
        kind: Stable backend-neutral failure category.
        backend: Configured backend name.
        query_id: Stable logical query identifier.
        retryable: Whether a bounded caller retry may succeed unchanged.
        http_status: Provider or proxy HTTP status when available.
        retry_after_seconds: Provider-requested retry delay when available.
    """

    def __init__(
        self,
        kind: QueryFailureKind,
        *,
        backend: str,
        query_id: str,
        message: str,
        retryable: bool,
        http_status: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        """Initialize a sanitized query failure."""
        super().__init__(message)
        self.kind = kind
        self.backend = backend
        self.query_id = query_id
        self.retryable = retryable
        self.http_status = http_status
        self.retry_after_seconds = retry_after_seconds

    def __str__(self) -> str:
        """Return a compact description that does not expose query or credentials."""
        status = f", status={self.http_status}" if self.http_status is not None else ""
        return f"{self.backend} query {self.query_id!r} failed ({self.kind}{status}): {super().__str__()}"
