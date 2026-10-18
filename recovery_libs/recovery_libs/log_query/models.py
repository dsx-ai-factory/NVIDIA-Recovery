# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Backend-neutral log-query contracts."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class BackendName(StrEnum):
    """Log-query providers with built-in backends; members compare and format as their string value."""

    LOKI = "loki"
    VICTORIALOGS = "victorialogs"


class QueryDialect(StrEnum):
    """Native query languages the built-in backends accept."""

    LOGQL = "logql"
    LOGSQL = "logsql"


class QueryOrder(StrEnum):
    """Timestamp order of the records a page returns."""

    FORWARD = "forward"
    BACKWARD = "backward"


def _freeze(value: object) -> object:
    """Recursively freeze provider diagnostics without changing scalar values."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class NativeQuery:
    """A provider-native query selected for one stable logical query identifier.

    ``revision`` namespaces page and durable consumer cursors. When omitted, it
    is derived from the dialect and exact query text.

    Attributes:
        query_id: Stable logical query name used for metrics and cursor identity.
        dialect: Native language accepted by the selected provider.
        text: Exact provider-native query text.
        revision: Explicit or derived revision bound into continuation tokens.
    """

    query_id: str
    dialect: QueryDialect
    text: str
    revision: str | None = None

    def __post_init__(self) -> None:
        """Validate the query and derive its immutable revision when needed."""
        if not self.query_id.strip():
            raise ValueError("query_id must not be empty")
        try:
            object.__setattr__(self, "dialect", QueryDialect(self.dialect))
        except ValueError:
            raise ValueError(f"unsupported query dialect: {self.dialect}") from None
        if not self.text.strip():
            raise ValueError("query text must not be empty")
        if self.revision is None:
            payload = f"{self.dialect}\0{self.text}".encode()
            object.__setattr__(self, "revision", hashlib.sha256(payload).hexdigest())
        elif not self.revision.strip():
            raise ValueError("revision must not be empty")


@dataclass(frozen=True, slots=True)
class QueryRequest:
    """One page request over the half-open UTC nanosecond range ``[start, end)``.

    Attributes:
        query: Selected provider-native query.
        start_ns: Inclusive UTC epoch-nanosecond boundary.
        end_ns: Exclusive UTC epoch-nanosecond boundary.
        order: Required global timestamp direction.
        page_size: Maximum normalized occurrences returned in this page.
        page_token: Opaque continuation token returned for the same request.
        timeout_seconds: Remaining budget supplied to client I/O and provider
            query timeout controls. Synchronous transports may apply this per
            I/O phase, so it is not by itself a hard wall-clock cancellation.
    """

    query: NativeQuery
    start_ns: int
    end_ns: int
    order: QueryOrder = QueryOrder.FORWARD
    page_size: int = 5_000
    page_token: str | None = None
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        """Reject requests whose semantics cannot be represented by the drivers."""
        if isinstance(self.start_ns, bool) or not isinstance(self.start_ns, int):
            raise ValueError("start_ns must be an integer")  # noqa: TRY004
        if isinstance(self.end_ns, bool) or not isinstance(self.end_ns, int):
            raise ValueError("end_ns must be an integer")  # noqa: TRY004
        if self.start_ns < 0 or self.end_ns < 0:
            raise ValueError("timestamps must be non-negative")
        if self.start_ns >= self.end_ns:
            raise ValueError("query time range must satisfy start_ns < end_ns")
        try:
            object.__setattr__(self, "order", QueryOrder(self.order))
        except ValueError:
            raise ValueError(f"unsupported query order: {self.order}") from None
        if isinstance(self.page_size, bool) or not isinstance(self.page_size, int) or self.page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        if self.page_token is not None and not self.page_token:
            raise ValueError("page_token must be non-empty when provided")


@dataclass(frozen=True, slots=True)
class LogRecord:
    """One normalized log occurrence with its exact returned body and fields.

    Attributes:
        timestamp_ns: UTC epoch nanoseconds without floating-point conversion.
        body: Exact message body returned by the provider-native row query.
        fields: Immutable canonical and provider-visible string fields.
        source_id: Genuine provider event identity when one is available.
    """

    timestamp_ns: int
    body: str
    fields: Mapping[str, str] = field(default_factory=dict)
    source_id: str | None = None

    def __post_init__(self) -> None:
        """Make fields deeply immutable and validate the normalized wire types."""
        if isinstance(self.timestamp_ns, bool) or not isinstance(self.timestamp_ns, int):
            raise ValueError("timestamp_ns must be an integer")  # noqa: TRY004
        if self.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be non-negative")
        if not isinstance(self.body, str):
            raise ValueError("body must be a string")  # noqa: TRY004
        if self.source_id is not None and not isinstance(self.source_id, str):
            raise ValueError("source_id must be a string when provided")
        normalized_fields: dict[str, str] = {}
        for key, value in self.fields.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("record field keys and values must be strings")  # noqa: TRY004
            normalized_fields[key] = value
        object.__setattr__(self, "fields", MappingProxyType(normalized_fields))


@dataclass(frozen=True, slots=True)
class QueryStats:
    """Portable request measurements plus opaque provider diagnostics.

    Attributes:
        duration_seconds: Client-observed duration for this provider request.
        attempts: Transport attempts made by this driver call.
        response_bytes: Buffered provider response size.
        returned_records: Normalized occurrences exposed in this page.
        provider: Immutable diagnostics with provider-specific semantics.
    """

    duration_seconds: float
    attempts: int
    response_bytes: int
    returned_records: int
    provider: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate counters and freeze provider-specific diagnostic data."""
        if self.duration_seconds < 0:
            raise ValueError("duration_seconds must be non-negative")
        if self.attempts <= 0:
            raise ValueError("attempts must be positive")
        if self.response_bytes < 0 or self.returned_records < 0:
            raise ValueError("response_bytes and returned_records must be non-negative")
        object.__setattr__(self, "provider", _freeze(self.provider))


@dataclass(frozen=True, slots=True)
class QueryPage:
    """A bounded normalized page and its opaque continuation state.

    Attributes:
        records: Ordered normalized occurrences in this page.
        next_page_token: Request-bound opaque continuation state, if incomplete.
        complete: Whether clean provider exhaustion has been observed.
        stats: Portable and provider-specific measurements for this request.
    """

    records: tuple[LogRecord, ...]
    next_page_token: str | None
    complete: bool
    stats: QueryStats

    def __post_init__(self) -> None:
        """Ensure completion and continuation signals cannot contradict each other."""
        object.__setattr__(self, "records", tuple(self.records))
        if self.complete and self.next_page_token is not None:
            raise ValueError("a complete page cannot contain a continuation token")
        if not self.complete and not self.next_page_token:
            raise ValueError("an incomplete page must contain a continuation token")


@runtime_checkable
class SyncLogQueryBackend(Protocol):
    """Synchronous backend contract used by request-oriented consumers."""

    name: BackendName
    """Provider this backend queries, used in metric labels and failures."""
    dialect: QueryDialect
    """Native query language that ``query_page`` accepts."""

    def query_page(self, request: QueryRequest) -> QueryPage:
        """Return one normalized page or raise ``QueryFailure``."""
        ...

    def close(self) -> None:
        """Release backend connection resources."""
        ...


@runtime_checkable
class AsyncLogQueryBackend(Protocol):
    """Asynchronous backend contract used by polling consumers."""

    name: BackendName
    """Provider this backend queries, used in metric labels and failures."""
    dialect: QueryDialect
    """Native query language that ``query_page`` accepts."""

    async def query_page(self, request: QueryRequest) -> QueryPage:
        """Return one normalized page or raise ``QueryFailure``."""
        ...

    async def close(self) -> None:
        """Release backend connection resources."""
        ...


@runtime_checkable
class CertificateExpiryReporter(Protocol):
    """Optional backend capability: report how long its configured TLS certificates remain valid."""

    def days_until_cert_expires(self) -> int | None:
        """Days until the client certificate expires, negative once expired; None when none is configured."""
        ...

    def days_until_ca_cert_expires(self) -> int | None:
        """Days until the CA certificate expires, negative once expired; None when none is configured."""
        ...
