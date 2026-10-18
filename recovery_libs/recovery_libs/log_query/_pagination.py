# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Opaque request-bound pagination for timestamp-ordered provider APIs."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from time import time

from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind
from recovery_libs.log_query.models import LogRecord, QueryOrder, QueryRequest

_TOKEN_VERSION = 1
_TOKEN_TTL_SECONDS = 15 * 60


def _now() -> float:
    """Return wall-clock seconds; isolated for deterministic expiry tests."""
    return time()


@dataclass(frozen=True, slots=True)
class PageContext:
    """Effective provider range and overlap state for one page request."""

    start_ns: int
    end_ns: int
    fetch_limit: int
    boundary_ns: int | None
    emitted_at_boundary: Counter[str]
    scope: str
    secret: bytes = field(repr=False)


def token_scope(*parts: object) -> str:
    """Derive a non-secret binding for endpoint and tenant identity."""
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _record_fingerprint(record: LogRecord) -> str:
    payload = [record.timestamp_ns, record.body, sorted(record.fields.items()), record.source_id]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _token_failure(backend: str, query_id: str, message: str) -> QueryFailure:
    return QueryFailure(
        QueryFailureKind.INVALID_REQUEST,
        backend=backend,
        query_id=query_id,
        message=message,
        retryable=False,
    )


def _decode_base64(value: str) -> bytes:
    encoded = value.encode("ascii")
    return base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)


def _decode_token(request: QueryRequest, backend: str, scope: str, secret: bytes) -> tuple[int, Counter[str]]:
    try:
        raw = request.page_token or ""
        parts = raw.split(".")
        if len(parts) != 2:
            raise ValueError("token must contain payload and signature")
        payload_part, signature_part = parts
        supplied_signature = _decode_base64(signature_part)
        expected_signature = hmac.new(secret, payload_part.encode("ascii"), hashlib.sha256).digest()
        if not hmac.compare_digest(supplied_signature, expected_signature):
            raise ValueError("token signature does not match")
        decoded = _decode_base64(payload_part)
        state = json.loads(decoded)
        if not isinstance(state, dict):
            raise ValueError("token state is not an object")  # noqa: TRY004
        expected = {
            "v": _TOKEN_VERSION,
            "backend": backend,
            "query_id": request.query.query_id,
            "revision": request.query.revision,
            "start_ns": request.start_ns,
            "end_ns": request.end_ns,
            "order": request.order,
            "scope": scope,
        }
        if any(state.get(key) != value for key, value in expected.items()):
            raise ValueError("token does not match request")
        issued_at = state.get("issued_at")
        expires_at = state.get("expires_at")
        if (
            isinstance(issued_at, bool)
            or not isinstance(issued_at, int)
            or isinstance(expires_at, bool)
            or not isinstance(expires_at, int)
            or expires_at != issued_at + _TOKEN_TTL_SECONDS
            or issued_at > _now()
        ):
            raise ValueError("token lifetime is invalid")
        if _now() >= expires_at:
            raise _token_failure(backend, request.query.query_id, "page token has expired")
        boundary_ns = state["boundary_ns"]
        emitted = state["emitted_at_boundary"]
        if isinstance(boundary_ns, bool) or not isinstance(boundary_ns, int):
            raise ValueError("invalid token boundary")  # noqa: TRY004
        if not isinstance(emitted, dict) or not emitted:
            raise ValueError("invalid token overlap state")
        counts: Counter[str] = Counter()
        for fingerprint, count in emitted.items():
            if not isinstance(fingerprint, str) or isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ValueError("invalid token overlap count")
            counts[fingerprint] = count
        return boundary_ns, counts
    except (
        ValueError,
        TypeError,
        KeyError,
        UnicodeDecodeError,
        UnicodeEncodeError,
        binascii.Error,
        json.JSONDecodeError,
    ) as error:
        raise _token_failure(backend, request.query.query_id, "invalid or mismatched page token") from error


def prepare_page(
    request: QueryRequest, backend: str, scope: str, secret: bytes, *, max_fetch_limit: int | None = None
) -> PageContext:
    """Resolve a token with overlap, bounded by the provider's request limit.

    A capped continuation can return fewer fresh records than ``page_size``.
    Exhaustion still depends on the actual fetch limit, and saturated overlap
    must fail rather than skipping an unproven timestamp boundary.
    """
    fetch_limit = min(request.page_size, max_fetch_limit) if max_fetch_limit is not None else request.page_size
    if request.page_token is None:
        return PageContext(request.start_ns, request.end_ns, fetch_limit, None, Counter(), scope, secret)
    boundary_ns, emitted = _decode_token(request, backend, scope, secret)
    if not request.start_ns <= boundary_ns < request.end_ns:
        raise _token_failure(backend, request.query.query_id, "page token boundary is outside the request range")
    if request.order is QueryOrder.FORWARD:
        start_ns, end_ns = boundary_ns, request.end_ns
    else:
        start_ns, end_ns = request.start_ns, min(request.end_ns, boundary_ns + 1)
    fetch_limit = request.page_size + sum(emitted.values())
    if max_fetch_limit is not None:
        fetch_limit = min(fetch_limit, max_fetch_limit)
    return PageContext(start_ns, end_ns, fetch_limit, boundary_ns, emitted, scope, secret)


def _encode_token(
    request: QueryRequest,
    backend: str,
    scope: str,
    secret: bytes,
    boundary_ns: int,
    emitted_at_boundary: Counter[str],
) -> str:
    issued_at = int(_now())
    state = {
        "v": _TOKEN_VERSION,
        "backend": backend,
        "query_id": request.query.query_id,
        "revision": request.query.revision,
        "start_ns": request.start_ns,
        "end_ns": request.end_ns,
        "order": request.order,
        "scope": scope,
        "issued_at": issued_at,
        "expires_at": issued_at + _TOKEN_TTL_SECONDS,
        "boundary_ns": boundary_ns,
        "emitted_at_boundary": dict(sorted(emitted_at_boundary.items())),
    }
    encoded = json.dumps(state, separators=(",", ":"), sort_keys=True).encode()
    payload_part = base64.urlsafe_b64encode(encoded).rstrip(b"=").decode()
    signature = hmac.new(secret, payload_part.encode("ascii"), hashlib.sha256).digest()
    signature_part = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    return f"{payload_part}.{signature_part}"


def finish_page(
    *,
    request: QueryRequest,
    backend: str,
    context: PageContext,
    records: Iterable[LogRecord],
    response_record_count: int,
) -> tuple[tuple[LogRecord, ...], str | None, bool]:
    """Remove only overlap introduced by this adapter and produce continuation state."""
    ordered = sorted(records, key=lambda record: record.timestamp_ns, reverse=request.order is QueryOrder.BACKWARD)
    remaining_overlap = context.emitted_at_boundary.copy()
    fresh: list[LogRecord] = []
    fresh_at_old_boundary = False
    for record in ordered:
        if context.boundary_ns is not None and record.timestamp_ns == context.boundary_ns:
            fingerprint = _record_fingerprint(record)
            if remaining_overlap[fingerprint] > 0:
                remaining_overlap[fingerprint] -= 1
                continue
            fresh_at_old_boundary = True
        fresh.append(record)

    provider_exhausted = response_record_count < context.fetch_limit
    if context.boundary_ns is not None and any(remaining_overlap.values()):
        raise QueryFailure(
            QueryFailureKind.INCOMPLETE,
            backend=backend,
            query_id=request.query.query_id,
            message="provider omitted records from the pagination overlap",
            retryable=True,
        )
    # A full response that changes the equal-timestamp cohort cannot prove
    # which occurrences were omitted because neither provider has a stable
    # event cursor or tie breaker.
    if not provider_exhausted and context.boundary_ns is not None and fresh_at_old_boundary:
        raise QueryFailure(
            QueryFailureKind.INCOMPLETE,
            backend=backend,
            query_id=request.query.query_id,
            message="cannot prove complete pagination across an equal-timestamp boundary",
            retryable=True,
        )

    page_records = tuple(fresh[: request.page_size])
    complete = provider_exhausted and len(fresh) <= request.page_size
    if complete:
        return page_records, None, True
    if not page_records:
        raise QueryFailure(
            QueryFailureKind.INCOMPLETE,
            backend=backend,
            query_id=request.query.query_id,
            message="pagination did not make progress",
            retryable=True,
        )

    boundary_ns = page_records[-1].timestamp_ns
    emitted = Counter(_record_fingerprint(record) for record in page_records if record.timestamp_ns == boundary_ns)
    if context.boundary_ns == boundary_ns:
        emitted.update(context.emitted_at_boundary)
    return page_records, _encode_token(request, backend, context.scope, context.secret, boundary_ns, emitted), False
