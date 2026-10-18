# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Retry transient HTTP failures around another transport."""

from __future__ import annotations

import itertools
import random
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import backoff
import httpx

RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})
RETRYABLE_METHODS = frozenset({"GET", "HEAD"})


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to try a request and how long to wait between tries.

    The defaults suit CI, where a service can briefly refuse connections or
    return 503 while it restarts. Status 500 and other client or server errors
    are never retried, because they report real defects. A `Retry-After` header
    sets the wait; otherwise waits grow exponentially from `base_delay` with full
    jitter unless `jitter` is false. Every wait is capped at `max_delay`.
    """

    attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 8.0
    jitter: bool = True


class _TransientStatus(Exception):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(response.status_code)
        self.response = response
        self.retry_after = _retry_after(response)


def _retry_after(response: httpx.Response) -> float | None:
    try:
        return max(0.0, float(response.headers["retry-after"]))
    except (KeyError, ValueError):
        return None


def _waits(*, policy: RetryPolicy) -> Generator[float, Any, None]:
    failure = yield  # type: ignore[misc]
    for retry in itertools.count():
        hint = failure.retry_after if isinstance(failure, _TransientStatus) else None
        if hint is not None:
            wait = hint
        else:
            limit = policy.base_delay * 2**retry
            wait = random.uniform(0, limit) if policy.jitter else limit
        failure = yield min(policy.max_delay, wait)


class RetryingTransport(httpx.BaseTransport):
    """Send requests through `inner`, retrying idempotent ones after transient failures."""

    def __init__(self, inner: httpx.BaseTransport, policy: RetryPolicy = RetryPolicy()) -> None:
        self._inner = inner
        self._send_with_retries = backoff.on_exception(
            _waits,
            (httpx.TransportError, _TransientStatus),
            max_tries=policy.attempts,
            jitter=None,
            logger=None,
            policy=policy,
        )(self._send)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.method not in RETRYABLE_METHODS or request.headers.get("content-length", "0") != "0":
            return self._inner.handle_request(request)
        try:
            return self._send_with_retries(request)
        except _TransientStatus as exhausted:
            return exhausted.response

    def _send(self, request: httpx.Request) -> httpx.Response:
        response = self._inner.handle_request(request)
        if response.status_code in RETRYABLE_STATUS_CODES:
            response.read()
            raise _TransientStatus(response)
        return response

    def close(self) -> None:
        self._inner.close()
