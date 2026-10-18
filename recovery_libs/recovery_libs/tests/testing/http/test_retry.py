# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable

import httpx
import pytest
from recovery_libs.testing.http.retry import RetryingTransport, RetryPolicy

POLICY = RetryPolicy(attempts=4, base_delay=0.5, max_delay=8.0, jitter=False)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []
    monkeypatch.setattr("time.sleep", recorded.append)
    return recorded


def _client(
    outcomes: Iterable[int | type[Exception]], policy: RetryPolicy = POLICY
) -> tuple[httpx.Client, list[httpx.Request]]:
    pending = iter(outcomes)
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        outcome = next(pending)
        if isinstance(outcome, type):
            raise outcome("transient", request=request)
        return httpx.Response(outcome, headers={"retry-after": "3"} if outcome == 429 else {})

    return httpx.Client(
        base_url="http://service.test", transport=RetryingTransport(httpx.MockTransport(respond), policy)
    ), requests


def test_transient_statuses_are_retried_with_exponential_backoff(sleeps: list[float]) -> None:
    http, requests = _client([503, 502, 504, 200])

    assert http.get("/advice").status_code == 200
    assert len(requests) == 4
    assert sleeps == [0.5, 1.0, 2.0]


def test_connection_errors_are_retried(sleeps: list[float]) -> None:
    http, requests = _client([httpx.ConnectError, httpx.ReadTimeout, 200])

    assert http.get("/advice").status_code == 200
    assert len(requests) == 3


def test_retry_after_header_sets_the_wait_without_jitter(sleeps: list[float]) -> None:
    http, _ = _client([429, 200], RetryPolicy(attempts=4, base_delay=0.5, max_delay=8.0))

    http.get("/advice")

    assert sleeps == [3.0]


def test_retry_after_is_capped_at_the_maximum_delay(sleeps: list[float]) -> None:
    http, _ = _client([429, 200], RetryPolicy(attempts=4, base_delay=0.5, max_delay=2.0))

    http.get("/advice")

    assert sleeps == [2.0]


@pytest.mark.parametrize("status_code", [500, 404, 400])
def test_defect_statuses_are_returned_without_retrying(sleeps: list[float], status_code: int) -> None:
    http, requests = _client([status_code])

    assert http.get("/advice").status_code == status_code
    assert len(requests) == 1
    assert sleeps == []


def test_last_transient_response_is_returned_after_the_final_attempt(sleeps: list[float]) -> None:
    http, requests = _client([503] * POLICY.attempts)

    assert http.get("/advice").status_code == 503
    assert len(requests) == POLICY.attempts


def test_last_connection_error_is_raised_after_the_final_attempt(sleeps: list[float]) -> None:
    http, requests = _client([httpx.ConnectError] * POLICY.attempts)

    with pytest.raises(httpx.ConnectError):
        http.get("/advice")
    assert len(requests) == POLICY.attempts


def test_requests_with_a_body_are_not_retried(sleeps: list[float]) -> None:
    http, requests = _client([503])

    assert http.post("/advice", json={"state": "Restarted"}).status_code == 503
    assert len(requests) == 1


def test_waits_never_exceed_the_maximum_delay(sleeps: list[float]) -> None:
    http, _ = _client([503] * 7 + [200], RetryPolicy(attempts=8, base_delay=0.5, max_delay=8.0, jitter=False))

    http.get("/advice")

    assert sleeps == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_jittered_waits_stay_within_the_backoff_bound(sleeps: list[float]) -> None:
    http, _ = _client([503, 503, 503, 200], RetryPolicy(attempts=4, base_delay=0.5, max_delay=8.0))

    http.get("/advice")

    assert all(0 <= wait <= bound for wait, bound in zip(sleeps, [0.5, 1.0, 2.0], strict=True))
