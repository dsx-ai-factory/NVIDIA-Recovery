# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from urllib.parse import parse_qs

import httpx
import pytest
from recovery_libs.log_query import (
    AsyncVictoriaLogsBackend,
    NativeQuery,
    QueryFailure,
    QueryFailureKind,
    QueryRequest,
    VictoriaLogsBackend,
    VictoriaLogsBackendConfig,
)


class _BrokenSyncStream(httpx.SyncByteStream):
    def __init__(self, request: httpx.Request) -> None:
        self.request = request

    def __iter__(self) -> Iterator[bytes]:
        yield b'{"_time":"2023-11-14T22:13:20Z","_msg":"partial","_stream":"{}"}\n'
        raise httpx.RemoteProtocolError("peer closed before complete body", request=self.request)


class _BrokenAsyncStream(httpx.AsyncByteStream):
    def __init__(self, request: httpx.Request) -> None:
        self.request = request

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"_time":"2023-11-14T22:13:20Z","_msg":"partial","_stream":"{}"}\n'
        raise httpx.RemoteProtocolError("peer closed before complete body", request=self.request)


def _request(
    *,
    order: str = "forward",
    page_size: int = 10,
    page_token: str | None = None,
) -> QueryRequest:
    return QueryRequest(
        NativeQuery("system", "logsql", '_stream:{component="journal"}'),
        1_700_000_000_123_456_789,
        1_700_000_001_000_000_001,
        order=order,  # type: ignore[arg-type]
        page_size=page_size,
        page_token=page_token,
        timeout_seconds=4.25,
    )


def _jsonl(values: list[tuple[int, str]]) -> str:
    return "\n".join(
        json.dumps({"_time": str(timestamp_ns), "_msg": body, "_stream": "{}"}) for timestamp_ns, body in values
    )


@pytest.mark.parametrize(
    ("timestamp", "expected_ns"),
    [
        ("2023-11-14T22:13:21Z", 1_700_000_001_000_000_000),
        ("2023-11-14T22:13:20.5Z", 1_700_000_000_500_000_000),
        ("2023-11-14T22:13:20.123456789Z", 1_700_000_000_123_456_789),
    ],
)
@pytest.mark.asyncio
async def test_victorialogs_rfc3339_timestamp_precision(timestamp: str, expected_ns: int) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text=json.dumps({"_time": timestamp, "_msg": "body"}) + "\n")
    )
    sync_backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"), client=httpx.Client(transport=transport)
    )
    async_backend = AsyncVictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"), client=httpx.AsyncClient(transport=transport)
    )
    for page in (sync_backend.query_page(_request()), await async_backend.query_page(_request())):
        assert page.complete
        assert [(record.timestamp_ns, record.body) for record in page.records] == [(expected_ns, "body")]


def test_victorialogs_posts_ordered_query_and_normalizes_jsonl() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = '{"_time":"2023-11-14T22:13:20.123456791Z","_msg":" second ","_stream":"{cluster=\\"c1\\",node_name=\\"n1\\"}","component":"journal"}\n{"_time":"2023-11-14T22:13:20.123456790Z","_msg":"first\\nline","_stream":"{cluster=\\"c1\\",node_name=\\"n1\\"}","count":2}'
        return httpx.Response(
            200,
            text=body,
            headers={"VL-Request-Duration-Seconds": "0.01", "AccountID": "12", "ProjectID": "3"},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig(
            "https://logs.example/",
            account_id=12,
            project_id=3,
            require_tenant=True,
            bearer_token="proxy-token",
            connect_timeout_seconds=1.5,
        ),
        client=client,
    )
    page = backend.query_page(_request())

    assert [record.timestamp_ns for record in page.records] == [1_700_000_000_123_456_790, 1_700_000_000_123_456_791]
    assert page.records[0].body == "first\nline"
    assert page.records[0].fields == {"cluster": "c1", "node_name": "n1", "count": "2"}
    assert page.records[1].fields["component"] == "journal"
    assert page.stats.provider == {
        "VL-Request-Duration-Seconds": "0.01",
        "AccountID": "12",
        "ProjectID": "3",
    }

    request = seen[0]
    form = parse_qs(request.content.decode())
    assert request.method == "POST"
    assert request.url.path == "/select/logsql/query"
    assert form["start"] == ["2023-11-14T22:13:20.123456789Z"]
    assert form["end"] == ["2023-11-14T22:13:21.000000001Z"]
    assert form["allow_partial_response"] == ["false"]
    assert form["timeout"] == ["4.25s"]
    assert form["query"][0].endswith("| sort by (_time) limit 10")
    assert request.headers["AccountID"] == "12"
    assert request.headers["ProjectID"] == "3"
    assert request.headers["Authorization"] == "Bearer proxy-token"
    assert request.extensions["timeout"] == {
        "connect": 1.5,
        "read": 4.25,
        "write": 4.25,
        "pool": 4.25,
    }


def test_victorialogs_backward_order_is_explicit_and_normalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        assert form["query"][0].endswith("| sort by (_time) desc limit 10")
        return httpx.Response(
            200,
            text='{"_time":"1700000000123456790","_msg":"older","_stream":"{}"}\n{"_time":"1700000000123456791","_msg":"newer","_stream":"{}"}',
        )

    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    page = backend.query_page(_request(order="backward"))
    assert [record.body for record in page.records] == ["newer", "older"]


def test_victorialogs_empty_result_is_success() -> None:
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=""))),
    )
    page = backend.query_page(_request())
    assert page.records == ()
    assert page.complete is True


def test_victorialogs_pagination_covers_short_exact_and_multi_page_results() -> None:
    values = [
        (1_700_000_000_123_456_789, "one"),
        (1_700_000_000_123_456_790, "two"),
        (1_700_000_000_123_456_791, "three"),
    ]
    calls: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        calls.append(form)
        response_values = values[:2] if len(calls) == 1 else values[1:]
        return httpx.Response(200, text=_jsonl(response_values))

    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=2))
    assert [record.body for record in first.records] == ["one", "two"]
    assert first.complete is False
    assert calls[0]["start"] == ["2023-11-14T22:13:20.123456789Z"]
    assert calls[0]["end"] == ["2023-11-14T22:13:21.000000001Z"]
    assert calls[0]["query"][0].endswith("| sort by (_time) limit 2")

    second = backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert [record.body for record in second.records] == ["three"]
    assert second.complete is True
    assert calls[1]["start"] == ["2023-11-14T22:13:20.123456790Z"]
    assert calls[1]["end"] == calls[0]["end"]
    assert calls[1]["query"][0].endswith("| sort by (_time) limit 3")

    short_backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, text=_jsonl(values[:1])))
        ),
    )
    short = short_backend.query_page(_request(page_size=2))
    assert [record.body for record in short.records] == ["one"]
    assert short.complete is True


def test_victorialogs_continuation_allows_smaller_page_without_skips_or_duplicates() -> None:
    values = [
        (1_700_000_000_123_456_789 + offset, body)
        for offset, body in enumerate(("one", "two", "three", "four", "five", "six"))
    ]
    calls: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        calls.append(form)
        if len(calls) == 1:
            selected = values[:3]
        elif len(calls) == 2:
            selected = values[2:5]
        else:
            selected = values[4:]
        return httpx.Response(200, text=_jsonl(selected))

    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=3))
    second = backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    third = backend.query_page(_request(page_size=2, page_token=second.next_page_token))

    assert [record.body for page in (first, second, third) for record in page.records] == [
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
    ]
    assert first.complete is False
    assert second.complete is False
    assert third.complete is True
    assert [form["start"][0] for form in calls] == [
        "2023-11-14T22:13:20.123456789Z",
        "2023-11-14T22:13:20.123456791Z",
        "2023-11-14T22:13:20.123456793Z",
    ]
    assert [form["end"][0] for form in calls] == ["2023-11-14T22:13:21.000000001Z"] * 3
    assert [form["query"][0].rsplit(" limit ", 1)[1] for form in calls] == ["3", "3", "3"]


def test_victorialogs_ambiguous_equal_timestamp_boundary_is_incomplete() -> None:
    boundary = 1_700_000_000_123_456_790
    calls: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        calls.append(form)
        values = [(boundary, "one"), (boundary, "two")]
        if len(calls) == 2:
            values.extend(((boundary, "three"), (boundary, "four")))
        return httpx.Response(200, text=_jsonl(values))

    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=2))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request(page_size=2, page_token=first.next_page_token))

    assert failure.value.kind is QueryFailureKind.INCOMPLETE
    assert calls[1]["start"] == ["2023-11-14T22:13:20.123456790Z"]
    assert calls[1]["end"] == ["2023-11-14T22:13:21.000000001Z"]
    assert calls[1]["query"][0].endswith("| sort by (_time) limit 4")


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"AccountID": "12", "ProjectID": "4"},
        {"AccountID": "11", "ProjectID": "3"},
    ],
)
def test_victorialogs_rejects_missing_or_mismatched_tenant_echo(headers: dict[str, str]) -> None:
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example", account_id=12, project_id=3),
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, text="", headers=headers))
        ),
    )
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is QueryFailureKind.FORBIDDEN_OR_TENANT
    assert failure.value.retryable is False


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        '{"_time":"bad","_msg":"message","_stream":"{}"}',
        '{"_time":"2023-11-14T22:13:20Z","_msg":7,"_stream":"{}"}',
        '{"_time":"2023-11-14T22:13:20Z","_msg":"message","_stream":"bad"}',
        '{"_time":"2023-11-14T22:13:20Z","_msg":"message","_stream":"{}","nested":{}}',
    ],
)
def test_victorialogs_rejects_malformed_jsonl(body: str) -> None:
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=body))),
    )
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is QueryFailureKind.MALFORMED_RESPONSE


@pytest.mark.parametrize(
    ("status", "kind", "retryable"),
    [
        (400, QueryFailureKind.INVALID_QUERY, False),
        (401, QueryFailureKind.UNAUTHENTICATED, False),
        (403, QueryFailureKind.FORBIDDEN_OR_TENANT, False),
        (502, QueryFailureKind.UNAVAILABLE, True),
    ],
)
def test_victorialogs_normalizes_http_failures(
    status: int,
    kind: QueryFailureKind,
    retryable: bool,
) -> None:
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status))),
    )
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is kind
    assert failure.value.retryable is retryable


@pytest.mark.parametrize(
    ("status", "kind", "retryable"),
    [
        (206, QueryFailureKind.INCOMPLETE, True),
        (204, QueryFailureKind.MALFORMED_RESPONSE, False),
    ],
)
def test_victorialogs_requires_http_200(status: int, kind: QueryFailureKind, retryable: bool) -> None:
    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status))),
    )
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is kind
    assert failure.value.retryable is retryable


def test_victorialogs_normalizes_premature_disconnect_as_incomplete() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_BrokenSyncStream(request))

    backend = VictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is QueryFailureKind.INCOMPLETE
    assert failure.value.retryable is True


@pytest.mark.asyncio
async def test_async_victorialogs_backend_uses_same_contract() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.extensions["timeout"] == {
            "connect": 2.5,
            "read": 4.25,
            "write": 4.25,
            "pool": 4.25,
        }
        return httpx.Response(
            200,
            text='{"_time":"2023-11-14T22:13:20.123456790Z","_msg":"ok","_stream":"{node_name=\\"n1\\"}"}',
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncVictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example", connect_timeout_seconds=2.5),
        client=client,
    )
    try:
        page = await backend.query_page(_request())
        assert page.records[0].fields == {"node_name": "n1"}
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_async_victorialogs_normalizes_premature_disconnect_as_incomplete() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_BrokenAsyncStream(request))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncVictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
    try:
        with pytest.raises(QueryFailure) as failure:
            await backend.query_page(_request())
        assert failure.value.kind is QueryFailureKind.INCOMPLETE
        assert failure.value.retryable is True
    finally:
        await client.aclose()


@pytest.mark.parametrize(
    ("status", "kind", "retryable"),
    [
        (206, QueryFailureKind.INCOMPLETE, True),
        (204, QueryFailureKind.MALFORMED_RESPONSE, False),
    ],
)
@pytest.mark.asyncio
async def test_async_victorialogs_requires_http_200(
    status: int,
    kind: QueryFailureKind,
    retryable: bool,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncVictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
    try:
        with pytest.raises(QueryFailure) as failure:
            await backend.query_page(_request())
        assert failure.value.kind is kind
        assert failure.value.retryable is retryable
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_async_victorialogs_backend_validates_tenant_echo() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="", headers={"AccountID": "12"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncVictoriaLogsBackend(
        VictoriaLogsBackendConfig("http://logs.example", account_id=12, project_id=3),
        client=client,
    )
    try:
        with pytest.raises(QueryFailure) as failure:
            await backend.query_page(_request())
        assert failure.value.kind is QueryFailureKind.FORBIDDEN_OR_TENANT
    finally:
        await client.aclose()
