# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provider-parametrized tests for the normalized backend contract."""

import asyncio
import json
from collections import Counter

import httpx
import pytest
from recovery_libs.log_query import (
    AsyncLokiBackend,
    AsyncVictoriaLogsBackend,
    BackendName,
    LokiBackend,
    LokiBackendConfig,
    NativeQuery,
    QueryFailure,
    QueryFailureKind,
    QueryRequest,
    VictoriaLogsBackend,
    VictoriaLogsBackendConfig,
    create_log_query_backend,
)

_START_NS = 1_700_000_000_000_000_000
_END_NS = _START_NS + 10
_FIXTURE_RECORDS = [
    (_START_NS, " α whitespace \n", {"cluster": "c1", "job_id": "42"}),
    (_START_NS + 1, '{"message":"JSON-looking"}', {"cluster": "c1", "job_id": "42"}),
    (_START_NS + 2, "duplicate", {"cluster": "c1", "job_id": "42"}),
    (_START_NS + 2, "duplicate", {"cluster": "c1", "job_id": "42"}),
    (_END_NS - 1, "before-end", {"cluster": "c1", "job_id": "42"}),
    (_END_NS, "excluded-end", {"cluster": "c1", "job_id": "42"}),
]


def _loki_backend() -> LokiBackend:
    streams = [
        {
            "stream": fields,
            "values": [[str(timestamp_ns), body]],
        }
        for timestamp_ns, body, fields in reversed(_FIXTURE_RECORDS)
    ]
    payload = {
        "status": "success",
        "data": {"resultType": "streams", "result": streams, "stats": {}},
    }
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)))
    return LokiBackend(LokiBackendConfig("http://loki.example"), client=client)


def _victorialogs_backend() -> VictoriaLogsBackend:
    lines = []
    for timestamp_ns, body, fields in reversed(_FIXTURE_RECORDS):
        seconds, nanos = divmod(timestamp_ns, 1_000_000_000)
        assert seconds == 1_700_000_000
        stream = '{cluster="c1",job_id="42"}'
        lines.append(
            json.dumps(
                {
                    "_time": f"2023-11-14T22:13:20.{nanos:09d}Z",
                    "_msg": body,
                    "_stream": stream,
                },
                ensure_ascii=False,
            )
        )
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text="\n".join(lines))))
    return VictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)


@pytest.mark.parametrize("backend_name", list(BackendName))
@pytest.mark.parametrize("order", ["forward", "backward"])
def test_backends_produce_equivalent_normalized_occurrences(backend_name: BackendName, order: str) -> None:
    backend = _loki_backend() if backend_name is BackendName.LOKI else _victorialogs_backend()
    request = QueryRequest(
        NativeQuery("contract", backend.dialect, "fixture query"),
        _START_NS,
        _END_NS,
        order=order,  # type: ignore[arg-type]
        page_size=20,
    )

    page = backend.query_page(request)

    expected = Counter(
        (timestamp_ns, body, tuple(sorted(fields.items())))
        for timestamp_ns, body, fields in _FIXTURE_RECORDS
        if _START_NS <= timestamp_ns < _END_NS
    )
    actual = Counter(
        (record.timestamp_ns, record.body, tuple(sorted(record.fields.items()))) for record in page.records
    )
    assert actual == expected
    timestamps = [record.timestamp_ns for record in page.records]
    assert timestamps == sorted(timestamps, reverse=order == "backward")
    assert page.records[0 if order == "forward" else -1].timestamp_ns == _START_NS
    assert len([record for record in page.records if record.body == "duplicate"]) == 2
    assert {record.body for record in page.records} >= {
        " α whitespace \n",
        '{"message":"JSON-looking"}',
        "before-end",
    }
    assert page.complete is True


@pytest.mark.parametrize(
    ("backend_name", "config"),
    [
        (BackendName.LOKI, LokiBackendConfig("http://loki.example")),
        (BackendName.VICTORIALOGS, VictoriaLogsBackendConfig("http://logs.example")),
    ],
)
def test_factory_backends_identify_their_provider(backend_name: BackendName, config) -> None:
    sync_backend = create_log_query_backend(backend_name, config, mode="sync")
    async_backend = create_log_query_backend(backend_name, config, mode="async")

    assert sync_backend.name == backend_name
    assert async_backend.name == backend_name
    assert async_backend.dialect == sync_backend.dialect
    sync_backend.close()
    asyncio.run(async_backend.close())


@pytest.mark.parametrize("backend_name", list(BackendName))
def test_backends_distinguish_successful_empty_from_failure(backend_name: BackendName) -> None:
    if backend_name is BackendName.LOKI:
        payload = {"status": "success", "data": {"resultType": "streams", "result": [], "stats": {}}}
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)))
        backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=client)
        query = NativeQuery("empty", "logql", "{}")
    else:
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text="")))
        backend = VictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
        query = NativeQuery("empty", "logsql", "*")

    page = backend.query_page(QueryRequest(query, _START_NS, _END_NS))
    assert page.records == ()
    assert page.complete is True
    assert page.next_page_token is None


@pytest.mark.parametrize("backend_name", list(BackendName))
def test_backends_normalize_transport_timeout(backend_name: BackendName) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("query timed out", request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    if backend_name is BackendName.LOKI:
        backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=client)
        query = NativeQuery("timeout", "logql", "{}")
    else:
        backend = VictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
        query = NativeQuery("timeout", "logsql", "*")

    with pytest.raises(QueryFailure) as failure:
        backend.query_page(QueryRequest(query, _START_NS, _END_NS))
    assert failure.value.kind is QueryFailureKind.TIMEOUT
    assert failure.value.retryable is True


@pytest.mark.parametrize("backend_name", list(BackendName))
@pytest.mark.asyncio
async def test_async_backends_normalize_transport_timeout(backend_name: BackendName) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("query timed out", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    if backend_name is BackendName.LOKI:
        backend = AsyncLokiBackend(LokiBackendConfig("http://loki.example"), client=client)
        query = NativeQuery("timeout", "logql", "{}")
    else:
        backend = AsyncVictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
        query = NativeQuery("timeout", "logsql", "*")

    try:
        with pytest.raises(QueryFailure) as failure:
            await backend.query_page(QueryRequest(query, _START_NS, _END_NS))
        assert failure.value.kind is QueryFailureKind.TIMEOUT
        assert failure.value.retryable is True
    finally:
        await client.aclose()


@pytest.mark.parametrize("backend_name", list(BackendName))
def test_backends_normalize_cancellation_status(backend_name: BackendName) -> None:
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(499)))
    if backend_name is BackendName.LOKI:
        backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=client)
        query = NativeQuery("cancelled", "logql", "{}")
    else:
        backend = VictoriaLogsBackend(VictoriaLogsBackendConfig("http://logs.example"), client=client)
        query = NativeQuery("cancelled", "logsql", "*")

    with pytest.raises(QueryFailure) as failure:
        backend.query_page(QueryRequest(query, _START_NS, _END_NS))
    assert failure.value.kind is QueryFailureKind.CANCELLED
    assert failure.value.retryable is False
