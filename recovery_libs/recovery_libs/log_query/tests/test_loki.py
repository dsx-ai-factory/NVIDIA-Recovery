# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import recovery_libs.log_query._pagination as pagination
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from recovery_libs.log_query import (
    AsyncLokiBackend,
    CertificateExpiryReporter,
    LokiBackend,
    LokiBackendConfig,
    LokiQueryStats,
    NativeQuery,
    QueryFailure,
    QueryFailureKind,
    QueryRequest,
    QueryStats,
)


def _response(streams: list[dict[str, object]], stats: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": streams,
            "stats": stats or {},
        },
    }


def _request(*, page_size: int = 10, page_token: str | None = None, order: str = "forward") -> QueryRequest:
    return QueryRequest(
        query=NativeQuery("application", "logql", '{appName="slurm-app-log"}'),
        start_ns=1_000_000_001,
        end_ns=2_000_000_001,
        order=order,  # type: ignore[arg-type]
        page_size=page_size,
        page_token=page_token,
        timeout_seconds=7.5,
    )


def test_loki_query_stats_reads_the_stats_block_of_a_loki_response() -> None:
    stats_block = {
        "summary": {
            "totalBytesProcessed": 4096,
            "totalLinesProcessed": 12,
            "totalEntriesReturned": 3,
            "execTime": 0.25,
            "queueTime": 0.01,
        },
        "ingester": {"totalChunksMatched": 2, "totalLinesSent": 5, "totalReached": 3},
        "store": {"totalChunksDownloaded": 1},
    }
    client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=_response([], stats_block))))
    page = LokiBackend(LokiBackendConfig("https://loki.example/"), client=client).query_page(_request())

    assert LokiQueryStats.from_query_stats(page.stats) == LokiQueryStats(
        bytes_processed=4096.0,
        lines_processed=12.0,
        entries_returned=3.0,
        exec_time_seconds=0.25,
        queue_time_seconds=0.01,
        ingester_chunks_matched=2.0,
        ingester_lines_sent=5.0,
        ingesters_reached=3.0,
        store_chunks_downloaded=1.0,
    )


def test_loki_query_stats_leaves_omitted_or_non_numeric_fields_empty() -> None:
    page_stats = QueryStats(
        duration_seconds=0.01,
        attempts=1,
        response_bytes=100,
        returned_records=0,
        provider={"summary": {"totalBytesProcessed": "4096", "execTime": True}, "ingester": [], "store": None},
    )

    assert LokiQueryStats.from_query_stats(page_stats) == LokiQueryStats()


def test_loki_normalizes_streams_and_uses_integer_nanoseconds() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        payload = _response(
            [
                {"stream": {"job_id": "7", "level": "warn"}, "values": [["1500000002", " second "]]},
                {
                    "stream": {"job_id": "7", "level": "info"},
                    "values": [["1500000001", "first\nline"]],
                },
            ],
            {"summary": {"totalEntriesReturned": 2}},
        )
        return httpx.Response(200, json=payload)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    backend = LokiBackend(
        LokiBackendConfig(
            "https://loki.example/",
            auth_token="secret",
            tenants="basic-user",
            org_id="tenant-a",
            connect_timeout_seconds=1.25,
        ),
        client=client,
    )
    page = backend.query_page(_request())

    assert [record.timestamp_ns for record in page.records] == [1_500_000_001, 1_500_000_002]
    assert [record.body for record in page.records] == ["first\nline", " second "]
    assert page.records[0].fields == {"job_id": "7", "level": "info"}
    assert page.complete is True
    assert page.next_page_token is None
    assert page.stats.provider["summary"]["totalEntriesReturned"] == 2
    assert page.stats.returned_records == 2

    request = seen[0]
    assert request.url.path == "/loki/api/v1/query_range"
    assert request.url.params["start"] == "1000000001"
    assert request.url.params["end"] == "2000000001"
    assert request.url.params["direction"] == "forward"
    assert request.headers["X-Scope-OrgID"] == "tenant-a"
    assert request.headers["Authorization"].startswith("Basic ")
    assert request.extensions["timeout"] == {
        "connect": 1.25,
        "read": 7.5,
        "write": 7.5,
        "pool": 7.5,
    }


def test_loki_empty_result_is_success() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=_response([])))
    page = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport)).query_page(
        _request()
    )
    assert page.records == ()
    assert page.complete is True


@pytest.mark.parametrize("order", ["forward", "backward"])
@pytest.mark.parametrize("record_count", [5_000, 10_001])
@pytest.mark.parametrize("page_size", [5_000, 10_000])
@pytest.mark.asyncio
async def test_loki_pagination_respects_server_cap(order: str, record_count: int, page_size: int) -> None:
    values = [[str(1_200_000_000 + index), str(index)] for index in range(record_count)]
    requested_limits: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        limit = int(request.url.params["limit"])
        requested_limits.append(limit)
        if limit > 5_000:
            return httpx.Response(400, text="max entries limit per query exceeded")
        start, end = int(request.url.params["start"]), int(request.url.params["end"])
        matching = [value for value in values if start <= int(value[0]) < end]
        if request.url.params["direction"] == "backward":
            matching.reverse()
        return httpx.Response(200, json=_response([{"stream": {}, "values": matching[:limit]}]))

    for asynchronous in (False, True):
        transport = httpx.MockTransport(handler)
        backend = (
            AsyncLokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.AsyncClient(transport=transport))
            if asynchronous
            else LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
        )
        records = []
        token = None
        for _ in range(10):
            request = _request(page_size=page_size, page_token=token, order=order)
            page = await backend.query_page(request) if asynchronous else backend.query_page(request)
            records.extend((record.timestamp_ns, record.body) for record in page.records)
            assert len(page.records) <= page_size
            if page.complete:
                assert page.next_page_token is None
                break
            token = page.next_page_token
            assert token is not None
        else:
            pytest.fail("pagination did not exhaust the fixed dataset")
        expected = [(int(timestamp), body) for timestamp, body in values]
        assert records == (expected[::-1] if order == "backward" else expected)
    assert max(requested_limits) == 5_000


def test_loki_saturated_overlap_fails_without_exceeding_server_cap() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert int(request.url.params["limit"]) <= 5_000
        return httpx.Response(200, json=_response([{"stream": {}, "values": [["1200000000", "same"]] * 5_000}]))

    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    first = backend.query_page(_request(page_size=5_000))
    assert not first.complete
    with pytest.raises(QueryFailure) as error:
        backend.query_page(_request(page_size=5_000, page_token=first.next_page_token))
    assert error.value.kind == QueryFailureKind.INCOMPLETE


def test_loki_pagination_uses_overlap_without_dropping_occurrences() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            values = [["1200000000", "one"], ["1300000000", "two"]]
        else:
            assert request.url.params["start"] == "1300000000"
            assert request.url.params["limit"] == "3"
            values = [["1300000000", "two"], ["1400000000", "three"]]
        return httpx.Response(200, json=_response([{"stream": {"job": "a"}, "values": values}]))

    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=2))
    assert first.complete is False
    assert first.next_page_token

    second = backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert [(record.timestamp_ns, record.body) for record in second.records] == [(1_400_000_000, "three")]
    assert second.complete is True


def test_loki_continuation_allows_smaller_page_without_skips_or_duplicates() -> None:
    values = [
        ["1200000000", "one"],
        ["1300000000", "two"],
        ["1400000000", "three"],
        ["1500000000", "four"],
        ["1600000000", "five"],
        ["1700000000", "six"],
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        start_ns = int(request.url.params["start"])
        limit = int(request.url.params["limit"])
        selected = [value for value in values if int(value[0]) >= start_ns][:limit]
        return httpx.Response(200, json=_response([{"stream": {"job": "a"}, "values": selected}]))

    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=3))
    assert first.complete is False

    second = backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert second.complete is False
    third = backend.query_page(_request(page_size=2, page_token=second.next_page_token))
    assert third.complete is True

    records = first.records + second.records + third.records
    assert [record.body for record in records] == ["one", "two", "three", "four", "five", "six"]
    assert len({(record.timestamp_ns, record.body) for record in records}) == 6


def test_loki_credential_refresh_preserves_second_page_token() -> None:
    old_requests: list[httpx.Request] = []
    new_requests: list[httpx.Request] = []

    def old_handler(request: httpx.Request) -> httpx.Response:
        old_requests.append(request)
        if len(old_requests) == 1:
            values = [["1200000000", "one"], ["1300000000", "two"]]
            return httpx.Response(200, json=_response([{"stream": {"job": "a"}, "values": values}]))
        return httpx.Response(401)

    def new_handler(request: httpx.Request) -> httpx.Response:
        new_requests.append(request)
        values = [["1300000000", "two"], ["1400000000", "three"]]
        return httpx.Response(200, json=_response([{"stream": {"job": "a"}, "values": values}]))

    original = LokiBackendConfig(
        "http://loki.example",
        auth_token="Bearer old-token",
        org_id="tenant-a",
    )
    backend = LokiBackend(
        original,
        client=httpx.Client(transport=httpx.MockTransport(old_handler)),
    )
    first = backend.query_page(_request(page_size=2))
    continuation = _request(page_size=2, page_token=first.next_page_token)
    with pytest.raises(QueryFailure) as unauthenticated:
        backend.query_page(continuation)
    assert unauthenticated.value.kind is QueryFailureKind.UNAUTHENTICATED

    replacement = LokiBackendConfig(
        "http://loki.example",
        auth_token="Bearer new-token",
        org_id="tenant-a",
    )
    backend.replace_config(
        replacement,
        client=httpx.Client(transport=httpx.MockTransport(new_handler)),
    )
    second = backend.query_page(continuation)

    assert [record.body for page in (first, second) for record in page.records] == ["one", "two", "three"]
    assert second.complete is True
    assert old_requests[-1].headers["Authorization"] == "Bearer old-token"
    assert new_requests[0].headers["Authorization"] == "Bearer new-token"


def test_loki_credential_refresh_rejects_scope_change() -> None:
    original_client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=_response([])))
    )
    backend = LokiBackend(
        LokiBackendConfig("http://loki.example", auth_token="Bearer old", org_id="tenant-a"),
        client=original_client,
    )

    with pytest.raises(QueryFailure) as failure:
        backend.replace_config(
            LokiBackendConfig("http://other-loki.example", auth_token="Bearer new", org_id="tenant-a")
        )
    assert failure.value.kind is QueryFailureKind.INVALID_REQUEST
    assert backend.query_page(_request()).complete is True


def test_loki_credential_refresh_closes_owned_client() -> None:
    original_client = MagicMock(spec=httpx.Client)
    replacement_client = MagicMock(spec=httpx.Client)
    with patch("recovery_libs.log_query.loki.httpx.Client", side_effect=[original_client, replacement_client]):
        backend = LokiBackend(LokiBackendConfig("http://loki.example", auth_token="Bearer old"))
        backend.replace_config(LokiBackendConfig("http://loki.example", auth_token="Bearer new"))
        original_client.close.assert_called_once_with()
        replacement_client.close.assert_not_called()
        backend.close()
        replacement_client.close.assert_called_once_with()


def test_loki_ambiguous_equal_timestamp_boundary_is_incomplete() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        values = [["1300000000", "one"], ["1300000000", "two"]]
        if calls == 2:
            values.extend([["1300000000", "three"], ["1300000000", "four"]])
        return httpx.Response(200, json=_response([{"stream": {}, "values": values}]))

    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=2))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert failure.value.kind is QueryFailureKind.INCOMPLETE
    assert failure.value.retryable is True


def test_loki_missing_overlap_is_incomplete_even_on_short_response() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        values = [["1200000000", "one"], ["1300000000", "two"]] if calls == 1 else [["1400000000", "three"]]
        return httpx.Response(200, json=_response([{"stream": {}, "values": values}]))

    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    first = backend.query_page(_request(page_size=2))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert failure.value.kind is QueryFailureKind.INCOMPLETE


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "error", "data": {}},
        {"status": "success", "data": {"resultType": "matrix", "result": [], "stats": {}}},
        {"status": "success", "data": {"resultType": "streams", "result": "bad", "stats": {}}},
    ],
)
def test_loki_rejects_malformed_success_payload(payload: dict[str, object]) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is QueryFailureKind.MALFORMED_RESPONSE


@pytest.mark.parametrize("timestamp", [1_500_000_001, 1_500_000_001.5, "1500000001.5", True])
def test_loki_rejects_non_decimal_string_timestamps(timestamp: object) -> None:
    payload = _response([{"stream": {}, "values": [[timestamp, "message"]]}])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))

    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())

    assert failure.value.kind is QueryFailureKind.MALFORMED_RESPONSE


def test_loki_normalizes_rate_limit_and_retry_after() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(429, headers={"Retry-After": "3"}))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is QueryFailureKind.RATE_LIMITED
    assert failure.value.retryable is True
    assert failure.value.retry_after_seconds == 3


@pytest.mark.parametrize(
    ("status", "kind", "retryable"),
    [
        (206, QueryFailureKind.INCOMPLETE, True),
        (204, QueryFailureKind.MALFORMED_RESPONSE, False),
    ],
)
def test_loki_requires_http_200(status: int, kind: QueryFailureKind, retryable: bool) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(status))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request())
    assert failure.value.kind is kind
    assert failure.value.retryable is retryable


def test_loki_rejects_mismatched_token_and_dialect() -> None:
    backend = LokiBackend(
        LokiBackendConfig("http://loki.example"),
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(500))),
    )
    with pytest.raises(QueryFailure) as token_failure:
        backend.query_page(_request(page_token="not-a-token"))
    assert token_failure.value.kind is QueryFailureKind.INVALID_REQUEST

    bad_dialect = QueryRequest(NativeQuery("q", "logsql", "*"), 1, 2)
    with pytest.raises(QueryFailure) as dialect_failure:
        backend.query_page(bad_dialect)
    assert dialect_failure.value.kind is QueryFailureKind.UNSUPPORTED_SEMANTICS


def test_loki_token_is_bound_to_endpoint_and_tenant() -> None:
    payload = _response([{"stream": {"job": "a"}, "values": [["1200000000", "one"], ["1300000000", "two"]]}])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    first_backend = LokiBackend(
        LokiBackendConfig("http://loki.example", org_id="tenant-a"),
        client=httpx.Client(transport=transport),
    )
    first = first_backend.query_page(_request(page_size=2))

    second_backend = LokiBackend(
        LokiBackendConfig("http://loki.example", org_id="tenant-b"),
        client=httpx.Client(transport=transport),
    )
    with pytest.raises(QueryFailure) as failure:
        second_backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert failure.value.kind is QueryFailureKind.INVALID_REQUEST


def test_loki_token_is_bound_to_one_backend_instance() -> None:
    payload = _response([{"stream": {"job": "a"}, "values": [["1200000000", "one"], ["1300000000", "two"]]}])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    config = LokiBackendConfig("http://loki.example", org_id="tenant-a")
    first_backend = LokiBackend(config, client=httpx.Client(transport=transport))
    first = first_backend.query_page(_request(page_size=2))

    second_backend = LokiBackend(config, client=httpx.Client(transport=transport))
    with pytest.raises(QueryFailure) as failure:
        second_backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert failure.value.kind is QueryFailureKind.INVALID_REQUEST


def test_loki_rejects_tampered_page_token() -> None:
    payload = _response([{"stream": {"job": "a"}, "values": [["1200000000", "one"], ["1300000000", "two"]]}])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
    first = backend.query_page(_request(page_size=2))
    assert first.next_page_token is not None
    replacement = "A" if first.next_page_token[0] != "A" else "B"
    tampered = replacement + first.next_page_token[1:]

    with pytest.raises(QueryFailure) as failure:
        backend.query_page(_request(page_size=2, page_token=tampered))
    assert failure.value.kind is QueryFailureKind.INVALID_REQUEST


def test_loki_rejects_expired_page_token(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _response([{"stream": {"job": "a"}, "values": [["1200000000", "one"], ["1300000000", "two"]]}])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    backend = LokiBackend(LokiBackendConfig("http://loki.example"), client=httpx.Client(transport=transport))
    monkeypatch.setattr(pagination, "_now", lambda: 1_000.0)
    first = backend.query_page(_request(page_size=2))
    monkeypatch.setattr(pagination, "_now", lambda: 1_900.0)

    with pytest.raises(QueryFailure, match="expired") as failure:
        backend.query_page(_request(page_size=2, page_token=first.next_page_token))
    assert failure.value.kind is QueryFailureKind.INVALID_REQUEST


@pytest.mark.asyncio
async def test_async_loki_backend_uses_same_contract() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.extensions["timeout"] == {
            "connect": 2.0,
            "read": 7.5,
            "write": 7.5,
            "pool": 7.5,
        }
        return httpx.Response(
            200,
            content=json.dumps(_response([{"stream": {"node_name": "n1"}, "values": [["1500000000", "ok"]]}])),
            headers={"Content-Type": "application/json"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncLokiBackend(
        LokiBackendConfig("http://loki.example", connect_timeout_seconds=2.0),
        client=client,
    )
    try:
        page = await backend.query_page(_request())
        assert page.records[0].fields == {"node_name": "n1"}
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
async def test_async_loki_requires_http_200(
    status: int,
    kind: QueryFailureKind,
    retryable: bool,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = AsyncLokiBackend(LokiBackendConfig("http://loki.example"), client=client)
    try:
        with pytest.raises(QueryFailure) as failure:
            await backend.query_page(_request())
        assert failure.value.kind is kind
        assert failure.value.retryable is retryable
    finally:
        await client.aclose()


def _write_certificate(directory: Path, name: str, valid_days: int) -> tuple[Path, Path]:
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=valid_days, hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = directory / f"{name}.pem", directory / f"{name}.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return cert_path, key_path


@pytest.mark.asyncio
async def test_async_backend_reports_mutual_tls_certificate_expiry(tmp_path: Path) -> None:
    cert_path, key_path = _write_certificate(tmp_path, "client", valid_days=30)
    ca_path, _ = _write_certificate(tmp_path, "ca", valid_days=365)
    backend = AsyncLokiBackend(
        LokiBackendConfig(
            "https://loki.example", cert_file=str(cert_path), key_file=str(key_path), ca_cert=str(ca_path)
        )
    )

    try:
        assert isinstance(backend, CertificateExpiryReporter)
        assert backend.days_until_cert_expires() == 30
        assert backend.days_until_ca_cert_expires() == 365
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_async_backend_without_certificates_reports_no_expiry() -> None:
    backend = AsyncLokiBackend(LokiBackendConfig("https://loki.example"))

    try:
        assert backend.days_until_cert_expires() is None
        assert backend.days_until_ca_cert_expires() is None
    finally:
        await backend.close()
