# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import ssl
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from log_query_fakes import fake_backend, log_query_config
from recovery_libs.log_query import (
    AsyncLokiBackend,
    BackendName,
    LogRecord,
    LokiBackendConfig,
    QueryFailure,
    QueryFailureKind,
    QueryStats,
)
from watcher.config import LokiConnectionSettings
from watcher.lib.plugins.log_query import LogQueryBatcher, LokiBackendFactory
from watcher.lib.utils.constants import HTTP_MAX_QUERY_WINDOW_SECS
from watcher.lib.utils.data import LabelRemapper, generate_sha256_signature
from watcher.lib.utils.metrics import (
    LOG_QUERY_EMPTY_RESPONSE,
    LOG_QUERY_ERROR_REASONS,
    LOG_QUERY_ERRORS,
    LOG_QUERY_WINDOW_CAPPED,
)


@pytest.fixture
def loki_config():
    return LokiConnectionSettings(
        url="http://localhost:3100",
        cert_file=None,
        key_file=None,
        ca_cert=None,
        x_scope_org_id=None,
    )


@pytest.fixture
def loki_client():
    return LogQueryBatcher(
        backend_factory=fake_backend,
        log_query_config=log_query_config(BackendName.LOKI),
        state_store={},
        max_lookback=60,
        query_delay=45,
        query_timeout=15,
        query_limit=1000,
        restart_on_auth_failure=True,
    )


@pytest.fixture
def loki_factory(loki_config):
    return LokiBackendFactory(log_query_config(BackendName.LOKI).selected_provider, loki_config, query_timeout=15)


def test_parse_records_from_response_single_entry(loki_client):
    response = [
        {
            "values": [
                [
                    "timestamp1",
                    '{"key1": "value1", "key2": "value2", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
                [
                    "timestamp2",
                    '{"key1": "value3", "key2": "value4", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
            ]
        }
    ]

    expected_output = [
        {
            "key1": "value1",
            "key2": "value2",
            "timestamp": "timestamp1",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
        {
            "key1": "value3",
            "key2": "value4",
            "timestamp": "timestamp2",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
    ]

    result = loki_client._parse_legacy_loki_response(response)
    assert result == expected_output


def test_parse_records_from_response_multiple_entries(loki_client):
    response = [
        {
            "values": [
                [
                    "timestamp3",
                    '{"key1": "value1", "key2": "value2", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
                [
                    "timestamp4",
                    '{"key1": "value3", "key2": "value4", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
            ]
        },
        {
            "values": [
                [
                    "timestamp5",
                    '{"key1": "value5", "key2": "value6", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
                [
                    "timestamp6",
                    '{"key1": "value7", "key2": "value8", "attributes":{"appname": "otelcol-contrib", "job_id":"3234872", "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log"}}',
                ],
            ]
        },
    ]

    expected_output = [
        {
            "key1": "value1",
            "key2": "value2",
            "timestamp": "timestamp3",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
        {
            "key1": "value3",
            "key2": "value4",
            "timestamp": "timestamp4",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
        {
            "key1": "value5",
            "key2": "value6",
            "timestamp": "timestamp5",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
        {
            "key1": "value7",
            "key2": "value8",
            "timestamp": "timestamp6",
            "attributes": {
                "appname": "otelcol-contrib",
                "job_id": "3234872",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
    ]

    result = loki_client._parse_legacy_loki_response(response)
    assert result == expected_output


def test_get_query_signature(loki_client):
    query = "{exporter='OTLP'}"
    query_id = "78257485-c7c7-4ccc-99de-3fd8dedebf94"

    signature = generate_sha256_signature(data=f"{query}-{query_id}")
    assert isinstance(signature, str)
    assert len(signature) == 64  # SHA-256 hash length


def test_get_query_last_execution_time(loki_client):
    query = "{exporter='OTLP'}"
    query_id = "78257485-c7c7-4ccc-99de-3fd8dedebf94"
    last_execution_time = 1_700_000_000_123_456_000
    loki_client._store_query_last_execution_time(query, last_execution_time, query_id)

    result = loki_client._get_query_last_execution_time(query, query_id)
    assert result == last_execution_time


def test_store_query_last_execution_time(loki_client):
    query = "{exporter='OTLP'}"
    query_id = "78257485-c7c7-4ccc-99de-3fd8dedebf94"
    end_time = 1_700_000_000_123_456_000

    loki_client._store_query_last_execution_time(query, end_time, query_id)

    assert loki_client._get_query_last_execution_time(query, query_id) == end_time


def test_clear_query_last_execution_time(loki_client):
    query = "{exporter='OTLP'}"
    query_id = "78257485-c7c7-4ccc-99de-3fd8dedebf94"
    last_execution_time = 1_700_000_000_123_456_000
    loki_client._store_query_last_execution_time(query, last_execution_time, query_id)

    loki_client.clear_query_last_execution_time(query, query_id)

    assert loki_client._get_query_last_execution_time(query, query_id) is None


def test_does_record_contain_jobid_present(loki_client):
    record = {"attributes": {"job_id": "3234872", "appname": "otelcol-contrib"}}
    assert loki_client._does_record_contain_jobid(record) is True


def test_does_record_contain_jobid_absent(loki_client):
    record = {"attributes": {"appname": "otelcol-contrib"}}
    assert loki_client._does_record_contain_jobid(record) is False


def test_does_record_contain_jobid_empty_attributes(loki_client):
    record = {"attributes": {}}
    assert loki_client._does_record_contain_jobid(record) is False


def test_does_record_contain_jobid_attributes_absent(loki_client):
    record = {}
    assert loki_client._does_record_contain_jobid(record) is False


@pytest.mark.asyncio
async def test_batch_query_with_ignore_query_delay_true(loki_client):
    mock_response = [
        LogRecord(1721253489153638946, '{"body":"2: Segmentation fault"}', {"app": "myapp", "level": "DEBUG"}),
        LogRecord(
            1721253489151511396,
            '{"body":"2: [hostname:1866911:0:1866911] Caught signal 11"}',
            {"app": "myapp", "level": "DEBUG"},
        ),
    ]
    loki_client.query_records = AsyncMock(return_value=(mock_response, None))
    query = "{exporter='OTLP'}"
    start_time = datetime.now(UTC) - timedelta(minutes=4)
    end_time = datetime.now(UTC)

    _ = await loki_client.batch_query(query=query, start=start_time, end=end_time, ignore_query_delay=True)

    loki_client.query_records.assert_called_once_with(
        query=query,
        start=start_time,
        end=end_time,
        limit=None,
        query_id=None,
    )


@pytest.mark.asyncio
async def test_batch_query_measures_lookback_from_delayed_end(loki_client):
    loki_client.query_records = AsyncMock(return_value=([], None))

    await loki_client.batch_query(query="{exporter='OTLP'}")

    query_window = loki_client.query_records.call_args.kwargs
    assert query_window["end"] - query_window["start"] == timedelta(seconds=loki_client.max_lookback)


def test_parse_records_with_filename(loki_client):
    response = [
        {
            "stream": {
                "app": "myapp",
                "level": "DEBUG",
                "job_id": "3234872",
                "filename": "/home/test-service-user-02/slurm_logs/test-user-03/gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
            "values": [
                ["1721253489153638946", "2: Segmentation fault"],
                ["1721253489151511396", "2: [hostname:1866911:0:1866911] Caught signal 11"],
            ],
        }
    ]

    expected_output = [
        {
            "timestamp": "1721253489153638946",
            "body": "2: Segmentation fault",
            "attributes": {
                "app": "myapp",
                "level": "DEBUG",
                "job_id": "3234872",
                "log.file.path": "/home/test-service-user-02/slurm_logs/test-user-03/gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
        {
            "timestamp": "1721253489151511396",
            "body": "2: [hostname:1866911:0:1866911] Caught signal 11",
            "attributes": {
                "app": "myapp",
                "level": "DEBUG",
                "job_id": "3234872",
                "log.file.path": "/home/test-service-user-02/slurm_logs/test-user-03/gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
                "log.file.name": "gpt3-15b-4t-fault-inject-hang-ub_546_date_25-01-24_time_12-03-01.log",
            },
        },
    ]

    result = loki_client._parse_legacy_loki_response(response)
    assert result == expected_output


def test_record_without_attributes(loki_client):
    """
    Test that a record without the 'attributes' key returns False.
    """
    record = {}  # No 'attributes'
    assert loki_client._does_record_contain_filename(record) is False


@pytest.mark.parametrize(
    "filename_key, filename",
    [
        ("filename", "myfile.txt"),
        ("log.file.name", "myfile.txt"),
        ("log_file_name", "myfile.txt"),
    ],
)
def test_record_with_valid_filename(loki_client, filename_key, filename):
    """
    Test that a record with a valid filename attribute (any of the three)
    returns True.
    """
    record = {"attributes": {filename_key: filename}}
    assert loki_client._does_record_contain_filename(record) is True


def test_record_with_additional_attributes(loki_client):
    """
    Test that a record with extra keys in the attributes dictionary but with one
    valid filename key returns True.
    """
    record = {"attributes": {"some_key": "some_value", "filename": "file.txt"}}
    assert loki_client._does_record_contain_filename(record) is True


def test_record_with_multiple_valid_keys(loki_client):
    """
    Test that a record with multiple valid filename keys returns True.
    """
    record = {"attributes": {"log.file.name": "file2.txt", "log_file_name": "file3.txt"}}
    assert loki_client._does_record_contain_filename(record) is True


def test_is_heartbeat_log_true(loki_client):
    record = {"body": '"type": "heartbeat_application_logs",', "attributes": {}}
    assert loki_client._is_heartbeat_log(record) is True


def test_is_heartbeat_log_false(loki_client):
    record = {"body": "regular log message", "attributes": {}}
    assert loki_client._is_heartbeat_log(record) is False


def test_parse_heartbeat_logs_without_jobid(loki_client):
    response = [
        {
            "stream": {},
            "values": [
                [
                    "1729695345000000000",
                    '{"body":"\\"type\\": \\"heartbeat_application_logs\\",","attributes":{"log.file.name":"heimdall_app_log_heartbeat_prod.log","log.file.path":"/home/test-service-user/otelfilelog_paths/prod/heimdall_app_log_heartbeat_prod.log"}}',
                ]
            ],
        }
    ]
    result = loki_client._parse_legacy_loki_response(response)
    assert len(result) == 1
    assert result[0]["body"] == '"type": "heartbeat_application_logs",'
    assert result[0]["attributes"]["job_id"] == "000000"


def test_get_loki_config_from_config(loki_factory):
    """Test getting Loki config from standard configuration."""
    with patch("watcher.lib.plugins.log_query.backends.read_api_token_from_file", return_value="test-token"):
        url, token = loki_factory._get_loki_config_from_config()

        assert url == loki_factory.provider.url
        assert token == "test-token"


@patch("watcher.lib.plugins.log_query.backends.requests.get")
def test_get_worker_cluster_info_success(mock_get, loki_factory):
    """Test getting worker cluster info from Lepton API successfully."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": {"properties": {"loki_url": "http://lepton-loki:3100", "loki_bearer_token": "lepton-token"}}
    }
    mock_get.return_value = mock_response

    result = loki_factory._get_worker_cluster_info("test-cluster", "http://lepton-api")

    assert result is not None
    assert result["status"]["properties"]["loki_url"] == "http://lepton-loki:3100"
    assert result["status"]["properties"]["loki_bearer_token"] == "lepton-token"


@patch("watcher.lib.plugins.log_query.backends.requests.get")
def test_get_worker_cluster_info_failure(mock_get, loki_factory):
    """Test getting worker cluster info from Lepton API when it fails."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    result = loki_factory._get_worker_cluster_info("test-cluster", "http://lepton-api")
    assert result is None


@patch.object(LokiBackendFactory, "_get_worker_cluster_info")
def test_get_loki_config_from_lepton_success(mock_get_worker_cluster_info, loki_factory):
    """Test getting Loki config from Lepton successfully."""
    mock_get_worker_cluster_info.return_value = {
        "status": {"properties": {"loki_url": "http://lepton-loki:3100", "loki_bearer_token": "lepton-token"}}
    }

    with patch.dict(
        "os.environ", {"LEPTON_WORKER_CLUSTER_NAME": "test-cluster", "LEPTON_BASE_URL": "http://lepton-api"}
    ):
        result = loki_factory._get_loki_config_from_lepton()

        assert result is not None
        url, token = result
        assert url == loki_factory.provider.url
        assert token == "lepton-token"
        mock_get_worker_cluster_info.assert_called_once_with("test-cluster", "http://lepton-api")


@patch.object(LokiBackendFactory, "_get_worker_cluster_info")
def test_get_loki_config_from_lepton_failure(mock_get_worker_cluster_info, loki_factory):
    """Test getting Loki config from Lepton when it fails."""
    mock_get_worker_cluster_info.return_value = None

    with patch.dict(
        "os.environ", {"LEPTON_WORKER_CLUSTER_NAME": "test-cluster", "LEPTON_BASE_URL": "http://lepton-api"}
    ):
        result = loki_factory._get_loki_config_from_lepton()
        assert result is None


@patch.object(LokiBackendFactory, "_get_loki_config_from_lepton")
def test_get_loki_config_with_lepton(mock_get_lepton_config, loki_factory):
    """Test getting Loki config when Lepton is enabled."""
    mock_get_lepton_config.return_value = ("http://lepton-loki:3100", "lepton-token")

    with patch.dict("os.environ", {"USE_LEPTON": "true"}):
        url, token = loki_factory._get_loki_config()

        assert url == loki_factory.provider.url
        assert token == "Bearer lepton-token"
        mock_get_lepton_config.assert_called_once()


@patch.object(LokiBackendFactory, "_get_loki_config_from_lepton")
def test_get_loki_config_without_lepton(mock_get_lepton_config, loki_factory):
    """Test getting Loki config when Lepton is not enabled."""
    with (
        patch.dict("os.environ", {"USE_LEPTON": "false"}),
        patch.object(
            loki_factory, "_get_loki_config_from_config", return_value=(loki_factory.provider.url, "test-token")
        ),
    ):
        url, token = loki_factory._get_loki_config()

        assert url == loki_factory.provider.url
        assert token == "test-token"
        mock_get_lepton_config.assert_not_called()


@patch.object(LokiBackendFactory, "_get_loki_config_from_lepton")
def test_get_loki_config_lepton_fallback(mock_get_lepton_config, loki_factory):
    """Test getting Loki config when Lepton is enabled but fails."""
    mock_get_lepton_config.return_value = None

    with (
        patch.dict("os.environ", {"USE_LEPTON": "true"}),
        patch.object(
            loki_factory, "_get_loki_config_from_config", return_value=(loki_factory.provider.url, "test-token")
        ),
    ):
        url, token = loki_factory._get_loki_config()

        assert url == loki_factory.provider.url
        assert token == "test-token"
        mock_get_lepton_config.assert_called_once()


@patch.object(LokiBackendFactory, "_get_worker_cluster_info")
def test_get_loki_config_full_lepton_flow(mock_get_worker_cluster_info, loki_factory):
    """Test the complete flow of getting Loki config from Lepton, including API response and token formatting."""
    # Mock the worker cluster response with all required fields
    mock_get_worker_cluster_info.return_value = {
        "status": {"properties": {"loki_url": "http://lepton-loki:3100", "loki_bearer_token": "lepton-token-123"}}
    }

    # Set up environment variables for Lepton
    with patch.dict(
        "os.environ",
        {"LEPTON_WORKER_CLUSTER_NAME": "test-cluster", "LEPTON_BASE_URL": "http://lepton-api", "USE_LEPTON": "true"},
    ):
        # Call the main config function
        url, token = loki_factory._get_loki_config()

        # Verify the results
        assert url == loki_factory.provider.url
        assert token == "Bearer lepton-token-123"

        # Verify the worker cluster info was requested with correct parameters
        mock_get_worker_cluster_info.assert_called_once_with("test-cluster", "http://lepton-api")


@pytest.mark.asyncio
async def test_batch_query_increments_error_metric_on_exception(loki_client):
    """Test LOG_QUERY_ERRORS metric is incremented on query_records exception."""
    query = "{exporter='OTLP'}"
    loki_client.query_records = AsyncMock(
        side_effect=ssl.SSLError("[SSL: TLSV13_ALERT_CERTIFICATE_REQUIRED] tlsv13 alert certificate required")
    )
    initial_count = LOG_QUERY_ERRORS.labels(query="anonymous")._value.get()
    with pytest.raises(ssl.SSLError):
        await loki_client.batch_query(query=query)
    assert LOG_QUERY_ERRORS.labels(query="anonymous")._value.get() == initial_count + 1


def _failure(kind: QueryFailureKind) -> QueryFailure:
    return QueryFailure(kind, backend=BackendName.LOKI, query_id="hang_app_0", message="failed", retryable=False)


def _counter_total(counter) -> float:
    """Return a counter's value summed across all of its label sets."""
    return sum(s.value for m in counter.collect() for s in m.samples if s.name.endswith("_total"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (_failure(QueryFailureKind.TIMEOUT), "timeout"),
        (asyncio.CancelledError(), "cancelled"),
        (_failure(QueryFailureKind.UNAUTHENTICATED), "unauthenticated"),
        (_failure(QueryFailureKind.RATE_LIMITED), "rate_limited"),
        (_failure(QueryFailureKind.UNAVAILABLE), "unavailable"),
        (ssl.SSLError("certificate required"), "other"),
    ],
)
async def test_batch_query_failure_counts_once_under_its_reason(loki_client, failure, reason):
    """A failed query counts once in LOG_QUERY_ERRORS and once in LOG_QUERY_ERROR_REASONS, under its reason."""
    loki_client.query_records = AsyncMock(side_effect=failure)
    loki_client.restart_client = MagicMock()
    errors = LOG_QUERY_ERRORS.labels(query="hang_app_0")
    reasons = LOG_QUERY_ERROR_REASONS.labels(backend=BackendName.LOKI, reason=reason)
    initial = (errors._value.get(), reasons._value.get(), _counter_total(LOG_QUERY_ERROR_REASONS))

    with pytest.raises(type(failure)):
        await loki_client.batch_query(query="{exporter='OTLP'}", query_id="hang_app_0")

    assert errors._value.get() == initial[0] + 1
    assert reasons._value.get() == initial[1] + 1
    assert _counter_total(LOG_QUERY_ERROR_REASONS) == initial[2] + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "restarts"),
    [
        (QueryFailureKind.UNAUTHENTICATED, True),
        (QueryFailureKind.FORBIDDEN_OR_TENANT, True),
        (QueryFailureKind.UNAVAILABLE, False),
    ],
)
async def test_batch_query_restarts_client_only_on_auth_failure(loki_client, kind, restarts):
    """The backend rejecting the watcher's credentials restarts the client; other failures do not."""
    loki_client.query_records = AsyncMock(side_effect=_failure(kind))
    loki_client.restart_client = MagicMock()

    with pytest.raises(QueryFailure):
        await loki_client.batch_query(query="{exporter='OTLP'}", query_id="hang_app_0")

    assert loki_client.restart_client.called is restarts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "restarts", "reason"),
    [
        (401, True, "unauthenticated"),
        (403, True, "forbidden_or_tenant"),
        (404, False, "invalid_request"),
        (429, False, "rate_limited"),
        (503, False, "unavailable"),
        (httpx.ReadTimeout, False, "timeout"),
    ],
)
async def test_loki_backend_failure_is_classified_by_its_cause(loki_client, outcome, restarts, reason):
    """A real Loki backend's HTTP and transport failures count under their reason and restart only on auth."""

    def respond(request: httpx.Request) -> httpx.Response:
        if isinstance(outcome, int):
            return httpx.Response(outcome, request=request)
        raise outcome("", request=request)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    loki_client.backend = AsyncLokiBackend(LokiBackendConfig("http://localhost:3100"), client=http_client)
    loki_client.restart_client = MagicMock()
    reasons = LOG_QUERY_ERROR_REASONS.labels(backend=BackendName.LOKI, reason=reason)
    initial = reasons._value.get()

    with pytest.raises(QueryFailure):
        await loki_client.batch_query(query="{exporter='OTLP'}", query_id="hang_app_0")

    assert reasons._value.get() == initial + 1
    assert loki_client.restart_client.called is restarts


@pytest.mark.asyncio
async def test_batch_query_auth_restarts_at_most_once_per_cooldown(loki_client):
    """Credentials that stay bad fail every query; the client restarts once per cooldown, not per failure."""
    loki_client.query_records = AsyncMock(side_effect=_failure(QueryFailureKind.UNAUTHENTICATED))
    loki_client.restart_client = MagicMock()

    for _ in range(3):
        with pytest.raises(QueryFailure):
            await loki_client.batch_query(query="{exporter='OTLP'}", query_id="hang_app_0")

    loki_client.restart_client.assert_called_once()


@pytest.mark.asyncio
async def test_batch_query_failed_restart_keeps_query_failure_and_retries_restart(loki_client):
    """A restart that raises must not replace the query failure or use up the restart cooldown."""
    loki_client.query_records = AsyncMock(side_effect=_failure(QueryFailureKind.UNAUTHENTICATED))
    loki_client.restart_client = MagicMock(side_effect=[RuntimeError("token file unreadable"), None])

    for _ in range(2):
        with pytest.raises(QueryFailure):
            await loki_client.batch_query(query="{exporter='OTLP'}", query_id="hang_app_0")

    assert loki_client.restart_client.call_count == 2


@pytest.mark.asyncio
async def test_batch_query_empty_response_metric_always_increments(loki_client):
    """Empty-response metric increments regardless of log_empty_response flag."""
    query = "{exporter='OTLP'}"
    start_time = datetime.now() - timedelta(minutes=4)
    end_time = datetime.now()
    mock_stats = [QueryStats(duration_seconds=0.01, attempts=1, response_bytes=0, returned_records=0)]
    loki_client.query_records = AsyncMock(return_value=([], mock_stats))

    initial = LOG_QUERY_EMPTY_RESPONSE.labels(query="anonymous")._value.get()
    await loki_client.batch_query(query=query, start=start_time, end=end_time, log_empty_response=False)
    assert LOG_QUERY_EMPTY_RESPONSE.labels(query="anonymous")._value.get() == initial + 1

    await loki_client.batch_query(query=query, start=start_time, end=end_time, log_empty_response=True)
    assert LOG_QUERY_EMPTY_RESPONSE.labels(query="anonymous")._value.get() == initial + 2


# LogQueryPoller tests
@pytest.fixture
def poller_setup(loki_client):
    """Setup for LogQueryPoller tests."""
    from watcher.lib.plugins.log_query import LogQueryPoller

    loki_client._get_query_last_execution_time = MagicMock(return_value=None)
    return loki_client, LogQueryPoller


@pytest.mark.asyncio
async def test_log_query_poller_start_stop(poller_setup):
    """Test LogQueryPoller lifecycle."""
    loki_client, LogQueryPoller = poller_setup
    poller = LogQueryPoller(loki_client, AsyncMock(), [{"name": "test"}])
    await poller.start()
    assert poller._running
    await poller.stop()
    assert not poller._running


@pytest.mark.asyncio
async def test_log_query_poller_processes_chunks(poller_setup):
    """Test chunk processing."""
    loki_client, LogQueryPoller = poller_setup
    callback = AsyncMock()
    poller = LogQueryPoller(loki_client, callback, [{"name": "test"}], chunk_size_secs=30)
    await poller._process_query_chunks({"name": "test"}, "test")
    assert callback.call_count >= 1


@pytest.mark.asyncio
async def test_log_query_poller_measures_lookback_from_delayed_end(poller_setup):
    loki_client, LogQueryPoller = poller_setup
    callback = AsyncMock()
    poller = LogQueryPoller(loki_client, callback, [{"name": "test"}], chunk_size_secs=120)

    await poller._process_query_chunks({"name": "test"}, "test")

    callback.assert_awaited_once()
    _, start_time, end_time = callback.await_args.args
    assert end_time - start_time == timedelta(seconds=loki_client.max_lookback)


@pytest.mark.asyncio
async def test_log_query_poller_backoff_on_failure(poller_setup):
    """Test backoff on failure."""
    loki_client, LogQueryPoller = poller_setup
    poller = LogQueryPoller(loki_client, AsyncMock(side_effect=Exception("Fail")), [{"name": "test"}])
    await poller._process_query_chunks({"name": "test"}, "test")
    assert poller.query_backoff["test"] > 0


@pytest.mark.asyncio
async def test_log_query_poller_resets_backoff_on_success(poller_setup):
    """Test backoff reset on success."""
    loki_client, LogQueryPoller = poller_setup
    poller = LogQueryPoller(loki_client, AsyncMock(), [{"name": "test"}])
    poller.query_backoff["test"] = 10.0
    await poller._process_query_chunks({"name": "test"}, "test")
    assert poller.query_backoff["test"] == 0.0


@pytest.mark.asyncio
async def test_log_query_poller_runs_queries_in_parallel(poller_setup):
    """All queries reach an internal barrier before any proceeds — proves concurrent dispatch
    without depending on wall-clock timing (immune to CI scheduler jitter)."""
    import asyncio

    n = 5
    started = asyncio.Event()
    proceed = asyncio.Event()
    arrived = 0

    async def gated(_):
        """Test helper: signal arrival then await ``proceed`` so all tasks pile up at the barrier."""
        nonlocal arrived
        arrived += 1
        if arrived == n:
            started.set()
        await proceed.wait()

    loki_client, LogQueryPoller = poller_setup
    poller = LogQueryPoller(loki_client, AsyncMock(), [{"name": f"q{i}"} for i in range(n)], iteration_budget_secs=10)
    with patch.object(poller, "_process_query_config", side_effect=gated):
        task = asyncio.create_task(poller._poll_iteration())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        assert arrived == n  # all queries dispatched before any completed → parallel
        proceed.set()
        await task


@pytest.mark.asyncio
async def test_log_query_poller_per_query_timeout_does_not_starve_others(poller_setup, caplog):
    """A query exceeding per-query budget is cancelled; others still run and budget counter increments."""
    import asyncio
    import logging

    async def by_name(cfg):
        """Test helper: sleep 500ms only for cfg["name"] == "slow" so it trips the 50ms per-query budget."""
        await asyncio.sleep(0.5 if cfg["name"] == "slow" else 0)

    loki_client, LogQueryPoller = poller_setup
    poller = LogQueryPoller(loki_client, AsyncMock(), [{"name": "slow"}, {"name": "fast"}], iteration_budget_secs=0.05)
    before = poller._budget_exceeded_metrics["slow"]._value.get()
    with patch.object(poller, "_process_query_config", side_effect=by_name), caplog.at_level(logging.WARNING):
        await poller._poll_iteration()
    assert poller._budget_exceeded_metrics["slow"]._value.get() - before == 1
    assert poller._budget_exceeded_metrics["fast"]._value.get() == 0
    assert "[LOG_QUERY_DEGRADED] Iteration finished" in caplog.text and "slow=timeout" in caplog.text


# Cursor-commit semantics: batch_query's `explicit_window` gate + the poller's
# post-callback commit prevent silent data loss on wait_for cancel / publish raise
# and fix the kvstore-key mismatch between the poller's read and write paths.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, expect_commit",
    [({"start": datetime.now() - timedelta(seconds=10), "end": datetime.now()}, False), ({}, True)],
    ids=["explicit-window-skips", "cursor-driven-commits"],
)
async def test_batch_query_cursor_commit_gated_by_explicit_window(loki_client, kwargs, expect_commit):
    """Bounded-window callers (LogQueryPoller) own the commit; legacy cursor-driven callers don't."""
    loki_client.query_records = AsyncMock(return_value=([], None))
    spy = MagicMock(wraps=loki_client._store_query_last_execution_time)
    loki_client._store_query_last_execution_time = spy
    await loki_client.batch_query(query="q", query_id="q", **kwargs)
    assert spy.called is expect_commit


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "callback, expect_committed",
    [
        (AsyncMock(), True),
        (AsyncMock(side_effect=RuntimeError("publish failed")), False),
        (AsyncMock(side_effect=asyncio.CancelledError()), False),
    ],
    ids=["success-commits", "publish-failure-skips", "wait_for-cancel-skips"],
)
async def test_poller_commits_cursor_only_after_chunk_publish_success(poller_setup, callback, expect_committed):
    """Success commits at the same kvstore key the poller reads (proves key alignment);
    any post-fetch raise — publish error or wait_for CancelledError — leaves it unchanged."""
    loki_client, LogQueryPoller = poller_setup
    del loki_client._get_query_last_execution_time  # restore real reader (fixture mocked it)
    loki_client.max_lookback, loki_client.query_delay = 5, 0
    poller = LogQueryPoller(loki_client, callback, [{"name": "q"}], chunk_size_secs=10)
    try:
        await poller._process_query_chunks({"name": "q"}, "q")
    except asyncio.CancelledError:
        pass  # bypasses `except Exception:` by design
    identity = loki_client.cursor_query_identity({"name": "q"}, "q")
    committed = loki_client._get_query_last_execution_time(query=identity, query_id="q") is not None
    assert committed is expect_committed


@pytest.mark.asyncio
async def test_batch_query_caps_oversized_window(loki_client):
    """HTTP_MAX_QUERY_WINDOW_SECS fast-forwards start; LOG_QUERY_WINDOW_CAPPED counts the skipped seconds."""
    loki_client.query_records = AsyncMock(return_value=([], None))
    end = datetime.now(UTC)
    before = LOG_QUERY_WINDOW_CAPPED.labels(query="q")._value.get()
    await loki_client.batch_query(
        query="q", query_id="q", start=end - timedelta(seconds=3 * HTTP_MAX_QUERY_WINDOW_SECS), end=end
    )
    passed_start = loki_client.query_records.call_args.kwargs["start"]
    assert (end - passed_start).total_seconds() == pytest.approx(HTTP_MAX_QUERY_WINDOW_SECS)
    assert LOG_QUERY_WINDOW_CAPPED.labels(query="q")._value.get() - before == pytest.approx(
        2 * HTTP_MAX_QUERY_WINDOW_SECS
    )


# Label Remapping Tests


@pytest.fixture
def loki_client_with_remapper():
    """LogQueryBatcher with label remapper configured."""
    remapper = LabelRemapper(
        simple={"job_id": "slurm_job_id", "uuid": "slurm_uuid"},
        composite=[
            {"attribute": "log.file.path", "template": "/log/{job_name}/{job_id}", "labels": ["job_name", "job_id"]}
        ],
    )
    return LogQueryBatcher(
        backend_factory=fake_backend,
        log_query_config=log_query_config(BackendName.LOKI),
        state_store={},
        max_lookback=60,
        label_remapper=remapper,
    )


def test_parse_records_with_label_remapping(loki_client_with_remapper):
    """Test label remapping applies correctly and takes precedence."""
    response = [
        {
            "stream": {
                "slurm_job_id": "999",
                "slurm_uuid": "abc-123",
                "job_name": "train",
                "job_id": "888",
                "log.file.name": "test.log",
            },
            "values": [["ts1", "log message"]],
        }
    ]
    result = loki_client_with_remapper._parse_legacy_loki_response(response)
    assert len(result) == 1
    attrs = result[0]["attributes"]
    assert attrs["job_id"] == "999"  # remapped from slurm_job_id (takes precedence)
    assert attrs["uuid"] == "abc-123"  # remapped from slurm_uuid
    assert attrs["log.file.path"] == "/log/train/888"  # composite template


def test_parse_records_without_remapper(loki_client):
    """Test no remapping when remapper is None (backward compatibility)."""
    response = [
        {
            "stream": {"slurm_job_id": "999", "job_id": "original", "log.file.name": "test.log"},
            "values": [["ts1", "log message"]],
        }
    ]
    result = loki_client._parse_legacy_loki_response(response)
    assert result[0]["attributes"]["job_id"] == "original"  # unchanged


@pytest.mark.parametrize(
    "value_json, expected_body",
    [
        ('{"message": "segfault"}', "segfault"),
        ('{"body": "existing", "message": "ignored"}', "existing"),
        ('{"body": "kept"}', "kept"),
    ],
    ids=["message-only", "body-takes-precedence", "body-without-message"],
)
def test_parse_json_body_normalization(loki_client, value_json, expected_body):
    """JSON events: 'message' is promoted to 'body' only when 'body' is absent."""
    stream = {"job_id": "1", "log.file.name": "f.log"}
    response = [{"stream": stream, "values": [["ts1", value_json]]}]
    result = loki_client._parse_legacy_loki_response(response)
    assert result[0]["body"] == expected_body


def test_parse_json_without_attributes_uses_stream_labels(loki_client):
    """JSON events without 'attributes' get attributes initialized from stream labels."""
    response = [
        {
            "stream": {"job_id": "42", "log.file.name": "out.log", "cluster": "c1"},
            "values": [["ts1", '{"message": "hello"}']],
        }
    ]
    result = loki_client._parse_legacy_loki_response(response)
    assert result[0]["attributes"]["job_id"] == "42"
    assert result[0]["attributes"]["cluster"] == "c1"


def test_parse_records_deduplicates_across_streams(loki_client):
    """Same log line ingested by multiple OTel collector replicas into separate streams."""
    body = '{"body": "training step 100", "attributes": {"job_id": "999", "log.file.name": "train.log"}}'
    response = [{"stream": {"collector": f"replica-{i}"}, "values": [["1775845243419012597", body]]} for i in range(5)]
    result = loki_client._parse_legacy_loki_response(response)
    assert len(result) == 1
    assert result[0]["body"] == "training step 100"
