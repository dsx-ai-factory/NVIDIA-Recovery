# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from dataclasses import FrozenInstanceError

import pytest
from recovery_libs.log_query import (
    AsyncLokiBackend,
    BackendName,
    LogRecord,
    LokiBackend,
    LokiBackendConfig,
    NativeQuery,
    QueryDialect,
    QueryFailure,
    QueryFailureKind,
    QueryRequest,
    QueryStats,
    VictoriaLogsBackendConfig,
    create_log_query_backend,
)


def test_native_query_revision_is_stable_across_enum_and_string_dialects() -> None:
    """Durable cursor keys embed this revision, so the dialect enum must not change it."""
    expected = "3cf0fe2d1aa8aeca6f589bc663120300123aab8c891e81f33225f0cad4da8d6c"

    assert NativeQuery("q", QueryDialect.LOGQL, '{app="watcher"}').revision == expected
    assert NativeQuery("q", "logql", '{app="watcher"}').revision == expected  # type: ignore[arg-type]


def test_contract_values_are_immutable() -> None:
    query = NativeQuery("job-logs", "logql", '{job_id="7"}')
    record = LogRecord(42, " body\n", {"job_id": "7"})
    stats = QueryStats(0.1, 1, 12, 1, {"summary": {"returned": 1}})

    assert query.revision is not None
    assert query.revision == NativeQuery("other-id", "logql", '{job_id="7"}').revision
    with pytest.raises(FrozenInstanceError):
        query.text = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        record.fields["job_id"] = "8"  # type: ignore[index]
    with pytest.raises(TypeError):
        stats.provider["new"] = 1  # type: ignore[index]
    with pytest.raises(TypeError):
        stats.provider["summary"]["returned"] = 2  # type: ignore[index]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"start_ns": 2, "end_ns": 2}, "start_ns < end_ns"),
        ({"start_ns": 1, "end_ns": 2, "page_size": 0}, "page_size"),
        ({"start_ns": 1, "end_ns": 2, "order": "sideways"}, "order"),
        ({"start_ns": 1, "end_ns": 2, "timeout_seconds": 0}, "timeout"),
    ],
)
def test_query_request_validation(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        QueryRequest(query=NativeQuery("q", "logql", "{}"), **kwargs)  # type: ignore[arg-type]


def test_provider_config_validation() -> None:
    with pytest.raises(ValueError, match="configured together"):
        LokiBackendConfig("https://loki.example", cert_file="client.crt")
    with pytest.raises(ValueError, match="connect_timeout_seconds"):
        LokiBackendConfig("https://loki.example", connect_timeout_seconds=0)
    with pytest.raises(ValueError, match="account_id and project_id"):
        VictoriaLogsBackendConfig("https://logs.example", account_id=1)
    with pytest.raises(ValueError, match="required"):
        VictoriaLogsBackendConfig("https://logs.example", require_tenant=True)
    with pytest.raises(ValueError, match="unsigned 32-bit"):
        VictoriaLogsBackendConfig("https://logs.example", account_id=2**32, project_id=0)
    with pytest.raises(ValueError, match="unsigned 32-bit"):
        VictoriaLogsBackendConfig("https://logs.example", account_id=True, project_id=0)
    with pytest.raises(ValueError, match="mutually exclusive"):
        VictoriaLogsBackendConfig(
            "https://logs.example",
            username="user",
            password="password",
            bearer_token="token",
        )
    with pytest.raises(ValueError, match="connect_timeout_seconds"):
        VictoriaLogsBackendConfig("https://logs.example", connect_timeout_seconds=float("nan"))


def test_static_factory_selects_mode_and_rejects_mismatched_config() -> None:
    config = LokiBackendConfig("http://loki.example")
    sync_backend = create_log_query_backend(BackendName.LOKI, config)
    async_backend = create_log_query_backend(BackendName.LOKI, config, mode="async")
    try:
        assert isinstance(sync_backend, LokiBackend)
        assert isinstance(async_backend, AsyncLokiBackend)
    finally:
        sync_backend.close()
        asyncio.run(async_backend.close())

    with pytest.raises(QueryFailure) as unknown:
        create_log_query_backend("unknown", config)
    assert unknown.value.kind is QueryFailureKind.INVALID_REQUEST

    with pytest.raises(QueryFailure, match="VictoriaLogsBackendConfig"):
        create_log_query_backend(BackendName.VICTORIALOGS, config)
