# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Static construction of built-in log-query backends."""

from __future__ import annotations

from types import MappingProxyType
from typing import Literal, overload

from recovery_libs.log_query.config import LokiBackendConfig, VictoriaLogsBackendConfig
from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind
from recovery_libs.log_query.loki import AsyncLokiBackend, LokiBackend
from recovery_libs.log_query.models import AsyncLogQueryBackend, BackendName, SyncLogQueryBackend
from recovery_libs.log_query.victorialogs import AsyncVictoriaLogsBackend, VictoriaLogsBackend

BackendConfig = LokiBackendConfig | VictoriaLogsBackendConfig
BackendMode = Literal["sync", "async"]

STATIC_BACKENDS = MappingProxyType(
    {
        BackendName.LOKI: (LokiBackendConfig, LokiBackend, AsyncLokiBackend),
        BackendName.VICTORIALOGS: (VictoriaLogsBackendConfig, VictoriaLogsBackend, AsyncVictoriaLogsBackend),
    }
)


@overload
def create_log_query_backend(
    backend_name: str,
    config: BackendConfig,
    *,
    mode: Literal["sync"] = "sync",
) -> SyncLogQueryBackend: ...


@overload
def create_log_query_backend(
    backend_name: str,
    config: BackendConfig,
    *,
    mode: Literal["async"],
) -> AsyncLogQueryBackend: ...


def create_log_query_backend(
    backend_name: str,
    config: BackendConfig,
    *,
    mode: BackendMode = "sync",
) -> SyncLogQueryBackend | AsyncLogQueryBackend:
    """Build one of the fixed built-in drivers after validating its config type."""
    try:
        registration = STATIC_BACKENDS[BackendName(backend_name)]
    except ValueError:
        raise QueryFailure(
            QueryFailureKind.INVALID_REQUEST,
            backend=backend_name,
            query_id="<configuration>",
            message="unknown log-query backend",
            retryable=False,
        ) from None
    if mode not in ("sync", "async"):
        raise QueryFailure(
            QueryFailureKind.INVALID_REQUEST,
            backend=backend_name,
            query_id="<configuration>",
            message="backend mode must be 'sync' or 'async'",
            retryable=False,
        )
    config_type, sync_type, async_type = registration
    if not isinstance(config, config_type):
        raise QueryFailure(
            QueryFailureKind.INVALID_REQUEST,
            backend=backend_name,
            query_id="<configuration>",
            message=f"backend requires {config_type.__name__}",
            retryable=False,
        )
    return sync_type(config) if mode == "sync" else async_type(config)
