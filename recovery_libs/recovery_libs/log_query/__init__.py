# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Replaceable, in-process log-query backend contract and built-in drivers."""

from recovery_libs.log_query.config import LokiBackendConfig, VictoriaLogsBackendConfig
from recovery_libs.log_query.errors import QueryFailure, QueryFailureKind
from recovery_libs.log_query.factory import STATIC_BACKENDS, BackendConfig, BackendMode, create_log_query_backend
from recovery_libs.log_query.loki import AsyncLokiBackend, LokiBackend, LokiQueryStats
from recovery_libs.log_query.models import (
    AsyncLogQueryBackend,
    BackendName,
    CertificateExpiryReporter,
    LogRecord,
    NativeQuery,
    QueryDialect,
    QueryOrder,
    QueryPage,
    QueryRequest,
    QueryStats,
    SyncLogQueryBackend,
)
from recovery_libs.log_query.victorialogs import AsyncVictoriaLogsBackend, VictoriaLogsBackend

__all__ = [
    "STATIC_BACKENDS",
    "AsyncLogQueryBackend",
    "AsyncLokiBackend",
    "AsyncVictoriaLogsBackend",
    "BackendConfig",
    "BackendMode",
    "BackendName",
    "CertificateExpiryReporter",
    "LogRecord",
    "LokiBackend",
    "LokiBackendConfig",
    "LokiQueryStats",
    "NativeQuery",
    "QueryDialect",
    "QueryFailure",
    "QueryFailureKind",
    "QueryOrder",
    "QueryPage",
    "QueryRequest",
    "QueryStats",
    "SyncLogQueryBackend",
    "VictoriaLogsBackend",
    "VictoriaLogsBackendConfig",
    "create_log_query_backend",
]
