# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Log-query test doubles shared by batcher and consumer tests."""

from unittest.mock import AsyncMock, MagicMock

from recovery_libs.log_query import BackendName
from watcher.models.config import LogQueryConfig

_DIALECTS = {BackendName.LOKI: "logql", BackendName.VICTORIALOGS: "logsql"}
_URLS = {BackendName.LOKI: "http://loki:3100", BackendName.VICTORIALOGS: "http://victorialogs:9428"}


def fake_backend(backend: BackendName = BackendName.LOKI) -> MagicMock:
    """A log-query backend whose identity is real and whose queries a test scripts on ``query_page``."""
    fake = MagicMock()
    fake.name = backend
    fake.dialect = _DIALECTS[backend]
    fake.query_page = AsyncMock()
    fake.close = AsyncMock()
    return fake


def log_query_config(backend: BackendName = BackendName.LOKI) -> LogQueryConfig:
    """Config selecting ``backend``, with a provider-specific prefix and ``crash`` workflow query."""
    return LogQueryConfig.from_json(
        {
            "backend": backend,
            "providers": {
                backend: {
                    "url": _URLS[backend],
                    "queries": {
                        "query_prefix": f"{backend}-prefix",
                        "workflows": {"crash": {"query": [f"{backend}-query"]}},
                    },
                }
            },
        }
    )
