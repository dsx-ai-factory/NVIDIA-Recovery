# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Watcher's log-query plugin: backend construction, batched queries, and the polling loop."""

from watcher.lib.plugins.log_query.backends import (
    LokiBackendFactory,
    VictoriaLogsBackendFactory,
    create_log_query_client,
)
from watcher.lib.plugins.log_query.batcher import LogQueryBatcher
from watcher.lib.plugins.log_query.poller import LogQueryPoller

__all__ = [
    "LogQueryBatcher",
    "LogQueryPoller",
    "LokiBackendFactory",
    "VictoriaLogsBackendFactory",
    "create_log_query_client",
]
