# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util

import pytest

# These tests import attribution_adapter (cross-service HTTP round-trip + the golden
# byte-identity test that composes the adapter and Policy builders). The per-folder CI
# `test` job for anomaly_detection_isolator installs only this package, so skip collecting
# them when attribution_adapter isn't importable. No CI job currently installs the full
# workspace, so they run only locally.
if importlib.util.find_spec("attribution_adapter") is None:
    collect_ignore_glob = ["integration/*"]
    collect_ignore = ["test_golden_message.py"]


@pytest.fixture(autouse=True)
def _strip_proxy_env(monkeypatch):
    """Remove proxy env vars so httpx doesn't try to use a SOCKS transport."""
    for var in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(var, raising=False)
