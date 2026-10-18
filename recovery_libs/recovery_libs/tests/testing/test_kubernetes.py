# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from recovery_libs.testing.kubernetes import port_forward


def fake_kubectl(directory: Path, output: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Put a `kubectl` on PATH that prints `output` and then keeps running, like a forward that stays open."""

    script = directory / "kubectl"
    script.write_text(f"#!/bin/sh\nprintf '%s' '{output}'\nexec sleep 30\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")


def test_forward_yields_the_local_port_kubectl_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_kubectl(tmp_path, "Forwarding from 127.0.0.1:43210 -> 8080\n", monkeypatch)

    with port_forward(namespace="ns", service="job-manager", port=8080, timeout=5) as url:
        assert url == "http://127.0.0.1:43210"


def test_a_silent_kubectl_cannot_outlast_the_startup_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """kubectl can stall without printing, for example when the API server is unreachable."""

    fake_kubectl(tmp_path, "", monkeypatch)
    started = time.monotonic()

    with (
        pytest.raises(RuntimeError, match="did not start"),
        port_forward(namespace="ns", service="job-manager", port=8080, timeout=0.5),
    ):
        pass

    assert time.monotonic() - started < 5
