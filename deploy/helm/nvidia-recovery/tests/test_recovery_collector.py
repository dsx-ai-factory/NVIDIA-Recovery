# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Render the actual collector template to verify independent signal routing.

The isolated chart avoids downloading unrelated database and UI dependencies;
Helm still validates the real template and its public values. These assertions
catch traces accidentally entering log filters or disabled exporters, and
credentials accidentally being embedded instead of resolved at runtime.
"""

import copy
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1]


@pytest.fixture
def collector_chart(tmp_path):
    if shutil.which("helm") is None:
        pytest.skip("Helm is required to render collector configuration")
    (tmp_path / "templates").mkdir()
    shutil.copy(CHART / "templates/otelcol-configmap.yaml", tmp_path / "templates")
    (tmp_path / "Chart.yaml").write_text("apiVersion: v2\nname: collector-test\nversion: 0.1.0\n")
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    collector = copy.deepcopy(values["opentelemetry-collector"])
    collector["enabled"] = True
    collector["image"] = {"repository": "collector.example.test/qualified", "tag": "test"}
    collector["config"]["receivers"]["otlp"]["enabled"] = True
    collector["config"]["exporters"]["traces"].update(enabled=True, endpoint="trace.example.test:4317")
    return tmp_path, collector


def render(chart, collector):
    (chart / "values.yaml").write_text(yaml.safe_dump({"opentelemetry-collector": collector}))
    return subprocess.run(["helm", "template", "test", str(chart)], capture_output=True, text=True, check=False)


def config(result):
    assert result.returncode == 0, result.stderr
    resources = list(yaml.safe_load_all(result.stdout))
    return yaml.safe_load(resources[0]["data"]["nvidia-recovery-otelcol-config.yaml"])


def test_trace_only_relay_has_no_log_filters_or_exporters(collector_chart):
    chart, collector = collector_chart
    rendered = config(render(chart, collector))
    assert rendered["service"]["pipelines"] == {
        "traces/recovery": {"receivers": ["otlp"], "processors": ["batch"], "exporters": ["otlp/recovery_traces"]}
    }
    assert set(rendered["receivers"]["otlp"]["protocols"]) == {"grpc", "http"}
    assert rendered["exporters"]["otlp/recovery_traces"]["tls"]["insecure"] is False


def test_combined_signals_keep_routes_and_auth_independent(collector_chart):
    chart, collector = collector_chart
    receiver = collector["config"]["receivers"]
    receiver["filelog"].update(enabled=True, monitoring_path="/logs/*.log", file_offsets_path="/offsets")
    receiver["otlp"].update(auth=True, auth_token_environment_variable="RECEIVER_TOKEN")
    collector["config"]["event_bus"]["watcher"]["enabled"] = True
    collector["config"]["exporters"]["traces"]["auth"].update(enabled=True, environmentVariable="TRACE_HEADER")
    rendered = config(render(chart, collector))
    pipelines = rendered["service"]["pipelines"]
    assert set(pipelines) == {"traces/recovery", "logs/filelog-watcher", "logs/otlp-watcher"}
    assert pipelines["traces/recovery"]["processors"] == ["batch"]
    assert pipelines["logs/filelog-watcher"]["receivers"] == ["filelog"]
    assert pipelines["logs/otlp-watcher"]["exporters"] == ["rabbitmq/watcher"]
    assert rendered["exporters"]["otlp/recovery_traces"]["headers"] == {"Authorization": "${env:TRACE_HEADER}"}
    assert rendered["extensions"]["bearertokenauth"]["token"] == "${env:RECEIVER_TOKEN}"
    assert set(rendered["service"]["extensions"]) == {"health_check", "bearertokenauth", "file_storage/filelogreceiver"}


def test_disabling_traces_preserves_logs(collector_chart):
    chart, collector = collector_chart
    collector["config"]["exporters"]["traces"]["enabled"] = False
    collector["config"]["event_bus"]["watcher"]["enabled"] = True
    rendered = config(render(chart, collector))
    assert set(rendered["service"]["pipelines"]) == {"logs/otlp-watcher"}
    assert "otlp/recovery_traces" not in rendered["exporters"]


@pytest.mark.parametrize("missing", ["receiver", "endpoint"])
def test_incomplete_trace_relay_is_rejected(collector_chart, missing):
    chart, collector = collector_chart
    if missing == "receiver":
        collector["config"]["receivers"]["otlp"]["enabled"] = False
    else:
        collector["config"]["exporters"]["traces"]["endpoint"] = ""
    assert render(chart, collector).returncode != 0
