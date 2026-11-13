# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cluster-free checks for portable Grafana dashboard publication."""

import json
import sys

import pytest
import update_dashboard


def test_dry_run_needs_no_deployment_target_or_token(tmp_path, monkeypatch, capsys) -> None:
    dashboard = tmp_path / "dashboard.json"
    dashboard.write_text(json.dumps({"title": "Recovery", "uid": "recovery"}))
    config = tmp_path / "config.yaml"
    config.write_text('grafana:\n  url: ""\n')
    monkeypatch.delenv("GRAFANA_TOKEN", raising=False)
    monkeypatch.delenv("GRAFANA_URL", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["update_dashboard.py", "--file", str(dashboard), "--config", str(config), "--dry-run"],
    )

    update_dashboard.main()

    assert "<not required for dry run>" in capsys.readouterr().out


def test_invalid_operator_org_id_is_rejected(tmp_path, monkeypatch) -> None:
    dashboard = tmp_path / "dashboard.json"
    dashboard.write_text(json.dumps({"title": "Recovery", "uid": "recovery"}))
    config = tmp_path / "config.yaml"
    config.write_text("{}\n")
    monkeypatch.setenv("GRAFANA_ORG_ID", "not-an-integer")
    monkeypatch.setattr(
        sys,
        "argv",
        ["update_dashboard.py", "--file", str(dashboard), "--config", str(config), "--dry-run"],
    )

    with pytest.raises(SystemExit, match="1"):
        update_dashboard.main()
