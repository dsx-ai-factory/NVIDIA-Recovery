# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from watcher.validate_config import main


@pytest.fixture(autouse=True)
def _no_service_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    monkeypatch.delenv("RULES_PATH", raising=False)


def test_accepts_a_config_the_service_can_load(watcher_config_path: Path):
    assert main([str(watcher_config_path)]) == 0


def test_defaults_to_the_files_the_service_reads(
    watcher_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    rules = tmp_path / "rules.yaml"
    rules.write_text("- name: group1\n  workflow: []\n- name: group2\n  workflow: []\n")
    monkeypatch.setenv("CONFIG_PATH", str(watcher_config_path))
    assert main([]) == 0

    monkeypatch.setenv("RULES_PATH", str(rules))
    assert main([]) == 1


def test_requires_a_config_when_none_is_configured():
    with pytest.raises(SystemExit) as exit_info:
        main([])

    assert exit_info.value.code == 2


def test_rejects_a_config_with_an_unknown_event_source(watcher_config_path: Path, tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text(watcher_config_path.read_text().replace("event_source: log_query", "event_source: bogus"))

    assert main([str(config)]) == 1


def test_rejects_a_config_that_is_not_a_mapping(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text("- not\n- a mapping\n")

    assert main([str(config)]) == 1


def test_validates_the_rules_file_the_service_would_use(watcher_config_path: Path, tmp_path: Path):
    rules = tmp_path / "rules.yaml"
    rules.write_text("- name: group1\n  workflow: []\n- name: group2\n  workflow: []\n")

    assert main([str(watcher_config_path), "--rules", str(rules)]) == 1
