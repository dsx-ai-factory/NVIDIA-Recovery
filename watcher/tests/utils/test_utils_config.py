# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from watcher.lib.utils import config as utils_config
from watcher.models.config import WatcherConfig


def test_load_config_success(watcher_config_path: Path):
    cfg = utils_config.load_typed_config(watcher_config_path)
    assert isinstance(cfg, WatcherConfig)
    # Check a few keys local.yaml config
    assert cfg.datasources.log_query_policy is not None
    assert cfg.event_source.value in {"log_query", "loki", "rabbitmq"}
    assert isinstance(cfg.rules, list)
    assert len(cfg.rules) > 0
    assert hasattr(cfg.rules[0], "workflow")


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        utils_config.load_typed_config("/tmp/this_file_does_not_exist.yaml")


def test_load_rules_rejects_empty_file(tmp_path: Path):
    empty_rules = tmp_path / "rules.yaml"
    empty_rules.write_text("[]\n")
    with pytest.raises(ValueError, match="must contain exactly one rule group"):
        utils_config.load_rules(empty_rules)


def test_load_rules_rejects_multiple_rule_groups(tmp_path: Path):
    multi_group_rules = tmp_path / "rules.yaml"
    multi_group_rules.write_text("- name: group1\n  workflow: []\n- name: group2\n  workflow: []\n")
    with pytest.raises(ValueError, match="must contain exactly one rule group"):
        utils_config.load_rules(multi_group_rules)


def test_load_default_rules_returns_bundled_ruleset():
    rules = utils_config.load_default_rules()
    assert len(rules) == 1
    assert hasattr(rules[0], "workflow")
    assert len(rules[0].workflow) > 0


def test_load_rules_rejects_nested_duplicate_keys(tmp_path: Path):
    duplicate_key_rules = tmp_path / "rules.yaml"
    duplicate_key_rules.write_text(
        "- name: group1\n"
        "  workflow:\n"
        "    - name: straggler\n"
        "      type: loki_query\n"
        "      attributes:\n"
        "        cache_key: a\n"
        "        cache_key: b\n"
    )
    with pytest.raises(Exception, match="duplicate key"):
        utils_config.load_rules(duplicate_key_rules)
