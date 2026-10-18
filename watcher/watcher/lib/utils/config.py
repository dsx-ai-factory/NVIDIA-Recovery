# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from importlib import resources
from pathlib import Path

import yaml

from watcher.models.config import RuleConfig, WatcherConfig


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader variant that rejects duplicate mapping keys, including nested ones."""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep)


def _safe_load_no_duplicates(stream):
    return yaml.load(stream, Loader=_UniqueKeyLoader)


def load_raw_config(path: str | Path) -> dict:
    """Parse a watcher config YAML file into a raw dict, without constructing WatcherConfig."""
    with open(path, "r") as f:
        raw = _safe_load_no_duplicates(f)

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise TypeError(f"Watcher config '{path}' must be a YAML mapping, got {type(raw).__name__}")
    return raw


def load_typed_config(path: str | Path) -> WatcherConfig:
    """
    Load a watcher config YAML file into a WatcherConfig dataclass.
    Handles nested structures and enums using the from_json methods on the data classes.

    Args:
        path (str | Path): Path to the YAML config file.

    Returns:
        WatcherConfig: The loaded configuration as a WatcherConfig dataclass instance.
    """
    return WatcherConfig.from_json(load_raw_config(path))


def load_watcher_config(config_path: str | Path, rules_path: str | Path | None = None) -> WatcherConfig:
    """Load the watcher service configuration, resolving its rules the way the service does.

    Rules come from `rules_path`, then the config's own `rules_path`, then inline `rules`, and finally the
    ruleset bundled with the package.
    """
    raw_config = load_raw_config(config_path)
    resolved_rules_path = rules_path or raw_config.get("rules_path")
    if resolved_rules_path:
        raw_config["rules"] = load_rules(resolved_rules_path)
    elif not raw_config.get("rules"):
        raw_config["rules"] = load_default_rules()
    return WatcherConfig.from_json(raw_config)


def load_rules(path: str | Path) -> list[RuleConfig]:
    """Load rules from a standalone rules YAML file (List[RuleConfig] format)."""
    with open(path, "r") as f:
        raw = _safe_load_no_duplicates(f)

    if not raw or len(raw) != 1:
        raise ValueError(
            f"Rules file '{path}' must contain exactly one rule group: "
            "main.py only ever reads rules[0], so additional groups would be silently ignored"
        )

    return [RuleConfig.from_json(r) for r in raw]


def load_default_rules() -> list[RuleConfig]:
    """Load the ruleset bundled with the watcher package (watcher/rules/default.yaml)."""
    raw = _safe_load_no_duplicates(resources.files("watcher.rules").joinpath("default.yaml").read_text())

    if not raw or len(raw) != 1:
        raise ValueError(
            "Bundled default rules file must contain exactly one rule group: "
            "main.py only ever reads rules[0], so additional groups would be silently ignored"
        )

    return [RuleConfig.from_json(r) for r in raw]
