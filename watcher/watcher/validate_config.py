# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate a watcher configuration file without starting the service."""

import argparse
import os
import sys

import yaml

from watcher.lib.utils.config import load_watcher_config


def main(argv: list[str] | None = None) -> int:
    """Load the config and rules the way the service would; report the first problem and return 1 on failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config",
        nargs="?",
        default=os.environ.get("CONFIG_PATH"),
        help="watcher config YAML (default: $CONFIG_PATH)",
    )
    parser.add_argument(
        "--rules",
        default=os.environ.get("RULES_PATH"),
        help="standalone rules YAML (default: $RULES_PATH)",
    )
    args = parser.parse_args(argv)
    if args.config is None:
        parser.error("a config path is required when CONFIG_PATH is not set")

    try:
        load_watcher_config(args.config, args.rules)
    except (OSError, TypeError, ValueError, yaml.YAMLError) as error:
        sys.stderr.write(f"{args.config}: invalid watcher config\n{error}\n")
        return 1
    sys.stdout.write(f"{args.config}: valid watcher config\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
