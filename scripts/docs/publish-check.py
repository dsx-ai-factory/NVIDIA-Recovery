#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Refuse publication until the maintainer supplies real hosting configuration."""

import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import yaml


def main() -> None:
    """Check the allocation and trusted ref without using a token or publishing."""
    root = Path(__file__).resolve().parents[2]
    config = json.loads((root / "fern/fern.config.json").read_text())
    docs = yaml.safe_load((root / "fern/docs.yml").read_text())
    if not config.get("organization") or config["organization"] == "pending-allocation":
        raise SystemExit("Blocked: Fern organization allocation is pending")
    if len(docs.get("instances", [])) != 1:
        raise SystemExit("Blocked: configure exactly one approved Fern instance")
    instance = docs["instances"][0]
    if not instance.get("url"):
        raise SystemExit("Blocked: assigned Fern instance URL is missing")
    url = urlsplit(os.environ.get("DOCS_NVIDIA_URL", ""))
    if url.scheme != "https" or url.netloc != "docs.nvidia.com" or url.path in ("", "/"):
        raise SystemExit("Blocked: approved docs.nvidia.com HTTPS subpath is pending")
    custom_domain = urlsplit("https://" + str(instance.get("custom-domain", "")).removeprefix("https://"))
    origin = urlsplit("https://" + str(instance["url"]).removeprefix("https://"))
    if custom_domain.netloc != url.netloc or custom_domain.path.rstrip("/") != url.path.rstrip("/"):
        raise SystemExit("Blocked: custom-domain must match the approved docs.nvidia.com subpath")
    if (
        not origin.hostname
        or not origin.hostname.endswith(".docs.buildwithfern.com")
        or origin.path != custom_domain.path
    ):
        raise SystemExit("Blocked: assigned Fern origin and custom-domain must share a subpath")
    if os.environ.get("GITHUB_REF") != "refs/heads/main" or os.environ.get("GITHUB_REF_PROTECTED") != "true":
        raise SystemExit("Blocked: publication requires protected main")
    print("Hosting inputs and protected main confirmed; environment approval is a separate GitHub control")


if __name__ == "__main__":
    main()
