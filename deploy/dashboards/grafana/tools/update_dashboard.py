#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Push local Grafana dashboard JSON files to a Grafana instance via the HTTP API."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.yaml"
TOKEN_ENV_VAR = "GRAFANA_TOKEN"
URL_ENV_VAR = "GRAFANA_URL"
ORG_ID_ENV_VAR = "GRAFANA_ORG_ID"
FOLDER_UID_ENV_VAR = "GRAFANA_FOLDER_UID"


def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise ValueError("dashboard configuration must be a YAML mapping")
    return config


def get_token() -> str:
    token = os.environ.get(TOKEN_ENV_VAR)
    if not token:
        print(f"Error: environment variable ${TOKEN_ENV_VAR} is not set.", file=sys.stderr)
        sys.exit(1)
    return token


def build_headers(token: str, org_id: Optional[int] = None) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if org_id:
        headers["X-Grafana-Org-Id"] = str(org_id)
    return headers


def collect_dashboard_files(path: Path) -> List[Path]:
    """Return a sorted list of .json dashboard files, skipping Datadog exports (dd-* prefix)."""
    files = sorted(f for f in path.glob("*.json") if not f.name.startswith("dd-"))
    if not files:
        print(f"No dashboard JSON files found in {path}", file=sys.stderr)
        sys.exit(1)
    return files


def push_dashboard(
    grafana_url: str,
    headers: Dict[str, str],
    dashboard_json: Dict[str, Any],
    folder_uid: str,
    overwrite: bool,
    message: str,
) -> Dict[str, Any]:
    """POST a dashboard to Grafana and return the response body."""
    dashboard_json["id"] = None

    payload = {
        "dashboard": dashboard_json,
        "folderUid": folder_uid,
        "overwrite": overwrite,
        "message": message,
    }

    url = f"{grafana_url.rstrip('/')}/api/dashboards/db"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def process_file(
    filepath: Path,
    grafana_url: str,
    headers: Dict[str, str],
    folder_uid: str,
    overwrite: bool,
    message: str,
    dry_run: bool,
) -> bool:
    """Load a single JSON file and push it. Returns True on success."""
    try:
        with open(filepath) as f:
            dashboard = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  SKIP  {filepath.name}  ({exc})", file=sys.stderr)
        return False

    title = dashboard.get("title", filepath.stem)
    uid = dashboard.get("uid", "")

    if not uid:
        print(
            f"  SKIP  {filepath.name}  title={title!r}  (missing or empty uid — would create duplicates)",
            file=sys.stderr,
        )
        return False

    if dry_run:
        print(f"  DRY   {filepath.name}  title={title!r}  uid={uid}")
        return True

    try:
        result = push_dashboard(grafana_url, headers, dashboard, folder_uid, overwrite, message)
        status = result.get("status", "unknown")
        version = result.get("version", "?")
        dash_url = result.get("url", "")
        print(f"  OK    {filepath.name}  status={status}  version={version}  url={grafana_url}{dash_url}")
        return True
    except requests.HTTPError as exc:
        body = exc.response.text if exc.response is not None else ""
        print(f"  FAIL  {filepath.name}  title={title!r}  {exc}  {body}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push Grafana dashboard JSON files to a Grafana instance.",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "-f",
        "--file",
        type=Path,
        help="Path to a single dashboard JSON file to push.",
    )
    source.add_argument(
        "-d",
        "--dir",
        type=Path,
        help="Path to a directory containing dashboard JSON files. All *.json files (excluding dd-* prefixed) will be pushed.",
    )

    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config file (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "-m",
        "--message",
        type=str,
        default=None,
        help="Commit message for the dashboard version history.",
    )
    parser.add_argument(
        "--folder-uid",
        type=str,
        default=None,
        help="Override the target folder UID from config.",
    )
    parser.add_argument(
        "--grafana-url",
        type=str,
        default=None,
        help=f"Grafana base URL (overrides ${URL_ENV_VAR} and config).",
    )
    parser.add_argument(
        "--org-id",
        type=int,
        default=None,
        help=f"Grafana organization ID (overrides ${ORG_ID_ENV_VAR} and config).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Fail instead of overwriting if a dashboard with the same uid/title exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and list dashboards without pushing.",
    )

    args = parser.parse_args()

    cfg = load_config(args.config)
    grafana_cfg = cfg.get("grafana", {})
    defaults = cfg.get("defaults", {})

    grafana_url = args.grafana_url or os.environ.get(URL_ENV_VAR) or grafana_cfg.get("url", "")
    raw_org_id = args.org_id or os.environ.get(ORG_ID_ENV_VAR) or grafana_cfg.get("org_id")
    try:
        org_id = int(raw_org_id) if raw_org_id not in (None, "") else None
    except (TypeError, ValueError):
        print(f"Error: {ORG_ID_ENV_VAR} or grafana.org_id must be an integer.", file=sys.stderr)
        sys.exit(1)
    folder_uid = (
        args.folder_uid
        if args.folder_uid is not None
        else os.environ.get(FOLDER_UID_ENV_VAR, grafana_cfg.get("folder_uid", ""))
    )
    overwrite = not args.no_overwrite and defaults.get("overwrite", True)
    message = args.message or defaults.get("message", "Dashboard update")

    if args.file:
        if not args.file.is_file():
            print(f"Error: {args.file} is not a file.", file=sys.stderr)
            sys.exit(1)
        files = [args.file]
    else:
        if not args.dir.is_dir():
            print(f"Error: {args.dir} is not a directory.", file=sys.stderr)
            sys.exit(1)
        files = collect_dashboard_files(args.dir)

    if not args.dry_run and not grafana_url:
        print(
            f"Error: set --grafana-url, ${URL_ENV_VAR}, or grafana.url in config.",
            file=sys.stderr,
        )
        sys.exit(1)

    headers = build_headers(get_token(), org_id) if not args.dry_run else {}

    print(f"Grafana:   {grafana_url or '<not required for dry run>'}")
    print(f"Org ID:    {org_id or 'default'}")
    print(f"Folder:    {folder_uid or 'General'}")
    print(f"Overwrite: {overwrite}")
    print(f"Files:     {len(files)}")
    print()

    ok = 0
    fail = 0
    for fp in files:
        if process_file(fp, grafana_url, headers, folder_uid, overwrite, message, args.dry_run):
            ok += 1
        else:
            fail += 1

    print(f"\nDone. {ok} succeeded, {fail} failed.")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
