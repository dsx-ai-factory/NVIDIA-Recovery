#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for repository-local agent capability projection."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sync-agent-capabilities.py"


def run_sync(root: Path, *args: str, expect_ok: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if expect_ok and result.returncode != 0:
        raise AssertionError(
            f"sync command failed with {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if not expect_ok and result.returncode == 0:
        raise AssertionError("sync command unexpectedly succeeded")
    return result


def write_skill(root: Path, name: str) -> None:
    skill_dir = root / "agent-capabilities" / "skills" / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        f"name: {name}\n"
        "description: Use when testing capability projection.\n"
        "---\n\n"
        "# Sample Skill\n\n"
        "Follow the fixture instructions.\n",
        encoding="utf-8",
    )


def write_agent(root: Path, name: str) -> None:
    agents_dir = root / "agent-capabilities" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    (agents_dir / f"{name}.json").write_text(
        json.dumps(
            {
                "name": name,
                "description": "Fixture agent for projection tests.",
                "prompt": "Inspect the repository and report concise findings.",
                "tools": ["Read", "Grep"],
                "codex": {"sandbox_mode": "read-only", "model_reasoning_effort": "medium"},
                "claude": {"color": "blue", "model": "sonnet"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def write_fixture(root: Path) -> None:
    write_skill(root, "sample-skill")
    write_agent(root, "sample-agent")

    mcp_dir = root / "agent-capabilities" / "mcp"
    mcp_dir.mkdir(parents=True)
    (mcp_dir / "context7.json").write_text(
        json.dumps({"name": "context7", "url": "https://mcp.context7.com/mcp"}, indent=2) + "\n",
        encoding="utf-8",
    )


def assert_generated(root: Path) -> None:
    expected_skill = (root / "agent-capabilities" / "skills" / "sample-skill" / "SKILL.md").read_text(encoding="utf-8")
    for target in [
        root / ".agents" / "skills" / "sample-skill" / "SKILL.md",
        root / ".claude" / "skills" / "sample-skill" / "SKILL.md",
    ]:
        assert target.read_text(encoding="utf-8") == expected_skill

    codex_agent = root / ".codex" / "agents" / "sample-agent.toml"
    claude_agent = root / ".claude" / "agents" / "sample-agent.md"
    assert 'name = "sample-agent"' in codex_agent.read_text(encoding="utf-8")
    assert "name: sample-agent" in claude_agent.read_text(encoding="utf-8")

    codex_config = root / ".codex" / "config.toml"
    claude_mcp = root / ".mcp.json"
    assert 'url = "https://mcp.context7.com/mcp"' in codex_config.read_text(encoding="utf-8")
    assert json.loads(claude_mcp.read_text(encoding="utf-8"))["mcpServers"]["context7"]["type"] == "http"


def main() -> int:
    if not SCRIPT.exists():
        print(f"missing sync script: {SCRIPT}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        root.mkdir()
        write_fixture(root)

        run_sync(root, "--write")
        assert_generated(root)
        run_sync(root, "--check")

        drifted = root / ".agents" / "skills" / "sample-skill" / "SKILL.md"
        drifted.write_text(drifted.read_text(encoding="utf-8") + "\nDrift.\n", encoding="utf-8")
        run_sync(root, "--check", expect_ok=False)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        root.mkdir()
        write_fixture(root)
        shutil.rmtree(root / "agent-capabilities" / "skills" / "sample-skill")
        (root / "agent-capabilities" / "skills" / "sample-skill").mkdir(parents=True)
        run_sync(root, "--check", expect_ok=False)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        root.mkdir()
        write_fixture(root)
        write_skill(root, "removed-skill")
        write_agent(root, "removed-agent")
        run_sync(root, "--write")

        shutil.rmtree(root / "agent-capabilities" / "skills" / "removed-skill")
        (root / "agent-capabilities" / "agents" / "removed-agent.json").unlink()
        run_sync(root, "--check", expect_ok=False)

        run_sync(root, "--write")
        stale_paths = [
            root / ".agents" / "skills" / "removed-skill",
            root / ".claude" / "skills" / "removed-skill",
            root / ".codex" / "agents" / "removed-agent.toml",
            root / ".claude" / "agents" / "removed-agent.md",
        ]
        for stale_path in stale_paths:
            assert not stale_path.exists(), f"stale projection was not removed: {stale_path}"
        run_sync(root, "--check")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        root.mkdir()
        write_fixture(root)
        run_sync(root, "--write")

        stale_private_skill = root / ".claude" / "skills" / "heimdall-on-call"
        stale_private_skill.mkdir(parents=True)
        (stale_private_skill / "SKILL.md").write_text("stale private skill\n", encoding="utf-8")
        run_sync(root, "--check", expect_ok=False)

        run_sync(root, "--write")
        assert not stale_private_skill.exists(), "stale private skill was not removed"
        run_sync(root, "--check")

    print("ok: sync-agent-capabilities self-test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
