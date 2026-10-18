#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Synchronize portable agent capabilities into Claude and Codex surfaces."""

from __future__ import annotations

import argparse
import filecmp
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
    tomllib = None


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path("agent-capabilities")
SKILL_SOURCE = SOURCE_ROOT / "skills"
AGENT_SOURCE = SOURCE_ROOT / "agents"
MCP_SOURCE = SOURCE_ROOT / "mcp" / "context7.json"

SKILL_TARGETS = (Path(".agents") / "skills", Path(".claude") / "skills")
CODEX_AGENT_TARGET = Path(".codex") / "agents"
CLAUDE_AGENT_TARGET = Path(".claude") / "agents"
CODEX_CONFIG = Path(".codex") / "config.toml"
CLAUDE_MCP = Path(".mcp.json")
UNMANAGED_SKILL_DIRS = {
    Path(".claude") / "skills": {"update-api-specs-and-bindings"},
}
GENERATED_AGENT_TARGETS = {
    CODEX_AGENT_TARGET: ".toml",
    CLAUDE_AGENT_TARGET: ".md",
}


class SyncError(RuntimeError):
    """Raised when capability sources or projections are invalid."""


def repo_path(root: Path, relative: Path) -> Path:
    return root / relative


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SyncError(f"invalid JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SyncError(f"expected JSON object: {path}")
    return value


def require_sources(root: Path) -> None:
    for path in (SKILL_SOURCE, AGENT_SOURCE, MCP_SOURCE):
        full_path = repo_path(root, path)
        if not full_path.exists():
            raise SyncError(f"missing required capability source: {path}")


def skill_dirs(root: Path) -> list[Path]:
    source = repo_path(root, SKILL_SOURCE)
    dirs = sorted(path for path in source.iterdir() if path.is_dir())
    if not dirs:
        raise SyncError(f"no skills found under {SKILL_SOURCE}")
    for directory in dirs:
        skill = directory / "SKILL.md"
        if not skill.is_file():
            raise SyncError(f"missing SKILL.md: {skill.relative_to(root)}")
    return dirs


def agent_specs(root: Path) -> list[Path]:
    source = repo_path(root, AGENT_SOURCE)
    specs = sorted(source.glob("*.json"))
    if not specs:
        raise SyncError(f"no agent specs found under {AGENT_SOURCE}")
    return specs


def copy_skill(source: Path, target: Path) -> None:
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)


def remove_stale_skills(root: Path, source_names: set[str]) -> None:
    for target_root in SKILL_TARGETS:
        target = repo_path(root, target_root)
        allowed = UNMANAGED_SKILL_DIRS.get(target_root, set())
        if not target.exists():
            continue
        for child in target.iterdir():
            if child.name in allowed or child.name in source_names:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


def remove_stale_agents(root: Path, expected: dict[Path, str]) -> None:
    expected_files = {relative for relative in expected if relative.parent in GENERATED_AGENT_TARGETS}
    for target_root, suffix in GENERATED_AGENT_TARGETS.items():
        target = repo_path(root, target_root)
        if not target.exists():
            continue
        for child in target.iterdir():
            relative = child.relative_to(root)
            if child.is_file() and child.suffix == suffix and relative not in expected_files:
                child.unlink()


def dirs_equal(left: Path, right: Path) -> bool:
    if not left.exists() or not right.exists():
        return False
    comparison = filecmp.dircmp(left, right)
    if comparison.left_only or comparison.right_only or comparison.diff_files or comparison.funny_files:
        return False
    return all(
        dirs_equal(Path(comparison.left) / name, Path(comparison.right) / name) for name in comparison.common_dirs
    )


def toml_string(value: str) -> str:
    return json.dumps(value)


def toml_multiline(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    return f'"""{escaped}\n"""'


def render_codex_agent(spec: dict[str, Any]) -> str:
    codex = spec.get("codex", {})
    if not isinstance(codex, dict):
        raise SyncError(f"agent codex field must be an object: {spec.get('name')}")

    fields: list[str] = [
        f"name = {toml_string(required_string(spec, 'name'))}",
        f"description = {toml_string(required_string(spec, 'description'))}",
    ]
    for key in ("model", "model_reasoning_effort", "sandbox_mode"):
        value = codex.get(key)
        if value:
            fields.append(f"{key} = {toml_string(str(value))}")
    fields.append(f"developer_instructions = {toml_multiline(required_string(spec, 'prompt'))}")
    return "\n".join(fields) + "\n"


def render_claude_agent(spec: dict[str, Any]) -> str:
    claude = spec.get("claude", {})
    if not isinstance(claude, dict):
        raise SyncError(f"agent claude field must be an object: {spec.get('name')}")

    frontmatter: list[str] = [
        "---",
        f"name: {required_string(spec, 'name')}",
        f"description: {required_string(spec, 'description')}",
    ]
    tools = spec.get("tools")
    if tools:
        if not isinstance(tools, list) or not all(isinstance(tool, str) for tool in tools):
            raise SyncError(f"agent tools field must be a string list: {spec.get('name')}")
        frontmatter.append(f"tools: {', '.join(tools)}")
    for key in ("model", "color"):
        value = claude.get(key)
        if value:
            frontmatter.append(f"{key}: {value}")
    frontmatter.append("---")
    return "\n".join(frontmatter) + "\n\n" + required_string(spec, "prompt").rstrip() + "\n"


def required_string(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SyncError(f"missing required string field {key!r}")
    return value


def render_codex_config(mcp: dict[str, Any]) -> str:
    name = required_string(mcp, "name")
    url = required_string(mcp, "url")
    return (
        "# Generated by scripts/sync-agent-capabilities.py from agent-capabilities/mcp/context7.json.\n"
        f"[mcp_servers.{name}]\n"
        f"url = {toml_string(url)}\n"
        "enabled = true\n"
    )


def render_claude_mcp(mcp: dict[str, Any]) -> str:
    name = required_string(mcp, "name")
    url = required_string(mcp, "url")
    return json.dumps({"mcpServers": {name: {"type": "http", "url": url}}}, indent=2, sort_keys=True) + "\n"


def validate_generated_text(root: Path, codex_config: str, claude_mcp: str) -> None:
    if tomllib is not None:
        tomllib.loads(codex_config)
    json.loads(claude_mcp)


def expected_outputs(root: Path) -> dict[Path, str]:
    outputs: dict[Path, str] = {}
    mcp = read_json(repo_path(root, MCP_SOURCE))
    codex_config = render_codex_config(mcp)
    claude_mcp = render_claude_mcp(mcp)
    validate_generated_text(root, codex_config, claude_mcp)
    outputs[CODEX_CONFIG] = codex_config
    outputs[CLAUDE_MCP] = claude_mcp

    for spec_path in agent_specs(root):
        spec = read_json(spec_path)
        name = required_string(spec, "name")
        outputs[CODEX_AGENT_TARGET / f"{name}.toml"] = render_codex_agent(spec)
        outputs[CLAUDE_AGENT_TARGET / f"{name}.md"] = render_claude_agent(spec)
    return outputs


def write_outputs(root: Path) -> None:
    require_sources(root)
    skills = skill_dirs(root)
    source_names = {source.name for source in skills}
    expected = expected_outputs(root)
    remove_stale_skills(root, source_names)
    remove_stale_agents(root, expected)

    for source in skills:
        for target_root in SKILL_TARGETS:
            copy_skill(source, repo_path(root, target_root) / source.name)

    for relative, content in expected.items():
        target = repo_path(root, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def check_outputs(root: Path) -> None:
    require_sources(root)
    errors: list[str] = []
    skills = skill_dirs(root)
    source_names = {source.name for source in skills}

    for target_root in SKILL_TARGETS:
        target = repo_path(root, target_root)
        allowed = UNMANAGED_SKILL_DIRS.get(target_root, set())
        if not target.exists():
            errors.append(f"missing generated skill root: {target_root}")
            continue
        for child in sorted(target.iterdir()):
            if child.name in allowed or child.name in source_names:
                continue
            errors.append(f"stale skill projection: {child.relative_to(root)}")

    for source in skills:
        for target_root in SKILL_TARGETS:
            target = repo_path(root, target_root) / source.name
            if not dirs_equal(source, target):
                errors.append(f"skill projection is out of sync: {target.relative_to(root)}")

    expected_map = expected_outputs(root)
    expected_agent_files = {relative for relative in expected_map if relative.parent in GENERATED_AGENT_TARGETS}
    for target_root, suffix in GENERATED_AGENT_TARGETS.items():
        target = repo_path(root, target_root)
        if not target.exists():
            errors.append(f"missing generated agent root: {target_root}")
            continue
        for child in sorted(target.iterdir()):
            relative = child.relative_to(root)
            if child.is_file() and child.suffix == suffix and relative not in expected_agent_files:
                errors.append(f"stale agent projection: {relative}")

    for relative, expected in expected_map.items():
        target = repo_path(root, relative)
        if not target.exists():
            errors.append(f"missing generated file: {relative}")
            continue
        actual = target.read_text(encoding="utf-8")
        if actual != expected:
            errors.append(f"generated file is out of sync: {relative}")

    if errors:
        raise SyncError("\n".join(errors))


def run_self_test() -> int:
    test = REPO_ROOT / "scripts" / "sync-agent-capabilities-self-test.py"
    result = subprocess.run([sys.executable, str(test)], cwd=REPO_ROOT, check=False)
    return result.returncode


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help=argparse.SUPPRESS)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="rewrite generated Claude and Codex capability files")
    mode.add_argument("--check", action="store_true", help="verify generated files match canonical sources")
    mode.add_argument("--self-test", action="store_true", help="run the generator regression self-test")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    try:
        if args.self_test:
            return run_self_test()
        if args.write:
            write_outputs(root)
            print("ok: agent capability projections written")
        elif args.check:
            check_outputs(root)
            print("ok: agent capability projections are in sync")
    except SyncError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
