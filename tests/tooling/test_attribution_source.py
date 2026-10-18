# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise source preparation with real Git and a local upstream repository."""

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
PREPARE = REPO_ROOT / "deploy/tilt/prepare-attribution-source.sh"
CLIENT = "service/service-app/nvidia_attribution_client"


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


@pytest.fixture
def upstream(tmp_path):
    """Create the public build-input layout without fetching provider code."""
    source = tmp_path / "upstream"
    source.mkdir()
    _git(source, "init", "--quiet")
    for name in [
        "LICENSE",
        ".gitignore",
        "service/service-app/Dockerfile",
        "service/service-app/pyproject.toml",
        "service/service-app/uv.lock",
        f"{CLIENT}/pyproject.toml",
        f"{CLIENT}/nvidia_attribution_client/__init__.py",
        "service/attribution-lib/pyproject.toml",
    ]:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n")
    _git(source, "add", ".")
    _git(
        source,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--no-gpg-sign",
        "--signoff",
        "-qm",
        "Build inputs",
    )
    return source, _git(source, "rev-parse", "HEAD")


@pytest.mark.parametrize("cached", [False, True])
def test_prepare_supplies_client_and_root_license(upstream, tmp_path, cached):
    """Fresh and earlier service-only caches supply all Docker build inputs."""
    source, commit = upstream
    destination = tmp_path / "checkout"
    if cached:
        subprocess.run(["git", "clone", "--quiet", "--no-checkout", str(source), str(destination)], check=True)
        _git(destination, "sparse-checkout", "set", "--no-cone", "/service/", "/.gitignore")
        _git(destination, "checkout", "--quiet", "--detach", commit)
        assert not (destination / "LICENSE").exists()
    subprocess.run([str(PREPARE), str(destination), str(source), commit], check=True, capture_output=True, text=True)
    assert (destination / "LICENSE").read_text() == "fixture\n"
    assert (destination / CLIENT / "nvidia_attribution_client/__init__.py").is_file()
    assert _git(destination, "rev-parse", "HEAD") == commit
    assert not _git(destination, "status", "--porcelain")


def test_prepare_preserves_dirty_cached_checkout(upstream, tmp_path):
    """Cache repair must not hide or overwrite caller edits."""
    source, commit = upstream
    destination = tmp_path / "checkout"
    subprocess.run([str(PREPARE), str(destination), str(source), commit], check=True, capture_output=True)
    license_path = destination / "LICENSE"
    license_path.write_text("local change\n")
    result = subprocess.run([str(PREPARE), str(destination), str(source), commit], capture_output=True, check=False)
    assert result.returncode != 0
    assert license_path.read_text() == "local change\n"
