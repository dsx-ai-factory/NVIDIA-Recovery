# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify preinstalled envtest assets remain usable from the module directory."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]


@pytest.mark.parametrize("relative", [True, False])
@pytest.mark.parametrize("missing", [None, "etcd", "kube-apiserver", "kubectl"])
def test_preinstalled_assets_survive_directory_change(tmp_path, relative, missing):
    """Run the setup entry point and use its result after changing into the module."""
    scripts = tmp_path / "scripts/ci"
    scripts.mkdir(parents=True)
    for name in ("setup-advice-operator-envtest.sh", "lib.sh", "go-tool-versions.env"):
        shutil.copyfile(REPO_ROOT / "scripts/ci" / name, scripts / name)
    commands = tmp_path / "bin"
    commands.mkdir()
    # Isolate repository discovery; no VCS operation or Go download is needed.
    for name, body in (("git", 'printf "%s\\n" "$TEST_REPO_ROOT"'), ("go", "exit 99")):
        command = commands / name
        command.write_text(f"#!/bin/sh\n{body}\n")
        command.chmod(0o755)
    assets = tmp_path / "preinstalled assets"
    assets.mkdir()
    for name in ("etcd", "kube-apiserver", "kubectl"):
        if name != missing:
            binary = assets / name
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o755)
    module = tmp_path / "advice-operator"
    module.mkdir()
    result = subprocess.run(
        ["bash", str(scripts / "setup-advice-operator-envtest.sh")],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
            "TEST_REPO_ROOT": str(tmp_path),
            "KUBEBUILDER_ASSETS": str(assets.relative_to(tmp_path) if relative else assets),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if missing:
        assert result.returncode != 0
        assert not result.stdout
        return
    assert result.returncode == 0, result.stderr
    selected = Path(result.stdout.strip())
    assert selected.is_absolute()
    for name in ("etcd", "kube-apiserver", "kubectl"):
        subprocess.run([str(selected / name)], cwd=module, check=True)
