# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from e2e_harness.actions import disable_attribution, staged_files
from recovery_libs.testing.scenario import Run


class CopyFailsStack:
    """Records the commands run on the login pod; copying a file into it fails, as a `kubectl cp` timeout would."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []

    def exec(self, selector: str, *command: str) -> str:
        self.commands.append(command)
        return ""

    def copy(self, selector: str, source: Path, destination: str) -> None:
        raise RuntimeError(f"kubectl cp {source} timed out")


def test_staged_files_removes_its_directory_when_a_copy_fails(tmp_path: Path) -> None:
    """The failed action never returns its run, so the scenario cannot remove the directory on the way out."""

    stack = CopyFailsStack()
    run = Run({}, None, SimpleNamespace(stack=stack))
    stage = staged_files("/home/e2e/staged", tmp_path / "job_output.txt")

    with pytest.raises(RuntimeError, match="timed out"):
        stage(run)

    assert stack.commands == [("mkdir", "-p", "--", "/home/e2e/staged"), ("rm", "-rf", "--", "/home/e2e/staged")]


class IsolatorStack:
    """One anomaly isolator whose settings ConfigMap is mounted as its `config` volume."""

    def __init__(self, settings: str) -> None:
        self.settings = settings
        self.running_settings = settings

    def get(self, kind: str, name: str) -> dict[str, Any]:
        if kind == "configmap":
            return {"data": {"settings.env": self.settings}}
        return {"spec": {"template": {"spec": {"volumes": [{"name": "config", "configMap": {"name": "adi-config"}}]}}}}

    def patch(self, kind: str, name: str, patch: dict[str, Any]) -> None:
        self.settings = patch["data"]["settings.env"]

    def restart(self, deployment: str, *, wait_for_old_pods: bool = False) -> None:
        self.running_settings = self.settings


def test_disable_attribution_runs_with_attribution_off_and_restores_the_exact_settings() -> None:
    original = "# operator note\nattribution_enabled=true\nattribution_service_url=http://adapter\n"
    stack = IsolatorStack(original)

    run = disable_attribution("adi")(Run({}, None, SimpleNamespace(stack=stack)))

    assert "attribution_enabled=false" in stack.running_settings
    for undo in run.cleanups.values():
        undo()
    assert stack.settings == stack.running_settings == original


@pytest.mark.parametrize("settings", ("attribution_enabled=false\n", "attribution_enabled=true\n" * 2))
def test_disable_attribution_refuses_a_stack_not_starting_with_attribution_on_once(settings: str) -> None:
    stack = IsolatorStack(settings)

    with pytest.raises(AssertionError, match="exactly once"):
        disable_attribution("adi")(Run({}, None, SimpleNamespace(stack=stack)))

    assert stack.settings == settings
