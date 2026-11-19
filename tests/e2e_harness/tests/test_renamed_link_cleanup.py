# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check scenario cleanup without Kubernetes or a Slurm allocation."""

from __future__ import annotations

import runpy
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest


class Stack:
    """Model desired Deployment state separately from its still-running pod."""

    def __init__(self) -> None:
        self.containers = [
            {
                "name": "slurm-advisor-controller",
                "env": [
                    {"name": "SLURM_ADVISOR_LOG_LINKS_DESTINATION", "value": "/home/log-links"},
                    {"name": "SLURM_ADVISOR_LOG_LINKS_ENABLED", "value": "true"},
                ],
            }
        ]
        self.running = deepcopy(self.containers)
        # Directories by path, so cleanup that removes the wrong one fails.
        self.directories: set[str] = set()
        self.fail_restart = False

    def get(self, *_: str) -> dict[str, Any]:
        return {"spec": {"template": {"spec": {"containers": deepcopy(self.containers)}}}}

    def patch(self, kind: str, name: str, patch: dict[str, Any]) -> None:
        self.containers = deepcopy(patch["spec"]["template"]["spec"]["containers"])

    def restart(self, name: str, *, wait_for_old_pods: bool) -> None:
        assert wait_for_old_pods
        if self.fail_restart:
            raise TimeoutError("old writer pod still running")
        self.running = deepcopy(self.containers)

    def exec(self, selector: str, *command: str) -> str:
        if command[0] == "mkdir":
            self.directories.add(command[-1])
        elif command[0] == "rm":
            active = {item["name"]: item.get("value") for item in self.running[0]["env"]}
            assert active["SLURM_ADVISOR_LOG_LINKS_DESTINATION"] != command[-1], "deleted storage under a live writer"
            self.directories.discard(command[-1])
        elif command[0] == "test":
            assert command[-1] not in self.directories
        return ""


class Run:
    """Retain registered cleanup actions, as the scenario runner does."""

    def __init__(self) -> None:
        self.cleanups: dict[str, Any] = {}

    def with_cleanup(self, name: str, cleanup: Any) -> Run:
        self.cleanups[name] = cleanup
        return self


def destination_of(containers: list[dict[str, Any]]) -> str | None:
    """The link destination that a set of containers is configured with."""

    env = {item["name"]: item.get("value") for item in containers[0]["env"]}
    return env.get("SLURM_ADVISOR_LOG_LINKS_DESTINATION")


@pytest.mark.parametrize("restoration_fails", [False, True])
def test_link_storage_is_removed_only_after_the_writer_has_stopped(restoration_fails: bool) -> None:
    """A failed rollout wait must retain ownership files even after desired env is restored."""

    isolated = "/home/log-links/scenario-isolated"
    path = Path(__file__).resolve().parents[2] / "e2e/scenarios/02-log-ingestion/test_renamed_log_ingestion.py"
    enable = runpy.run_path(str(path))["_enable_renamed_links"]
    stack, run = Stack(), Run()
    original = deepcopy(stack.containers)
    enable(stack, isolated)(run)
    assert stack.directories == {isolated}
    # The protection below only matters if the live writer really moved to the
    # disposable directory. Without this, both cases would pass trivially when
    # setup creates the directory but leaves the writer on shared storage.
    assert destination_of(stack.running) == isolated
    stack.fail_restart = restoration_fails
    cleanup = next(iter(run.cleanups.values()))
    if restoration_fails:
        with pytest.raises(TimeoutError):
            cleanup()
        assert stack.directories == {isolated}
        # Desired state is back to normal, but the old writer is still running on
        # the disposable directory, which is why that directory must be kept.
        assert destination_of(stack.containers) == destination_of(original)
        assert destination_of(stack.running) == isolated
    else:
        cleanup()
        assert not stack.directories
        assert stack.running == original
