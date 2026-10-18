# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The retry-policy fixture changes Job Manager on the shared stack; every exit must restore it exactly."""

from __future__ import annotations

import json
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml
from e2e_harness.recovery_client import ApiResponse
from e2e_harness.retry_policy import system_retry_limit

JOB_MANAGER = "nvidia-recovery-job-manager"
CONFIG = f"{JOB_MANAGER}-config"
KEY = "job_manager_config.yaml"
# Comments and key order show that restoration writes back the original text, not a re-serialized copy.
ORIGINAL = """service:
  # operator note
  system_max_requeue_times: 5
  checkpoint_based_requeue_stop_threshold: null
  unrelated: kept
"""


class FakeStack:
    """ConfigMaps and deployments in memory; restarting Job Manager loads its current ConfigMap."""

    def __init__(
        self, *, eager: bool = False, failing_patches: tuple[int, ...] = (), refused_reads_after_restart: int = 0
    ) -> None:
        self.config_maps = {CONFIG: {KEY: ORIGINAL}}
        self.eager = eager
        self.failing_patches = failing_patches
        self.patches = 0
        self.loaded = yaml.safe_load(ORIGINAL)["service"]
        self.refused_reads_after_restart = refused_reads_after_restart
        self.refused_reads = 0

    def get(self, kind: str, name: str) -> dict[str, Any]:
        if kind == "configmap":
            return {"data": dict(self.config_maps[name])}
        env = [{"name": "SLURM_ADVISOR_ENABLE_EAGER_REQUEUE", "value": "true" if self.eager else "false"}]
        return {"spec": {"template": {"spec": {"containers": [{"name": name, "env": env}]}}}}

    def patch(self, kind: str, name: str, patch: dict[str, Any]) -> None:
        self.patches += 1
        if self.patches in self.failing_patches:
            raise RuntimeError(f"patch {self.patches} failed")
        self.config_maps[name].update(patch["data"])

    def restart(self, deployment: str, *, wait_for_old_pods: bool = False) -> None:
        self.loaded = yaml.safe_load(self.config_maps[CONFIG][KEY])["service"]
        self.refused_reads = self.refused_reads_after_restart


def recovery_reading(stack: FakeStack, *, ignores_threshold: bool = False) -> SimpleNamespace:
    def requeue_config() -> ApiResponse:
        if stack.refused_reads:
            stack.refused_reads -= 1
            raise httpx.ConnectError("[Errno 111] Connection refused")
        threshold = None if ignores_threshold else stack.loaded["checkpoint_based_requeue_stop_threshold"]
        body = {
            "max_requeue_times": stack.loaded["system_max_requeue_times"],
            "checkpoint_based_requeue_stop_threshold": threshold,
        }
        return ApiResponse("http://job-manager/config/requeue", 200, json.dumps(body))

    return SimpleNamespace(job_manager=SimpleNamespace(requeue_config=requeue_config))


@pytest.mark.parametrize("scenario_fails", (False, True))
def test_policy_applies_during_the_scenario_and_restores_the_exact_original(scenario_fails: bool) -> None:
    stack = FakeStack()
    seen: dict[str, Any] = {}

    with (
        pytest.raises(AssertionError) if scenario_fails else nullcontext(),
        system_retry_limit(
            stack,
            recovery_reading(stack),
            slurm_stack=stack,
            job_manager=JOB_MANAGER,
            limit=1,
            checkpoint_threshold=3,
        ),
    ):
        seen.update(stack.loaded)
        if scenario_fails:
            raise AssertionError("scenario failed")

    assert seen["system_max_requeue_times"] == 1
    assert seen["checkpoint_based_requeue_stop_threshold"] == 3
    assert stack.config_maps[CONFIG][KEY] == ORIGINAL
    assert stack.loaded["system_max_requeue_times"] == 5


def test_policy_is_read_once_the_restarted_job_manager_is_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The port-forward to Job Manager reconnects only after the restart returns."""

    monkeypatch.setattr("e2e_harness.retry_policy.time.sleep", lambda _seconds: None)
    stack = FakeStack(refused_reads_after_restart=8)

    with system_retry_limit(stack, recovery_reading(stack), slurm_stack=stack, job_manager=JOB_MANAGER, limit=1):
        assert stack.loaded["system_max_requeue_times"] == 1

    assert stack.config_maps[CONFIG][KEY] == ORIGINAL


def test_a_policy_job_manager_did_not_load_is_rejected_and_restored() -> None:
    stack = FakeStack()
    entered = False

    with (
        pytest.raises(AssertionError, match="checkpoint threshold"),
        system_retry_limit(
            stack,
            recovery_reading(stack, ignores_threshold=True),
            slurm_stack=stack,
            job_manager=JOB_MANAGER,
            checkpoint_threshold=3,
        ),
    ):
        entered = True

    assert not entered
    assert stack.config_maps[CONFIG][KEY] == ORIGINAL


def test_eager_requeue_is_refused_before_anything_changes() -> None:
    """SAC runs in the Slurm namespace, which can differ from the Job Manager's."""

    stack, slurm_stack = FakeStack(), FakeStack(eager=True)

    with (
        pytest.raises(AssertionError, match="eager requeue"),
        system_retry_limit(stack, recovery_reading(stack), slurm_stack=slurm_stack, job_manager=JOB_MANAGER),
    ):
        pass

    assert stack.patches == 0


def test_a_failed_restore_does_not_hide_the_scenario_failure() -> None:
    stack = FakeStack(failing_patches=(2,))

    with (
        pytest.raises(BaseExceptionGroup) as raised,
        system_retry_limit(stack, recovery_reading(stack), slurm_stack=stack, job_manager=JOB_MANAGER),
    ):
        raise AssertionError("scenario failed")

    messages = [str(error) for error in raised.value.exceptions]
    assert messages == ["scenario failed", "patch 2 failed"]
