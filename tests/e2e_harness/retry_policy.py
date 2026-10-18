# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Temporary production policy configuration for retry-stop scenarios."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import yaml
from recovery_libs.testing.kubernetes import StackControl

from e2e_harness.recovery_client import RecoveryClient

_SAC = "slurm-advisor-controller"
_CONFIG_KEY = "job_manager_config.yaml"
_EAGER_ENV = "SLURM_ADVISOR_ENABLE_EAGER_REQUEUE"
_RECONNECT_TIMEOUT_SECONDS = 30.0
_RECONNECT_POLL_SECONDS = 0.5


def eager_requeue_enabled(slurm_stack: StackControl) -> bool:
    """Read the eager-requeue flag rendered by the E2E Helm chart, from SAC in the Slurm namespace."""

    deployment = slurm_stack.get("deployment", _SAC)
    controller = next(
        container for container in deployment["spec"]["template"]["spec"]["containers"] if container["name"] == _SAC
    )
    return any(entry["name"] == _EAGER_ENV and entry.get("value") == "true" for entry in controller.get("env", []))


def _apply_job_manager_config(stack: StackControl, job_manager: str, value: str) -> None:
    stack.patch("configmap", f"{job_manager}-config", {"data": {_CONFIG_KEY: value}})
    # SAC's existing HTTP connections must not send the new scenario to a terminating JM.
    stack.restart(job_manager, wait_for_old_pods=True)


def _loaded_requeue_config(recovery: RecoveryClient) -> Any:
    """Read the policy JM loaded, waiting for the restarted JM to become reachable.

    A port-forward to JM stays attached to the replaced pod and reconnects only after that pod is deleted, which is
    when the restart returns, so JM refuses connections for a few seconds.
    """

    deadline = time.monotonic() + _RECONNECT_TIMEOUT_SECONDS
    while True:
        try:
            return recovery.job_manager.requeue_config().json()
        except httpx.TransportError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(_RECONNECT_POLL_SECONDS)


@contextmanager
def system_retry_limit(
    stack: StackControl,
    recovery: RecoveryClient,
    *,
    slurm_stack: StackControl,
    job_manager: str,
    limit: int = 1,
    checkpoint_threshold: int | None = None,
) -> Iterator[None]:
    """Temporarily configure JM retry limits with SAC eager recovery disabled.

    Checkpoint stopping is disabled by default to isolate the system retry limit.
    Supply a checkpoint threshold and a higher system limit (or -1 to disable it)
    to isolate checkpoint stopping instead. Omit the job-level limit or set it
    higher than the expected final attempt.

    This changes shared JM settings and is for the serial, disposable E2E stack.
    SAC must already have eager recovery disabled and remains untouched. Only the
    JM config data key is patched, and its exact original contents are restored
    even if setup or the scenario fails. Restarts wait for prior JM pods to
    disappear before allowing the scenario or the next test to proceed.
    """

    if limit != -1 and limit < 1:
        raise ValueError("system retry limit must be positive or -1")
    if checkpoint_threshold is not None and checkpoint_threshold < 1:
        raise ValueError("checkpoint threshold must be positive or None")
    if eager_requeue_enabled(slurm_stack):
        raise AssertionError("retry-policy scenarios require SAC eager requeue to be disabled")

    original_config = stack.get("configmap", f"{job_manager}-config")["data"][_CONFIG_KEY]
    updated_config = yaml.safe_load(original_config)
    updated_config["service"]["system_max_requeue_times"] = limit
    # Both policies emit the same terminal cause, so each scenario must isolate
    # its policy through these settings and the submitted job-level retry limit.
    updated_config["service"]["checkpoint_based_requeue_stop_threshold"] = checkpoint_threshold
    config_attempted = False
    primary_error: BaseException | None = None
    try:
        # A failed response can still mean the write was applied.
        config_attempted = True
        _apply_job_manager_config(stack, job_manager, yaml.safe_dump(updated_config))
        observed = _loaded_requeue_config(recovery)
        if observed.get("max_requeue_times") != limit:
            raise AssertionError(f"JM did not load system retry limit {limit}: {observed}")
        if observed["checkpoint_based_requeue_stop_threshold"] != checkpoint_threshold:
            raise AssertionError(f"JM did not load checkpoint threshold {checkpoint_threshold}: {observed}")
        yield
    except BaseException as error:
        primary_error = error
        raise
    finally:
        errors: list[Exception] = []
        if config_attempted:
            try:
                _apply_job_manager_config(stack, job_manager, original_config)
            except Exception as error:  # noqa: BLE001
                errors.append(error)
        if errors:
            if primary_error is not None:
                raise BaseExceptionGroup("retry-policy fixture and restoration failed", [primary_error, *errors])
            raise ExceptionGroup("retry-policy fixture restoration failed", errors)
