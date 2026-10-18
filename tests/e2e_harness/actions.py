# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scenario actions: changes a scenario makes to its jobs or to the deployed stack, as a user or operator would."""

from __future__ import annotations

import functools
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

from recovery_libs.testing.launch.launcher import SlurmLauncher
from recovery_libs.testing.scenario import JOB, Action, Run, unsafe

from e2e_harness.dag import parse_job_status_response
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.services import E2EServices

SLURM_LOGIN_POD = "app.kubernetes.io/component=login"


def staging_directory() -> str:
    """A new directory path on the Slurm shared filesystem, unique to one scenario run."""

    return f"/home/e2e/python-e2e.{uuid.uuid4().hex[:6]}"


def cancel_job(role: str = JOB) -> Action[RecoveryClient]:
    """Cancel the job launched as `role` through the launcher, as a user would."""

    def action(run: Run[RecoveryClient]) -> None:
        run.launcher.cancel(job_id=run.jobs[role].job_id)

    return action


def requeue_hold_job(role: str = JOB) -> Action[RecoveryClient]:
    """Stop the job launched as `role` and requeue it held, as an operator would."""

    def action(run: Run[RecoveryClient]) -> None:
        _slurm_launcher(run).requeue_hold(job_id=run.jobs[role].job_id)

    return action


@unsafe
def scale_to_zero(deployment: str) -> Action[E2EServices]:
    """Stop every pod of `deployment`; the scenario restores it with `restore_scale`, or on the way out."""

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        stack = run.services.stack
        replicas = stack.replicas(deployment)
        if replicas < 1:
            raise AssertionError(f"deployment {deployment} starts with {replicas} replicas")
        undo = functools.partial(stack.scale, deployment, replicas)
        try:
            stack.scale(deployment, 0)
        except BaseException:
            undo()
            raise
        return run.with_cleanup(f"scale {deployment}", undo)

    return action


@unsafe
def restore_scale(deployment: str) -> Action[E2EServices]:
    """Restore a deployment that `scale_to_zero` stopped, to its earlier replica count."""

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        name = f"scale {deployment}"
        run.cleanups[name]()
        return run.without_cleanup(name)

    return action


@unsafe
def disable_attribution(deployment: str) -> Action[E2EServices]:
    """Turn attribution off in the anomaly isolator's settings and restart it; the scenario restores the exact
    settings on the way out."""

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        stack = run.services.stack
        volumes = stack.get("deployment", deployment)["spec"]["template"]["spec"]["volumes"]
        config_map = next(volume["configMap"]["name"] for volume in volumes if volume["name"] == "config")
        original = stack.get("configmap", config_map)["data"]["settings.env"]
        disabled, replacements = re.subn(r"(?m)^attribution_enabled=true$", "attribution_enabled=false", original)
        if replacements != 1:
            raise AssertionError("protected stack must start with attribution_enabled=true exactly once")

        def apply(settings: str) -> None:
            stack.patch("configmap", config_map, {"data": {"settings.env": settings}})
            # Ready replacement pods can coexist with terminating workers that still have attribution on.
            stack.restart(deployment, wait_for_old_pods=True)

        undo = functools.partial(apply, original)
        try:
            apply(disabled)
        except BaseException:
            undo()
            raise
        return run.with_cleanup(f"attribution {deployment}", undo)

    return action


@unsafe
def restart(deployment: str) -> Action[E2EServices]:
    """Replace every pod of `deployment` and wait until the new ones are ready."""

    def action(run: Run[E2EServices]) -> None:
        run.services.stack.restart(deployment)

    return action


def shared_directory(path: str) -> Action[E2EServices]:
    """Create `path` on the Slurm shared filesystem from the login pod; the scenario removes it on the way out."""

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        stack = run.services.stack
        stack.exec(SLURM_LOGIN_POD, "mkdir", "-p", "--", path)
        remove = functools.partial(stack.exec, SLURM_LOGIN_POD, "rm", "-rf", "--", path)
        return run.with_cleanup(f"directory {path}", remove)

    return action


def staged_files(directory: str, *sources: Path) -> Action[E2EServices]:
    """Copy local `sources` by name into a new `directory` on the Slurm shared filesystem; the scenario removes
    the directory on the way out.

    If a copy fails, the directory is removed here: the failed action never returns the run that holds its cleanup.
    """

    create = shared_directory(directory)

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        run = create(run)
        try:
            for source in sources:
                run.services.stack.copy(SLURM_LOGIN_POD, source, f"{directory}/{source.name}")
        except BaseException as error:
            try:
                run.cleanups[f"directory {directory}"]()
            except Exception as cleanup_error:  # noqa: BLE001
                error.add_note(f"removing {directory} also failed: {cleanup_error!r}")
            raise
        return run

    return action


def touch_file(path: str) -> Action[E2EServices]:
    """Create `path` on the Slurm shared filesystem from the login pod, opening a gate a job waits on."""

    def action(run: Run[E2EServices]) -> None:
        run.services.stack.exec(SLURM_LOGIN_POD, "touch", "--", path)

    return action


def post_job_state(role: str, *, state: str, reason: str, attempt: int = 0) -> Action[RecoveryClient]:
    """Replay a scheduler observation of the job launched as `role` through Job Manager's production ingress.

    Use it for scheduler states the launcher cannot produce, such as STOPPED, which Slinky has no command for.
    """

    def action(run: Run[RecoveryClient]) -> None:
        job_id = run.jobs[role].job_id
        response = run.services.job_manager.jobs(job_id=job_id)
        status = dict(parse_job_status_response(response.text, job_id=job_id, attempt=attempt))
        observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        status.update(state=state, state_reason=reason, last_updated_at=observed_at)
        metadata = status.pop("schedulerMetadata", None)
        if metadata is None:
            metadata = status.pop("scheduler_metadata", {})
        status["scheduler_metadata"] = {
            **metadata,
            "slurm_state": state,
            "slurm_reason": reason,
            "slurm_end_time": observed_at,
        }
        persisted = run.services.job_manager.post_job(status)
        if persisted.json().get("state") != state:
            raise AssertionError(f"{state} production ingress did not persist: {persisted.text}")

    return action


def _slurm_launcher(run: Run[RecoveryClient]) -> SlurmLauncher:
    if not isinstance(run.launcher, SlurmLauncher):
        raise TypeError(f"this scenario acts on Slurm holds and needs a Slurm launcher, not {run.launcher!r}")
    return run.launcher
