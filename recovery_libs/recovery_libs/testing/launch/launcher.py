# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Submit, inspect, wait for, and cancel jobs without knowing how the scheduler is reached."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Protocol

from recovery_libs.testing.http.tracing import traced
from recovery_libs.testing.launch.clients import SlurmClient, SlurmHttpClient, SlurmKubectlClient, SlurmSshClient
from recovery_libs.testing.launch.jobs import Job, JobNotFound, JobSpec, JobState

_ENVIRONMENT_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_UNREPRESENTABLE_ENVIRONMENT = frozenset(("\x00", "\n", "\r", ","))
_UNSAFE_NAME = frozenset(("\x00", "\n", "\r"))
_STATES = {
    "PENDING": JobState.PENDING,
    "REQUEUED": JobState.PENDING,
    "REQUEUE_HOLD": JobState.PENDING,
    "CONFIGURING": JobState.PENDING,
    "RUNNING": JobState.RUNNING,
    "COMPLETING": JobState.RUNNING,
    "SUSPENDED": JobState.RUNNING,
    "COMPLETED": JobState.COMPLETED,
    "CANCELLED": JobState.CANCELLED,
    "TIMEOUT": JobState.TIMEOUT,
    "FAILED": JobState.FAILED,
    "NODE_FAIL": JobState.FAILED,
    "OUT_OF_MEMORY": JobState.FAILED,
    "BOOT_FAIL": JobState.FAILED,
    "DEADLINE": JobState.FAILED,
    "PREEMPTED": JobState.FAILED,
}


class Launcher(Protocol):
    """Submit, inspect, wait for, and cancel jobs."""

    def submit(self, spec: JobSpec) -> Job: ...

    def get(self, *, job_id: str) -> Job: ...

    def history(self, *, job_id: str) -> Job:
        """The job as the scheduler's history recorded it, for a job `get` no longer finds."""
        ...

    def wait(self, *, job_id: str, until: Callable[[Job], bool], timeout: float) -> Job: ...

    def cancel(self, *, job_id: str) -> None: ...

    def list(self, *, name: str | None = None) -> tuple[Job, ...]: ...


class SlurmLauncher:
    """A `Launcher` for Slurm, reached through any `SlurmClient`.

    It translates each generic `JobSpec` into a Slurm job description, and each Slurm job record back into a `Job`.
    """

    def __init__(
        self,
        client: SlurmClient,
        *,
        poll_interval: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = client
        self._poll_interval = poll_interval
        self._clock = clock
        self._sleep = sleep

    @traced("launcher.submit")
    def submit(self, spec: JobSpec) -> Job:
        """Submit the job and return it as Slurm first reports it.

        If the job cannot be read back, it is cancelled before the error is raised: the caller never receives its
        ID, so nothing else could cancel it.
        """

        _validate(spec)
        job_id = self._client.submit(slurm_job_description(spec))
        try:
            return self.get(job_id=job_id)
        except BaseException as error:
            try:
                self._client.cancel(job_id=job_id)
            except Exception as cancel_error:  # noqa: BLE001
                error.add_note(f"cancelling job {job_id} also failed: {cancel_error!r}")
            raise

    @traced("launcher.get")
    def get(self, *, job_id: str) -> Job:
        record = self._client.get(job_id=job_id)
        if record is None:
            raise JobNotFound(job_id)
        return job_from_record(record)

    @traced("launcher.history")
    def history(self, *, job_id: str) -> Job:
        """The job as Slurm accounting last recorded it, after the controller has forgotten a finished job."""

        record = self._client.accounting(job_id=job_id)
        if record is None:
            raise JobNotFound(job_id)
        return job_from_accounting(record)

    @traced("launcher.wait")
    def wait(self, *, job_id: str, until: Callable[[Job], bool], timeout: float) -> Job:
        deadline = self._clock() + timeout
        job = self.get(job_id=job_id)
        while not until(job):
            if self._clock() >= deadline:
                raise TimeoutError(
                    f"job {job_id} did not reach the expected condition within {timeout:g}s: {job.state}"
                )
            self._sleep(self._poll_interval)
            job = self.get(job_id=job_id)
        return job

    @traced("launcher.cancel")
    def cancel(self, *, job_id: str) -> None:
        self._client.cancel(job_id=job_id)

    @traced("launcher.release")
    def release(self, *, job_id: str) -> None:
        """Release a held job so Slurm can schedule it."""

        self._client.release(job_id=job_id)

    @property
    def supports_requeue_hold(self) -> bool:
        return self._client.supports_requeue_hold

    @traced("launcher.requeue_hold")
    def requeue_hold(self, *, job_id: str) -> None:
        """Stop the job and requeue it held, as a later attempt."""

        self._client.requeue_hold(job_id=job_id)

    @traced("launcher.list")
    def list(self, *, name: str | None = None) -> tuple[Job, ...]:
        return tuple(job_from_record(record) for record in self._client.list(name=name))


DEFAULT_LAUNCHER = "slurm-kubectl"
LAUNCHERS: Mapping[str, Callable[[], Launcher]] = MappingProxyType(
    {
        "slurm-ssh": lambda: SlurmLauncher(SlurmSshClient.from_environment()),
        "slurm-kubectl": lambda: SlurmLauncher(SlurmKubectlClient.from_environment()),
        "slurm-http": lambda: SlurmLauncher(SlurmHttpClient.from_environment()),
    }
)


def launcher_named(name: str) -> Launcher:
    """Build a launcher by name. Its client reads `LAUNCHER_<NAME>_*` settings, for example `LAUNCHER_SLURM_SSH_HOST`."""

    try:
        factory = LAUNCHERS[name]
    except KeyError:
        raise ValueError(f"unknown launcher {name!r}; expected one of {sorted(LAUNCHERS)}") from None
    return factory()


@dataclass(frozen=True)
class SlurmWorkload:
    """What a Slurm job runs: `JobSpec.workload` for `SlurmLauncher`."""

    script: str
    working_directory: str = "/tmp"
    hold: bool = False

    @classmethod
    def of(cls, spec: JobSpec) -> SlurmWorkload:
        try:
            return cls(**spec.workload)
        except TypeError as error:
            raise ValueError(f"job {spec.name!r} has no valid Slurm workload: {error}") from None


def slurm_job_description(spec: JobSpec) -> dict[str, Any]:
    """Translate a job into a Slurm job description; Slurm carries the job's metadata as its comment."""

    workload = SlurmWorkload.of(spec)
    description: dict[str, Any] = {
        "name": spec.name,
        "script": workload.script,
        "environment": [f"{name}={value}" for name, value in spec.environment.items()],
        "current_working_directory": workload.working_directory,
        "hold": workload.hold,
    }
    if spec.time_limit_minutes is not None:
        description["time_limit"] = {"set": True, "number": spec.time_limit_minutes}
    if spec.metadata:
        description["comment"] = json.dumps(dict(spec.metadata), sort_keys=True, separators=(",", ":"))
    return description


def job_from_record(record: Mapping[str, Any]) -> Job:
    """Read a job from Slurm's JSON job record, which every client returns."""

    states = record.get("job_state") or ["UNKNOWN"]
    exit_code = record.get("exit_code", {}).get("return_code", {})
    return Job(
        job_id=str(record["job_id"]),
        name=record.get("name", ""),
        state=_STATES.get(states[0], JobState.UNKNOWN),
        nodes=tuple(node for node in record.get("nodes", "").split(",") if node),
        exit_code=exit_code.get("number") if exit_code.get("set") else None,
        restart_count=record.get("restart_cnt", 0),
        submitted_at=_timestamp(record.get("submit_time")),
        started_at=_timestamp(record.get("start_time")),
        ended_at=_timestamp(record.get("end_time")),
        metadata=MappingProxyType(dict(record)),
    )


def job_from_accounting(record: Mapping[str, Any]) -> Job:
    """Read a job from a Slurm accounting record, as `sacct --json` and slurmrestd's `slurmdb` API return it."""

    states = record.get("state", {}).get("current") or ["UNKNOWN"]
    exit_code = record.get("exit_code", {}).get("return_code", {})
    times = record.get("time", {})
    return Job(
        job_id=str(record["job_id"]),
        name=record.get("name", ""),
        state=_STATES.get(states[0], JobState.UNKNOWN),
        nodes=tuple(node for node in record.get("nodes", "").split(",") if node),
        exit_code=exit_code.get("number") if exit_code.get("set") else None,
        restart_count=record.get("restart_cnt", 0),
        submitted_at=_epoch(times.get("submission")),
        started_at=_epoch(times.get("start")),
        ended_at=_epoch(times.get("end")),
        metadata=MappingProxyType(dict(record)),
    )


def _epoch(value: int | None) -> datetime | None:
    return datetime.fromtimestamp(value, UTC) if value else None


def _timestamp(value: Mapping[str, Any] | None) -> datetime | None:
    """A Slurm time field; Slurm reports times that have not happened yet as zero."""

    if not value or not value.get("set") or not value.get("number"):
        return None
    return datetime.fromtimestamp(value["number"], UTC)


def _validate(spec: JobSpec) -> None:
    if not spec.name or any(character in spec.name for character in _UNSAFE_NAME):
        raise ValueError(f"invalid job name: {spec.name!r}")
    for name, value in spec.environment.items():
        if name in ("ALL", "NONE") or _ENVIRONMENT_NAME.fullmatch(name) is None:
            raise ValueError(f"invalid job environment name: {name!r}")
        if any(character in value for character in _UNREPRESENTABLE_ENVIRONMENT):
            raise ValueError(f"job environment value for {name} is not representable")
    if spec.time_limit_minutes is not None and spec.time_limit_minutes < 1:
        raise ValueError(f"time limit must be at least one minute, got {spec.time_limit_minutes}")
