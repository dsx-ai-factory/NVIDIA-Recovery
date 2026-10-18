# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Immutable descriptions of jobs to submit and jobs as the scheduler reports them."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any

EMPTY: Mapping[str, Any] = MappingProxyType({})


class JobState(StrEnum):
    """Normalized job state. The scheduler's own state stays in `Job.metadata`."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"

    @property
    def is_final(self) -> bool:
        return self in {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED, JobState.TIMEOUT}


@dataclass(frozen=True)
class JobSpec:
    """A job to submit; each launcher translates it for its scheduler.

    `metadata` is JSON-serializable data that travels with the job for tools that read it, such as Heimdall's
    recovery policy; each launcher carries it in its scheduler's own place. `workload` describes what runs and how,
    in the selected launcher's terms, for example a Slurm batch script; the launcher rejects keys it does not know.
    """

    name: str
    workload: Mapping[str, Any]
    environment: Mapping[str, str] = EMPTY
    time_limit_minutes: int | None = None
    metadata: Mapping[str, Any] = EMPTY

    def __post_init__(self) -> None:
        object.__setattr__(self, "workload", MappingProxyType(dict(self.workload)))
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class Job:
    """A job as the scheduler reports it.

    `metadata` is the scheduler's own record, read-only, for details the typed
    fields do not cover.
    """

    job_id: str
    name: str
    state: JobState
    nodes: tuple[str, ...]
    exit_code: int | None
    restart_count: int
    submitted_at: datetime | None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default=EMPTY)


class JobNotFound(LookupError):
    """The scheduler has no record of the job."""
