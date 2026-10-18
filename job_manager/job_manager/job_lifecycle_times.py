# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping
from datetime import datetime
from typing import NamedTuple

from recovery_libs.storage.dataclass import SlurmJobFinalStates
from recovery_libs.storage.timestamps import normalize_mysql_timestamp

SLURM_SCHEDULER = "slurm"
SCHEDULER_KEY = "scheduler"
SLURM_SUBMIT_TIME_KEY = "slurm_submit_time"
SLURM_START_TIME_KEY = "slurm_start_time"
SLURM_END_TIME_KEY = "slurm_end_time"
SLURM_FINAL_STATES = frozenset(state.value for state in SlurmJobFinalStates)
# These states can describe an attempt that has begun executing.
SLURM_EXECUTION_START_STATES = SLURM_FINAL_STATES | {"RUNNING", "COMPLETING", "RESIZING"}

INVALID_SLURM_LIFECYCLE_TIMES_DETAIL = "invalid Slurm lifecycle time metadata"


class SlurmLifecycleTimesValidationError(ValueError):
    """Raised when Slurm-owned lifecycle time metadata is malformed."""


class JobLifecycleTimes(NamedTuple):
    submit_time: datetime | None
    start_time: datetime | None
    end_time: datetime | None


def normalize_slurm_lifecycle_time(raw_time: object, *, metadata_key: str) -> datetime:
    """Validate and parse one Slurm-owned ISO 8601 lifecycle timestamp."""
    if type(raw_time) is not str or not raw_time:
        raise SlurmLifecycleTimesValidationError(f"{metadata_key} must be a nonempty string")

    try:
        parsed = datetime.fromisoformat(raw_time)
    except ValueError as error:
        raise SlurmLifecycleTimesValidationError(f"{metadata_key} must be an ISO 8601 timestamp") from error

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SlurmLifecycleTimesValidationError(f"{metadata_key} must include a UTC offset")
    try:
        return normalize_mysql_timestamp(parsed, field_name=metadata_key)
    except (TypeError, ValueError) as error:
        raise SlurmLifecycleTimesValidationError(str(error)) from error


def project_job_lifecycle_times(
    *,
    scheduler_metadata: Mapping[str, object],
    state: str,
    submit_time: datetime | None,
    start_time: datetime | None,
    end_time: datetime | None,
) -> JobLifecycleTimes:
    """Project Slurm execution timestamps into Job Manager's typed fields."""
    if scheduler_metadata.get(SCHEDULER_KEY) != SLURM_SCHEDULER:
        return JobLifecycleTimes(submit_time, start_time, end_time)

    projected: list[datetime | None] = []
    for metadata_key, current_value in (
        (SLURM_SUBMIT_TIME_KEY, submit_time),
        (SLURM_START_TIME_KEY, start_time),
        (SLURM_END_TIME_KEY, end_time),
    ):
        # An unstarted Slurm job may report an estimated start. Suppress both
        # metadata and top-level estimates; a null partial update preserves
        # any execution start already stored for this attempt.
        if metadata_key == SLURM_START_TIME_KEY and state not in SLURM_EXECUTION_START_STATES:
            projected.append(None)
            continue
        # squeue's end_time is the scheduled time-limit deadline for an active
        # job, not its actual completion time. Only sacct-backed final records
        # may project slurm_end_time into the typed lifecycle field.
        if metadata_key == SLURM_END_TIME_KEY and state not in SLURM_FINAL_STATES:
            projected.append(current_value)
            continue
        if metadata_key not in scheduler_metadata:
            projected.append(current_value)
            continue
        projected.append(
            normalize_slurm_lifecycle_time(
                scheduler_metadata[metadata_key],
                metadata_key=metadata_key,
            )
        )

    return JobLifecycleTimes(*projected)
