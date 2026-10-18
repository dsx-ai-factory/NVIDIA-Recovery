# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import math
from collections.abc import Mapping
from typing import NamedTuple

SLURM_SCHEDULER = "slurm"
SCHEDULER_KEY = "scheduler"

SLURM_CLUSTER_KEY = "slurm_cluster"
SLURM_PARTITION_KEY = "slurm_partition"
SLURM_USER_KEY = "slurm_user"
SLURM_COMMAND_KEY = "slurm_command"
SLURM_REASON_KEY = "slurm_reason"
SLURM_NODE_COUNT_KEY = "slurm_node_count"
SLURM_GPU_COUNT_KEY = "slurm_total_gpu_count"
SLURM_CPU_COUNT_KEY = "slurm_total_cpu_count"
SLURM_MEMORY_MB_KEY = "slurm_total_memory_mb"

INVALID_SLURM_JOB_METADATA_DETAIL = "invalid Slurm job metadata"
MAX_PERSISTED_COUNT = 2**31 - 1


class SlurmJobMetadataValidationError(ValueError):
    """Raised when Slurm-owned job metadata cannot populate typed storage fields."""


class JobSchedulerFields(NamedTuple):
    """Typed Job Manager fields projected from scheduler-owned metadata."""

    cluster: str | None
    partition: str | None
    user: str | None
    command: str | None
    state_reason: str | None
    node_count: int | None
    total_gpu_count: int | None
    total_cpu_count: int | None
    total_memory_mb: int | None


def _project_string(metadata: Mapping[str, object], key: str, current: str | None) -> str | None:
    if key not in metadata:
        return current
    value = metadata[key]
    if type(value) is not str or not value:
        raise SlurmJobMetadataValidationError(f"{key} must be a nonempty string")
    return value


def _project_count(metadata: Mapping[str, object], key: str, current: int | None) -> int | None:
    if key not in metadata:
        return current
    value = metadata[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SlurmJobMetadataValidationError(f"{key} must be an integer")
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        raise SlurmJobMetadataValidationError(f"{key} must be an integer")
    normalized = int(value)
    if normalized < 0 or normalized > MAX_PERSISTED_COUNT:
        raise SlurmJobMetadataValidationError(f"{key} is outside the persisted integer range")
    return normalized


def project_job_scheduler_fields(
    *,
    scheduler_metadata: Mapping[str, object],
    cluster: str | None,
    partition: str | None,
    user: str | None,
    command: str | None,
    state_reason: str | None,
    node_count: int | None,
    total_gpu_count: int | None,
    total_cpu_count: int | None,
    total_memory_mb: int | None,
) -> JobSchedulerFields:
    """Project validated Slurm metadata into Job Manager's typed persistence fields."""
    current = JobSchedulerFields(
        cluster,
        partition,
        user,
        command,
        state_reason,
        node_count,
        total_gpu_count,
        total_cpu_count,
        total_memory_mb,
    )
    if scheduler_metadata.get(SCHEDULER_KEY) != SLURM_SCHEDULER:
        return current

    return JobSchedulerFields(
        cluster=_project_string(scheduler_metadata, SLURM_CLUSTER_KEY, cluster),
        partition=_project_string(scheduler_metadata, SLURM_PARTITION_KEY, partition),
        user=_project_string(scheduler_metadata, SLURM_USER_KEY, user),
        command=_project_string(scheduler_metadata, SLURM_COMMAND_KEY, command),
        state_reason=_project_string(scheduler_metadata, SLURM_REASON_KEY, state_reason),
        node_count=_project_count(scheduler_metadata, SLURM_NODE_COUNT_KEY, node_count),
        total_gpu_count=_project_count(scheduler_metadata, SLURM_GPU_COUNT_KEY, total_gpu_count),
        total_cpu_count=_project_count(scheduler_metadata, SLURM_CPU_COUNT_KEY, total_cpu_count),
        total_memory_mb=_project_count(scheduler_metadata, SLURM_MEMORY_MB_KEY, total_memory_mb),
    )
