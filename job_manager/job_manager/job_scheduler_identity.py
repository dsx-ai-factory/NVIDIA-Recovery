# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Slurm identity validation at Job Manager's scheduler compatibility boundary."""

import re
from collections.abc import Mapping

from recovery_libs.storage.base import RecoveryStorageInputValidationError
from recovery_libs.storage.scheduler_metadata import preserve_scheduler_metadata

# Retained identity is immutable within a task attempt; other metadata remains replaceable.
SLURM_IDENTITY_KEYS = (
    "scheduler",
    "slurm_job_id",
    "slurm_raw_job_id",
    "slurm_array_job_id",
    "slurm_array_task_id",
)


def preserve_job_scheduler_identity(
    job_id: str,
    incoming: Mapping[str, object] | None,
    *known_metadata: Mapping[str, object] | None,
) -> dict | None:
    """Validate and retain Slurm identity without interpreting other schedulers' IDs.

    Known identity is merged before validation so a partial observation can omit
    one or both array fields. Callers must invoke this before recovery side effects.
    """
    merged = dict(incoming or {})
    for known in known_metadata:
        if known and known.get("scheduler") == "slurm":
            merged = preserve_scheduler_metadata(merged, known, SLURM_IDENTITY_KEYS)
    if merged.get("scheduler") != "slurm":
        return dict(incoming) if incoming is not None else None
    if "slurm_job_id" in merged and merged["slurm_job_id"] != job_id:
        raise RecoveryStorageInputValidationError("slurm_job_id must match job_id")
    for key in ("slurm_raw_job_id", "slurm_array_job_id", "slurm_array_task_id"):
        if key not in merged:
            continue
        value = merged[key]
        pattern = r"(?:0|[1-9][0-9]*)" if key == "slurm_array_task_id" else r"[1-9][0-9]*"
        if (
            not isinstance(value, str)
            or re.fullmatch(pattern, value) is None
            or len(value) > 10
            or int(value) >= 2**32 - 2
        ):
            raise RecoveryStorageInputValidationError(f"{key} must be a canonical decimal string below Slurm sentinels")
    parent = merged.get("slurm_array_job_id")
    task = merged.get("slurm_array_task_id")
    if (parent is None) != (task is None):
        raise RecoveryStorageInputValidationError("Slurm array identity requires both parent and task IDs")
    if parent is not None and job_id != f"{parent}_{task}":
        raise RecoveryStorageInputValidationError("Slurm array identity must match job_id")
    return merged
