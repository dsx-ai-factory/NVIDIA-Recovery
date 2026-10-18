# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Mapping

SLURM_SCHEDULER = "slurm"
SCHEDULER_KEY = "scheduler"
SLURM_LOG_FILE_PATHS_KEY = "slurm_log_file_paths"

MAX_SLURM_LOG_FILE_PATHS = 256
MAX_SLURM_LOG_FILE_PATH_BYTES = 4096

INVALID_SLURM_LOG_FILE_PATHS_DETAIL = "invalid Slurm log file paths metadata"


class SlurmLogFilePathsValidationError(ValueError):
    """Raised when Slurm-owned log file path metadata is malformed."""


def normalize_slurm_log_file_paths(raw_paths: object) -> list[str] | None:
    """Validate and deterministically normalize Slurm-owned log file paths."""
    if not isinstance(raw_paths, list):
        raise SlurmLogFilePathsValidationError("Slurm log file paths must be a list")
    if len(raw_paths) > MAX_SLURM_LOG_FILE_PATHS:
        raise SlurmLogFilePathsValidationError(f"Slurm log file path count exceeds {MAX_SLURM_LOG_FILE_PATHS}")

    unique_paths: set[str] = set()
    for index, path in enumerate(raw_paths):
        if type(path) is not str:
            raise SlurmLogFilePathsValidationError(f"Slurm log file path at index {index} must be a string")
        if not path:
            raise SlurmLogFilePathsValidationError(f"Slurm log file path at index {index} is empty")
        if "\x00" in path:
            raise SlurmLogFilePathsValidationError(f"Slurm log file path at index {index} contains NUL")
        try:
            encoded_path = path.encode("utf-8")
        except UnicodeEncodeError as error:
            raise SlurmLogFilePathsValidationError(
                f"Slurm log file path at index {index} is not valid UTF-8"
            ) from error
        if len(encoded_path) > MAX_SLURM_LOG_FILE_PATH_BYTES:
            raise SlurmLogFilePathsValidationError(
                f"Slurm log file path at index {index} exceeds {MAX_SLURM_LOG_FILE_PATH_BYTES} bytes"
            )
        unique_paths.add(path)

    return sorted(unique_paths) or None


def project_job_log_file_paths(
    *,
    scheduler_metadata: Mapping[str, object],
    log_file_paths: list[str] | None,
) -> list[str] | None:
    """Project scheduler metadata into Job Manager's legacy log-file field."""
    if scheduler_metadata.get(SCHEDULER_KEY) == SLURM_SCHEDULER:
        if SLURM_LOG_FILE_PATHS_KEY not in scheduler_metadata:
            return log_file_paths
        return normalize_slurm_log_file_paths(scheduler_metadata[SLURM_LOG_FILE_PATHS_KEY])
    return log_file_paths


def needs_heimdall_job_id_placeholder(
    *,
    scheduler_metadata: Mapping[str, object],
    log_file_paths: list[str] | None,
) -> bool:
    """Return whether a legacy Heimdall-ID placeholder must be populated."""
    return scheduler_metadata.get(SCHEDULER_KEY) != SLURM_SCHEDULER and log_file_paths is None
