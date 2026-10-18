# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

SCHEDULER_METADATA_SCHEDULER_KEY = "scheduler"
SCHEDULER_SLURM = "slurm"


def ensure_slurm_scheduler_metadata(metadata: dict | None) -> dict:
    """Ensure advice carries scheduler=slurm for slurm-advisor-controller ownership."""
    result = dict(metadata or {})
    result[SCHEDULER_METADATA_SCHEDULER_KEY] = SCHEDULER_SLURM
    return result
