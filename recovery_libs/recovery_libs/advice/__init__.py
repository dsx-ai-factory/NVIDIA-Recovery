# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared advice generation helpers for the Job Manager."""

from recovery_libs.advice.fact import build_fact_attribution, build_fact_issues
from recovery_libs.advice.metadata import (
    SCHEDULER_METADATA_SCHEDULER_KEY,
    SCHEDULER_SLURM,
    ensure_slurm_scheduler_metadata,
)
from recovery_libs.advice.nodes import compute_exclusion_nodes, compute_sticky_exclusion_nodes
from recovery_libs.advice.requeue import check_max_requeue_reached
from recovery_libs.advice.terminal import build_terminal_advice, map_advice_state

__all__ = [
    "SCHEDULER_METADATA_SCHEDULER_KEY",
    "SCHEDULER_SLURM",
    "build_fact_attribution",
    "build_fact_issues",
    "build_terminal_advice",
    "check_max_requeue_reached",
    "compute_exclusion_nodes",
    "compute_sticky_exclusion_nodes",
    "ensure_slurm_scheduler_metadata",
    "map_advice_state",
]
