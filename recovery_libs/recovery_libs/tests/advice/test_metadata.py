# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from recovery_libs.advice.metadata import (
    SCHEDULER_METADATA_SCHEDULER_KEY,
    SCHEDULER_SLURM,
    ensure_slurm_scheduler_metadata,
)


def test_ensure_slurm_scheduler_metadata_from_none():
    assert ensure_slurm_scheduler_metadata(None) == {SCHEDULER_METADATA_SCHEDULER_KEY: SCHEDULER_SLURM}


def test_ensure_slurm_scheduler_metadata_from_empty_dict():
    assert ensure_slurm_scheduler_metadata({}) == {SCHEDULER_METADATA_SCHEDULER_KEY: SCHEDULER_SLURM}


def test_ensure_slurm_scheduler_metadata_merges_existing_keys():
    metadata = {"namespace": "training", "kind": "MPIJob"}
    assert ensure_slurm_scheduler_metadata(metadata) == {
        "namespace": "training",
        "kind": "MPIJob",
        SCHEDULER_METADATA_SCHEDULER_KEY: SCHEDULER_SLURM,
    }


def test_ensure_slurm_scheduler_metadata_preserves_slurm_scheduler():
    metadata = {SCHEDULER_METADATA_SCHEDULER_KEY: SCHEDULER_SLURM, "managed_by": "slurm-advisor-controller"}
    assert ensure_slurm_scheduler_metadata(metadata) == metadata


def test_ensure_slurm_scheduler_metadata_overwrites_non_slurm_scheduler():
    metadata = {SCHEDULER_METADATA_SCHEDULER_KEY: "kubernetes"}
    assert ensure_slurm_scheduler_metadata(metadata)[SCHEDULER_METADATA_SCHEDULER_KEY] == SCHEDULER_SLURM
