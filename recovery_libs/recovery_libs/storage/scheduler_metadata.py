# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scheduler-neutral preservation of immutable metadata selected by callers."""

from collections.abc import Mapping, Sequence

from recovery_libs.storage.base import RecoveryStorageInputValidationError


class SchedulerMetadataConflictError(RecoveryStorageInputValidationError):
    """An update conflicts with previously established scheduler identity."""


def preserve_scheduler_metadata(
    incoming: Mapping[str, object] | None,
    known: Mapping[str, object] | None,
    immutable_keys: Sequence[str],
) -> dict:
    """Retain omitted immutable keys and reject changes, replacing other metadata."""
    merged = dict(incoming or {})
    for key in immutable_keys:
        if known is None or key not in known:
            continue
        if key in merged and merged[key] != known[key]:
            raise SchedulerMetadataConflictError(f"scheduler metadata conflicts with established {key}")
        merged[key] = known[key]
    return merged
