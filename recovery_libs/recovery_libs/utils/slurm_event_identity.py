# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Slurm event identity at scheduler ingestion and compatibility boundaries."""

import re
from collections.abc import Callable, Sequence
from typing import Protocol

from recovery_libs.schema.watcher.model import Event


class SchedulerJob(Protocol):
    """Minimal read-only record used to establish scheduler identity."""

    @property
    def job_id(self) -> str:
        """Canonical scheduler-neutral job identifier."""
        ...

    @property
    def scheduler_metadata(self) -> dict | None:
        """Opaque scheduler-owned identity metadata."""
        ...


def _decimal(value: object, *, zero: bool = False) -> str:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"0|[1-9][0-9]*" if zero else r"[1-9][0-9]*", value) is None
        or len(value) > 10
        or int(value) >= 2**32 - 2
    ):
        raise ValueError("Slurm identity must be a canonical decimal string below the unset sentinels")
    return value


def _canonical(value: str) -> str:
    parts = value.split("_")
    if len(parts) != 2:
        raise ValueError("Slurm task identity requires an array ID and task index")
    return f"{_decimal(parts[0])}_{_decimal(parts[1], zero=True)}"


def normalize_slurm_event_identity(
    event: Event,
    lookup: Callable[[dict[str, str]], Sequence[SchedulerJob] | None],
) -> bool:
    """Resolve an event's job label in place before lookups, deduplication or audit.

    Explicit array/task pairs may replace an array-level label. Raw IDs require
    explicit provenance and a complete, unambiguous metadata lookup; a numeric
    label alone never triggers raw alias resolution. Failed lookup, conflicting
    evidence, and malformed identity leave the event untouched and return False.
    The caller retains ordinary registration redelivery for unresolved events.
    """
    if event.attributes is None:
        return False
    labels = event.attributes.model_extra or {}
    metadata = labels.get("scheduler_metadata", {})
    if not isinstance(metadata, dict):
        return False
    metadata = dict(metadata)
    for key in ("slurm_array_job_id", "slurm_array_task_id", "slurm_raw_job_id"):
        if key in labels:
            if key in metadata and metadata[key] != labels[key]:
                return False
            metadata[key] = labels[key]
    identity_keys = ("slurm_job_id", "slurm_raw_job_id", "slurm_array_job_id", "slurm_array_task_id")
    if metadata.get("scheduler") not in (None, "slurm") and not any(key in metadata for key in identity_keys):
        return True
    # Other schedulers retain opaque identifiers. Slurm-shaped labels are validated.
    if (
        not any(key in metadata for key in identity_keys)
        and metadata.get("scheduler") != "slurm"
        and not event.job_id.isdigit()
        and ("_" not in event.job_id or not event.job_id.split("_", 1)[0].isdigit())
    ):
        return True
    if metadata.get("scheduler", "slurm") != "slurm":
        return False
    try:
        label = event.job_id
        parent = metadata.get("slurm_array_job_id")
        task = metadata.get("slurm_array_task_id")
        if ("slurm_array_job_id" in metadata) != ("slurm_array_task_id" in metadata):
            return False
        candidate = _canonical(label) if "_" in label else None
        raw = _decimal(metadata["slurm_raw_job_id"]) if "slurm_raw_job_id" in metadata else None
        if "slurm_array_job_id" in metadata:
            pair = f"{_decimal(parent)}_{_decimal(task, zero=True)}"
            if candidate is not None and candidate != pair:
                return False
            if label not in (pair, parent, raw):
                return False
            candidate = pair
        declared = metadata.get("slurm_job_id")
        if "slurm_job_id" in metadata:
            if not isinstance(declared, str):
                return False
            declared = _canonical(declared) if "_" in declared else _decimal(declared)
            if candidate is not None and declared != candidate:
                return False
            if candidate is None and label != declared and label != raw:
                return False
            candidate = declared
        if raw is not None:
            records = lookup({"scheduler": "slurm", "slurm_raw_job_id": raw})
            if records is None or not records:
                return False
            identities = set()
            for record in records:
                known = record.scheduler_metadata or {}
                if known.get("scheduler") != "slurm" or known.get("slurm_raw_job_id") != raw:
                    return False
                known_parent, known_task = known.get("slurm_array_job_id"), known.get("slurm_array_task_id")
                if known_parent is not None or known_task is not None:
                    identity = f"{_decimal(known_parent)}_{_decimal(known_task, zero=True)}"
                    if identity != record.job_id:
                        return False
                else:
                    identity = _decimal(record.job_id)
                if known.get("slurm_job_id", identity) != identity:
                    return False
                identities.add(identity)
            if len(identities) != 1:
                return False
            resolved = identities.pop()
            if candidate is not None and candidate != resolved:
                return False
            if candidate is None and label != raw:
                return False
            candidate = resolved
        if candidate is None:
            candidate = _decimal(label)
        if "_" not in candidate and raw is None:
            children = lookup({"scheduler": "slurm", "slurm_array_job_id": candidate})
            if children is None or any(
                (record.scheduler_metadata or {}).get("slurm_array_job_id") == candidate for record in children
            ):
                return False
        event.attributes.job_id = candidate
        if metadata:
            event.attributes.scheduler_metadata = metadata
        return True
    except (ValueError, TypeError):
        return False
