# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scheduler metadata filtering against real MySQL JSON and pagination."""

import uuid
from datetime import UTC, datetime

from recovery_libs.storage.dataclass import SlurmJobExecutionRecord


def test_metadata_filter_keeps_exact_task_identity_and_counts_before_pagination(storage):
    parent = str(uuid.uuid4().int % 1_000_000_000 + 1)
    marker = str(uuid.uuid4())
    for index in (0, 1):
        for attempt in (0, 1):
            storage.upsert_job_record(
                SlurmJobExecutionRecord(
                    job_id=f"{parent}_{index}",
                    job_attempt_index=attempt,
                    workflow_id=marker,
                    last_updated_at=datetime.now(UTC),
                    scheduler_metadata={
                        "scheduler": "slurm",
                        "slurm_array_job_id": parent,
                        "slurm_raw_job_id": str(int(parent) + index),
                        "lookup_marker": marker,
                    },
                )
            )
    # A JSON number is not a match for a string identity, even if its digits agree.
    storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=f"numeric-{marker}",
            job_attempt_index=0,
            workflow_id=marker,
            last_updated_at=datetime.now(UTC),
            scheduler_metadata={"lookup_marker": marker, "slurm_raw_job_id": int(parent)},
        )
    )
    records, count = storage.read_job_records(
        scheduler_metadata={"lookup_marker": marker, "slurm_raw_job_id": parent},
        limit=1,
    )
    assert count == 2
    assert len(records) == 1
    assert records[0].job_id == f"{parent}_0"
    records, count = storage.read_job_records(
        scheduler_metadata={"lookup_marker": marker, "slurm_array_job_id": parent},
        limit=10,
    )
    assert count == 4
    assert {record.job_id for record in records} == {f"{parent}_0", f"{parent}_1"}
    assert storage.read_job_records(scheduler_metadata={"lookup_marker": marker, "missing": parent}) == ([], 0)
