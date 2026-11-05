# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Persistence of caller-selected identity across partial Job and Advice writes."""

import uuid
from datetime import UTC, datetime
from functools import partial

import pytest
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage.dataclass import AdviceRecord, SlurmJobExecutionRecord
from recovery_libs.storage.scheduler_metadata import SchedulerMetadataConflictError, preserve_scheduler_metadata


def test_task_identity_round_trip_and_conflict_atomicity(storage):
    """Sibling tasks remain separate, and partial writes cannot erase or retarget either."""
    parent_id = str(1_000_000 + uuid.uuid4().int % 1_000_000_000)
    ids = [f"{parent_id}_0", f"{parent_id}_1"]
    identities = [
        {
            "scheduler": "slurm",
            "slurm_job_id": job_id,
            "slurm_raw_job_id": str(int(parent_id) + task),
            "slurm_array_job_id": parent_id,
            "slurm_array_task_id": str(task),
        }
        for task, job_id in enumerate(ids)
    ]
    merge = partial(preserve_scheduler_metadata, immutable_keys=tuple(identities[0]))
    for job_id, identity in zip(ids, identities, strict=True):
        storage.upsert_job_record(
            SlurmJobExecutionRecord(
                job_id=job_id,
                job_attempt_index=0,
                workflow_id=parent_id,
                state="RUNNING",
                scheduler_metadata=identity,
                last_updated_at=datetime.now(UTC),
            ),
            scheduler_metadata_merge=merge,
        )
        storage.upsert_advice(
            AdviceRecord(
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                heimdall_id=generate_heimdall_job_id(job_id, 0),
                job_id=job_id,
                attempt_index=0,
                scheduler_metadata=identity,
                visible=True,
                node_inclusions=["node-a"],
            ),
            scheduler_metadata_merge=merge,
        )
    assert storage.read_job_record(ids[0], 0).heimdall_job_id != storage.read_job_record(ids[1], 0).heimdall_job_id
    for job_id, identity in zip(ids, identities, strict=True):
        storage.upsert_job_record(
            SlurmJobExecutionRecord(
                job_id=job_id,
                job_attempt_index=0,
                workflow_id=parent_id,
                state="RUNNING",
                scheduler_metadata={"diagnostic": "latest"},
                last_updated_at=datetime.now(UTC),
            ),
            scheduler_metadata_merge=merge,
        )
        storage.upsert_advice(
            AdviceRecord(
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                heimdall_id=generate_heimdall_job_id(job_id, 0),
                job_id=job_id,
                attempt_index=0,
                scheduler_metadata={"diagnostic": "latest"},
                visible=True,
                node_inclusions=["node-a"],
            ),
            scheduler_metadata_merge=merge,
        )
        job = storage.read_job_record(job_id, 0)
        advice = storage.read_advice_by_job(job_id, 0)
        assert job.scheduler_metadata == identity | {"diagnostic": "latest"}
        assert advice.scheduler_metadata == job.scheduler_metadata
        assert advice.generation == 1
        assert advice.heimdall_id == job.heimdall_job_id == generate_heimdall_job_id(job_id, 0)

        with pytest.raises(SchedulerMetadataConflictError):
            storage.upsert_job_record(
                SlurmJobExecutionRecord(
                    job_id=job_id,
                    job_attempt_index=0,
                    workflow_id=parent_id,
                    state="FAILED",
                    scheduler_metadata=identity | {"slurm_raw_job_id": "99"},
                    last_updated_at=datetime.now(UTC),
                ),
                scheduler_metadata_merge=merge,
            )
        with pytest.raises(SchedulerMetadataConflictError):
            storage.upsert_advice(
                AdviceRecord(
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                    heimdall_id=generate_heimdall_job_id(job_id, 0),
                    job_id=job_id,
                    attempt_index=0,
                    scheduler_metadata=identity | {"slurm_raw_job_id": "99"},
                    visible=True,
                    advice_state="Terminated",
                ),
                scheduler_metadata_merge=merge,
            )
        unchanged_job = storage.read_job_record(job_id, 0)
        unchanged_advice = storage.read_advice_by_job(job_id, 0)
        assert unchanged_job.state == "RUNNING"
        assert unchanged_job.scheduler_metadata == job.scheduler_metadata
        assert unchanged_job.last_updated_at == job.last_updated_at
        assert unchanged_advice.advice_state is None
        assert unchanged_advice.generation == advice.generation
        assert unchanged_advice.updated_at == advice.updated_at
        assert unchanged_advice.scheduler_metadata == advice.scheduler_metadata


@pytest.mark.parametrize("metadata", [None, {}, {"diagnostic": "latest"}])
def test_partial_update_preserves_identity_and_omitted_metadata(storage, metadata):
    """Queue updates retain an omitted map and merge identity into explicit maps."""
    job_id = f"partial-{uuid.uuid4().hex}"
    identity = {"scheduler": "slurm", "opaque_identity": "task-zero"}
    original = identity | {"environment": "cluster-a", "managed_by": "controller"}
    merge = partial(preserve_scheduler_metadata, immutable_keys=tuple(identity))
    storage.create_job_record(
        SlurmJobExecutionRecord(
            job_id=job_id,
            job_attempt_index=0,
            workflow_id=job_id,
            state="PENDING",
            scheduler_metadata=original,
            last_updated_at=datetime.now(UTC),
        )
    )
    storage.update_job_record(
        job_id,
        0,
        state="RUNNING",
        scheduler_metadata=metadata,
        scheduler_metadata_merge=merge,
    )
    persisted = storage.read_job_record(job_id, 0)
    assert persisted.state == "RUNNING"
    assert persisted.scheduler_metadata == (identity | metadata if metadata else original)
    with pytest.raises(SchedulerMetadataConflictError):
        storage.update_job_record(
            job_id,
            0,
            state="FAILED",
            scheduler_metadata={"opaque_identity": "sibling"},
            scheduler_metadata_merge=merge,
        )
    unchanged = storage.read_job_record(job_id, 0)
    assert unchanged.state == "RUNNING"
    assert unchanged.scheduler_metadata == persisted.scheduler_metadata
