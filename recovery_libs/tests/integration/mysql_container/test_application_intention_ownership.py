# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Persist ownership decisions on real MySQL, including pre-registration races."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from recovery_libs.storage.base import ApplicationIntentionOwnerUnavailable, ApplicationIntentionPermissionDenied
from recovery_libs.storage.dataclass import ApplicationIntentionRecord, SlurmJobExecutionRecord
from recovery_libs.storage.mysql_storage import MySQLStorage

pytestmark = [pytest.mark.integration, pytest.mark.regression]


def _request(job_id: str, requester: str) -> ApplicationIntentionRecord:
    return ApplicationIntentionRecord(
        job_id=job_id,
        desired_state="CANCELLED",
        requested_by=requester,
        reason="training complete",
        created_at=datetime.now(UTC),
    )


def _register(storage: MySQLStorage, job_id: str, owner: str | None, attempt: int = 0) -> None:
    storage.upsert_job_record(
        SlurmJobExecutionRecord(
            job_id=job_id,
            job_attempt_index=attempt,
            workflow_id=f"workflow-{job_id}",
            state="RUNNING",
            user=owner,
            last_updated_at=datetime.now(UTC),
        )
    )


@pytest.mark.parametrize("requester", ["bob", "Alice", "alice@example.com"])
def test_known_owner_mismatch_denies_without_persisting(storage: MySQLStorage, requester: str) -> None:
    job_id = uuid.uuid4().hex
    _register(storage, job_id, "alice")

    with pytest.raises(ApplicationIntentionPermissionDenied):
        storage.create_or_get_application_intention(_request(job_id, requester))

    assert storage.read_application_intention(job_id, requester) is None


def test_known_owner_is_authorized_and_duplicate_preserves_first_reason(storage: MySQLStorage) -> None:
    job_id = uuid.uuid4().hex
    _register(storage, job_id, "alice")
    first, created = storage.create_or_get_application_intention(_request(job_id, "alice"))
    duplicate = _request(job_id, "alice")
    duplicate.reason = "changed reason"
    second, created_again = storage.create_or_get_application_intention(duplicate)

    assert created and not created_again
    assert first.id == second.id
    assert second.authorization_status == "AUTHORIZED"
    assert second.reason == "training complete"


@pytest.mark.parametrize("owner", [None, "unknown"])
def test_registered_job_without_owner_fails_closed(storage: MySQLStorage, owner: str | None) -> None:
    job_id = uuid.uuid4().hex
    _register(storage, job_id, owner)
    with pytest.raises(ApplicationIntentionOwnerUnavailable):
        storage.create_or_get_application_intention(_request(job_id, "alice"))
    assert storage.read_application_intention(job_id, "alice") is None


def test_wrong_pending_request_cannot_block_owner_and_rejection_survives_rereads(storage: MySQLStorage) -> None:
    job_id = uuid.uuid4().hex
    wrong, _ = storage.create_or_get_application_intention(_request(job_id, "bob"))
    correct, _ = storage.create_or_get_application_intention(_request(job_id, "alice"))
    assert wrong.authorization_status == correct.authorization_status == "PENDING"
    assert storage.reconcile_application_intention_authorization(job_id, None) is None

    _register(storage, job_id, "alice")
    authorized = storage.reconcile_application_intention_authorization(job_id, "alice")
    assert authorized is not None and authorized.id == correct.id
    assert authorized.authorization_status == "AUTHORIZED"
    rejected = storage.read_application_intention(job_id, "bob")
    assert rejected is not None and rejected.authorization_status == "REJECTED"
    assert rejected.reason_code == "REQUESTER_NOT_JOB_OWNER"
    assert storage.read_application_intention(job_id, "charlie") is None

    # New sessions read the durable decision; polling does not resurrect a rejection.
    assert storage.reconcile_application_intention_authorization(job_id, "bob") is None
    assert storage.read_application_intention(job_id, "bob").authorization_status == "REJECTED"
    _register(storage, job_id, "alice", attempt=1)
    assert storage.reconcile_application_intention_authorization(job_id, "alice").id == correct.id


def test_case_distinct_pending_principals_do_not_share_an_idempotency_slot(storage: MySQLStorage) -> None:
    job_id = uuid.uuid4().hex
    upper, _ = storage.create_or_get_application_intention(_request(job_id, "Alice"))
    lower, _ = storage.create_or_get_application_intention(_request(job_id, "alice"))
    assert upper.id != lower.id
    assert storage.read_application_intention(job_id, "Alice").id == upper.id
    assert storage.read_application_intention(job_id, "alice").id == lower.id


def test_concurrent_duplicate_requests_and_resolution_converge(storage: MySQLStorage) -> None:
    job_id = uuid.uuid4().hex
    with ThreadPoolExecutor(max_workers=6) as workers:
        results = list(
            workers.map(lambda _: storage.create_or_get_application_intention(_request(job_id, "alice")), range(6))
        )
    assert len({row.id for row, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    _register(storage, job_id, "alice")
    with ThreadPoolExecutor(max_workers=6) as workers:
        authorized = list(
            workers.map(lambda _: storage.reconcile_application_intention_authorization(job_id, "alice"), range(6))
        )
    assert all(row is not None and row.authorization_status == "AUTHORIZED" for row in authorized)
    claimed, first = storage.claim_application_intention_audit_attempt(job_id, 0)
    assert first and claimed is not None and claimed.audit_attempt_index == 0
    assert storage.claim_application_intention_audit_attempt(job_id, 1) == (None, False)


def test_legacy_intention_without_verified_requester_never_becomes_actionable(storage: MySQLStorage) -> None:
    job_id = uuid.uuid4().hex
    legacy = _request(job_id, "alice")
    legacy.requested_by = None
    with storage.connection.write_session() as session:
        session.add(legacy)
        session.commit()
    assert storage.reconcile_application_intention_authorization(job_id, "alice") is None
    _register(storage, job_id, "alice")
    owner_request, created = storage.create_or_get_application_intention(_request(job_id, "alice"))
    assert created and owner_request.authorization_status == "AUTHORIZED"
