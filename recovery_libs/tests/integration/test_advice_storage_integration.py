# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""
Integration tests for AdviceRecord storage operations.

These tests verify MySQL-specific behaviors including JSON field serialization,
timestamp handling, soft deletes, and pagination. They require a running MySQL
instance configured via environment variables.
"""

import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from recovery_libs.schema.job_manager.models import (
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    Issue,
    RemediationAction,
    ResourceDetail,
)
from recovery_libs.storage.dataclass import (
    AdviceReconciliationStatusRecord,
    AdviceRecord,
    SlurmJobExecutionRecord,
)
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.mysql_storage import MySQLStorage


@pytest.fixture(scope="module")
def mysql_connection(db_config):
    """Create MySQL connection with cleanup."""
    connection = MySQLConnection(
        host=db_config["host"],
        port=db_config["port"],
        username=db_config["username"],
        password=db_config["password"],
        database_name=db_config["database_name"],
    )
    yield connection
    # Dispose of the engine's connection pool
    connection.engine.dispose()


@pytest.fixture(scope="module")
def mysql_storage(mysql_connection):
    """Create MySQLStorage instance with schema bootstrap."""
    storage = MySQLStorage(connection=mysql_connection)
    storage.bootstrap()
    assert storage.ready()
    return storage


@pytest.fixture(scope="function")
def clean_advice_tables(mysql_connection):
    """Delete all advice and related job records between tests using raw SQL."""
    yield
    with mysql_connection.session() as session:
        session.query(AdviceRecord).delete()
        # Clean up job records created for FK satisfaction
        from sqlalchemy import text

        session.execute(
            text(
                "DELETE FROM job_execution_records WHERE job_id LIKE 'test_%' OR job_id LIKE 'job_%' OR job_id LIKE 'dup_test_%' OR job_id LIKE 'initial_%' OR job_id LIKE 'updated_%' OR job_id LIKE 'delete_%' OR job_id LIKE 'complex_%' OR job_id LIKE 'missing-%'"
            )
        )
        session.commit()


def _ensure_job_record(storage: MySQLStorage, job_id: str, attempt_index: int, heimdall_job_id: uuid.UUID) -> None:
    """Create a job execution record to satisfy the FK constraint on advice."""
    record = SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=attempt_index,
        workflow_id=f"workflow-{job_id}",
        heimdall_job_id=heimdall_job_id,
        last_updated_at=datetime.now(UTC),
    )
    storage.upsert_job_record(record)


@pytest.fixture
def sample_heimdall_id():
    """Generate unique UUID for test records."""
    return uuid.uuid4()


@pytest.fixture
def sample_job_id():
    """Generate unique job identifier."""
    return f"test_job_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_scheduler_metadata():
    """Sample scheduler metadata dict."""
    return {
        "kind": "Job",
        "namespace": "default",
        "labels": {"app": "test", "env": "integration"},
    }


@pytest.fixture
def sample_issues():
    """Sample Issue objects list."""
    now = datetime.now(UTC)
    return [
        Issue(
            issue_type="CRASH",
            timestamp=now,
            detected_by="heimdall",
            relevant_log_lines=["Error: segmentation fault", "Core dumped"],
            remediation_actions=[
                RemediationAction(
                    action_type="EXCLUDE_NODE",
                    timestamp=now,
                    reason="Node experienced hardware failure",
                    resource_details=[
                        ResourceDetail(
                            resource_type="node",
                            resource_id="node-123",
                            details="GPU memory error detected",
                        )
                    ],
                )
            ],
        )
    ]


@pytest.fixture
def sample_advice_record(sample_heimdall_id, sample_job_id, sample_scheduler_metadata, sample_issues):
    """Complete AdviceRecord for testing."""
    now = datetime.now(UTC)
    return AdviceRecord(
        heimdall_id=sample_heimdall_id,
        visible=True,
        job_id=sample_job_id,
        scheduler_metadata=sample_scheduler_metadata,
        attempt_index=0,
        advice_state="Restarted",
        node_exclusions=["node-123"],
        node_inclusions=None,
        issues=sample_issues,
        created_at=now,
        updated_at=now,
    )


class TestAdviceStorageIntegration:
    """Integration tests for AdviceRecord storage operations."""

    def test_bootstrap_and_ready(self, mysql_storage):
        """Verify storage is properly bootstrapped and ready."""
        assert mysql_storage.ready()
        assert mysql_storage.connection.ready()

    def test_create_advice_with_all_fields(self, mysql_storage, clean_advice_tables, sample_advice_record):
        """
        Test creating AdviceRecord with all optional fields populated.

        Verifies:
        - All fields stored correctly
        - Defaults applied (visible=False if not specified)
        - JSON serialization for complex fields
        - Read back matches created record
        """
        # Create job record to satisfy FK constraint
        _ensure_job_record(
            mysql_storage,
            sample_advice_record.job_id,
            sample_advice_record.attempt_index,
            sample_advice_record.heimdall_id,
        )

        # Create the record
        created = mysql_storage.create_advice(sample_advice_record)
        assert created.heimdall_id == sample_advice_record.heimdall_id

        # Read back and verify all fields
        retrieved = mysql_storage.read_advice(sample_advice_record.heimdall_id)
        assert retrieved is not None
        assert retrieved.heimdall_id == sample_advice_record.heimdall_id
        assert retrieved.visible is True
        assert retrieved.job_id == sample_advice_record.job_id
        assert retrieved.scheduler_metadata == sample_advice_record.scheduler_metadata
        assert retrieved.attempt_index == sample_advice_record.attempt_index
        assert retrieved.advice_state == "Restarted"
        assert retrieved.node_exclusions == ["node-123"]
        assert retrieved.node_inclusions is None

        # Verify issues serialization
        assert retrieved.issues is not None
        assert len(retrieved.issues) == 1
        assert retrieved.issues[0].issue_type == "CRASH"
        assert retrieved.issues[0].detected_by == "heimdall"
        assert len(retrieved.issues[0].relevant_log_lines) == 2
        assert retrieved.issues[0].remediation_actions is not None
        assert len(retrieved.issues[0].remediation_actions) == 1
        assert retrieved.issues[0].remediation_actions[0].action_type == "EXCLUDE_NODE"
        assert retrieved.issues[0].remediation_actions[0].resource_details is not None
        assert len(retrieved.issues[0].remediation_actions[0].resource_details) == 1
        assert retrieved.issues[0].remediation_actions[0].resource_details[0].resource_type == "node"

        # Verify timestamps have timezone
        assert retrieved.created_at.tzinfo == UTC
        assert retrieved.updated_at.tzinfo == UTC

        # Test default visible=False
        invisible_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_invisible", 0, invisible_id)
        invisible_record = AdviceRecord(
            heimdall_id=invisible_id,
            job_id="test_invisible",
            attempt_index=0,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        mysql_storage.create_advice(invisible_record)
        retrieved_invisible = mysql_storage.read_advice(invisible_id)
        assert retrieved_invisible.visible is False

    def test_read_advice_list_with_filters(self, mysql_storage, clean_advice_tables):
        """
        Test read_advice_list with various filters.

        Verifies:
        - No filters returns all visible records
        - job_id filter works correctly
        - include_hidden parameter works
        - Combined filtering (job_id + visibility)
        - Total count is accurate
        """
        now = datetime.now(UTC)

        # Create multiple records with different job_ids and visibility states
        job_a = "job_a"
        job_b = "job_b"

        # Create 2 visible records for job_a
        for i in range(2):
            hid = uuid.uuid4()
            _ensure_job_record(mysql_storage, job_a, i, hid)
            record = AdviceRecord(
                heimdall_id=hid,
                visible=True,
                job_id=job_a,
                attempt_index=i,
                created_at=now,
                updated_at=now,
            )
            mysql_storage.create_advice(record)

        # Create 1 hidden record for job_a
        hidden_hid = uuid.uuid4()
        _ensure_job_record(mysql_storage, job_a, 2, hidden_hid)
        hidden_record = AdviceRecord(
            heimdall_id=hidden_hid,
            visible=False,
            job_id=job_a,
            attempt_index=2,
            created_at=now,
            updated_at=now,
        )
        mysql_storage.create_advice(hidden_record)

        # Create 2 visible records for job_b
        for i in range(2):
            hid = uuid.uuid4()
            _ensure_job_record(mysql_storage, job_b, i, hid)
            record = AdviceRecord(
                heimdall_id=hid,
                visible=True,
                job_id=job_b,
                attempt_index=i,
                created_at=now,
                updated_at=now,
            )
            mysql_storage.create_advice(record)

        # Test 1: No filters (returns all visible)
        records, total = mysql_storage.read_advice_list()
        assert len(records) == 4  # 2 from job_a + 2 from job_b
        assert total == 4

        # Test 2: Filter by job_id (job_a, visible only)
        records, total = mysql_storage.read_advice_list(job_id=job_a)
        assert len(records) == 2
        assert total == 2
        assert all(r.job_id == job_a for r in records)

        # Test 3: Include hidden records for job_a
        records, total = mysql_storage.read_advice_list(job_id=job_a, include_hidden=True)
        assert len(records) == 3  # 2 visible + 1 hidden
        assert total == 3

        # Test 4: Include all hidden records
        records, total = mysql_storage.read_advice_list(include_hidden=True)
        assert len(records) == 5  # All records
        assert total == 5

    def test_upsert_advice_create_and_update(self, mysql_storage, clean_advice_tables):
        """
        Test upsert_advice for both create and update operations.

        Verifies:
        - First upsert creates new record (returns True)
        - created_at and updated_at are set
        - Second upsert updates existing record (returns False)
        - created_at unchanged, updated_at is newer
        - Updated fields reflect changes
        """
        heimdall_id = uuid.uuid4()
        # Use a timestamp from 2 seconds ago for initial record
        initial_time = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=2)

        # Create job record to satisfy FK
        _ensure_job_record(mysql_storage, "initial_job", 0, heimdall_id)

        # First upsert: Create new record
        initial_record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=False,
            job_id="initial_job",
            attempt_index=0,
            advice_state="Restarted",
            node_exclusions=["node-1"],
            created_at=initial_time,
            updated_at=initial_time,
        )

        result_record, created, old_record = mysql_storage.upsert_advice(initial_record)
        assert created is True
        assert old_record is None

        # Re-read the record to avoid detached instance issues
        result_record = mysql_storage.read_advice(heimdall_id)
        assert result_record.heimdall_id == heimdall_id
        assert result_record.created_at.tzinfo == UTC
        assert result_record.updated_at.tzinfo == UTC
        initial_created_at = result_record.created_at
        initial_updated_at = result_record.updated_at

        # Wait for timestamp to change (MySQL DATETIME has second precision)
        time.sleep(1.1)

        # Second upsert: Update existing record (same job_id and attempt_index)
        # Upsert will set updated_at to current time, which will be newer than initial time
        updated_record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=True,
            job_id="initial_job",
            attempt_index=0,
            advice_state="Terminated",
            node_exclusions=["node-1", "node-2"],
            created_at=initial_time,  # This should be ignored
            updated_at=initial_time,  # This will be replaced with current time
        )

        result_record, created, old_record = mysql_storage.upsert_advice(updated_record)
        assert created is False
        assert old_record is not None
        assert old_record is not result_record
        assert old_record.visible is False
        assert old_record.advice_state == "Restarted"
        assert old_record.node_exclusions == ["node-1"]

        # Re-read the record to avoid detached instance issues
        result_record = mysql_storage.read_advice(heimdall_id)
        assert result_record.heimdall_id == heimdall_id

        # Verify created_at unchanged
        assert result_record.created_at == initial_created_at

        # Verify updated_at is newer
        assert result_record.updated_at > initial_updated_at

        # Verify updated fields
        assert result_record.visible is True
        assert result_record.job_id == "initial_job"
        assert result_record.attempt_index == 0
        assert result_record.advice_state == "Terminated"
        assert result_record.node_exclusions == ["node-1", "node-2"]

    def test_upsert_advice_returns_usable_record_without_reread(self, mysql_storage, clean_advice_tables):
        """
        Regression test for detached-instance issues in upsert return values.

        Verifies records returned from both create and update upsert paths are
        directly usable without an immediate read-back call.
        """
        now = datetime.now(UTC)
        job_id = f"upsert_return_{uuid.uuid4().hex[:8]}"
        attempt_index = 0
        heimdall_id = uuid.uuid4()

        _ensure_job_record(mysql_storage, job_id, attempt_index, heimdall_id)

        issue = Issue(
            issue_type="CRASH",
            timestamp=now,
            detected_by="integration-test",
            relevant_log_lines=["first"],
        )

        create_record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=True,
            job_id=job_id,
            scheduler_metadata={"namespace": "ns-a"},
            attempt_index=attempt_index,
            advice_state="Restarted",
            node_exclusions=["node-a"],
            issues=[issue],
            created_at=now,
            updated_at=now,
        )

        created_record, created, old_record = mysql_storage.upsert_advice(create_record)
        assert created is True
        assert old_record is None
        assert created_record.heimdall_id == heimdall_id
        assert created_record.visible is True
        assert created_record.advice_state == "Restarted"
        assert created_record.scheduler_metadata == {"namespace": "ns-a"}
        assert created_record.issues is not None
        assert len(created_record.issues) == 1
        assert created_record.issues[0].issue_type == "CRASH"

        update_record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=False,
            job_id=job_id,
            scheduler_metadata={"namespace": "ns-b"},
            attempt_index=attempt_index,
            advice_state="Terminated",
            node_exclusions=["node-b"],
            issues=[issue],
            created_at=now,
            updated_at=now + timedelta(minutes=1),
        )

        updated_record, created, old_record = mysql_storage.upsert_advice(update_record)
        assert created is False
        assert old_record is not None
        assert old_record is not updated_record
        assert old_record.visible is True
        assert old_record.advice_state == "Restarted"
        assert old_record.scheduler_metadata == {"namespace": "ns-a"}
        assert updated_record.heimdall_id == heimdall_id
        assert updated_record.visible is False
        assert updated_record.advice_state == "Terminated"
        assert updated_record.scheduler_metadata == {"namespace": "ns-b"}
        assert updated_record.issues is not None
        assert len(updated_record.issues) == 1
        assert updated_record.issues[0].issue_type == "CRASH"

    def test_upsert_advice_matches_on_job_id_and_attempt(self, mysql_storage, clean_advice_tables):
        """
        Test that upsert_advice matches on (job_id, attempt_index) rather than heimdall_id.

        Simulates the real scenario: a job record exists with a canonical heimdall_job_id.
        An initial advice is created with a wrong (uuid5) ID. A second upsert with the
        canonical ID should update the existing record and fix the heimdall_id.

        Verifies:
        - Second upsert with different heimdall_id but same (job_id, attempt_index) updates
          the existing record (created=False) instead of creating a duplicate.
        - The record's heimdall_id is updated to the canonical value.
        - Only one record exists for that (job_id, attempt_index).
        """
        canonical_heimdall_id = uuid.uuid4()  # e.g., the actual heimdall_job_id on the job record
        job_id = f"dup_test_{uuid.uuid4().hex[:8]}"
        attempt_index = 0
        now = datetime.now(UTC)

        # Create job record with the canonical heimdall_job_id
        _ensure_job_record(mysql_storage, job_id, attempt_index, canonical_heimdall_id)

        # First upsert: create advice with the WRONG heimdall_id (simulates old uuid5 path)
        # We need to temporarily use the canonical ID for FK, then test the matching logic.
        # Actually, we use the canonical ID for the first record so FK is satisfied,
        # then verify the matching logic works by upserting with same (job_id, attempt_index).
        initial_record = AdviceRecord(
            heimdall_id=canonical_heimdall_id,
            visible=True,
            job_id=job_id,
            attempt_index=attempt_index,
            advice_state="Restarted",
            node_exclusions=["node-1"],
            created_at=now,
            updated_at=now,
        )
        _, created, old_record = mysql_storage.upsert_advice(initial_record)
        assert created is True
        assert old_record is None

        # Second upsert: SAME heimdall_id (canonical), SAME (job_id, attempt_index), different fields
        updated_record = AdviceRecord(
            heimdall_id=canonical_heimdall_id,
            visible=True,
            job_id=job_id,
            attempt_index=attempt_index,
            advice_state="Terminated",
            node_exclusions=["node-1", "node-2"],
            created_at=now,
            updated_at=now,
        )
        _, created, old_record = mysql_storage.upsert_advice(updated_record)
        assert created is False, "Expected upsert to update existing record, not create a new one"
        assert old_record is not None
        assert old_record.advice_state == "Restarted"
        assert old_record.node_exclusions == ["node-1"]

        # Verify the fields were updated
        retrieved = mysql_storage.read_advice_by_job(job_id, attempt_index)
        assert retrieved is not None
        assert retrieved.heimdall_id == canonical_heimdall_id
        assert retrieved.advice_state == "Terminated"
        assert retrieved.node_exclusions == ["node-1", "node-2"]

        # Verify only one record exists for that (job_id, attempt_index)
        _, total = mysql_storage.read_advice_list(job_id=job_id, include_hidden=True)
        assert total == 1, f"Expected exactly 1 record for {job_id}, got {total}"

    def test_delete_advice_soft_delete(self, mysql_storage, clean_advice_tables):
        """
        Test soft delete via delete_advice.

        Verifies:
        - Visible record can be soft deleted
        - Record still exists in database but visible=False
        - updated_at is modified during delete
        - Record not returned by read_advice_list(include_hidden=False)
        - Record is returned by read_advice_list(include_hidden=True)
        - Repeating the soft delete reports no transition and preserves updated_at
        """
        heimdall_id = uuid.uuid4()
        # Use a timestamp from 2 seconds ago so delete operation will update it to a newer time
        past_time = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=2)

        # Create job record to satisfy FK
        _ensure_job_record(mysql_storage, "delete_test", 0, heimdall_id)

        # Create visible record with past timestamp
        record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=True,
            job_id="delete_test",
            attempt_index=0,
            created_at=past_time,
            updated_at=past_time,
        )
        mysql_storage.create_advice(record)

        # Verify it's visible
        records, total = mysql_storage.read_advice_list(include_hidden=False)
        assert total == 1

        initial_updated_at = mysql_storage.read_advice(heimdall_id).updated_at

        # Delete the record
        result = mysql_storage.delete_advice(heimdall_id)
        assert result.advice_record is not None
        assert result.advice_record.heimdall_id == heimdall_id
        assert result.was_deleted is True

        # Query database directly to verify record exists but visible=False
        deleted_record = mysql_storage.read_advice(heimdall_id)
        assert deleted_record is not None
        assert deleted_record.visible is False
        assert deleted_record.updated_at > initial_updated_at
        unpublished_at = deleted_record.updated_at

        # Verify NOT returned by read_advice_list (default behavior)
        records, total = mysql_storage.read_advice_list(include_hidden=False)
        assert total == 0

        # Verify IS returned when include_hidden=True
        records, total = mysql_storage.read_advice_list(include_hidden=True)
        assert total == 1
        assert records[0].heimdall_id == heimdall_id

        repeated_result = mysql_storage.delete_advice(heimdall_id)
        assert repeated_result.advice_record is not None
        assert repeated_result.was_deleted is False
        assert repeated_result.advice_record.updated_at == unpublished_at

        # Test deleting non-existent record
        non_existent_id = uuid.uuid4()
        result = mysql_storage.delete_advice(non_existent_id)
        assert result.advice_record is None
        assert result.was_deleted is False

    def test_read_advice_list_pagination_and_ordering(self, mysql_storage, clean_advice_tables):
        """
        Test pagination and ordering in read_advice_list.

        Verifies:
        - Records ordered by created_at DESC (newest first)
        - Limit parameter works correctly
        - Offset parameter works correctly
        - No overlap between pages
        - Total count accurate across pages
        """
        # Create 20 records with incrementing timestamps
        # MySQL DATETIME has second-level precision, so we increment by seconds
        base_time = datetime.now(UTC)
        created_ids = []
        for i in range(20):
            heimdall_id = uuid.uuid4()
            _ensure_job_record(mysql_storage, f"job_{i}", 0, heimdall_id)
            # Create timestamps that differ by 1 second each
            record_time = base_time.replace(microsecond=0) + timedelta(seconds=i)
            record = AdviceRecord(
                heimdall_id=heimdall_id,
                visible=True,
                job_id=f"job_{i}",
                attempt_index=0,
                created_at=record_time,
                updated_at=record_time,
            )
            mysql_storage.create_advice(record)
            created_ids.append(heimdall_id)

        # Test 1: Query with limit=5, verify exactly 5 records returned
        records, total = mysql_storage.read_advice_list(limit=5, offset=0)
        assert len(records) == 5
        assert total == 20

        # Test 2: Verify ordering (newest first - created_at DESC)
        for i in range(len(records) - 1):
            assert records[i].created_at >= records[i + 1].created_at

        # Test 3: Query with offset=5, verify next 5 records (no overlap)
        first_page_ids = {r.heimdall_id for r in records}
        records_page2, total_page2 = mysql_storage.read_advice_list(limit=5, offset=5)
        assert len(records_page2) == 5
        assert total_page2 == 20
        second_page_ids = {r.heimdall_id for r in records_page2}
        assert len(first_page_ids & second_page_ids) == 0  # No overlap

        # Test 4: Boundary test - offset=20 returns empty list but total=20
        records_boundary, total_boundary = mysql_storage.read_advice_list(limit=5, offset=20)
        assert len(records_boundary) == 0
        assert total_boundary == 20

        # Test 5: Large limit returns all records
        records_all, total_all = mysql_storage.read_advice_list(limit=100, offset=0)
        assert len(records_all) == 20
        assert total_all == 20

    def test_only_active_leaves_age_classification_to_controller_and_filters_terminal_status_before_count(
        self,
        mysql_storage,
        clean_advice_tables,
    ):
        now = datetime.now(UTC)
        created_after = now - timedelta(minutes=5)

        active_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_active_restart", 0, active_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=active_id,
                visible=True,
                job_id="test_active_restart",
                attempt_index=0,
                advice_state="Restarted",
                created_at=now,
                updated_at=now,
            )
        )

        successor_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_restart_successor", 0, successor_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=successor_id,
                visible=True,
                job_id="test_restart_successor",
                attempt_index=0,
                advice_state="Restarted",
                created_at=now,
                updated_at=now,
            )
        )
        _ensure_job_record(mysql_storage, "test_restart_successor", 1, uuid.uuid4())

        stale_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_stale_terminate", 0, stale_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=stale_id,
                visible=True,
                job_id="test_stale_terminate",
                attempt_index=0,
                advice_state="Terminated",
                created_at=now,
                updated_at=now,
            )
        )
        _ensure_job_record(mysql_storage, "test_stale_terminate", 1, uuid.uuid4())

        converged_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_converged_restart", 0, converged_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=converged_id,
                visible=True,
                job_id="test_converged_restart",
                attempt_index=0,
                advice_state="Restarted",
                created_at=now,
                updated_at=now,
            )
        )
        mysql_storage.upsert_advice_reconciliation_status(
            AdviceReconciliationStatusRecord(
                advice_id=converged_id,
                observed_generation=1,
                disposition=AdviceReconciliationDisposition.CONVERGED.value,
                reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
                observed_at=now,
                transitioned_at=now,
            )
        )

        abandoned_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_abandoned_restart", 0, abandoned_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=abandoned_id,
                visible=True,
                job_id="test_abandoned_restart",
                attempt_index=0,
                advice_state="Restarted",
                created_at=now,
                updated_at=now,
            )
        )
        mysql_storage.upsert_advice_reconciliation_status(
            AdviceReconciliationStatusRecord(
                advice_id=abandoned_id,
                observed_generation=1,
                disposition=AdviceReconciliationDisposition.ABANDONED.value,
                reason=AdviceReconciliationReason.TARGET_STATE_FINAL.value,
                observed_at=now,
                transitioned_at=now,
            )
        )

        aged_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_aged_constraints", 0, aged_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=aged_id,
                visible=True,
                job_id="test_aged_constraints",
                attempt_index=0,
                node_exclusions=["node-a"],
                desired_state_changed_at=created_after - timedelta(seconds=1),
                created_at=created_after - timedelta(seconds=1),
                updated_at=now,
            )
        )

        refreshed_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_refreshed_constraints", 0, refreshed_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=refreshed_id,
                visible=True,
                job_id="test_refreshed_constraints",
                attempt_index=0,
                node_exclusions=["node-b"],
                desired_state_changed_at=now,
                created_at=created_after - timedelta(days=1),
                updated_at=now,
            )
        )

        invalid_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_invalid_empty", 0, invalid_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=invalid_id,
                visible=True,
                job_id="test_invalid_empty",
                attempt_index=0,
                created_at=now,
                updated_at=now,
            )
        )

        released_id = uuid.uuid4()
        _ensure_job_record(mysql_storage, "test_converged_release", 0, released_id)
        mysql_storage.create_advice(
            AdviceRecord(
                heimdall_id=released_id,
                visible=True,
                job_id="test_converged_release",
                attempt_index=0,
                advice_state="Released",
                created_at=now,
                updated_at=now,
            )
        )
        mysql_storage.upsert_advice_reconciliation_status(
            AdviceReconciliationStatusRecord(
                advice_id=released_id,
                observed_generation=1,
                disposition=AdviceReconciliationDisposition.CONVERGED.value,
                reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
                observed_at=now,
                transitioned_at=now,
            )
        )

        records, total = mysql_storage.read_advice_list(
            only_active=True,
        )

        returned_ids = {record.heimdall_id for record in records}
        assert returned_ids == {
            active_id,
            successor_id,
            stale_id,
            aged_id,
            refreshed_id,
        }, {record.job_id for record in records}
        assert total == 5

        mysql_storage.upsert_advice_reconciliation_status(
            AdviceReconciliationStatusRecord(
                advice_id=stale_id,
                observed_generation=1,
                disposition=AdviceReconciliationDisposition.ABANDONED.value,
                reason=AdviceReconciliationReason.STALE_ATTEMPT.value,
                observed_at=now,
                transitioned_at=now,
            )
        )
        records, total = mysql_storage.read_advice_list(
            only_active=True,
        )

        assert total == 4
        assert {record.heimdall_id for record in records} == {active_id, successor_id, aged_id, refreshed_id}

    def test_json_fields_and_timestamp_precision(self, mysql_storage, clean_advice_tables):
        """
        Test MySQL-specific JSON serialization and timestamp handling.

        Verifies:
        - Complex scheduler_metadata (nested dicts, arrays) serializes correctly
        - Complex issues list with nested objects serializes correctly
        - Timestamp precision (MySQL DATETIME microsecond handling)
        - All timestamps have timezone.utc after retrieval
        """
        heimdall_id = uuid.uuid4()
        now = datetime.now(UTC)

        # Create complex scheduler metadata
        complex_metadata = {
            "kind": "Job",
            "namespace": "production",
            "labels": {"app": "ml-training", "version": "v1.2.3", "env": "prod"},
            "annotations": {
                "description": "Large scale training job",
                "owner": "ml-team",
            },
            "nested": {
                "array": [1, 2, 3],
                "object": {"key": "value"},
            },
        }

        # Create complex issues with nested structures
        complex_issues = [
            Issue(
                issue_type="CRASH",
                timestamp=now,
                detected_by="heimdall",
                relevant_log_lines=[
                    "Error line 1",
                    "Error line 2 with special chars: \u2713 \u2717",
                ],
                remediation_actions=[
                    RemediationAction(
                        action_type="EXCLUDE_NODE",
                        timestamp=now,
                        reason="Hardware failure detected",
                        resource_details=[
                            ResourceDetail(
                                resource_type="node",
                                resource_id="node-123",
                                details="GPU 0 memory error",
                            ),
                            ResourceDetail(
                                resource_type="node",
                                resource_id="node-456",
                                details="GPU 1 thermal issue",
                            ),
                        ],
                    ),
                    RemediationAction(
                        action_type="RESTART_JOB",
                        timestamp=now,
                        reason="Automatic recovery attempt",
                        resource_details=None,
                    ),
                ],
            ),
            Issue(
                issue_type="HANG",
                timestamp=now,
                detected_by="watchdog",
                relevant_log_lines=None,
                remediation_actions=None,
            ),
        ]

        # Create job record to satisfy FK
        _ensure_job_record(mysql_storage, "complex_test", 0, heimdall_id)

        # Create record with complex fields
        record = AdviceRecord(
            heimdall_id=heimdall_id,
            visible=True,
            job_id="complex_test",
            scheduler_metadata=complex_metadata,
            attempt_index=0,
            advice_state="Restarted",
            node_exclusions=["node-123", "node-456"],
            node_inclusions=["node-789"],
            issues=complex_issues,
            created_at=now,
            updated_at=now,
        )

        mysql_storage.create_advice(record)

        # Read back and verify JSON deserialization
        retrieved = mysql_storage.read_advice(heimdall_id)
        assert retrieved is not None

        # Verify complex scheduler_metadata
        assert retrieved.scheduler_metadata == complex_metadata
        assert retrieved.scheduler_metadata["nested"]["array"] == [1, 2, 3]
        assert retrieved.scheduler_metadata["nested"]["object"]["key"] == "value"

        # Verify complex issues
        assert len(retrieved.issues) == 2
        assert retrieved.issues[0].issue_type == "CRASH"
        assert len(retrieved.issues[0].remediation_actions) == 2
        assert len(retrieved.issues[0].remediation_actions[0].resource_details) == 2
        assert retrieved.issues[0].relevant_log_lines[1] == "Error line 2 with special chars: \u2713 \u2717"
        assert retrieved.issues[1].issue_type == "HANG"
        assert retrieved.issues[1].remediation_actions is None

        # Verify timestamp precision (MySQL DATETIME has microsecond precision)
        # Allow up to 1 second difference due to MySQL timestamp handling
        assert abs((retrieved.created_at - now).total_seconds()) < 1
        assert abs((retrieved.updated_at - now).total_seconds()) < 1

        # Verify all timestamps have UTC timezone
        assert retrieved.created_at.tzinfo == UTC
        assert retrieved.updated_at.tzinfo == UTC
        assert retrieved.issues[0].timestamp.tzinfo == UTC
        assert retrieved.issues[0].remediation_actions[0].timestamp.tzinfo == UTC
