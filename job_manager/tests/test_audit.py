# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
from copy import deepcopy
from unittest.mock import MagicMock, patch

import job_manager.main as main_module
import pytest
from job_manager.audit import advice_changes, job_changes
from job_manager.audit.events import current_unix_microseconds
from job_manager.storage_utils import persist_advice_record, persist_job_record, update_job_record
from recovery_libs.audit import AuditBuffer, AuditEventBuffer
from recovery_libs.schema.job_manager.models import AdviceState
from recovery_libs.storage.base import RecoveryStorageException, UpdateJobResult
from recovery_libs.storage.mysql_storage import AdviceIssueMerge, MySQLStorage, UpsertAdviceResult, UpsertJobResult
from test_utils import create_example_advice_record, create_example_slurm_job_record


@pytest.fixture
def storage() -> MagicMock:
    """Provide storage whose results and failures are independent of audit delivery."""
    return MagicMock(spec=MySQLStorage)


@pytest.fixture
def audit_buffer() -> MagicMock:
    """Observe audit submissions without starting background delivery."""
    return MagicMock(spec=AuditBuffer)


def test_current_unix_microseconds_preserves_microsecond_precision() -> None:
    with patch("job_manager.audit.events.time.time_ns", return_value=1_700_000_000_123_456_789):
        assert current_unix_microseconds() == 1_700_000_000_123_456


def test_main_stops_audit_buffer_when_service_exits() -> None:
    config = {"service": {}}
    job_manager = MagicMock()
    audit_buffer = MagicMock(spec=AuditEventBuffer)
    http_thread = MagicMock(spec=threading.Thread)

    with (
        patch.object(main_module, "job_manager", None),
        patch("job_manager.main.signal.signal"),
        patch("job_manager.main.load_service_config_from_path", return_value=config),
        patch("job_manager.main.validate_service_config"),
        patch("job_manager.main.set_up_trace_provider"),
        patch("job_manager.main.JobManager", return_value=job_manager) as job_manager_class,
        patch("job_manager.main.create_audit_buffer", return_value=audit_buffer),
        patch("job_manager.main.threading.Thread", return_value=http_thread),
    ):
        main_module.main()

    audit_buffer.start.assert_called_once_with()
    job_manager_class.assert_called_once_with(config, main_module.shutdown_event, audit_buffer=audit_buffer)
    http_thread.join.assert_called_once_with()
    audit_buffer.stop.assert_called_once_with()


@pytest.mark.parametrize(
    ("created", "builder_name"),
    [
        pytest.param(True, "build_advice_created_event", id="create"),
        pytest.param(False, "build_advice_updated_event", id="update"),
    ],
)
def test_audit_advice_upsert_preserves_committed_result_on_build_error(
    storage: MagicMock, audit_buffer: MagicMock, created: bool, builder_name: str
) -> None:
    """Non-validation errors while building either Advice event cannot fail a committed upsert."""
    old_record = create_example_advice_record()
    updated_record = deepcopy(old_record)
    updated_record.advice_state = AdviceState.TERMINATED.value
    committed_result = UpsertAdviceResult(updated_record, created, None if created else old_record)
    storage.upsert_advice.return_value = committed_result

    with (
        patch.object(advice_changes, builder_name, side_effect=TypeError("snapshot conversion failed")) as build_event,
        patch.object(advice_changes.logger, "exception") as log_exception,
    ):
        result = persist_advice_record(storage, audit_buffer, updated_record)

    assert result is committed_result
    storage.upsert_advice.assert_called_once_with(updated_record, issue_merge=AdviceIssueMerge.REPLACE)
    build_event.assert_called_once()
    audit_buffer.add.assert_not_called()
    log_exception.assert_called_once()


@pytest.mark.parametrize("created", [True, False], ids=["create", "update"])
def test_audit_advice_upsert_preserves_committed_result_on_enqueue_error(
    storage: MagicMock, audit_buffer: MagicMock, created: bool
) -> None:
    """Unexpected buffer failures cannot fail a committed Advice creation or update."""
    old_record = create_example_advice_record()
    updated_record = deepcopy(old_record)
    updated_record.advice_state = AdviceState.TERMINATED.value
    committed_result = UpsertAdviceResult(updated_record, created, None if created else old_record)
    storage.upsert_advice.return_value = committed_result
    audit_buffer.add.side_effect = RuntimeError("buffer unavailable")

    with patch.object(advice_changes.logger, "exception") as log_exception:
        result = persist_advice_record(storage, audit_buffer, updated_record)

    assert result is committed_result
    storage.upsert_advice.assert_called_once_with(updated_record, issue_merge=AdviceIssueMerge.REPLACE)
    audit_buffer.add.assert_called_once()
    log_exception.assert_called_once()


def test_persist_advice_record_propagates_storage_error(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """Best-effort audit handling must not hide an Advice storage failure."""
    storage_error = RecoveryStorageException("database unavailable")
    storage.upsert_advice.side_effect = storage_error

    with pytest.raises(RecoveryStorageException) as raised:
        persist_advice_record(storage, audit_buffer, create_example_advice_record())

    assert raised.value is storage_error
    audit_buffer.add.assert_not_called()


@pytest.mark.parametrize(
    ("created", "builder_name"),
    [
        pytest.param(True, "build_job_attempt_first_seen_event", id="create"),
        pytest.param(False, "build_job_attempt_updated_event", id="update"),
    ],
)
def test_audit_job_upsert_preserves_committed_result_on_build_error(
    storage: MagicMock, audit_buffer: MagicMock, created: bool, builder_name: str
) -> None:
    """The outer audit boundary must contain unexpected errors from either Job builder."""
    old_record = create_example_slurm_job_record(state="PENDING")
    updated_record = deepcopy(old_record)
    updated_record.state = "RUNNING"
    committed_result = UpsertJobResult(updated_record, created, None if created else old_record)
    storage.upsert_job_record.return_value = committed_result

    with (
        patch.object(job_changes, builder_name, side_effect=TypeError("snapshot serialization failed")) as build_event,
        patch.object(job_changes.logger, "exception") as log_exception,
    ):
        result = persist_job_record(storage, audit_buffer, updated_record)

    assert result is committed_result
    storage.upsert_job_record.assert_called_once_with(updated_record, populate_heimdall_id_log_path=False)
    build_event.assert_called_once()
    audit_buffer.add.assert_not_called()
    log_exception.assert_called_once()


@pytest.mark.parametrize("created", [True, False], ids=["create", "update"])
def test_audit_job_upsert_preserves_committed_result_on_enqueue_error(
    storage: MagicMock, audit_buffer: MagicMock, created: bool
) -> None:
    """Unexpected buffer failures cannot fail a committed Job creation or update."""
    old_record = create_example_slurm_job_record(state="PENDING")
    updated_record = deepcopy(old_record)
    updated_record.state = "RUNNING"
    committed_result = UpsertJobResult(updated_record, created, None if created else old_record)
    storage.upsert_job_record.return_value = committed_result
    audit_buffer.add.side_effect = RuntimeError("buffer unavailable")

    with patch.object(job_changes.logger, "exception") as log_exception:
        result = persist_job_record(storage, audit_buffer, updated_record)

    assert result is committed_result
    storage.upsert_job_record.assert_called_once_with(updated_record, populate_heimdall_id_log_path=False)
    audit_buffer.add.assert_called_once()
    log_exception.assert_called_once()


def test_persist_job_record_propagates_storage_error(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """Best-effort audit handling must not hide a Job upsert storage failure."""
    storage_error = RecoveryStorageException("database unavailable")
    storage.upsert_job_record.side_effect = storage_error

    with pytest.raises(RecoveryStorageException) as raised:
        persist_job_record(storage, audit_buffer, create_example_slurm_job_record())

    assert raised.value is storage_error
    audit_buffer.add.assert_not_called()


def test_audit_job_update_preserves_success_on_build_error(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """An unexpected event-building error cannot fail a committed partial Job update."""
    old_record = create_example_slurm_job_record(state="PENDING")
    updated_record = deepcopy(old_record)
    updated_record.state = "RUNNING"
    storage.update_job_record.return_value = UpdateJobResult(updated_record, old_record)

    with (
        patch.object(
            job_changes, "build_job_attempt_updated_event", side_effect=TypeError("snapshot serialization failed")
        ) as build_event,
        patch.object(job_changes.logger, "exception") as log_exception,
    ):
        result = update_job_record(
            storage, audit_buffer, old_record.job_id, old_record.job_attempt_index, state="RUNNING"
        )

    assert result is True
    storage.update_job_record.assert_called_once_with(
        job_id=old_record.job_id, job_attempt_index=old_record.job_attempt_index, state="RUNNING"
    )
    build_event.assert_called_once()
    audit_buffer.add.assert_not_called()
    log_exception.assert_called_once()


def test_audit_job_update_preserves_success_on_enqueue_error(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """An unexpected buffer failure cannot fail a committed partial Job update."""
    old_record = create_example_slurm_job_record(state="PENDING")
    updated_record = deepcopy(old_record)
    updated_record.state = "RUNNING"
    storage.update_job_record.return_value = UpdateJobResult(updated_record, old_record)
    audit_buffer.add.side_effect = RuntimeError("buffer unavailable")

    with patch.object(job_changes.logger, "exception") as log_exception:
        result = update_job_record(
            storage, audit_buffer, old_record.job_id, old_record.job_attempt_index, state="RUNNING"
        )

    assert result is True
    storage.update_job_record.assert_called_once_with(
        job_id=old_record.job_id, job_attempt_index=old_record.job_attempt_index, state="RUNNING"
    )
    audit_buffer.add.assert_called_once()
    log_exception.assert_called_once()


def test_audit_job_update_skips_audit_when_no_record_matches(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """A completed partial update with no matching record remains successful and emits no event."""
    storage.update_job_record.return_value = UpdateJobResult(updated_record=None, old_record=None)

    result = update_job_record(storage, audit_buffer, "missing-job", 0, state="RUNNING")

    assert result is True
    audit_buffer.add.assert_not_called()


def test_update_job_record_propagates_storage_error(storage: MagicMock, audit_buffer: MagicMock) -> None:
    """Best-effort audit handling must not hide a partial Job update storage failure."""
    storage_error = RecoveryStorageException("database unavailable")
    storage.update_job_record.side_effect = storage_error

    with pytest.raises(RecoveryStorageException) as raised:
        update_job_record(storage, audit_buffer, "test-job", 0, state="RUNNING")

    assert raised.value is storage_error
    audit_buffer.add.assert_not_called()
