# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

from job_manager.dag_utils import (
    _read_latest_workflow_jobs_by_exact_name,
    propagate_terminated_advice,
    reconcile_registered_job,
    refresh_dag_dependency_observability,
    register_dag_dependencies,
)
from job_manager.storage_utils import (
    create_terminal_advice_for_job,
    maybe_persist_requeue_terminal_advice,
)
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, ProducerEvent
from recovery_libs.schema.aps_spec.spec import (
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
    DagDependencyStep,
    JobReferenceById,
    JobReferenceByName,
)
from recovery_libs.schema.job_manager.models import AdviceState, JobTerminalAdvice, SystemRequeueConfig
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    DagDependency,
    DagDependencyActionStatus,
    DagDependencyReasonCode,
    SlurmJobExecutionRecord,
)
from test_utils import create_example_slurm_job_record


class DagStorage:
    """Small in-memory implementation of the production storage surface used by DAG policy tests."""

    def __init__(self) -> None:
        self.jobs: dict[tuple[str, int], SlurmJobExecutionRecord] = {}
        self.advice: dict[tuple[str, int], AdviceRecord] = {}
        self.dependencies: list[DagDependency] = []
        self._next_dependency_id = 1

    def add_job(self, record: SlurmJobExecutionRecord) -> None:
        self.jobs[(record.job_id, record.job_attempt_index)] = record

    def read_job_record(
        self,
        job_id: str,
        job_attempt_index: int | None = None,
    ) -> SlurmJobExecutionRecord | None:
        if job_attempt_index is not None:
            return self.jobs.get((job_id, job_attempt_index))
        candidates = [record for (candidate_id, _), record in self.jobs.items() if candidate_id == job_id]
        return max(candidates, key=lambda record: record.job_attempt_index) if candidates else None

    def read_job_records(
        self,
        *,
        job_name: str | None = None,
        job_name_match: str = "prefix",
        workflow_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        **_kwargs,
    ) -> tuple[list[SlurmJobExecutionRecord], int]:
        records = list(self.jobs.values())
        if job_name is not None:
            assert job_name_match == "exact"
            records = [record for record in records if record.job_name == job_name]
        if workflow_id is not None:
            records = [record for record in records if record.workflow_id == workflow_id]
        return records[offset : offset + limit], len(records)

    def upsert_dag_dependency(self, record: DagDependency) -> tuple[DagDependency, bool]:
        for existing in self.dependencies:
            if (
                existing.dependent_job_id == record.dependent_job_id
                and existing.dependency_job_identifier == record.dependency_job_identifier
                and existing.dependency_job_identifier_type == record.dependency_job_identifier_type
            ):
                return existing, False
        record.id = self._next_dependency_id
        self._next_dependency_id += 1
        self.dependencies.append(record)
        return record, True

    def read_dag_dependencies(
        self,
        *,
        dependent_job_id: str | None = None,
        dependency_job_identifier: str | None = None,
        dependency_job_identifier_type: str | None = None,
        action_statuses: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
        **_kwargs,
    ) -> tuple[list[DagDependency], int]:
        records = list(self.dependencies)
        if dependent_job_id is not None:
            records = [record for record in records if record.dependent_job_id == dependent_job_id]
        if dependency_job_identifier is not None:
            records = [
                record
                for record in records
                if record.dependency_job_identifier == dependency_job_identifier
                and record.dependency_job_identifier_type == dependency_job_identifier_type
            ]
        if action_statuses:
            records = [record for record in records if record.action_status in action_statuses]
        return records[offset : offset + limit], len(records)

    def update_dag_dependency_status(
        self,
        id: int,
        action_status: str,
        action_time: datetime | None = None,
        reason_code: str | None = None,
        reason_message: str | None = None,
        reason_details: dict[str, object] | None = None,
        last_evaluated_at: datetime | None = None,
    ) -> bool:
        dependency = next(record for record in self.dependencies if record.id == id)
        dependency.action_status = action_status
        if action_time is not None:
            dependency.action_time = action_time
        dependency.reason_code = reason_code
        dependency.reason_message = reason_message
        dependency.reason_details = reason_details
        dependency.last_evaluated_at = last_evaluated_at
        return True

    def read_advice_by_job(self, job_id: str, attempt_index: int) -> AdviceRecord | None:
        return self.advice.get((job_id, attempt_index))

    def upsert_advice(
        self, advice: AdviceRecord, *, issue_merge: object = None
    ) -> tuple[AdviceRecord, bool, AdviceRecord | None]:
        # DAG policy tests write serially and use writers that replace Advice or that
        # carry every stored Issue forward, so issue_merge does not change the outcome here.
        del issue_merge
        key = (advice.job_id, advice.attempt_index)
        existing = self.advice.get(key)
        if existing is not None:
            advice.created_at = existing.created_at
        self.advice[key] = advice
        return advice, existing is None, existing


def _job(
    job_id: str,
    *,
    name: str,
    workflow_id: str = "workflow-a",
    state: str = "PENDING",
    attempt_index: int = 0,
    max_requeue_times: int | None = None,
    dependencies: list[DagDependencyStep] | None = None,
) -> SlurmJobExecutionRecord:
    record = create_example_slurm_job_record(
        job_id=job_id,
        attempt_index=attempt_index,
        state=state,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        max_requeue_times=max_requeue_times,
    )
    record.job_name = name
    record.workflow_id = workflow_id
    record.aps_spec = ApsSlurmJobSpec(
        workflow_id=workflow_id,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        max_requeue_times=max_requeue_times,
        dag_dependencies=dependencies,
    )
    return record


def _persist_parent_decision(
    storage: DagStorage,
    parent: SlurmJobExecutionRecord,
    terminal_advice: JobTerminalAdvice,
    audit_buffer: AuditBuffer[ProducerEvent] | None = None,
) -> None:
    if audit_buffer is None:
        audit_buffer = NoOpAuditBuffer()
    written = create_terminal_advice_for_job(
        storage,  # type: ignore[arg-type]
        audit_buffer=audit_buffer,
        job_id=parent.job_id,
        job_attempt_index=parent.job_attempt_index,
        terminal_advice=terminal_advice,
        on_advice_persisted=lambda advice, reason: propagate_terminated_advice(
            storage,  # type: ignore[arg-type]
            advice,
            audit_buffer=audit_buffer,
            terminal_reason=reason,
        ),
    )
    assert written == terminal_advice


def test_terminated_parent_propagates_to_held_child_and_grandchild_idempotently() -> None:
    storage = DagStorage()
    audit_buffer = MagicMock()
    parent = _job("100", name="parent", state="CANCELLED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    grandchild = _job(
        "300",
        name="grandchild",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=child.job_id))],
    )
    for record in (parent, child, grandchild):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]
    register_dag_dependencies(storage, grandchild)  # type: ignore[arg-type]

    _persist_parent_decision(storage, parent, JobTerminalAdvice.USER_CANCELED, audit_buffer)
    _persist_parent_decision(storage, parent, JobTerminalAdvice.USER_CANCELED, audit_buffer)

    assert storage.advice[(parent.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert storage.advice[(child.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert storage.advice[(grandchild.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert {dependency.action_status for dependency in storage.dependencies} == {
        DagDependencyActionStatus.ABORTED.value
    }
    assert set(storage.jobs) == {(parent.job_id, 0), (child.job_id, 0), (grandchild.job_id, 0)}
    dag_termination_events = [
        call.args[0]
        for call in audit_buffer.add.call_args_list
        if getattr(call.args[0].payload, "creation_reasons", None) == ["DAG_PARENT_TERMINATED"]
    ]
    assert len(dag_termination_events) == 2

    child_dependency = next(dep for dep in storage.dependencies if dep.dependent_job_id == child.job_id)
    assert child_dependency.reason_code == DagDependencyReasonCode.PARENT_TERMINATED.value
    assert child_dependency.reason_details == {
        "parent_attempt": 0,
        "parent_job_id": parent.job_id,
        "parent_scheduler_state": "CANCELLED",
        "parent_terminal_advice_reason": JobTerminalAdvice.USER_CANCELED.value,
        "workflow_id": child.workflow_id,
    }


def test_later_cancelled_child_observation_does_not_duplicate_grandchild_evidence() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="CANCELLED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    grandchild = _job(
        "300",
        name="grandchild",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=child.job_id))],
    )
    for record in (parent, child, grandchild):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]
    register_dag_dependencies(storage, grandchild)  # type: ignore[arg-type]

    _persist_parent_decision(storage, parent, JobTerminalAdvice.USER_CANCELED)
    child.state = "CANCELLED"
    propagate_terminated_advice(
        storage,  # type: ignore[arg-type]
        storage.advice[(child.job_id, 0)],
        audit_buffer=NoOpAuditBuffer(),
    )

    grandchild_dependency = next(dep for dep in storage.dependencies if dep.dependent_job_id == grandchild.job_id)
    assert grandchild_dependency.reason_code == DagDependencyReasonCode.PARENT_TERMINATED.value
    assert grandchild_dependency.reason_details["parent_scheduler_state"] == "CANCELLED"


def test_one_terminated_parent_closes_every_gate_for_a_multi_parent_child() -> None:
    storage = DagStorage()
    aborted = _job("100", name="aborted", state="STOPPED")
    incomplete = _job("101", name="incomplete", state="PENDING")
    child = _job(
        "200",
        name="child",
        dependencies=[
            DagDependencyStep(target=JobReferenceById(id=aborted.job_id)),
            DagDependencyStep(target=JobReferenceById(id=incomplete.job_id)),
        ],
    )
    for record in (aborted, incomplete, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    _persist_parent_decision(storage, aborted, JobTerminalAdvice.USER_CANCELED)

    assert storage.advice[(child.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert all(
        dependency.action_status == DagDependencyActionStatus.ABORTED.value for dependency in storage.dependencies
    )


def test_name_reference_resolves_only_within_dependent_workflow() -> None:
    storage = DagStorage()
    intended_parent = _job("100", name="shared-name", workflow_id="workflow-a", state="CANCELLED")
    other_workflow_parent = _job("900", name="shared-name", workflow_id="workflow-b", state="CANCELLED")
    child = _job(
        "200",
        name="child",
        workflow_id="workflow-a",
        dependencies=[DagDependencyStep(target=JobReferenceByName(name="shared-name"))],
    )
    for record in (intended_parent, other_workflow_parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    _persist_parent_decision(storage, intended_parent, JobTerminalAdvice.USER_CANCELED)

    assert storage.advice[(child.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert storage.dependencies[0].reason_code == DagDependencyReasonCode.PARENT_TERMINATED.value
    assert storage.dependencies[0].reason_details["parent_job_id"] == intended_parent.job_id


def test_exact_name_query_retains_only_latest_attempt_per_job_id() -> None:
    storage = DagStorage()
    records = [
        _job("100", name="shared-name", attempt_index=0),
        _job("100", name="shared-name", attempt_index=2),
        _job("100", name="shared-name", attempt_index=1),
        _job("101", name="shared-name", attempt_index=0),
        _job("101", name="shared-name", attempt_index=1),
        _job("900", name="shared-name", workflow_id="workflow-b", attempt_index=5),
    ]
    for record in records:
        storage.add_job(record)

    latest = _read_latest_workflow_jobs_by_exact_name(  # type: ignore[arg-type]
        storage,
        workflow_id="workflow-a",
        job_name="shared-name",
    )

    assert {(record.job_id, record.job_attempt_index) for record in latest} == {
        ("100", 2),
        ("101", 1),
    }


def test_ambiguous_name_reference_fails_closed_without_terminal_advice() -> None:
    storage = DagStorage()
    first_parent = _job("100", name="shared-name", state="CANCELLED")
    second_parent = _job("101", name="shared-name", state="CANCELLED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceByName(name="shared-name"))],
    )
    for record in (first_parent, second_parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    _persist_parent_decision(storage, first_parent, JobTerminalAdvice.USER_CANCELED)

    assert (child.job_id, 0) not in storage.advice
    assert storage.dependencies[0].action_status == DagDependencyActionStatus.NOT_ELIGIBLE.value


def test_child_registered_after_terminated_parent_is_reconciled_immediately() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="CANCELLED")
    storage.add_job(parent)
    _persist_parent_decision(storage, parent, JobTerminalAdvice.USER_CANCELED)

    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    storage.add_job(child)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]
    completed_parents = reconcile_registered_job(  # type: ignore[arg-type]
        storage,
        child,
        audit_buffer=NoOpAuditBuffer(),
    )

    assert completed_parents == []
    assert storage.advice[(child.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert storage.dependencies[0].action_status == DagDependencyActionStatus.ABORTED.value


def test_external_terminated_advice_without_reason_uses_state_as_causal_reason() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="CANCELLED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    for record in (parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]
    now = datetime.now(UTC)
    parent_advice = AdviceRecord(
        heimdall_id=uuid.uuid4(),
        visible=True,
        job_id=parent.job_id,
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=parent.job_attempt_index,
        advice_state=AdviceState.TERMINATED.value,
        created_at=now,
        updated_at=now,
    )
    storage.upsert_advice(parent_advice)

    propagate_terminated_advice(storage, parent_advice, audit_buffer=NoOpAuditBuffer())

    assert storage.dependencies[0].reason_code == DagDependencyReasonCode.PARENT_TERMINATED.value
    assert storage.dependencies[0].reason_details["parent_terminal_advice_reason"] == AdviceState.TERMINATED.value


def test_retryable_parent_decision_does_not_terminate_dependent() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="FAILED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    for record in (parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    _persist_parent_decision(storage, parent, JobTerminalAdvice.JOB_RELEASED)

    assert storage.advice[(parent.job_id, 0)].advice_state == AdviceState.RESTARTED.value
    assert (child.job_id, 0) not in storage.advice
    assert storage.dependencies[0].action_status == DagDependencyActionStatus.NOT_ELIGIBLE.value


def test_dependency_reason_waits_for_parent_registration() -> None:
    storage = DagStorage()
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id="100"))],
    )
    storage.add_job(child)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    refresh_dag_dependency_observability(storage, child)  # type: ignore[arg-type]

    dependency = storage.dependencies[0]
    assert dependency.reason_code == DagDependencyReasonCode.WAITING_FOR_PARENT_REGISTRATION.value
    assert dependency.reason_details == {
        "dependency_job_identifier": "100",
        "dependency_job_identifier_type": "id",
        "workflow_id": child.workflow_id,
    }
    assert dependency.last_evaluated_at is not None


def test_dependency_reason_waits_for_registered_parent_completion() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="RUNNING")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    storage.add_job(parent)
    storage.add_job(child)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    refresh_dag_dependency_observability(storage, child)  # type: ignore[arg-type]

    dependency = storage.dependencies[0]
    assert dependency.reason_code == DagDependencyReasonCode.WAITING_FOR_PARENT_COMPLETION.value
    assert dependency.reason_details["parent_job_id"] == parent.job_id
    assert dependency.reason_details["parent_state"] == "RUNNING"


def test_completed_dependency_explains_that_other_parents_remain() -> None:
    storage = DagStorage()
    completed_parent = _job("100", name="complete", state="COMPLETED")
    running_parent = _job("101", name="running", state="RUNNING")
    child = _job(
        "200",
        name="child",
        dependencies=[
            DagDependencyStep(target=JobReferenceById(id=completed_parent.job_id)),
            DagDependencyStep(target=JobReferenceById(id=running_parent.job_id)),
        ],
    )
    for record in (completed_parent, running_parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    refresh_dag_dependency_observability(storage, child)  # type: ignore[arg-type]

    reasons = {dep.dependency_job_identifier: dep.reason_code for dep in storage.dependencies}
    assert reasons == {
        completed_parent.job_id: DagDependencyReasonCode.DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS.value,
        running_parent.job_id: DagDependencyReasonCode.WAITING_FOR_PARENT_COMPLETION.value,
    }


def test_ambiguous_parent_name_is_structured_and_workflow_scoped() -> None:
    storage = DagStorage()
    child = _job(
        "200",
        name="child",
        workflow_id="workflow-a",
        dependencies=[DagDependencyStep(target=JobReferenceByName(name="shared-name"))],
    )
    for record in (
        _job("100", name="shared-name", workflow_id="workflow-a"),
        _job("101", name="shared-name", workflow_id="workflow-a"),
        _job("900", name="shared-name", workflow_id="workflow-b"),
        child,
    ):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    refresh_dag_dependency_observability(storage, child)  # type: ignore[arg-type]

    dependency = storage.dependencies[0]
    assert dependency.reason_code == DagDependencyReasonCode.PARENT_NAME_AMBIGUOUS.value
    assert dependency.reason_details["candidate_job_ids"] == ["100", "101"]


def test_exhausted_parent_requeue_limit_terminates_dependent() -> None:
    storage = DagStorage()
    parent = _job(
        "100",
        name="parent",
        state="FAILED",
        attempt_index=5,
        max_requeue_times=5,
    )
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    for record in (parent, child):
        storage.add_job(record)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    maybe_persist_requeue_terminal_advice(
        storage,  # type: ignore[arg-type]
        parent,
        audit_buffer=NoOpAuditBuffer(),
        system_requeue_config=SystemRequeueConfig(max_requeue_times=75),
        get_jobs=lambda **_kwargs: [],
        on_advice_persisted=lambda advice, reason: propagate_terminated_advice(
            storage,  # type: ignore[arg-type]
            advice,
            audit_buffer=NoOpAuditBuffer(),
            terminal_reason=reason,
        ),
    )

    assert storage.advice[(parent.job_id, 5)].advice_state == AdviceState.TERMINATED.value
    assert storage.advice[(child.job_id, 0)].advice_state == AdviceState.TERMINATED.value
    assert storage.dependencies[0].action_status == DagDependencyActionStatus.ABORTED.value


def test_completed_parent_registered_before_child_is_returned_for_release_reconciliation() -> None:
    storage = DagStorage()
    parent = _job("100", name="parent", state="COMPLETED")
    child = _job(
        "200",
        name="child",
        dependencies=[DagDependencyStep(target=JobReferenceById(id=parent.job_id))],
    )
    storage.add_job(parent)
    storage.add_job(child)
    register_dag_dependencies(storage, child)  # type: ignore[arg-type]

    assert reconcile_registered_job(  # type: ignore[arg-type]
        storage,
        child,
        audit_buffer=NoOpAuditBuffer(),
    ) == [parent]
    assert (child.job_id, 0) not in storage.advice
