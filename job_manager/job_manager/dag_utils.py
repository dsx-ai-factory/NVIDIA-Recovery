# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum

from recovery_libs.advice import ensure_slurm_scheduler_metadata
from recovery_libs.audit import AuditBuffer, ProducerEvent
from recovery_libs.schema.aps_spec.spec import JobReferenceByName
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import Advice, AdviceRecommendation, AdviceState
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    DagDependency,
    DagDependencyActionStatus,
    DagDependencyReasonCode,
    SlurmJobExecutionRecord,
    SlurmJobFinalStates,
    SlurmJobNonFinalStates,
)
from recovery_libs.storage.mysql_storage import MySQLStorage

from job_manager.advice_mapping import http_to_storage_advice, storage_to_http_advice
from job_manager.logger import logger
from job_manager.storage_utils import (
    persist_advice_record,
    read_all_dag_dependencies,
)

# Kept only to interpret Advice written by older versions during a rolling
# upgrade. New DAG provenance is stored on the dependency record.
DAG_PARENT_TERMINATED_ISSUE_TYPE = "DAG_PARENT_TERMINATED"
DAG_PARENT_TERMINATED_REASON = "dag_parent_terminated"
_DAG_TERMINATION_AUDIT_REASON = "DAG_PARENT_TERMINATED"
_DAG_JOB_READ_PAGE_SIZE = 50


class DagParentResolutionStatus(str, Enum):
    """Outcome of resolving one dependency parent identity."""

    RESOLVED = "resolved"
    NOT_REGISTERED = "not_registered"
    AMBIGUOUS = "ambiguous"
    INVALID_IDENTIFIER_TYPE = "invalid_identifier_type"


@dataclass(frozen=True)
class DagParentResolution:
    """Result of resolving one dependency edge to an authoritative parent.

    ``parent`` is populated only for ``RESOLVED``. ``candidate_job_ids``
    records the distinct workflow-scoped IDs considered for diagnostics; it
    contains one ID for a resolved name or ID reference and every conflicting
    ID for an ambiguous name reference.
    """

    status: DagParentResolutionStatus
    parent: SlurmJobExecutionRecord | None = None
    candidate_job_ids: tuple[str, ...] = ()


def register_dag_dependencies(storage: MySQLStorage, record: SlurmJobExecutionRecord) -> list[DagDependency]:
    """Register immutable dependency gates for an initial job attempt.

    Repeated calls safely return the stored gates without regressing an action
    that has already progressed beyond ``NOT_ELIGIBLE``.

    Args:
        storage: Durable DAG dependency storage.
        record: Initial attempt whose APS spec declares DAG dependencies.

    Returns:
        Stored dependency rows, or an empty list when the record has no DAG
        dependencies or is a retry attempt.
    """
    if record.job_attempt_index != 0:
        logger.warning(
            f"DAG dependency registration skipped for job {record.job_id} attempt "
            f"{record.job_attempt_index}: only the initial attempt owns the dependency gate."
        )
        return []
    if not record.aps_spec or not record.aps_spec.dag_dependencies:
        return []

    dag_action = record.aps_spec.dag_action.value if record.aps_spec.dag_action else "release"
    stored: list[DagDependency] = []
    for dep_step in record.aps_spec.dag_dependencies:
        identifier = dep_step.target.name if isinstance(dep_step.target, JobReferenceByName) else dep_step.target.id
        dag_dep = DagDependency(
            dependent_job_id=record.job_id,
            dependent_job_action=dag_action,
            dependency_job_identifier=identifier,
            dependency_job_identifier_type=dep_step.target.type,
            dependency_trigger_condition=dep_step.trigger.value,
        )
        persisted, updated = storage.upsert_dag_dependency(dag_dep)
        stored.append(persisted)
        if updated:
            logger.info(
                f"Registered DAG dependency for job {record.job_id}: "
                f"depends on {dep_step.target.type}={identifier}, trigger={dep_step.trigger.value}"
            )
    return stored


def _read_latest_workflow_jobs_by_exact_name(
    storage: MySQLStorage,
    *,
    workflow_id: str,
    job_name: str,
) -> list[SlurmJobExecutionRecord]:
    """Read the latest attempt of each exact-name job in one workflow."""
    latest_by_job_id: dict[str, SlurmJobExecutionRecord] = {}
    offset = 0
    while True:
        page, total = storage.read_job_records(
            job_name=job_name,
            job_name_match="exact",
            workflow_id=workflow_id,
            limit=_DAG_JOB_READ_PAGE_SIZE,
            offset=offset,
        )
        for record in page:
            current = latest_by_job_id.get(record.job_id)
            if current is None or record.job_attempt_index > current.job_attempt_index:
                latest_by_job_id[record.job_id] = record
        offset += len(page)
        if not page or offset >= total:
            return list(latest_by_job_id.values())


def resolve_dag_parent_with_diagnostics(
    storage: MySQLStorage,
    dep: DagDependency,
    dependent_record: SlurmJobExecutionRecord,
    *,
    observed_parent: SlurmJobExecutionRecord | None = None,
) -> DagParentResolution:
    """Resolve the authoritative parent attempt for one DAG dependency.

    ID references resolve directly to the latest stored attempt for that
    scheduler job ID. Name references are scoped to the dependent job's
    workflow and must identify exactly one distinct scheduler job ID; zero or
    multiple matching IDs fail closed. When one ID matches, its latest attempt
    is authoritative.

    ``observed_parent`` represents an event that may not have been persisted
    yet. It replaces the stored attempt only when it matches the dependency
    identity and is at least as recent as the stored attempt. Release and
    terminal-advice propagation both use this resolver so they cannot disagree
    about parent identity.

    Args:
        storage: Job and dependency storage.
        dep: Dependency edge whose parent is being resolved.
        dependent_record: Child record providing the workflow namespace for a
            name reference.
        observed_parent: Optional just-observed parent record that may be newer
            than durable storage.

    Returns:
        A structured resolution outcome containing the authoritative parent
        attempt or the reason that identity could not be resolved.
    """
    if dep.dependency_job_identifier_type == "id":
        parent_record = storage.read_job_record(dep.dependency_job_identifier)
        if (
            observed_parent is not None
            and observed_parent.job_id == dep.dependency_job_identifier
            and (parent_record is None or observed_parent.job_attempt_index >= parent_record.job_attempt_index)
        ):
            parent_record = observed_parent
        if parent_record is None:
            return DagParentResolution(DagParentResolutionStatus.NOT_REGISTERED)
        return DagParentResolution(
            DagParentResolutionStatus.RESOLVED,
            parent=parent_record,
            candidate_job_ids=(parent_record.job_id,),
        )

    if dep.dependency_job_identifier_type != "name":
        logger.warning(f"Unknown dependency identifier type: {dep.dependency_job_identifier_type}")
        return DagParentResolution(DagParentResolutionStatus.INVALID_IDENTIFIER_TYPE)

    candidates = _read_latest_workflow_jobs_by_exact_name(
        storage,
        workflow_id=dependent_record.workflow_id,
        job_name=dep.dependency_job_identifier,
    )
    if (
        observed_parent is not None
        and observed_parent.workflow_id == dependent_record.workflow_id
        and observed_parent.job_name == dep.dependency_job_identifier
    ):
        stored_attempt = next(
            (candidate for candidate in candidates if candidate.job_id == observed_parent.job_id),
            None,
        )
        if stored_attempt is None or observed_parent.job_attempt_index >= stored_attempt.job_attempt_index:
            candidates = [candidate for candidate in candidates if candidate.job_id != observed_parent.job_id]
            candidates.append(observed_parent)

    job_ids = {candidate.job_id for candidate in candidates}
    if len(job_ids) != 1:
        reason = "unresolved" if not job_ids else "ambiguous"
        logger.warning(
            f"DAG parent name '{dep.dependency_job_identifier}' is {reason} in workflow "
            f"'{dependent_record.workflow_id}' for dependent job {dependent_record.job_id}; failing closed."
        )
        status = DagParentResolutionStatus.NOT_REGISTERED if not job_ids else DagParentResolutionStatus.AMBIGUOUS
        return DagParentResolution(status, candidate_job_ids=tuple(sorted(job_ids)))

    return DagParentResolution(
        DagParentResolutionStatus.RESOLVED,
        parent=candidates[0],
        candidate_job_ids=tuple(sorted(job_ids)),
    )


def resolve_dag_parent(
    storage: MySQLStorage,
    dep: DagDependency,
    dependent_record: SlurmJobExecutionRecord,
    *,
    observed_parent: SlurmJobExecutionRecord | None = None,
) -> SlurmJobExecutionRecord | None:
    """Return the resolved parent while preserving the existing public helper contract."""
    return resolve_dag_parent_with_diagnostics(
        storage,
        dep,
        dependent_record,
        observed_parent=observed_parent,
    ).parent


def dependency_is_satisfied(
    storage: MySQLStorage,
    dep: DagDependency,
    dependent_record: SlurmJobExecutionRecord,
    *,
    observed_parent: SlurmJobExecutionRecord | None = None,
) -> bool:
    """Return whether one dependency resolves to a completed parent.

    Resolution is fail-closed: a missing, ambiguous, or invalid parent cannot
    satisfy the gate. ``observed_parent`` allows an event that has not yet been
    committed to supersede an older stored attempt without changing the
    durable identity rules.
    """
    parent_record = resolve_dag_parent(
        storage,
        dep,
        dependent_record,
        observed_parent=observed_parent,
    )
    if parent_record is None:
        return False
    return (
        dep.dependency_trigger_condition == "completed" and parent_record.state == SlurmJobFinalStates.COMPLETED.value
    )


def refresh_dag_dependency_observability(
    storage: MySQLStorage,
    dependent_record: SlurmJobExecutionRecord,
    *,
    observed_parent: SlurmJobExecutionRecord | None = None,
) -> None:
    """Persist the latest explanation for every unresolved dependency gate.

    The caller invokes this at meaningful dependency events: child
    registration, potential-parent observation, parent completion, and
    terminal propagation. For each ``NOT_ELIGIBLE`` edge, the function stores
    whether the parent is unregistered, still running, ambiguous, or complete
    while another edge remains unresolved. When every edge is satisfied, the
    reason is cleared because the release path owns the next state transition.

    All edges in one evaluation share ``last_evaluated_at``. Each row's status
    and explanation are written together by storage, so an API reader cannot
    observe a new status paired with a stale reason. This function is
    deliberately scheduler-neutral: it never creates Advice, releases a job,
    or changes eligibility by itself.
    """
    dependencies = read_all_dag_dependencies(storage, dependent_job_id=dependent_record.job_id)
    unresolved = [dep for dep in dependencies if dep.action_status == DagDependencyActionStatus.NOT_ELIGIBLE.value]
    if not unresolved:
        return

    resolutions = {
        dep.id: resolve_dag_parent_with_diagnostics(
            storage,
            dep,
            dependent_record,
            observed_parent=observed_parent,
        )
        for dep in unresolved
    }
    all_satisfied = all(
        resolution.parent is not None
        and dep.dependency_trigger_condition == "completed"
        and resolution.parent.state == SlurmJobFinalStates.COMPLETED.value
        for dep in unresolved
        for resolution in [resolutions[dep.id]]
    )
    evaluated_at = datetime.now(UTC)

    for dep in unresolved:
        resolution = resolutions[dep.id]
        reason_code: str | None
        reason_message: str | None
        details: dict[str, object] | None

        if resolution.status == DagParentResolutionStatus.NOT_REGISTERED:
            reason_code = DagDependencyReasonCode.WAITING_FOR_PARENT_REGISTRATION.value
            reason_message = f"Waiting for parent {dep.dependency_job_identifier} to be registered"
            details = {
                "dependency_job_identifier": dep.dependency_job_identifier,
                "dependency_job_identifier_type": dep.dependency_job_identifier_type,
                "workflow_id": dependent_record.workflow_id,
            }
        elif resolution.status == DagParentResolutionStatus.AMBIGUOUS:
            reason_code = DagDependencyReasonCode.PARENT_NAME_AMBIGUOUS.value
            reason_message = (
                f"Parent name {dep.dependency_job_identifier} matches multiple jobs in workflow "
                f"{dependent_record.workflow_id}"
            )
            details = {
                "candidate_job_ids": list(resolution.candidate_job_ids),
                "dependency_job_identifier": dep.dependency_job_identifier,
                "workflow_id": dependent_record.workflow_id,
            }
        elif resolution.parent is not None and resolution.parent.state == SlurmJobFinalStates.COMPLETED.value:
            if all_satisfied:
                reason_code = None
                reason_message = None
                details = None
            else:
                reason_code = DagDependencyReasonCode.DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS.value
                reason_message = f"Parent job {resolution.parent.job_id} completed; waiting for other dependencies"
                details = {
                    "parent_attempt": resolution.parent.job_attempt_index,
                    "parent_job_id": resolution.parent.job_id,
                    "parent_state": resolution.parent.state,
                    "workflow_id": dependent_record.workflow_id,
                }
        elif resolution.parent is not None:
            reason_code = DagDependencyReasonCode.WAITING_FOR_PARENT_COMPLETION.value
            reason_message = f"Waiting for parent job {resolution.parent.job_id} to complete"
            details = {
                "parent_attempt": resolution.parent.job_attempt_index,
                "parent_job_id": resolution.parent.job_id,
                "parent_state": resolution.parent.state,
                "workflow_id": dependent_record.workflow_id,
            }
        else:
            # Unsupported identifier/trigger values are rejected by the APS
            # contract. Keep the public reason empty instead of inventing an
            # unreachable runtime diagnosis.
            reason_code = None
            reason_message = None
            details = None

        storage.update_dag_dependency_status(
            id=dep.id,
            action_status=dep.action_status,
            reason_code=reason_code,
            reason_message=reason_message,
            reason_details=details,
            last_evaluated_at=evaluated_at,
        )


def _terminal_reason_from_advice(advice: AdviceRecord) -> str:
    """Extract a stable terminal cause while supporting pre-ADR-0014 Advice."""
    for issue in reversed(advice.issues or []):
        if issue.issue_type == DAG_PARENT_TERMINATED_ISSUE_TYPE:
            return DAG_PARENT_TERMINATED_REASON
        if issue.issue_type in {
            "user_canceled",
            "max_requeue_reached",
            "manual_kill",
        }:
            return issue.issue_type
    return AdviceState.TERMINATED.value


def _build_dependent_terminated_advice(
    storage: MySQLStorage,
    *,
    dependent_record: SlurmJobExecutionRecord,
) -> AdviceRecord:
    """Build exact-attempt terminal Advice without adding a synthetic Issue.

    Existing Advice is reused so unrelated Issues and metadata survive the DAG
    transition. Only scheduler intent, visibility, and normalized scheduler
    metadata are updated here; termination causality lives on the dependency
    row.
    """
    existing_record = storage.read_advice_by_job(
        job_id=dependent_record.job_id,
        attempt_index=dependent_record.job_attempt_index,
    )
    if existing_record is not None:
        advice = storage_to_http_advice(existing_record)
    else:
        advice = Advice(
            id=str(
                dependent_record.heimdall_job_id
                or generate_heimdall_job_id(dependent_record.job_id, dependent_record.job_attempt_index)
            ),
            visible=True,
            job_id=dependent_record.job_id,
            restart_count=dependent_record.job_attempt_index,
            scheduler_metadata=dependent_record.scheduler_metadata or {},
            advice=AdviceRecommendation(state=AdviceState.TERMINATED, node_constraints=None),
        )
    advice.visible = True
    advice.advice = AdviceRecommendation(state=AdviceState.TERMINATED, node_constraints=None)
    advice.scheduler_metadata = ensure_slurm_scheduler_metadata(advice.scheduler_metadata)
    return http_to_storage_advice(advice)


def _matching_dependents_for_parent(
    storage: MySQLStorage,
    parent_record: SlurmJobExecutionRecord,
) -> dict[str, SlurmJobExecutionRecord]:
    """Return held-child candidates whose edge resolves to this exact attempt.

    The initial ID/name query finds possible edges only. Every candidate is
    then passed through the authoritative resolver to enforce workflow scoping,
    ambiguity handling, and exact-attempt matching before terminal Advice can
    propagate.
    """
    candidates = read_all_dag_dependencies(
        storage,
        dependency_job_identifier=parent_record.job_id,
        dependency_job_identifier_type="id",
    )
    if parent_record.job_name:
        candidates.extend(
            read_all_dag_dependencies(
                storage,
                dependency_job_identifier=parent_record.job_name,
                dependency_job_identifier_type="name",
            )
        )

    resolved: dict[str, SlurmJobExecutionRecord] = {}
    for dep in candidates:
        dependent_record = storage.read_job_record(
            job_id=dep.dependent_job_id,
            job_attempt_index=0,
        )
        if dependent_record is None:
            logger.warning(
                f"Cannot terminate DAG dependent {dep.dependent_job_id}: initial attempt record is not available."
            )
            continue
        resolved_parent = resolve_dag_parent(
            storage,
            dep,
            dependent_record,
            observed_parent=parent_record,
        )
        if resolved_parent is None or resolved_parent.job_id != parent_record.job_id:
            continue
        if resolved_parent.job_attempt_index != parent_record.job_attempt_index:
            logger.warning(
                f"DAG dependency for child {dependent_record.job_id} resolved to parent "
                f"{resolved_parent.job_id} attempt {resolved_parent.job_attempt_index}, not terminal Advice attempt "
                f"{parent_record.job_attempt_index}; failing closed."
            )
            continue
        resolved[dependent_record.job_id] = dependent_record
    return resolved


def propagate_terminated_advice(
    storage: MySQLStorage,
    parent_advice: AdviceRecord,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
    terminal_reason: str | None = None,
) -> None:
    """Abort every reachable held descendant when a parent has terminal Advice.

    Each descendant gate is marked ``ABORTED`` with parent-causality details,
    and the child receives exact-attempt ``TERMINATED`` Advice. The
    breadth-first traversal continues through already-final intermediaries so
    their held descendants cannot be stranded. A processed-attempt set makes
    repeated calls and invalid cyclic input safe.

    Args:
        storage: Durable DAG and Advice storage.
        parent_advice: Terminal Advice that starts propagation.
        audit_buffer: Buffer that records each committed descendant Advice transition.
        terminal_reason: Optional specific terminal cause to include in the
            dependency explanation; inferred from the parent Advice when omitted.
    """
    if parent_advice.advice_state != AdviceState.TERMINATED.value:
        return

    pending: deque[tuple[AdviceRecord, str]] = deque(
        [(parent_advice, terminal_reason or _terminal_reason_from_advice(parent_advice))]
    )
    processed: set[tuple[str, int]] = set()

    while pending:
        current_advice, current_reason = pending.popleft()
        parent_key = (current_advice.job_id, current_advice.attempt_index)
        if parent_key in processed:
            continue
        processed.add(parent_key)

        parent_record = storage.read_job_record(
            job_id=current_advice.job_id,
            job_attempt_index=current_advice.attempt_index,
        )
        if parent_record is None:
            logger.warning(
                f"Cannot propagate terminal DAG Advice for job {current_advice.job_id} attempt "
                f"{current_advice.attempt_index}: exact parent record is not available."
            )
            continue

        for dependent_record in _matching_dependents_for_parent(storage, parent_record).values():
            existing_child_advice = storage.read_advice_by_job(
                job_id=dependent_record.job_id,
                attempt_index=dependent_record.job_attempt_index,
            )
            child_is_already_terminated = (
                existing_child_advice is not None and existing_child_advice.advice_state == AdviceState.TERMINATED.value
            )
            if dependent_record.state != SlurmJobNonFinalStates.PENDING.value and not child_is_already_terminated:
                logger.warning(
                    f"DAG dependent {dependent_record.job_id} is {dependent_record.state}, not PENDING; "
                    "the held-child invariant is not satisfied, so termination propagation fails closed."
                )
                continue

            now = datetime.now(UTC)
            all_child_deps = read_all_dag_dependencies(storage, dependent_job_id=dependent_record.job_id)
            for child_dep in all_child_deps:
                storage.update_dag_dependency_status(
                    id=child_dep.id,
                    action_status=DagDependencyActionStatus.ABORTED.value,
                    action_time=now,
                    reason_code=DagDependencyReasonCode.PARENT_TERMINATED.value,
                    reason_message=(
                        f"Parent job {parent_record.job_id} reached terminal Advice; "
                        f"dependent job {dependent_record.job_id} cannot be released"
                    ),
                    reason_details={
                        "parent_attempt": current_advice.attempt_index,
                        "parent_job_id": parent_record.job_id,
                        "parent_scheduler_state": parent_record.state,
                        "parent_terminal_advice_reason": current_reason,
                        "workflow_id": dependent_record.workflow_id,
                    },
                    last_evaluated_at=now,
                )

            child_advice = _build_dependent_terminated_advice(
                storage,
                dependent_record=dependent_record,
            )
            persisted_child, _, _ = persist_advice_record(
                storage,
                audit_buffer,
                child_advice,
                audit_reason=_DAG_TERMINATION_AUDIT_REASON,
            )
            logger.info(
                f"Persisted DAG termination Advice for job {dependent_record.job_id} attempt "
                f"{dependent_record.job_attempt_index} because parent {parent_record.job_id} attempt "
                f"{parent_record.job_attempt_index} is terminal."
            )
            pending.append((persisted_child, DAG_PARENT_TERMINATED_REASON))


def reconcile_registered_job(
    storage: MySQLStorage,
    dependent_record: SlurmJobExecutionRecord,
    *,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> list[SlurmJobExecutionRecord]:
    """Apply already-persisted terminal parents and return completed parents.

    This closes the registration race where a child arrives after a parent has
    already completed or been terminated. A terminal parent aborts the child
    immediately; completed parents are returned so the caller can evaluate
    whether all of the child's gates are now satisfied.
    """
    completed_parents: list[SlurmJobExecutionRecord] = []
    for dep in read_all_dag_dependencies(storage, dependent_job_id=dependent_record.job_id):
        parent_record = resolve_dag_parent(storage, dep, dependent_record)
        if parent_record is None:
            continue
        parent_advice = storage.read_advice_by_job(
            job_id=parent_record.job_id,
            attempt_index=parent_record.job_attempt_index,
        )
        if parent_advice is not None and parent_advice.advice_state == AdviceState.TERMINATED.value:
            propagate_terminated_advice(
                storage,
                parent_advice,
                audit_buffer=audit_buffer,
            )
            return []
        if parent_record.state == SlurmJobFinalStates.COMPLETED.value:
            completed_parents.append(parent_record)
    return completed_parents
