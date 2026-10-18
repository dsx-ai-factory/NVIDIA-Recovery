# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assertions over the audit trail exposed by Notifications."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

AUDIT_PAGE_LIMIT = 100
AUDIT_MAX_PAGES = 50
# Producer identities used by the Notifications audit API.
JOB_MANAGER_PRODUCER = "job-manager"
ADI_PRODUCER = "anomaly-detection-isolator"
SAC_PRODUCER = "slurm-advisor-controller"
WATCHDOG_PRODUCER = "slurm-watchdog"
DAG_RELEASE_ADVICE_REASON = "DAG_DEPENDENCIES_SATISFIED"
DAG_TERMINATION_ADVICE_REASON = "DAG_PARENT_TERMINATED"

JOB_SNAPSHOT_FIELDS = frozenset(
    {
        "job_id",
        "heimdall_job_id",
        "job_attempt_index",
        "state",
        "state_reason",
        "submit_time",
        "start_time",
        "end_time",
        "cluster",
        "partition",
        "reservation",
        "namespace",
        "worker_cluster_name",
        "node_count",
        "total_gpu_count",
        "total_cpu_count",
        "total_memory_mb",
        "allocated_node_list",
        "inclusion_node_list",
        "exclusion_node_list",
        "failed_node_list",
        "sticky_exclusion_node_list",
        "aps_spec",
        "config_validation_failure_types",
    }
)
JOB_UPDATE_SNAPSHOT_FIELDS = JOB_SNAPSHOT_FIELDS
JOB_OBSERVED_PAYLOAD_FIELDS = frozenset({"job_id", "attempt_index", "job"})
JOB_UPDATED_PAYLOAD_FIELDS = frozenset({"job_id", "attempt_index", "old_job", "new_job"})
ADVICE_SNAPSHOT_FIELDS = frozenset(
    {
        "id",
        "visible",
        "job_id",
        "restart_count",
        "scheduler_metadata",
        "advice",
        "generation",
        "desired_state_changed_at",
        "reconciliation_status",
        "issues",
        "fact_attribution",
        "recovery_context",
        "created_at",
    }
)
ADVICE_RECOMMENDATION_FIELDS = frozenset({"state", "node_constraints"})
NODE_CONSTRAINTS_FIELDS = frozenset({"exclusions", "inclusions"})
RECOVERY_CORRELATION_FIELDS = frozenset({"recovery_sequence_id", "trace_id", "span_id"})
ADVICE_CREATED_PAYLOAD_FIELDS = (
    frozenset({"advice_id", "job_id", "attempt_index", "advice", "creation_reasons", "correlation_uuid"})
    | RECOVERY_CORRELATION_FIELDS
)
ADVICE_UPDATED_PAYLOAD_FIELDS = (
    frozenset(
        {
            "advice_id",
            "job_id",
            "attempt_index",
            "old_advice",
            "new_advice",
            "change_reasons",
            "correlation_uuid",
        }
    )
    | RECOVERY_CORRELATION_FIELDS
)


@dataclass(frozen=True, slots=True)
class SubmittedJob:
    job_id: str
    submitted_at: str


@dataclass(frozen=True, slots=True)
class OneOf:
    """Match one value from a finite set of accepted observations."""

    values: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class ObservedAuditEvent:
    """Public event representation returned by Notifications."""

    heimdall_job_id: str
    event_producer: str
    event_type: str
    event_level: str
    payload: Mapping[str, Any]
    event_produced_at: int


@dataclass(frozen=True, slots=True)
class EventExpectation:
    """One audit fact, optionally linked to a companion by a nonempty payload field."""

    name: str
    event_producer: str | None = None
    event_type: str | None = None
    event_level: str | None = None
    payload_subset: Mapping[str, object] = field(default_factory=dict)
    matcher: Callable[[ObservedAuditEvent], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    paired_with: EventExpectation | None = None
    pair_field: str = "request_uuid"


@dataclass(frozen=True, slots=True)
class AuditTrailEvaluation:
    """One deterministic comparison of observations with expectations."""

    satisfied: bool
    irreversible_failure: bool
    summary: str


def heimdall_job_id(job_id: str, attempt: int) -> str:
    """Return the canonical cross-component identity for a Slurm job attempt."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{job_id}-{attempt}"))


def job_payload(job: SubmittedJob, attempt: int) -> dict[str, object]:
    """Return the identity fields common to Job Manager audit payloads."""

    return {"job_id": job.job_id, "attempt_index": attempt}


def job_observed_event(
    job: SubmittedJob,
    *,
    attempt: int = 0,
    job_subset: Mapping[str, object] | None = None,
    name: str = "job-observed",
) -> EventExpectation:
    """Return the initial observation fact for one job attempt."""

    observed_payload: dict[str, object] = job_payload(job, attempt)
    observed_payload["job"] = {
        **(job_subset or {}),
        "job_id": job.job_id,
        "job_attempt_index": attempt,
    }
    return EventExpectation(
        name,
        event_producer=JOB_MANAGER_PRODUCER,
        event_type="job_attempt_observed",
        event_level="operational",
        payload_subset=observed_payload,
    )


def job_state_transition_event(
    job: SubmittedJob,
    *,
    new_state: str,
    old_states: tuple[str, ...],
    attempt: int = 0,
    old_job_subset: Mapping[str, object] | None = None,
    new_job_subset: Mapping[str, object] | None = None,
    name: str | None = None,
) -> EventExpectation:
    """Return a meaningful state-transition fact for one job attempt."""

    return EventExpectation(
        name or f"job-{new_state.lower()}",
        event_producer=JOB_MANAGER_PRODUCER,
        event_type="job_attempt_updated",
        event_level="operational",
        payload_subset={
            **job_payload(job, attempt),
            "old_job": {**(old_job_subset or {}), "state": OneOf(old_states)},
            "new_job": {**(new_job_subset or {}), "state": new_state},
        },
    )


def job_started_event(
    job: SubmittedJob,
    *,
    attempt: int = 0,
) -> EventExpectation:
    """Return the expected transition from pending to running."""

    return EventExpectation(
        "job-started",
        event_producer=JOB_MANAGER_PRODUCER,
        event_type="job_attempt_updated",
        event_level="operational",
        payload_subset={
            **job_payload(job, attempt),
            "old_job": {"state": "PENDING"},
            "new_job": {"state": "RUNNING"},
        },
    )


def _advice_payload(job: SubmittedJob, attempt: int) -> dict[str, object]:
    return {
        "advice_id": heimdall_job_id(job.job_id, attempt),
        **job_payload(job, attempt),
    }


def adi_fast_attribution_exchange_event(
    job: SubmittedJob, *, attempt: int = 0, response_subset: Mapping[str, object] | None = None
) -> EventExpectation:
    """Require a persisted ADI fast request and its matching response."""

    identity = _advice_payload(job, attempt)
    request = EventExpectation(
        "adi-fast-attribution-request",
        event_producer=ADI_PRODUCER,
        event_type="attribution_request_forwarded",
        event_level="operational",
        payload_subset={
            **identity,
            "request_payload": {
                "workload_id": job.job_id,
                "phase": "fast",
                "workload_context": {"attempt_index": attempt},
            },
        },
    )
    return EventExpectation(
        "adi-fast-attribution-exchange",
        event_producer=ADI_PRODUCER,
        event_type="attribution_response_received",
        event_level="operational",
        payload_subset={
            **identity,
            "response_payload": {"request_id": job.job_id, "error": None, **(response_subset or {})},
        },
        paired_with=request,
    )


def advice_reached_event(
    job: SubmittedJob,
    *,
    state: str,
    cause: str,
    attempt: int = 0,
    visible: bool | None = None,
    advice_subset: Mapping[str, object] | None = None,
) -> EventExpectation:
    """Match Advice creation or update reaching a scenario-significant state."""

    def reached(event: ObservedAuditEvent) -> bool:
        if event.event_type == "advice_created":
            snapshot = event.payload.get("advice")
            reasons = event.payload.get("creation_reasons")
        elif event.event_type == "advice_updated":
            snapshot = event.payload.get("new_advice")
            reasons = event.payload.get("change_reasons")
        else:
            return False

        if not isinstance(snapshot, Mapping):
            return False
        recommendation = snapshot.get("advice")
        if not isinstance(recommendation, Mapping) or recommendation.get("state") != state:
            return False
        if visible is not None and snapshot.get("visible") is not visible:
            return False
        if advice_subset is not None and not _matches(advice_subset, snapshot):
            return False
        if isinstance(reasons, Sequence) and not isinstance(reasons, (str, bytes)) and cause in reasons:
            return True

        issues = snapshot.get("issues")
        if (
            isinstance(issues, Sequence)
            and not isinstance(issues, (str, bytes))
            and any(isinstance(issue, Mapping) and issue.get("issue_type") == cause for issue in issues)
        ):
            return True

        attribution = snapshot.get("fact_attribution")
        return isinstance(attribution, Mapping) and attribution.get("anomaly_type") == cause

    return EventExpectation(
        f"advice-reached-{state.lower()}-{cause.lower()}",
        event_producer=JOB_MANAGER_PRODUCER,
        event_level="recovery",
        payload_subset=_advice_payload(job, attempt),
        matcher=reached,
    )


def scheduler_action_event(
    job: SubmittedJob,
    *,
    action: str,
    attempt: int = 0,
) -> EventExpectation:
    """Return a successful scheduler-action completion expectation."""

    return EventExpectation(
        f"scheduler-{action.removeprefix('slurm.').replace('_', '-')}",
        event_producer=SAC_PRODUCER,
        event_type="advice_scheduler_action_completed",
        event_level="recovery",
        payload_subset={
            **_advice_payload(job, attempt),
            "action": action,
            "success": True,
        },
    )


def advice_application_applied_event(
    job: SubmittedJob,
    *,
    attempt: int = 0,
) -> EventExpectation:
    """Return the fact that SAC successfully applied the Advice."""

    return EventExpectation(
        "advice-application-applied",
        event_producer=SAC_PRODUCER,
        event_type="advice_application_applied",
        event_level="recovery",
        payload_subset=_advice_payload(job, attempt),
    )


def advice_application_evaluated_event(
    job: SubmittedJob,
    *,
    action: str,
    attempt: int = 0,
) -> EventExpectation:
    """Return an observe-mode Advice evaluation expectation."""

    return EventExpectation(
        "advice-application-evaluated",
        event_producer=SAC_PRODUCER,
        event_type="advice_application_evaluated",
        event_level="recovery",
        payload_subset={
            **_advice_payload(job, attempt),
            "outcome": "planned",
            "planned_actions": (action,),
        },
    )


def advice_reconciliation_converged_event(
    job: SubmittedJob,
    *,
    attempt: int = 0,
    reason: str = "DESIRED_STATE_OBSERVED",
) -> EventExpectation:
    """Return the fact that SAC observed the Advice's desired state."""

    return EventExpectation(
        "advice-reconciliation-converged",
        event_producer=SAC_PRODUCER,
        event_type="advice_reconciliation_converged",
        event_level="recovery",
        payload_subset={**_advice_payload(job, attempt), "reason": reason},
    )


def watcher_anomaly_detected_event(
    job: SubmittedJob,
    *,
    anomaly_type: str,
    attempt: int = 0,
) -> EventExpectation:
    """Return a Watcher anomaly-detection expectation."""

    def has_source_occurrence_time(event: ObservedAuditEvent) -> bool:
        occurred_at = event.payload.get("event_occurred_at")
        return isinstance(occurred_at, int) and not isinstance(occurred_at, bool) and occurred_at >= 0

    return EventExpectation(
        "watcher-anomaly-detected",
        event_producer="watcher",
        event_type="anomaly_detected",
        event_level="operational",
        payload_subset={
            "job_id": job.job_id,
            "attempt_index": attempt,
            "anomaly_type": anomaly_type,
        },
        matcher=has_source_occurrence_time,
    )


def automatic_requeue_hold_event(
    job: SubmittedJob,
    *,
    attempt: int = 0,
) -> EventExpectation:
    """Return an automatic requeue-hold completion expectation."""

    return EventExpectation(
        "auto-requeue-hold-completed",
        event_producer=SAC_PRODUCER,
        event_type="job_auto_requeue_hold_completed",
        event_level="recovery",
        payload_subset={**job_payload(job, attempt), "success": True},
    )


def watchdog_release_completed_event(
    job: SubmittedJob,
    *,
    attempt: int,
) -> EventExpectation:
    """Return the fact that Watchdog successfully released the attempt."""

    return EventExpectation(
        "watchdog-release-completed",
        event_producer=WATCHDOG_PRODUCER,
        event_type="watchdog_release_completed",
        event_level="recovery",
        payload_subset={**job_payload(job, attempt), "success": True},
    )


def _matches(expected: object, observed: object) -> bool:
    if isinstance(expected, OneOf):
        return observed in expected.values
    if isinstance(expected, Mapping):
        return isinstance(observed, Mapping) and all(
            key in observed and _matches(value, observed[key]) for key, value in expected.items()
        )
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        return (
            isinstance(observed, Sequence)
            and not isinstance(observed, (str, bytes))
            and len(expected) == len(observed)
            and all(
                _matches(expected_value, observed_value) for expected_value, observed_value in zip(expected, observed)
            )
        )
    return observed == expected


def _event_matches(expectation: EventExpectation, event: ObservedAuditEvent) -> bool:
    return (
        (expectation.event_producer is None or event.event_producer == expectation.event_producer)
        and (expectation.event_type is None or event.event_type == expectation.event_type)
        and (expectation.event_level is None or event.event_level == expectation.event_level)
        and _matches(expectation.payload_subset, event.payload)
        and (expectation.matcher is None or expectation.matcher(event))
    )


def _invalid_snapshot_fields(
    snapshot: Mapping[str, object],
    *,
    expected_fields: frozenset[str],
    snapshot_name: str,
) -> str | None:
    observed_fields = frozenset(snapshot)
    if observed_fields == expected_fields:
        return None
    missing = sorted(expected_fields - observed_fields)
    unexpected = sorted(observed_fields - expected_fields)
    return f"{snapshot_name} is not complete: missing={missing!r}, unexpected={unexpected!r}"


def _invalid_job_event(event: ObservedAuditEvent) -> str | None:
    if event.event_producer != JOB_MANAGER_PRODUCER:
        return None
    expected_identity = {
        "job_id": event.payload.get("job_id"),
        "job_attempt_index": event.payload.get("attempt_index"),
    }
    if event.event_type == "job_attempt_observed":
        expected_payload_fields = JOB_OBSERVED_PAYLOAD_FIELDS
        expected_snapshot_fields = JOB_SNAPSHOT_FIELDS
        snapshots = (("job", event.payload.get("job")),)
    elif event.event_type == "job_attempt_updated":
        expected_payload_fields = JOB_UPDATED_PAYLOAD_FIELDS
        expected_snapshot_fields = JOB_UPDATE_SNAPSHOT_FIELDS
        snapshots = (
            ("old_job", event.payload.get("old_job")),
            ("new_job", event.payload.get("new_job")),
        )
    else:
        return None
    if invalid_fields := _invalid_snapshot_fields(
        event.payload,
        expected_fields=expected_payload_fields,
        snapshot_name=f"{event.event_type} payload",
    ):
        return invalid_fields

    for snapshot_name, snapshot in snapshots:
        if not isinstance(snapshot, Mapping):
            return f"{event.event_type} has no complete {snapshot_name} snapshot"
        if invalid_fields := _invalid_snapshot_fields(
            snapshot,
            expected_fields=expected_snapshot_fields,
            snapshot_name=snapshot_name,
        ):
            return invalid_fields
        observed_identity = {
            "job_id": snapshot.get("job_id"),
            "job_attempt_index": snapshot.get("job_attempt_index"),
        }
        if observed_identity != expected_identity:
            return (
                f"{event.event_type} {snapshot_name} identity {observed_identity!r} "
                f"does not match payload identity {expected_identity!r}"
            )
        if snapshot.get("heimdall_job_id") != event.heimdall_job_id:
            return (
                f"{event.event_type} {snapshot_name} heimdall identity "
                f"{snapshot.get('heimdall_job_id')!r} does not match event identity {event.heimdall_job_id!r}"
            )
    if event.event_type == "job_attempt_updated" and snapshots[0][1] == snapshots[1][1]:
        return "job_attempt_updated snapshots are unchanged"
    return None


def _invalid_advice_snapshot(
    event: ObservedAuditEvent,
    snapshot_name: str,
    snapshot: object,
) -> str | None:
    if not isinstance(snapshot, Mapping):
        return f"{event.event_type} has no complete {snapshot_name} snapshot"
    if invalid_fields := _invalid_snapshot_fields(
        snapshot,
        expected_fields=ADVICE_SNAPSHOT_FIELDS,
        snapshot_name=snapshot_name,
    ):
        return invalid_fields
    recommendation = snapshot.get("advice")
    if not isinstance(recommendation, Mapping):
        return f"{snapshot_name}.advice is not a complete recommendation"
    if invalid_fields := _invalid_snapshot_fields(
        recommendation,
        expected_fields=ADVICE_RECOMMENDATION_FIELDS,
        snapshot_name=f"{snapshot_name}.advice",
    ):
        return invalid_fields
    node_constraints = recommendation.get("node_constraints")
    if node_constraints is not None:
        if not isinstance(node_constraints, Mapping):
            return f"{snapshot_name}.advice.node_constraints is not a complete object"
        if invalid_fields := _invalid_snapshot_fields(
            node_constraints,
            expected_fields=NODE_CONSTRAINTS_FIELDS,
            snapshot_name=f"{snapshot_name}.advice.node_constraints",
        ):
            return invalid_fields
    expected_identity = {
        "id": event.payload.get("advice_id"),
        "job_id": event.payload.get("job_id"),
        "restart_count": event.payload.get("attempt_index"),
    }
    observed_identity = {field: snapshot.get(field) for field in expected_identity}
    if observed_identity != expected_identity:
        return (
            f"{event.event_type} {snapshot_name} identity {observed_identity!r} "
            f"does not match payload identity {expected_identity!r}"
        )
    return None


def _invalid_reasons(value: object, *, allow_empty: bool) -> bool:
    return (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or (not value and not allow_empty)
        or any(not isinstance(reason, str) or not reason for reason in value)
    )


def _invalid_advice_event(event: ObservedAuditEvent) -> str | None:
    if event.event_producer != JOB_MANAGER_PRODUCER:
        return None
    if event.event_type not in ("advice_created", "advice_updated"):
        return None
    advice_id = event.payload.get("advice_id")
    if event.heimdall_job_id != advice_id:
        return f"{event.event_type} has inconsistent advice or event identities"
    # Recovery sequences can span attempts; their correlation ID is independent
    # of this attempt's Advice ID. Legacy events use the Advice ID as a fallback.
    correlation_uuid = event.payload.get("correlation_uuid")
    if not isinstance(correlation_uuid, str) or not correlation_uuid:
        return f"{event.event_type} has no non-empty correlation identity"
    if event.event_type == "advice_created":
        if invalid_fields := _invalid_snapshot_fields(
            event.payload,
            expected_fields=ADVICE_CREATED_PAYLOAD_FIELDS,
            snapshot_name="advice_created payload",
        ):
            return invalid_fields
        reasons = event.payload.get("creation_reasons")
        if _invalid_reasons(reasons, allow_empty=False):
            return "advice_created has no non-empty creation reasons"
        return _invalid_advice_snapshot(event, "advice", event.payload.get("advice"))

    if invalid_fields := _invalid_snapshot_fields(
        event.payload,
        expected_fields=ADVICE_UPDATED_PAYLOAD_FIELDS,
        snapshot_name="advice_updated payload",
    ):
        return invalid_fields
    change_reasons = event.payload.get("change_reasons")
    if _invalid_reasons(change_reasons, allow_empty=True):
        return "advice_updated has invalid change reasons"
    old_advice = event.payload.get("old_advice")
    new_advice = event.payload.get("new_advice")
    for snapshot_name, snapshot in (("old_advice", old_advice), ("new_advice", new_advice)):
        if invalid_snapshot := _invalid_advice_snapshot(event, snapshot_name, snapshot):
            return invalid_snapshot
    if old_advice == new_advice:
        return "advice_updated snapshots are unchanged"
    return None


def _event_name(event: ObservedAuditEvent, index: int) -> str:
    return f"event[{index}]={event.event_producer}/{event.event_type}/{event.event_level}"


def _matching_indices(expected: EventExpectation, events: Sequence[ObservedAuditEvent]) -> list[int]:
    candidates = [index for index, event in enumerate(events) if _event_matches(expected, event)]
    if expected.paired_with is None:
        return candidates

    # ADI delivers the request and response independently. A response only
    # satisfies this expectation when its request is in the observed list.
    partner_keys = {
        key
        for event in events
        if _event_matches(expected.paired_with, event)
        if isinstance(key := event.payload.get(expected.pair_field), str) and key
    }
    return [
        index
        for index in candidates
        if isinstance(key := events[index].payload.get(expected.pair_field), str) and key in partner_keys
    ]


def evaluate_audit_trail(
    events: Sequence[ObservedAuditEvent],
    contains: Sequence[EventExpectation],
) -> AuditTrailEvaluation:
    """Validate observations and require each scenario-significant fact."""

    failures = [
        failure
        for event in events
        for validator in (_invalid_job_event, _invalid_advice_event)
        if (failure := validator(event)) is not None
    ]
    if failures:
        return AuditTrailEvaluation(False, True, "; ".join(failures))

    matches = [(expected.name, _matching_indices(expected, events)) for expected in contains]
    missing = [name for name, indices in matches if not indices]
    if missing:
        matched_indices = {index for _name, indices in matches for index in indices}
        context = [_event_name(event, index) for index, event in enumerate(events) if index not in matched_indices]
        summary = f"missing audit facts: {missing!r}"
        if context:
            summary += f"; unmatched valid events: {context!r}"
        return AuditTrailEvaluation(False, False, summary)

    summary = ", ".join(f"{name}>={len(indices)}" for name, indices in matches)
    return AuditTrailEvaluation(True, False, summary or "audit trail satisfied")
