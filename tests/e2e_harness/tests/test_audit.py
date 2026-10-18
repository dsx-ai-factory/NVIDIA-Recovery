# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cluster-free contract tests for the E2E audit oracle."""

from __future__ import annotations

from dataclasses import replace

import pytest
from e2e_harness.audit import (
    DAG_RELEASE_ADVICE_REASON,
    DAG_TERMINATION_ADVICE_REASON,
    EventExpectation,
    ObservedAuditEvent,
    OneOf,
    SubmittedJob,
    adi_fast_attribution_exchange_event,
    advice_application_applied_event,
    advice_application_evaluated_event,
    advice_reached_event,
    advice_reconciliation_converged_event,
    automatic_requeue_hold_event,
    evaluate_audit_trail,
    heimdall_job_id,
    job_observed_event,
    job_started_event,
    job_state_transition_event,
    scheduler_action_event,
    watchdog_release_completed_event,
    watcher_anomaly_detected_event,
)


def _recovery_correlation(recovery_sequence_id: str | None = None) -> dict[str, object]:
    return {"recovery_sequence_id": recovery_sequence_id, "trace_id": None, "span_id": None}


def _event(
    event_type: str,
    *,
    level: str = "operational",
    payload: dict[str, object] | None = None,
    produced_at: int = 100,
    heimdall_id: str = "attempt-id",
) -> ObservedAuditEvent:
    return ObservedAuditEvent(
        heimdall_job_id=heimdall_id,
        event_producer="job-manager",
        event_type=event_type,
        event_level=level,
        payload=payload or {},
        event_produced_at=produced_at,
    )


def _job_snapshot(*, job_id: str, attempt: int, state: str) -> dict[str, object]:
    # Specify the producer projection independently of the oracle: deriving this
    # fixture from its field set would hide requirements for retired fields.
    return {
        "job_id": job_id,
        "heimdall_job_id": "attempt-id",
        "job_attempt_index": attempt,
        "state": state,
        "state_reason": None,
        "submit_time": "2026-07-24T01:19:53Z",
        "start_time": None,
        "end_time": None,
        "cluster": "slurm",
        "partition": "batch",
        "reservation": None,
        "namespace": None,
        "worker_cluster_name": None,
        "node_count": 1,
        "total_gpu_count": 0,
        "total_cpu_count": 1,
        "total_memory_mb": 1024,
        "allocated_node_list": [],
        "inclusion_node_list": [],
        "exclusion_node_list": [],
        "failed_node_list": [],
        "sticky_exclusion_node_list": [],
        "aps_spec": None,
        "config_validation_failure_types": [],
    }


def _advice_snapshot(
    *,
    advice_id: str,
    job_id: str,
    attempt: int,
    state: str | None,
    visible: bool,
) -> dict[str, object]:
    # Audit Advice snapshots omit updated_at even though the public API has it.
    return {
        "id": advice_id,
        "visible": visible,
        "job_id": job_id,
        "restart_count": attempt,
        "scheduler_metadata": {},
        "advice": {"state": state, "node_constraints": None},
        "generation": 1,
        "desired_state_changed_at": None,
        "reconciliation_status": None,
        "issues": [],
        "fact_attribution": None,
        "recovery_context": None,
        "created_at": "2026-07-24T01:19:53Z",
    }


def test_evaluator_requires_facts_without_imposing_order_or_exclusivity() -> None:
    events = (
        _event(
            "scheduler_action",
            level="recovery",
            payload={"action": "slurm.release", "success": True},
            produced_at=300,
        ),
        ObservedAuditEvent(
            heimdall_job_id="attempt-id",
            event_producer="future-component",
            event_type="new-valid-event",
            event_level="operational",
            payload={"detail": "additional evidence"},
            event_produced_at=50,
        ),
        _event("decision", payload={"state": "Released", "cause": "DAG"}, produced_at=200),
        _event("decision", payload={"state": "Released", "cause": "DAG"}, produced_at=201),
    )
    contains = (
        EventExpectation("released", event_type="decision", payload_subset={"state": "Released"}),
        EventExpectation("dag-caused", event_type="decision", payload_subset={"cause": "DAG"}),
        EventExpectation(
            "scheduler-release",
            event_type="scheduler_action",
            payload_subset={"action": "slurm.release", "success": True},
        ),
    )

    evaluation = evaluate_audit_trail(events, contains)

    assert evaluation.satisfied, evaluation.summary
    assert "released>=2" in evaluation.summary
    assert "dag-caused>=2" in evaluation.summary


def test_evaluator_treats_missing_facts_as_retryable() -> None:
    evaluation = evaluate_audit_trail(
        (_event("decision", payload={"state": "Released"}),),
        (
            EventExpectation("released", event_type="decision", payload_subset={"state": "Released"}),
            EventExpectation("applied", event_type="advice_application_applied"),
        ),
    )

    assert not evaluation.satisfied
    assert not evaluation.irreversible_failure
    assert "'applied'" in evaluation.summary


def test_adi_fast_exchange_requires_a_persisted_request_response_pair() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    attempt_id = heimdall_job_id(job.job_id, 0)
    identity = {"advice_id": attempt_id, "job_id": job.job_id, "attempt_index": 0}
    request = ObservedAuditEvent(
        heimdall_job_id=attempt_id,
        event_producer="anomaly-detection-isolator",
        event_type="attribution_request_forwarded",
        event_level="operational",
        payload={
            **identity,
            "request_uuid": "fast-request",
            "request_payload": {
                "workload_id": job.job_id,
                "phase": "fast",
                "workload_context": {"attempt_index": 0},
            },
        },
        event_produced_at=100,
    )
    response = ObservedAuditEvent(
        heimdall_job_id=attempt_id,
        event_producer="anomaly-detection-isolator",
        event_type="attribution_response_received",
        event_level="operational",
        payload={
            **identity,
            "request_uuid": "fast-request",
            "response_payload": {"request_id": job.job_id, "error": None},
        },
        event_produced_at=101,
    )
    expected = (adi_fast_attribution_exchange_event(job),)

    # The list API may return independently delivered events in either order.
    assert evaluate_audit_trail((response, request), expected).satisfied

    unrelated_response = replace(response, payload={**response.payload, "request_uuid": "other-request"})
    unmatched = evaluate_audit_trail((request, unrelated_response), expected)
    assert not unmatched.satisfied and not unmatched.irreversible_failure
    assert "adi-fast-attribution-exchange" in unmatched.summary

    slow_request = replace(
        request,
        payload={**request.payload, "request_payload": {**request.payload["request_payload"], "phase": "slow"}},
    )
    assert not evaluate_audit_trail((slow_request, response), expected).satisfied
    wrong_attempt = replace(response, payload={**response.payload, "attempt_index": 1})
    assert not evaluate_audit_trail((request, wrong_attempt), expected).satisfied
    missing_uuid = replace(response, payload={**response.payload, "request_uuid": ""})
    assert not evaluate_audit_trail((request, missing_uuid), expected).satisfied
    assert not evaluate_audit_trail((response,), expected).satisfied
    assert not evaluate_audit_trail((request,), expected).satisfied

    unavailable = replace(
        response,
        payload={
            **response.payload,
            "response_payload": {"request_id": job.job_id, "attributions": [], "error": {"type": "service_error"}},
        },
    )
    assert not evaluate_audit_trail((request, unavailable), expected).satisfied
    outage_expected = (
        adi_fast_attribution_exchange_event(
            job, response_subset={"attributions": [], "error": {"type": "service_error"}}
        ),
    )
    assert evaluate_audit_trail((request, unavailable), outage_expected).satisfied
    assert not evaluate_audit_trail((request, response), outage_expected).satisfied
    falsely_attributed = replace(
        unavailable,
        payload={
            **unavailable.payload,
            "response_payload": {**unavailable.payload["response_payload"], "attributions": [{"node": "node-a"}]},
        },
    )
    assert not evaluate_audit_trail((request, falsely_attributed), outage_expected).satisfied


def test_evaluator_rejects_incomplete_or_unchanged_lifecycle_payloads() -> None:
    events = (
        _event(
            "job_attempt_updated",
            payload={"job_id": "123", "attempt_index": 0, "old_job": {}, "new_job": {}},
        ),
        _event(
            "job_attempt_updated",
            payload={
                "job_id": "123",
                "attempt_index": 0,
                "old_job": _job_snapshot(job_id="123", attempt=0, state="RUNNING"),
                "new_job": _job_snapshot(job_id="123", attempt=0, state="RUNNING"),
            },
            produced_at=101,
        ),
        _event(
            "advice_created",
            level="recovery",
            payload={
                "advice_id": "attempt-id",
                "job_id": "123",
                "attempt_index": 0,
                "advice": _advice_snapshot(
                    advice_id="attempt-id",
                    job_id="123",
                    attempt=0,
                    state=None,
                    visible=False,
                ),
                "creation_reasons": [],
                "correlation_uuid": "attempt-id",
                **_recovery_correlation(),
            },
            produced_at=102,
        ),
        _event(
            "advice_updated",
            level="recovery",
            payload={
                "advice_id": "attempt-id",
                "job_id": "123",
                "attempt_index": 0,
                "correlation_uuid": "attempt-id",
                **_recovery_correlation(),
            },
            produced_at=103,
        ),
        _event(
            "advice_updated",
            level="recovery",
            payload={
                "advice_id": "attempt-id",
                "job_id": "123",
                "attempt_index": 0,
                "old_advice": _advice_snapshot(
                    advice_id="attempt-id",
                    job_id="123",
                    attempt=0,
                    state="Restarted",
                    visible=True,
                ),
                "new_advice": _advice_snapshot(
                    advice_id="attempt-id",
                    job_id="123",
                    attempt=0,
                    state="Restarted",
                    visible=True,
                ),
                "change_reasons": [],
                "correlation_uuid": "attempt-id",
                **_recovery_correlation(),
            },
            produced_at=104,
        ),
    )

    evaluation = evaluate_audit_trail(events, ())

    assert not evaluation.satisfied
    assert evaluation.irreversible_failure
    assert "old_job is not complete" in evaluation.summary
    assert "snapshots are unchanged" in evaluation.summary
    assert "has no non-empty creation reasons" in evaluation.summary
    assert "advice_updated payload is not complete" in evaluation.summary
    assert "advice_updated snapshots are unchanged" in evaluation.summary


def test_evaluator_rejects_incomplete_nested_advice_snapshot() -> None:
    snapshot = _advice_snapshot(
        advice_id="attempt-id",
        job_id="123",
        attempt=0,
        state="Restarted",
        visible=True,
    )
    snapshot["advice"] = {"state": "Restarted"}
    event = _event(
        "advice_created",
        level="recovery",
        payload={
            "advice_id": "attempt-id",
            "job_id": "123",
            "attempt_index": 0,
            "advice": snapshot,
            "creation_reasons": ["CRASH"],
            "correlation_uuid": "attempt-id",
            **_recovery_correlation(),
        },
    )

    evaluation = evaluate_audit_trail((event,), ())

    assert not evaluation.satisfied
    assert evaluation.irreversible_failure
    assert "advice.advice is not complete" in evaluation.summary


def test_evaluator_rejects_storage_timestamps_from_update_events() -> None:
    complete_old_advice = _advice_snapshot(
        advice_id="attempt-id",
        job_id="123",
        attempt=0,
        state=None,
        visible=False,
    )
    complete_new_advice = _advice_snapshot(
        advice_id="attempt-id",
        job_id="123",
        attempt=0,
        state="Restarted",
        visible=True,
    )
    events = (
        _event(
            "job_attempt_updated",
            payload={
                "job_id": "123",
                "attempt_index": 0,
                "record_updated_at": "2026-07-24T01:20:00+00:00",
                "old_job": _job_snapshot(job_id="123", attempt=0, state="RUNNING"),
                "new_job": _job_snapshot(job_id="123", attempt=0, state="COMPLETED"),
            },
        ),
        _event(
            "job_attempt_updated",
            payload={
                "job_id": "123",
                "attempt_index": 0,
                "old_job": {
                    **_job_snapshot(job_id="123", attempt=0, state="RUNNING"),
                    "last_updated_at": "2026-07-24T01:19:59+00:00",
                },
                "new_job": {
                    **_job_snapshot(job_id="123", attempt=0, state="COMPLETED"),
                    "last_updated_at": "2026-07-24T01:20:00+00:00",
                },
            },
            produced_at=101,
        ),
        _event(
            "advice_updated",
            level="recovery",
            payload={
                "advice_id": "attempt-id",
                "job_id": "123",
                "attempt_index": 0,
                "record_updated_at": "2026-07-24T01:20:00+00:00",
                "old_advice": complete_old_advice,
                "new_advice": complete_new_advice,
                "change_reasons": [],
                "correlation_uuid": "attempt-id",
                **_recovery_correlation(),
            },
            produced_at=102,
        ),
    )

    evaluation = evaluate_audit_trail(events, ())

    assert not evaluation.satisfied
    assert evaluation.irreversible_failure
    assert "job_attempt_updated payload is not complete" in evaluation.summary
    assert "unexpected=['record_updated_at']" in evaluation.summary
    assert "old_job is not complete: missing=[], unexpected=['last_updated_at']" in evaluation.summary
    assert "advice_updated payload is not complete" in evaluation.summary


def test_evaluator_accepts_valid_unlisted_events() -> None:
    events = (
        _event("decision", payload={"state": "Released"}),
        _event("new_event_added_without_e2e_expectation", produced_at=101),
    )

    evaluation = evaluate_audit_trail(
        events,
        (EventExpectation("released", event_type="decision"),),
    )

    assert evaluation.satisfied, evaluation.summary


def test_job_expectations_use_the_requested_attempt_identity() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    events = (
        _event(
            "job_attempt_observed",
            payload={
                "job_id": "123",
                "attempt_index": 2,
                "job": {
                    **_job_snapshot(job_id="123", attempt=2, state="RUNNING"),
                    "aps_spec": {"workflow_id": "workflow-a"},
                },
            },
            produced_at=100,
        ),
        _event(
            "job_attempt_updated",
            payload={
                "job_id": "123",
                "attempt_index": 2,
                "old_job": _job_snapshot(job_id="123", attempt=2, state="RUNNING"),
                "new_job": _job_snapshot(job_id="123", attempt=2, state="FAILED"),
            },
            produced_at=200,
        ),
    )

    observed = evaluate_audit_trail(
        events[:1],
        (
            job_observed_event(
                job,
                attempt=2,
                job_subset={"aps_spec": {"workflow_id": "workflow-a"}},
            ),
        ),
    )
    lifecycle = evaluate_audit_trail(
        events,
        (
            job_observed_event(
                job,
                attempt=2,
                job_subset={"aps_spec": {"workflow_id": "workflow-a"}},
            ),
            job_state_transition_event(
                job,
                attempt=2,
                old_states=("RUNNING",),
                new_state="FAILED",
            ),
        ),
    )

    assert observed.satisfied, observed.summary
    assert lifecycle.satisfied, lifecycle.summary
    assert not evaluate_audit_trail(
        events,
        (
            job_observed_event(
                job,
                attempt=1,
                job_subset={"aps_spec": {"workflow_id": "workflow-a"}},
            ),
        ),
    ).satisfied


def test_lifecycle_builders_encode_attempt_and_state_transitions() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")

    observed = job_observed_event(job, attempt=1, job_subset={"state": "PENDING"})
    started = job_started_event(job, attempt=1)
    completed = job_state_transition_event(
        job,
        attempt=1,
        old_states=("RUNNING", "COMPLETING"),
        new_state="COMPLETED",
    )

    assert observed.payload_subset["attempt_index"] == 1
    assert observed.payload_subset["job"]["state"] == "PENDING"
    assert started.payload_subset["old_job"] == {"state": "PENDING"}
    assert started.payload_subset["new_job"] == {"state": "RUNNING"}
    assert completed.payload_subset["old_job"] == {
        "state": OneOf(("RUNNING", "COMPLETING")),
    }
    assert completed.payload_subset["new_job"] == {"state": "COMPLETED"}


@pytest.mark.parametrize("recovery_sequence_id", [None, "23489c94-e25c-40e0-b3f2-6b294637ea19"])
def test_advice_reached_matches_creation_state_and_cause(recovery_sequence_id: str | None) -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 0)
    expected = advice_reached_event(
        job,
        state="Restarted",
        cause="CRASH",
        visible=True,
    )

    matching = _event(
        "advice_created",
        level="recovery",
        payload={
            "advice_id": expected_id,
            "job_id": "123",
            "attempt_index": 0,
            "advice": _advice_snapshot(
                advice_id=expected_id,
                job_id="123",
                attempt=0,
                state="Restarted",
                visible=True,
            ),
            "creation_reasons": ["CRASH"],
            "correlation_uuid": recovery_sequence_id or expected_id,
            **_recovery_correlation(recovery_sequence_id),
        },
        heimdall_id=expected_id,
    )
    wrong_reason = _event(
        "advice_created",
        level="recovery",
        payload={**matching.payload, "creation_reasons": ["HANGNOOUTPUT"]},
        heimdall_id=expected_id,
    )

    assert evaluate_audit_trail(
        (matching,),
        (expected,),
    ).satisfied
    assert not evaluate_audit_trail(
        (wrong_reason,),
        (expected,),
    ).satisfied


@pytest.mark.parametrize("recovery_sequence_id", [None, "23489c94-e25c-40e0-b3f2-6b294637ea19"])
@pytest.mark.parametrize("checkpoint_type", [None, "CHECKPOINTLOAD", "CHECKPOINTSAVE"])
def test_advice_reached_matches_update_state_and_change_reason(
    recovery_sequence_id: str | None, checkpoint_type: str | None
) -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 0)
    expected = advice_reached_event(
        job,
        state="Restarted",
        cause="CRASH",
    )
    matching = _event(
        "advice_updated",
        level="recovery",
        payload={
            "advice_id": expected_id,
            "job_id": "123",
            "attempt_index": 0,
            "old_advice": _advice_snapshot(
                advice_id=expected_id,
                job_id="123",
                attempt=0,
                state=None,
                visible=False,
            ),
            "new_advice": _advice_snapshot(
                advice_id=expected_id,
                job_id="123",
                attempt=0,
                state="Restarted",
                visible=True,
            ),
            "change_reasons": ["CRASH"],
            "correlation_uuid": recovery_sequence_id or expected_id,
            **_recovery_correlation(recovery_sequence_id),
        },
        heimdall_id=expected_id,
    )

    if checkpoint_type:
        # Mixed updates retain checkpoint issues, but recovery assertions must
        # need neither a checkpoint reason nor a separate checkpoint-only event.
        matching.payload["new_advice"]["issues"] = [{"issue_type": checkpoint_type}]
        matching.payload["change_reasons"].append(checkpoint_type)

    evaluation = evaluate_audit_trail(
        (matching,),
        (expected,),
    )

    assert evaluation.satisfied, evaluation.summary


@pytest.mark.parametrize("event_type", ["advice_created", "advice_updated"])
@pytest.mark.parametrize("correlation_id", [None, "", 123])
def test_evaluator_rejects_missing_or_invalid_advice_correlation(event_type: str, correlation_id: object) -> None:
    snapshot = _advice_snapshot(advice_id="attempt-id", job_id="123", attempt=0, state="Restarted", visible=True)
    payload = {
        "advice_id": "attempt-id",
        "job_id": "123",
        "attempt_index": 0,
        "correlation_uuid": correlation_id,
        **_recovery_correlation(),
    }
    if event_type == "advice_created":
        payload.update(advice=snapshot, creation_reasons=["CRASH"])
    else:
        payload.update(
            old_advice=_advice_snapshot(advice_id="attempt-id", job_id="123", attempt=0, state=None, visible=False),
            new_advice=snapshot,
            change_reasons=["CRASH"],
        )

    evaluation = evaluate_audit_trail((_event(event_type, level="recovery", payload=payload),), ())

    assert not evaluation.satisfied
    assert evaluation.irreversible_failure


def test_advice_reached_matches_fact_attribution_and_snapshot_subset() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 0)
    expected = advice_reached_event(
        job,
        state="Restarted",
        cause="CRASH",
        advice_subset={"fact_attribution": {"anomaly_type": "CRASH"}},
    )
    old_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id="123",
        attempt=0,
        state=None,
        visible=False,
    )
    new_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id="123",
        attempt=0,
        state="Restarted",
        visible=True,
    )
    new_advice["fact_attribution"] = {"anomaly_type": "CRASH"}
    matching = _event(
        "advice_updated",
        level="recovery",
        payload={
            "advice_id": expected_id,
            "job_id": "123",
            "attempt_index": 0,
            "old_advice": old_advice,
            "new_advice": new_advice,
            "change_reasons": [],
            "correlation_uuid": expected_id,
            **_recovery_correlation(),
        },
        heimdall_id=expected_id,
    )

    evaluation = evaluate_audit_trail(
        (matching,),
        (expected,),
    )

    assert evaluation.satisfied, evaluation.summary


@pytest.mark.parametrize(
    ("state", "cause", "attribution"),
    (
        ("Terminated", "CRASH", "CRASH"),
        ("Restarted", "HANGNOOUTPUT", "HANGNOOUTPUT"),
        ("Restarted", "CRASH", None),
    ),
)
def test_advice_reached_rejects_wrong_state_cause_or_snapshot(
    state: str,
    cause: str,
    attribution: str | None,
) -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 0)
    expected = advice_reached_event(
        job,
        state="Restarted",
        cause="CRASH",
        advice_subset={"fact_attribution": {"anomaly_type": "CRASH"}},
    )
    old_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id="123",
        attempt=0,
        state=None,
        visible=False,
    )
    new_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id="123",
        attempt=0,
        state=state,
        visible=True,
    )
    if attribution is not None:
        new_advice["fact_attribution"] = {"anomaly_type": attribution}
    observed = _event(
        "advice_updated",
        level="recovery",
        payload={
            "advice_id": expected_id,
            "job_id": "123",
            "attempt_index": 0,
            "old_advice": old_advice,
            "new_advice": new_advice,
            "change_reasons": [cause],
            "correlation_uuid": expected_id,
            **_recovery_correlation(),
        },
        heimdall_id=expected_id,
    )

    evaluation = evaluate_audit_trail(
        (observed,),
        (expected,),
    )

    assert not evaluation.satisfied
    assert not evaluation.irreversible_failure


def test_advice_reached_matches_terminal_requeue_limit_decision() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 1)
    expected = advice_reached_event(
        job,
        attempt=1,
        state="Terminated",
        cause="max_requeue_reached",
    )
    old_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id=job.job_id,
        attempt=1,
        state=None,
        visible=False,
    )
    new_advice = _advice_snapshot(
        advice_id=expected_id,
        job_id=job.job_id,
        attempt=1,
        state="Terminated",
        visible=True,
    )
    new_advice["issues"] = [{"issue_type": "max_requeue_reached"}]
    event = _event(
        "advice_updated",
        level="recovery",
        payload={
            "advice_id": expected_id,
            "job_id": job.job_id,
            "attempt_index": 1,
            "old_advice": old_advice,
            "new_advice": new_advice,
            "change_reasons": ["max_requeue_reached"],
            "correlation_uuid": expected_id,
            **_recovery_correlation(),
        },
        heimdall_id=expected_id,
    )

    assert evaluate_audit_trail((event,), (expected,)).satisfied


def test_dag_advice_expectations_require_explicit_causal_reasons() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    expected_id = heimdall_job_id(job.job_id, 0)

    release = advice_reached_event(
        job,
        state="Released",
        cause=DAG_RELEASE_ADVICE_REASON,
        visible=True,
    )
    terminate = advice_reached_event(
        job,
        state="Terminated",
        cause=DAG_TERMINATION_ADVICE_REASON,
        visible=True,
    )

    for expectation, state, reason in (
        (release, "Released", DAG_RELEASE_ADVICE_REASON),
        (terminate, "Terminated", DAG_TERMINATION_ADVICE_REASON),
    ):
        event = _event(
            "advice_created",
            level="recovery",
            payload={
                "advice_id": expected_id,
                "job_id": job.job_id,
                "attempt_index": 0,
                "advice": _advice_snapshot(
                    advice_id=expected_id,
                    job_id=job.job_id,
                    attempt=0,
                    state=state,
                    visible=True,
                ),
                "creation_reasons": [reason],
                "correlation_uuid": expected_id,
                **_recovery_correlation(),
            },
            heimdall_id=expected_id,
        )

        evaluation = evaluate_audit_trail(
            (event,),
            (expectation,),
        )

        assert evaluation.satisfied, evaluation.summary


def test_action_and_convergence_builders_target_the_requested_attempt() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")

    facts = (
        automatic_requeue_hold_event(job, attempt=2),
        scheduler_action_event(job, action="slurm.release", attempt=2),
        advice_application_applied_event(job, attempt=2),
        advice_application_evaluated_event(job, action="slurm.release", attempt=2),
        advice_reconciliation_converged_event(job, attempt=2),
        watchdog_release_completed_event(job, attempt=2),
    )

    assert all(fact.payload_subset["attempt_index"] == 2 for fact in facts)
    assert facts[1].payload_subset["action"] == "slurm.release"
    assert facts[2].event_type == "advice_application_applied"
    assert facts[3].event_type == "advice_application_evaluated"
    assert facts[4].event_type == "advice_reconciliation_converged"
    assert facts[5].event_producer == "slurm-watchdog"


@pytest.mark.parametrize("attempt", [0, 2])
def test_watcher_expectation_matches_current_payload_and_rejects_other_attempts(attempt: int) -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")
    event = ObservedAuditEvent(
        heimdall_job_id=heimdall_job_id(job.job_id, attempt),
        event_producer="watcher",
        event_type="anomaly_detected",
        event_level="operational",
        payload={
            "job_id": job.job_id,
            "attempt_index": attempt,
            "anomaly_type": "CRASH",
            "anomaly_uuid": "23489c94-e25c-40e0-b3f2-6b294637ea19",
            "workflow_step": None,
            "event_occurred_at": 90,
        },
        event_produced_at=100,
    )

    matching = watcher_anomaly_detected_event(job, anomaly_type="CRASH", attempt=attempt)
    wrong_attempt = watcher_anomaly_detected_event(job, anomaly_type="CRASH", attempt=attempt + 1)
    missing_occurrence_time = replace(
        event,
        payload={key: value for key, value in event.payload.items() if key != "event_occurred_at"},
    )

    assert evaluate_audit_trail((event,), (matching,)).satisfied
    assert not evaluate_audit_trail((event,), (wrong_attempt,)).satisfied
    assert not evaluate_audit_trail((missing_occurrence_time,), (matching,)).satisfied


def test_atomic_events_distinguish_watcher_and_automatic_requeue_paths() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")

    watcher_events = (
        watcher_anomaly_detected_event(job, anomaly_type="CRASH"),
        scheduler_action_event(job, action="slurm.requeue_hold"),
        scheduler_action_event(job, action="slurm.release"),
    )
    automatic_events = (
        automatic_requeue_hold_event(job),
        scheduler_action_event(job, action="slurm.release"),
    )
    hybrid_events = automatic_events

    assert "anomaly_detected" in {event.event_type for event in watcher_events}
    assert "job_auto_requeue_hold_completed" not in {event.event_type for event in watcher_events}
    assert "job_auto_requeue_hold_completed" in {event.event_type for event in automatic_events}
    assert "anomaly_detected" not in {event.event_type for event in automatic_events}
    assert "job_auto_requeue_hold_completed" in {event.event_type for event in hybrid_events}
    assert "anomaly_detected" not in {event.event_type for event in hybrid_events}
    assert {
        event.payload_subset.get("action")
        for event in watcher_events
        if event.event_type == "advice_scheduler_action_completed"
    } == {"slurm.requeue_hold", "slurm.release"}
    assert {
        event.payload_subset.get("action")
        for event in automatic_events
        if event.event_type == "advice_scheduler_action_completed"
    } == {"slurm.release"}


def test_job_state_builder_encodes_semantic_transition() -> None:
    job = SubmittedJob("123", "2026-07-24T01:19:53Z")

    failed = job_state_transition_event(
        job,
        old_states=("RUNNING",),
        new_state="FAILED",
    )

    assert failed.payload_subset["old_job"] == {
        "state": OneOf(("RUNNING",)),
    }
    assert failed.payload_subset["new_job"] == {
        "state": "FAILED",
    }
