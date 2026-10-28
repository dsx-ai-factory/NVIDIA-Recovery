# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from notifications.audit.summary.summary import summarize_audit_event
from notifications.models.audit_events import (
    AuditEventBase,
    AuditEventHumanSummaryGranularityClass,
    AuditEventLevel,
)


def _audit_event(event_type: str, payload: dict) -> AuditEventBase:
    return AuditEventBase(
        heimdall_job_id="heimdall-job-id",
        event_producer="job-manager",
        event_type=event_type,
        event_level=AuditEventLevel.OPERATIONAL,
        payload=payload,
        event_produced_at=1,
    )


@pytest.mark.parametrize(
    ("event_type", "payload", "expected_title", "expected_description", "expected_granularity"),
    [
        pytest.param(
            "anomaly_detected",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "anomaly_type": "CRASH",
                "anomaly_uuid": "anomaly-1",
                "workflow_step": "crash_action",
            },
            "Anomaly detected",
            "Anomaly CRASH detected",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="anomaly detected",
        ),
        pytest.param(
            "anomaly_detected",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "anomaly_type": "CHECKPOINTLOAD",
                "anomaly_uuid": "checkpoint-load-1",
            },
            "Job lifecycle observed",
            "Checkpoint loaded",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="checkpoint load observed",
        ),
        pytest.param(
            "anomaly_detected",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "anomaly_type": "CHECKPOINTSAVE",
                "anomaly_uuid": "checkpoint-save-1",
            },
            "Job lifecycle observed",
            "Checkpoint saved",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="checkpoint save observed",
        ),
        pytest.param(
            "attribution_request_forwarded",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "request_payload": {"phase": "fast"},
                "request_uuid": "request-1",
            },
            "Attribution request sent",
            "Attribution request (fast) sent",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="attribution request forwarded",
        ),
        pytest.param(
            "attribution_response_received",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "request_uuid": "request-1",
                "response_payload": {"service_identifier": "attribution-adapter"},
            },
            "Attribution response received",
            "Attribution response received",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="attribution response received",
        ),
        pytest.param(
            "application_termination_requested",
            {
                "application_intention_id": 7,
                "job_id": "12345",
                "attempt_index": 3,
                "desired_state": "CANCELLED",
                "reason": "application cannot continue safely",
                "requested_at": "2026-10-27T21:00:00Z",
            },
            "Application termination requested",
            "Application requested that recovery stop and the job be terminated",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="application termination requested",
        ),
        pytest.param(
            "job_attempt_observed",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "job": {"state": "RUNNING"},
            },
            "Job observed",
            "Job 12345 (2) was first observed in RUNNING",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="job attempt observed",
        ),
        pytest.param(
            "advice_created",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "advice": {
                    "advice": {"state": "Restarted"},
                    "visible": True,
                },
                "creation_reasons": ["CRASH"],
                "correlation_uuid": "correlation-1",
            },
            "Advice published",
            "Advice published with state Restarted (Reason: CRASH)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="visible advice created",
        ),
        pytest.param(
            "advice_created",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "advice": {
                    "advice": {"state": "Restarted"},
                    "visible": False,
                },
                "creation_reasons": ["CRASH"],
                "correlation_uuid": "correlation-1",
            },
            "Advice created",
            "Advice created with state Restarted, but not published",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="hidden advice created",
        ),
        pytest.param(
            "advice_unpublished",
            {"advice_id": "advice-1", "job_id": "12345", "attempt_index": 2, "reason": "superseded"},
            "Advice unpublished",
            "Advice unpublished",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="advice unpublished",
        ),
        pytest.param(
            "advice_scheduler_action_completed",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "action": "slurm.release",
                "success": True,
            },
            "Scheduler action performed",
            "Action slurm.release performed successfully",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="scheduler action succeeded",
        ),
        pytest.param(
            "advice_scheduler_action_completed",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "action": "slurm.cancel",
                "success": False,
                "error": "scheduler unavailable",
            },
            "Action failed with scheduler",
            "Action slurm.cancel failed (Error: scheduler unavailable)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="scheduler action failed",
        ),
        pytest.param(
            "advice_application_applied",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "correlation_uuid": "correlation-1",
            },
            "Advice application completed",
            "Advice application completed",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="advice application applied",
        ),
        pytest.param(
            "advice_application_evaluated",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "outcome": "planned",
                "planned_actions": ["slurm.requeue_hold", "slurm.release"],
            },
            "Advice evaluated",
            "Actions would be applied based on Advice: slurm.requeue_hold, slurm.release",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="advice application evaluated",
        ),
        pytest.param(
            "advice_application_failed",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "error": "application failed",
            },
            "Advice failed",
            "Failed to apply Advice (Error: application failed)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="advice application failed",
        ),
        pytest.param(
            "advice_reconciliation_converged",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "generation": 7,
                "reason": "DESIRED_STATE_OBSERVED",
            },
            "Advice converged",
            "State matches Advice",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="advice reconciliation converged",
        ),
        pytest.param(
            "advice_reconciliation_abandoned",
            {
                "advice_id": "advice-1",
                "job_id": "12345",
                "attempt_index": 2,
                "generation": 7,
                "reason": "ADVICE_EXPIRED",
            },
            "Advice no longer considered",
            "Advice no longer considered (Reason: ADVICE_EXPIRED)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="advice reconciliation abandoned",
        ),
        pytest.param(
            "job_auto_requeue_hold_completed",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "correlation_uuid": "correlation-1",
                "success": True,
            },
            "Job set to REQUEUE_HOLD",
            "Job set to REQUEUE_HOLD due to detected failure",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="auto requeue hold succeeded",
        ),
        pytest.param(
            "job_auto_requeue_hold_completed",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "correlation_uuid": "correlation-1",
                "success": False,
                "error": "scheduler unavailable",
            },
            "Job failed to be set to REQUEUE_HOLD",
            "Failed to set to REQUEUE_HOLD due to detected failure (Error: scheduler unavailable)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="auto requeue hold failed",
        ),
        pytest.param(
            "watchdog_release_completed",
            {"job_id": "12345", "attempt_index": 2, "success": True},
            "Watchdog released",
            "Job released by Watchdog after hold timeout",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="watchdog release succeeded",
        ),
        pytest.param(
            "watchdog_release_completed",
            {
                "job_id": "12345",
                "attempt_index": 2,
                "success": False,
                "error": "scheduler unavailable",
            },
            "Watchdog failed to release",
            "Job failed to be released by Watchdog (Error: scheduler unavailable)",
            AuditEventHumanSummaryGranularityClass.COARSE,
            id="watchdog release failed",
        ),
        pytest.param(
            "future_event",
            {"detail": "value"},
            "future_event",
            "future event",
            AuditEventHumanSummaryGranularityClass.FINE,
            id="unknown event",
        ),
    ],
)
def test_other_audit_event_summaries(
    event_type: str,
    payload: dict,
    expected_title: str,
    expected_description: str,
    expected_granularity: AuditEventHumanSummaryGranularityClass,
):
    summary = summarize_audit_event(_audit_event(event_type, payload))

    assert summary is not None
    assert summary.title == expected_title
    assert summary.description == expected_description
    assert summary.granularity is expected_granularity


def test_job_state_update_lists_raw_fields_and_silently_truncates_other_changes():
    event = _audit_event(
        "job_attempt_updated",
        {
            "job_id": "123",
            "attempt_index": 4,
            "old_job": {
                "state": "PENDING",
                "allocated_node_list": ["node-1"],
                "inclusion_node_list": ["node-1"],
                "aps_spec": {
                    "auto_resume_mode": "requeue",
                    "dag_action": "release",
                },
                "partition": "batch",
                "user": "old-user",
            },
            "new_job": {
                "state": "RUNNING",
                "allocated_node_list": ["node-2"],
                "inclusion_node_list": ["node-2"],
                "aps_spec": {
                    "auto_resume_mode": "singleton_dependency",
                    "dag_action": "cancel",
                },
                "partition": "interactive",
                "user": "new-user",
            },
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == "Job state updated"
    assert summary.description == (
        "Job changed state from PENDING to RUNNING; other changes: allocated_node_list, inclusion_node_list, partition"
    )
    assert summary.granularity is AuditEventHumanSummaryGranularityClass.COARSE
    assert "+" not in summary.description
    assert "aps spec" not in summary.description
    assert "DAG policy" not in summary.description


def test_job_update_omits_aps_and_dag_policy_changes():
    event = _audit_event(
        "job_attempt_updated",
        {
            "job_id": "123",
            "attempt_index": 4,
            "old_job": {"state": "PENDING", "aps_spec": {"dag_action": "release"}},
            "new_job": {"state": "PENDING", "aps_spec": {"dag_action": "cancel"}},
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == "Job updated"
    assert summary.description == "Job updated"
    assert summary.granularity is AuditEventHumanSummaryGranularityClass.FINE


def test_advice_publish_includes_reason_and_lists_other_changed_fields():
    event = _audit_event(
        "advice_updated",
        {
            "change_reasons": ["HANG"],
            "old_advice": {
                "advice": {
                    "state": "Restarted",
                    "node_constraints": {"exclusions": ["node-1"]},
                },
                "visible": False,
                "generation": 1,
                "issues": [{"issue_type": "CRASH"}],
            },
            "new_advice": {
                "advice": {
                    "state": "Terminated",
                    "node_constraints": {"exclusions": ["node-2"]},
                },
                "visible": True,
                "generation": 2,
                "issues": [
                    {"issue_type": "CRASH"},
                    {"issue_type": "HANG"},
                ],
            },
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == "Advice published"
    assert summary.description == (
        "Advice published with state Terminated (Reason: HANG); other changes: advice.node_constraints.exclusions"
    )
    assert summary.granularity is AuditEventHumanSummaryGranularityClass.COARSE
    assert "+" not in summary.description
    assert "visibility" not in summary.description
    assert "generation" not in summary.description
    assert "issues" not in summary.description


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        pytest.param("Restarted", "CRASH", id="non-DAG restart"),
        pytest.param("Terminated", "DAG_PARENT_TERMINATED", id="termination"),
        pytest.param("Released", "DAG_DEPENDENCIES_SATISFIED", id="release"),
    ],
)
def test_published_advice_summaries_include_reason_and_are_coarse(state: str, reason: str):
    created_summary = summarize_audit_event(
        _audit_event(
            "advice_created",
            {
                "advice": {"advice": {"state": state}, "visible": True},
                "creation_reasons": [reason],
            },
        )
    )
    updated_summary = summarize_audit_event(
        _audit_event(
            "advice_updated",
            {
                "old_advice": {"advice": {"state": "Restarted"}, "visible": False},
                "new_advice": {"advice": {"state": state}, "visible": True},
                "change_reasons": [reason],
            },
        )
    )

    for summary in (created_summary, updated_summary):
        assert summary.title == "Advice published"
        assert summary.description == f"Advice published with state {state} (Reason: {reason})"
        assert summary.granularity is AuditEventHumanSummaryGranularityClass.COARSE


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        pytest.param(
            "advice_created",
            {
                "advice": {"advice": {"state": "Terminated"}, "visible": False},
                "creation_reasons": ["DAG_PARENT_TERMINATED"],
            },
            id="created but not published",
        ),
        pytest.param(
            "advice_updated",
            {
                "old_advice": {"advice": {"state": "Restarted"}, "visible": True},
                "new_advice": {"advice": {"state": "Released"}, "visible": True},
                "change_reasons": ["DAG_DEPENDENCIES_SATISFIED"],
            },
            id="updated without publishing",
        ),
    ],
)
def test_unpublished_dag_advice_summaries_omit_reason_and_remain_fine(event_type: str, payload: dict):
    summary = summarize_audit_event(_audit_event(event_type, payload))

    assert "DAG_" not in summary.description
    assert summary.granularity is AuditEventHumanSummaryGranularityClass.FINE


def test_advice_update_lists_raw_node_constraint_path_but_omits_issue_changes():
    event = _audit_event(
        "advice_updated",
        {
            "change_reasons": [],
            "old_advice": {
                "advice": {
                    "state": "Restarted",
                    "node_constraints": {"exclusions": ["node-1"]},
                },
                "issues": [
                    {"issue_type": "CRASH"},
                    {"issue_type": "HANG"},
                ],
            },
            "new_advice": {
                "advice": {
                    "state": "Restarted",
                    "node_constraints": {"exclusions": ["node-2"]},
                },
                "issues": [
                    {"issue_type": "CRASH"},
                    {"issue_type": "TIMEOUT"},
                ],
            },
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == "Advice updated"
    assert summary.description == "Fields advice.node_constraints.exclusions updated"


def test_advice_update_omits_metadata_and_issue_changes():
    event = _audit_event(
        "advice_updated",
        {
            "change_reasons": [],
            "old_advice": {
                "advice": {"state": "Restarted"},
                "generation": 1,
                "desired_state_changed_at": "2026-09-15T12:00:00Z",
                "issues": [{"issue_type": "CRASH"}],
            },
            "new_advice": {
                "advice": {"state": "Restarted"},
                "generation": 2,
                "desired_state_changed_at": "2026-09-15T12:01:00Z",
                "issues": [{"issue_type": "HANG"}],
            },
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == "Advice updated"
    assert summary.description == "Advice updated"


@pytest.mark.parametrize(
    ("old_visible", "new_visible", "expected_title", "expected_description", "expected_granularity"),
    [
        (
            False,
            True,
            "Advice published",
            "Advice published with state Restarted",
            AuditEventHumanSummaryGranularityClass.COARSE,
        ),
        (
            True,
            False,
            "Advice updated",
            "Advice updated",
            AuditEventHumanSummaryGranularityClass.FINE,
        ),
    ],
)
def test_advice_update_reports_publish_but_ignores_unpublish(
    old_visible: bool,
    new_visible: bool,
    expected_title: str,
    expected_description: str,
    expected_granularity: AuditEventHumanSummaryGranularityClass,
):
    event = _audit_event(
        "advice_updated",
        {
            "change_reasons": [],
            "old_advice": {
                "advice": {"state": "Restarted"},
                "visible": old_visible,
            },
            "new_advice": {
                "advice": {"state": "Restarted"},
                "visible": new_visible,
            },
        },
    )

    summary = summarize_audit_event(event)

    assert summary is not None
    assert summary.title == expected_title
    assert summary.description == expected_description
    assert summary.granularity is expected_granularity
