# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cluster-free contracts for production recovery-stack evidence."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest
from e2e_harness.audit import SubmittedJob
from e2e_harness.recovery import (
    EVIDENCE_TEXT_LIMIT,
    evaluate_advice_response,
    evaluate_attribution_response,
    evaluate_dag_job_spec_response,
    evaluate_detection_response,
    evaluate_healthy_status_response,
    evaluate_log_response,
    evaluate_no_hardware_attribution_response,
    evaluate_no_recovery_actions_response,
    evaluate_recovery_action_response,
    evaluate_scheduler_state_response,
    evaluate_scheduler_terminal_advice_response,
    evaluate_system_log_response,
    evaluate_terminated_advice_response,
)
from e2e_harness.workloads import HEALTHY_OBSERVE, JOB_STATE_CRASH_RECOVERY, WATCHER_CRASH_RECOVERY

STARTED_AT = "2026-07-14T12:00:00Z"
EVIDENCE_AT = "2026-07-14T12:00:01Z"
MARKER = "HEIMDALL_E2E_CRASH_123_attempt_0"


def _job() -> SubmittedJob:
    return SubmittedJob("123", STARTED_AT)


def _attribution_event(exclusion_type: str) -> dict[str, object]:
    return {
        "node": "e2e-0",
        "job_id": "123",
        "job_attempt_index": 0,
        "exclusion_type": exclusion_type,
        "error_attribution_origin": "FACT",
        "fact_status": "OK",
    }


def _watcher_event(
    *, marker: str = MARKER, job_id: str = "123", event_type: str = "CRASH", job_manager_generated: bool = False
) -> str:
    attributes: dict[str, object] = {"job_id": job_id, "uuid": "event-1"}
    if job_manager_generated:
        attributes.update(job_manager_generated=True, slurm_job_attempt=0)
    return json.dumps(
        {
            "body": f"fatal fixture: {marker}",
            "type": event_type,
            "timestamp": "1784030401000000000",
            "attributes": attributes,
        }
    )


def _status_response(**anomaly_overrides: object) -> str:
    anomaly = {
        "anomaly_type": "CRASH",
        "timestamp": EVIDENCE_AT,
        "original_watcher_anomaly_event": _watcher_event(),
    }
    anomaly.update(anomaly_overrides)
    return json.dumps({"job_id": "123", "job_attempt_index": 0, "anomalies": [anomaly]})


def _advice_response(**overrides: object) -> str:
    issue = {
        "issueType": "CRASH",
        "timestamp": EVIDENCE_AT,
        "detectedBy": "Heimdall Watcher",
        "relevantLogLines": [f"fatal fixture: {MARKER}"],
    }
    payload = {
        "jobId": "123",
        "restartCount": 0,
        "advice": {"state": "Restarted"},
        "issues": [issue],
        "desiredStateChangedAt": EVIDENCE_AT,
        "updatedAt": EVIDENCE_AT,
    }
    payload.update(overrides)
    return json.dumps(payload)


def _recovery_action_response(**template_overrides: object) -> str:
    template = {
        "template_id": "/templates/JobRequeued",
        "job_id": "123",
        "job_attempt_index": 0,
        "event_type": "CRASH",
    }
    template.update(template_overrides)
    return json.dumps(
        {
            "items": [
                {
                    "job_id": "123",
                    "created_time": EVIDENCE_AT,
                    "template_data": template,
                }
            ],
            "total_items": 1,
        }
    )


def test_durable_detection_requires_exact_source_marker() -> None:
    evidence = evaluate_detection_response(
        _status_response(),
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed
    assert not evidence.state_based_recovery_observed


@pytest.mark.parametrize(
    "response",
    (
        json.dumps({"job_id": "123", "job_attempt_index": 1, "anomalies": []}),
        _status_response(original_watcher_anomaly_event=_watcher_event(marker="different marker")),
        _status_response(timestamp="2026-07-14T11:59:59Z"),
    ),
    ids=("wrong-attempt", "missing-marker", "stale-anomaly"),
)
def test_detection_oracle_goes_red_for_mutated_evidence(response: str) -> None:
    evidence = evaluate_detection_response(
        response,
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


@pytest.mark.parametrize(
    "original_event",
    [None, _watcher_event(job_manager_generated=True)],
    ids=("legacy-null-event", "job-manager-generated-event"),
)
def test_job_state_based_crash_cannot_substitute_for_watcher_detection(original_event: str | None) -> None:
    evidence = evaluate_detection_response(
        _status_response(original_watcher_anomaly_event=original_event),
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed
    assert evidence.state_based_recovery_observed


@pytest.mark.parametrize("body", ("State-based CRASH event for failed job", MARKER))
def test_serialized_job_state_crash_is_not_watcher_evidence(body: str) -> None:
    event = json.loads(_watcher_event())
    event["body"] = body
    event["attributes"].update(job_manager_generated=True, slurm_job_attempt=0)
    evidence = evaluate_detection_response(
        _status_response(original_watcher_anomaly_event=json.dumps(event)),
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert evidence.state_based_recovery_observed
    assert evidence.observed_at == EVIDENCE_AT
    assert not evidence.observed


@pytest.mark.parametrize(
    ("job_id", "event_type", "event_attempt", "timestamp"),
    (
        ("other-job", "CRASH", 0, EVIDENCE_AT),
        ("123", "TIMEOUT", 0, EVIDENCE_AT),
        ("123", "CRASH", 1, EVIDENCE_AT),
        ("123", "CRASH", None, EVIDENCE_AT),
        ("123", "CRASH", False, EVIDENCE_AT),
        ("123", "CRASH", 0, "2026-07-14T11:59:59Z"),
    ),
    ids=("wrong-job", "wrong-type", "wrong-attempt", "missing-attempt", "invalid-attempt", "stale"),
)
def test_serialized_job_state_crash_requires_current_matching_identity(
    job_id: str, event_type: str, event_attempt: object, timestamp: str
) -> None:
    event = json.loads(_watcher_event(job_id=job_id, event_type=event_type))
    event["attributes"].update(job_manager_generated=True, slurm_job_attempt=event_attempt)
    evidence = evaluate_detection_response(
        _status_response(original_watcher_anomaly_event=json.dumps(event), timestamp=timestamp),
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert not evidence.state_based_recovery_observed
    assert not evidence.observed


def test_advice_requires_exact_attempt_issue_source_and_marker() -> None:
    evidence = evaluate_advice_response(
        _advice_response(),
        job_id="123",
        attempt=0,
        issue_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


def test_advice_time_ignores_evidence_appended_after_the_decision() -> None:
    evidence = evaluate_advice_response(
        _advice_response(updatedAt="2026-07-14T12:05:00Z"),
        job_id="123",
        attempt=0,
        issue_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed
    assert evidence.observed_at == EVIDENCE_AT


def test_aps_requeue_limit_accepts_terminal_job_manager_advice() -> None:
    marker = "State-based CRASH event for failed job"
    evidence = evaluate_advice_response(
        _advice_response(
            advice={"state": "Terminated"},
            issues=[
                {
                    "issueType": "CRASH",
                    "timestamp": EVIDENCE_AT,
                    "detectedBy": "Heimdall Job Manager",
                    "relevantLogLines": [marker],
                }
            ],
        ),
        job_id="123",
        attempt=0,
        issue_type="CRASH",
        marker=marker,
        submitted_at=STARTED_AT,
        expected_state="Terminated",
        expected_source="Heimdall Job Manager",
    )

    assert evidence.observed


def test_scheduler_terminal_advice_requires_exact_slurm_owned_attempt() -> None:
    evidence = evaluate_scheduler_terminal_advice_response(
        _advice_response(
            advice={"state": "Terminated"},
            issues=[],
            schedulerMetadata={"scheduler": "slurm"},
        ),
        job_id="123",
        attempt=0,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


def test_terminated_advice_does_not_require_a_synthetic_issue() -> None:
    evidence = evaluate_terminated_advice_response(
        _advice_response(
            advice={"state": "Terminated"},
            issues=[],
        ),
        job_id="123",
        attempt=0,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


def test_dag_job_spec_requires_exact_attempt_dependencies_and_action() -> None:
    response = json.dumps(
        {
            "statuses": [
                {
                    "job_id": "123",
                    "job_attempt_index": 0,
                    "last_updated_at": EVIDENCE_AT,
                    "aps_spec": {
                        "dag_dependencies": [
                            {"target": {"type": "id", "id": "111"}, "trigger": "completed"},
                            {"target": {"type": "name", "name": "prepare"}, "trigger": "completed"},
                        ],
                        "dag_action": "release",
                    },
                }
            ]
        }
    )

    evidence = evaluate_dag_job_spec_response(
        response,
        job_id="123",
        attempt=0,
        expected_dependencies={("id", "111"), ("name", "prepare")},
        expected_action="release",
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


@pytest.mark.parametrize(
    "mutation",
    (
        {"job_attempt_index": 1},
        {"last_updated_at": "2026-07-14T11:59:59Z"},
        {"aps_spec": {"dag_dependencies": [], "dag_action": "release"}},
        {
            "aps_spec": {
                "dag_dependencies": [
                    {"target": {"type": "id", "id": "111"}, "trigger": "failed"},
                ],
                "dag_action": "release",
            }
        },
    ),
    ids=("wrong-attempt", "stale", "missing-dependency", "wrong-trigger"),
)
def test_dag_job_spec_oracle_rejects_mutated_evidence(mutation: dict[str, object]) -> None:
    status: dict[str, object] = {
        "job_id": "123",
        "job_attempt_index": 0,
        "last_updated_at": EVIDENCE_AT,
        "aps_spec": {
            "dag_dependencies": [
                {"target": {"type": "id", "id": "111"}, "trigger": "completed"},
            ],
            "dag_action": "release",
        },
    }
    status.update(mutation)

    evidence = evaluate_dag_job_spec_response(
        json.dumps({"statuses": [status]}),
        job_id="123",
        attempt=0,
        expected_dependencies={("id", "111")},
        expected_action="release",
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


@pytest.mark.parametrize(
    "response",
    (
        _advice_response(restartCount=1, advice={"state": "Terminated"}, schedulerMetadata={"scheduler": "slurm"}),
        _advice_response(restartCount=True, advice={"state": "Terminated"}, schedulerMetadata={"scheduler": "slurm"}),
        _advice_response(advice={"state": "Restarted"}, schedulerMetadata={"scheduler": "slurm"}),
        _advice_response(advice={"state": "Terminated"}, schedulerMetadata={"scheduler": "kubernetes"}),
        _advice_response(
            advice={"state": "Terminated"},
            schedulerMetadata={"scheduler": "slurm"},
            updatedAt="2026-07-14T11:59:59Z",
        ),
    ),
    ids=("wrong-attempt", "boolean-attempt", "wrong-state", "wrong-owner", "stale"),
)
def test_scheduler_terminal_advice_oracle_goes_red_for_mutated_evidence(response: str) -> None:
    evidence = evaluate_scheduler_terminal_advice_response(
        response,
        job_id="123",
        attempt=0,
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


def test_scheduler_state_requires_exact_raw_slurm_classification() -> None:
    response = json.dumps(
        {
            "job_id": "123",
            "job_attempt_index": 0,
            "state": "FAILED",
            "last_updated_at": EVIDENCE_AT,
            "schedulerMetadata": {"scheduler": "slurm", "slurm_state": "TIMEOUT"},
            "anomalies": [],
        }
    )

    evidence = evaluate_scheduler_state_response(
        response,
        job_id="123",
        attempt=0,
        expected_status_state="FAILED",
        expected_slurm_state="TIMEOUT",
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


@pytest.mark.parametrize(
    "mutation",
    (
        {"job_attempt_index": 1},
        {"job_attempt_index": True},
        {"state": "CANCELLED"},
        {"schedulerMetadata": {"scheduler": "slurm", "slurm_state": "FAILED"}},
        {"schedulerMetadata": {"scheduler": "kubernetes", "slurm_state": "TIMEOUT"}},
        {"anomalies": [{"anomaly_type": "CRASH"}]},
        {"last_updated_at": "2026-07-14T11:59:59Z"},
    ),
    ids=("wrong-attempt", "boolean-attempt", "wrong-status", "wrong-raw-state", "wrong-owner", "anomaly", "stale"),
)
def test_scheduler_state_oracle_goes_red_for_mutated_evidence(mutation: dict[str, object]) -> None:
    payload = {
        "job_id": "123",
        "job_attempt_index": 0,
        "state": "FAILED",
        "last_updated_at": EVIDENCE_AT,
        "schedulerMetadata": {"scheduler": "slurm", "slurm_state": "TIMEOUT"},
        "anomalies": [],
    }
    payload.update(mutation)

    evidence = evaluate_scheduler_state_response(
        json.dumps(payload),
        job_id="123",
        attempt=0,
        expected_status_state="FAILED",
        expected_slurm_state="TIMEOUT",
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


@pytest.mark.parametrize(
    "response",
    (
        _advice_response(restartCount=1),
        _advice_response(issues=[]),
        _advice_response(advice={"state": "Terminated"}),
    ),
    ids=("wrong-attempt", "missing-issue", "wrong-action"),
)
def test_advice_oracle_goes_red_for_mutated_evidence(response: str) -> None:
    evidence = evaluate_advice_response(
        response,
        job_id="123",
        attempt=0,
        issue_type="CRASH",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


def test_recovery_action_requires_current_exact_attempt_audit() -> None:
    evidence = evaluate_recovery_action_response(
        _recovery_action_response(),
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        submitted_at=STARTED_AT,
    )

    assert evidence.observed


@pytest.mark.parametrize(
    "response",
    (
        _recovery_action_response(template_id="/templates/JobStopped"),
        _recovery_action_response(job_attempt_index=1),
        json.dumps(
            {
                "items": [
                    {
                        "job_id": "123",
                        "created_time": "2026-07-14T11:59:59Z",
                        "template_data": {
                            "template_id": "/templates/JobRequeued",
                            "job_id": "123",
                            "job_attempt_index": 0,
                            "event_type": "CRASH",
                        },
                    }
                ]
            }
        ),
    ),
    ids=("wrong-action", "wrong-attempt", "stale-audit"),
)
def test_recovery_action_oracle_goes_red_for_mutated_evidence(response: str) -> None:
    evidence = evaluate_recovery_action_response(
        response,
        job_id="123",
        attempt=0,
        anomaly_type="CRASH",
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


def test_healthy_negative_control_accepts_only_clean_completion() -> None:
    clean = json.dumps(
        {
            "job_id": "123",
            "job_attempt_index": 0,
            "state": "COMPLETED",
            "anomalies": [],
        }
    )

    assert evaluate_healthy_status_response(clean, job_id="123", attempt=0).observed
    assert evaluate_no_recovery_actions_response('{"items":[]}', job_id="123").observed


@pytest.mark.parametrize(
    "mutation",
    (
        {"job_id": "123", "job_attempt_index": 1, "state": "COMPLETED", "anomalies": []},
        {"job_id": "123", "job_attempt_index": 0, "state": "FAILED", "anomalies": []},
        {"job_id": "123", "job_attempt_index": 0, "state": "COMPLETED", "anomalies": [{}]},
    ),
    ids=("extra-attempt", "wrong-state", "failure-anomaly"),
)
def test_healthy_negative_control_goes_red_for_mutations(mutation: dict[str, object]) -> None:
    evidence = evaluate_healthy_status_response(json.dumps(mutation), job_id="123", attempt=0)

    assert not evidence.observed


def test_scenario_recovery_policies_are_explicit_and_distinct() -> None:
    watcher = json.loads(WATCHER_CRASH_RECOVERY.comment())["APS"]
    state = json.loads(JOB_STATE_CRASH_RECOVERY.comment())["APS"]
    healthy = json.loads(HEALTHY_OBSERVE.comment())["APS"]

    assert watcher["anomaly_detection_config"]["log_based_crash_enabled"] is True
    assert state["anomaly_detection_config"]["log_based_crash_enabled"] is False
    assert healthy["mode"] == "observe"


def test_scenario_retry_cap_can_be_omitted_without_changing_other_policy() -> None:
    default = json.loads(JOB_STATE_CRASH_RECOVERY.comment())["APS"]
    omitted = json.loads(JOB_STATE_CRASH_RECOVERY.comment({"max_requeue_times": None}))["APS"]
    explicit = json.loads(JOB_STATE_CRASH_RECOVERY.comment({"max_requeue_times": 5}))["APS"]

    assert default.pop("max_requeue_times") == 1
    assert explicit.pop("max_requeue_times") == 5
    assert omitted == default == explicit


def test_collection_oracle_requires_exact_current_job_marker() -> None:
    response = json.dumps(
        {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "cluster": "slurm",
                            "appName": "slurm-app-log",
                            "job_id": "123",
                            "filename": "/home/log-links/slurm-123.out",
                        },
                        "values": [["1784030401000000000", f"fixture {MARKER}"]],
                    }
                ],
            },
        }
    )

    assert evaluate_log_response(response, job_id="123", marker=MARKER, submitted_at=STARTED_AT).observed
    assert not evaluate_log_response(response, job_id="124", marker=MARKER, submitted_at=STARTED_AT).observed
    assert not evaluate_log_response(response, job_id="123", marker="missing", submitted_at=STARTED_AT).observed


def _system_loki_response(**stream_overrides: str) -> str:
    stream = {
        "cluster": "slurm",
        "component": "journal",
        "node_name": "e2e-a-0",
        "filename": "/var/log/syslog",
    }
    stream.update(stream_overrides)
    return json.dumps(
        {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": stream,
                        "values": [["1784030401000000000", f"kernel: {MARKER}"]],
                    }
                ],
            },
        }
    )


def test_system_collection_requires_canonical_node_and_no_workload_labels() -> None:
    evidence = evaluate_system_log_response(
        _system_loki_response(),
        node_name="e2e-a-0",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert evidence.observed
    assert "without workload labels" in evidence.summary


@pytest.mark.parametrize(
    "stream_overrides",
    (
        {"cluster": "other"},
        {"component": "application"},
        {"node_name": "e2e-b-0"},
        {"job_id": "123"},
        {"slurm_job_id": "123"},
        {"slurm_job_attempt": "0"},
        {"appName": "slurm-app-log"},
    ),
    ids=("cluster", "component", "node", "job-id", "legacy-job-id", "attempt", "application-domain"),
)
def test_system_collection_oracle_goes_red_for_contract_mutations(
    stream_overrides: dict[str, str],
) -> None:
    evidence = evaluate_system_log_response(
        _system_loki_response(**stream_overrides),
        node_name="e2e-a-0",
        marker=MARKER,
        submitted_at=STARTED_AT,
    )

    assert not evidence.observed


def test_valid_temporary_attribution_is_observed() -> None:
    response = json.dumps({"events": [_attribution_event("TEMPORARY")]})

    evidence = evaluate_attribution_response(
        response,
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
    )

    assert evidence.observed
    assert "attribution_matches=1" in evidence.summary


def test_conflicting_confirmed_attribution_is_rejected() -> None:
    response = json.dumps({"events": [_attribution_event("TEMPORARY"), _attribution_event("CONFIRMED")]})

    evidence = evaluate_attribution_response(
        response,
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
    )

    assert not evidence.observed
    assert "conflicts=1" in evidence.summary
    assert "conflicting_types=CONFIRMED" in evidence.summary


def test_no_syslog_negative_control_accepts_none_or_no_attribution_event() -> None:
    none = json.dumps({"events": [_attribution_event("NONE")]})
    absent = json.dumps({"events": []})

    assert evaluate_no_hardware_attribution_response(none, job_id="123", attempt=0, node="e2e-0").observed
    assert evaluate_no_hardware_attribution_response(absent, job_id="123", attempt=0, node="e2e-0").observed


@pytest.mark.parametrize("exclusion_type", ("TEMPORARY", "CONFIRMED"))
def test_no_syslog_negative_control_rejects_hardware_attribution(exclusion_type: str) -> None:
    evidence = evaluate_no_hardware_attribution_response(
        json.dumps({"events": [_attribution_event(exclusion_type)]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
    )

    assert not evidence.observed


@pytest.mark.parametrize(
    ("field", "malformed_value", "expected_type"),
    (
        ("job_attempt_index", False, "TEMPORARY"),
        ("exclusion_type", None, "NONE"),
    ),
)
def test_malformed_attribution_values_do_not_coerce_into_matches(
    field: str,
    malformed_value: object,
    expected_type: str,
) -> None:
    event = _attribution_event("TEMPORARY")
    event[field] = malformed_value

    evidence = evaluate_attribution_response(
        json.dumps({"events": [event]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type=expected_type,
    )

    assert not evidence.observed


def test_required_signature_conflict_is_rejected() -> None:
    event = _attribution_event("TEMPORARY")
    event["job_execution_error"] = {"signature": "XID-149"}

    evidence = evaluate_attribution_response(
        json.dumps({"events": [event]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
        signature="XID-79",
    )

    assert not evidence.observed
    assert "conflicting_signatures=XID-149" in evidence.summary


def test_numeric_signature_does_not_match_string_requirement() -> None:
    event = _attribution_event("TEMPORARY")
    event["job_execution_error"] = {"signature": 79}

    evidence = evaluate_attribution_response(
        json.dumps({"events": [event]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
        signature="79",
    )

    assert not evidence.observed
    assert "conflicting_signatures=<invalid>" in evidence.summary


def test_attribution_summary_is_bounded() -> None:
    event = _attribution_event("TEMPORARY")
    event["job_execution_error"] = {"signature": "X" * 4_096}

    evidence = evaluate_attribution_response(
        json.dumps({"events": [event]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
        signature="different",
    )

    assert not evidence.observed
    assert len(evidence.summary) <= EVIDENCE_TEXT_LIMIT


def test_malformed_attribution_json_returns_frozen_per_call_evidence() -> None:
    valid = evaluate_attribution_response(
        json.dumps({"events": [_attribution_event("TEMPORARY")]}),
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
    )
    malformed = evaluate_attribution_response(
        "not-json",
        job_id="123",
        attempt=0,
        node="e2e-0",
        exclusion_type="TEMPORARY",
    )

    assert valid.observed
    assert not malformed.observed
    assert malformed.summary == "response is not valid JSON"
    with pytest.raises(FrozenInstanceError):
        malformed.summary = "changed"  # type: ignore[misc]
