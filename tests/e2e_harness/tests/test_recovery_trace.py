# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reject false recovery evidence from unrelated attempts or reporting time."""

from __future__ import annotations

import base64
import json
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest
from e2e_harness.audit import ObservedAuditEvent, heimdall_job_id
from e2e_harness.checks import recovery_trace
from e2e_harness.config import e2e_config_from_environment
from e2e_harness.recovery_client import (
    ControllerApi,
    GatewayApi,
    JobManagerApi,
    LogsApi,
    NotificationsApi,
    RecoveryClient,
    TempoApi,
)
from e2e_harness.recovery_trace import evaluate_recovery_trace, recovery_context_from_advice
from recovery_libs.testing.launch.jobs import Job, JobState
from recovery_libs.testing.scenario import Run

TRACE_ID = "0123456789abcdef0123456789abcdef"
ROOT = "0123456789abcdef"
RECONCILE = "abcdef0123456789"
RELEASE = "1122334455667788"
SEQUENCE = "bfce9910-5263-4cf4-a514-d559376e45bb"
START = 1_787_011_200_000_000_000
END = START + 2_000_000_000


def _advice(operation: str = "restart") -> dict:
    return {
        "jobId": "123",
        "restartCount": 0,
        "recoveryContext": {
            "sequenceId": SEQUENCE,
            "traceparent": f"00-{TRACE_ID}-{ROOT}-01",
            "operation": operation,
        },
    }


def _span(name: str, span_id: str, parent: str = ROOT, *, operation: str = "restart", **extra) -> dict:
    attributes = {
        "recovery.sequence.id": SEQUENCE,
        "recovery.operation": operation,
        "job.id": "123",
        "job.attempt.source": 0,
        **extra,
    }
    return {
        "name": name,
        "traceId": TRACE_ID,
        "spanId": span_id,
        "parentSpanId": parent,
        "startTimeUnixNano": str(START),
        "endTimeUnixNano": str(END),
        "attributes": [
            {
                "key": key,
                "value": {
                    "boolValue"
                    if isinstance(value, bool)
                    else "intValue"
                    if isinstance(value, int)
                    else "stringValue": value
                },
            }
            for key, value in attributes.items()
        ],
    }


def _trace(operation: str = "restart") -> dict:
    spans = [
        _span("job.recovery.trigger", ROOT, "", operation=operation),
        _span("job.recovery.advice", "2233445566778899", operation=operation),
        _span("job.recovery.reconcile", RECONCILE, operation=operation),
        _span(
            "job.recovery.scheduler_action",
            RELEASE,
            RECONCILE,
            operation=operation,
            **{
                "scheduler.action": "slurm.release",
                "scheduler.action.outcome": "success",
                "recovery.completes_sequence": True,
                "recovery.slo.eligible": operation == "restart",
            },
        ),
    ]
    return {"trace": {"resourceSpans": [{"scopeSpans": [{"spans": spans}]}]}}


def _audit() -> ObservedAuditEvent:
    return ObservedAuditEvent(
        heimdall_job_id("123", 0),
        "slurm-advisor-controller",
        "advice_scheduler_action_completed",
        "recovery",
        {
            "job_id": "123",
            "attempt_index": 0,
            "action": "slurm.release",
            "success": True,
            "recovery_sequence_id": SEQUENCE,
            "trace_id": TRACE_ID,
            "span_id": RELEASE,
        },
        END // 1_000,
    )


def _evaluate(trace: dict, event: ObservedAuditEvent | None = None, operation: str = "restart", **kwargs):
    return evaluate_recovery_trace(
        json.dumps(trace),
        context=recovery_context_from_advice(
            json.dumps(_advice(operation)), job_id="123", attempt=0, operation=operation
        ),
        job_id="123",
        attempt=0,
        operation=operation,
        audit_events=[event or _audit()],
        require_attribution=kwargs.get("require_attribution", False),
    )


@pytest.mark.parametrize("operation", ["restart", "dag_release"])
def test_canonical_trace_and_exact_audit_completion(operation: str) -> None:
    evidence = _evaluate(_trace(operation), operation=operation)
    assert evidence.observed
    assert evidence.trace_id == TRACE_ID


def test_tempo_protobuf_json_base64_ids_are_supported() -> None:
    trace = _trace()
    for span in trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]:
        for field in ("traceId", "spanId", "parentSpanId"):
            span[field] = base64.b64encode(bytes.fromhex(span[field])).decode()
    assert _evaluate(trace).observed


def test_audit_reporting_delay_is_not_command_completion() -> None:
    event = replace(_audit(), event_produced_at=END // 1_000 + 2_000_000)
    assert not _evaluate(_trace(), event).observed


@pytest.mark.parametrize(
    "field,value",
    [
        ("recovery_sequence_id", "other-sequence"),
        ("trace_id", "0" * 32),
        ("span_id", "0" * 16),
        ("attempt_index", 1),
        ("success", False),
    ],
)
def test_unrelated_audit_or_failed_release_cannot_supply_completion(field, value) -> None:
    event = _audit()
    event = replace(event, payload={**event.payload, field: value})
    assert not _evaluate(_trace(), event).observed


@pytest.mark.parametrize(
    "mutation", ["other-attempt", "poll-parent", "planned", "nonfinal", "missing-root", "dag-in-slo"]
)
def test_unrelated_or_unexecuted_spans_do_not_prove_recovery(mutation: str) -> None:
    trace = _trace()
    spans = trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    release = spans[-1]
    if mutation == "other-attempt":
        next(a for a in release["attributes"] if a["key"] == "job.attempt.source")["value"] = {"intValue": 1}
    elif mutation == "poll-parent":
        release["parentSpanId"] = "0000000000000001"
    elif mutation == "planned":
        next(a for a in release["attributes"] if a["key"] == "scheduler.action.outcome")["value"] = {
            "stringValue": "planned"
        }
    elif mutation == "missing-root":
        spans.pop(0)
    elif mutation == "nonfinal":
        next(a for a in release["attributes"] if a["key"] == "recovery.completes_sequence")["value"] = {
            "boolValue": False
        }
    else:
        next(a for a in release["attributes"] if a["key"] == "recovery.slo.eligible")["value"] = {"boolValue": False}
    assert not _evaluate(trace).observed


def test_credentialed_lane_requires_attribution_span() -> None:
    trace = _trace()
    assert not _evaluate(trace, require_attribution=True).observed
    trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"].append(
        _span("job.recovery.attribution", "3344556677889900")
    )
    assert _evaluate(trace, require_attribution=True).observed


def test_convergence_observation_cannot_replace_release_command() -> None:
    trace = _trace()
    trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][-1]["name"] = "job.recovery.state_check"
    assert not _evaluate(trace).observed


@pytest.mark.parametrize("invalid_id", ["", "0" * 16, "invalid"])
def test_invalid_parent_and_reconcile_ids_cannot_match(invalid_id: str) -> None:
    trace = _trace()
    spans = trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    spans[2]["spanId"] = invalid_id
    spans[3]["parentSpanId"] = invalid_id
    assert not _evaluate(trace).observed


def test_malformed_span_name_is_incomplete_evidence() -> None:
    trace = _trace()
    trace["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] = {}
    assert not _evaluate(trace).observed


@pytest.mark.parametrize("response", ["not-json", "{}", "[]", '{"trace": null}'])
def test_malformed_trace_is_not_evidence(response: str) -> None:
    context = recovery_context_from_advice(json.dumps(_advice()), job_id="123", attempt=0, operation="restart")
    assert not evaluate_recovery_trace(
        response,
        context=context,
        job_id="123",
        attempt=0,
        operation="restart",
        require_attribution=False,
        audit_events=[_audit()],
    ).observed


def test_context_read_requires_exact_attempt_and_operation() -> None:
    for attempt, operation in [(1, "restart"), (0, "dag_release")]:
        with pytest.raises(ValueError):
            recovery_context_from_advice(json.dumps(_advice()), job_id="123", attempt=attempt, operation=operation)


SUBMITTED = datetime(2026, 8, 18, tzinfo=UTC)


def _services(*, trace_ingested: bool = True) -> tuple[RecoveryClient, list[str]]:
    """A recovery client whose Job Manager, Tempo, and Notifications answer from the canonical fixtures."""

    event = _audit()
    audit_page = {
        "cursor": {"next_cursor": ""},
        "items": [
            {
                "heimdall_job_id": event.heimdall_job_id,
                "event_producer": event.event_producer,
                "event_type": event.event_type,
                "event_level": event.event_level,
                "payload": event.payload,
                "event_produced_at": event.event_produced_at,
            }
        ],
    }
    replies = {
        "/advice/by-job/123/0": (200, _advice()),
        f"/api/v2/traces/{TRACE_ID}": (200, _trace()) if trace_ingested else (404, {"error": "trace not found"}),
        "/v1/audit/events": (200, audit_page),
    }
    paths: list[str] = []

    def reply(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        status, body = replies[request.url.path]
        return httpx.Response(status, json=body)

    http = httpx.Client(base_url="http://service.test", transport=httpx.MockTransport(reply))
    client = RecoveryClient(
        JobManagerApi(http),
        NotificationsApi(http),
        GatewayApi(http),
        LogsApi(http, backend="loki"),
        ControllerApi(http),
        TempoApi(http),
    )
    return client, paths


def _run(services: RecoveryClient) -> Run[RecoveryClient]:
    job = Job("123", "job", JobState.RUNNING, ("node-1",), None, 1, SUBMITTED)
    return Run({"job": job}, None, services, submitted={"job": SUBMITTED})


def test_check_reads_trace_named_by_advice_and_matches_audit() -> None:
    services, paths = _services()
    assert recovery_trace(require_attribution=False)(_run(services)).observed
    assert paths == ["/advice/by-job/123/0", f"/api/v2/traces/{TRACE_ID}", "/v1/audit/events"]


def test_check_waits_while_tempo_has_not_ingested_the_trace() -> None:
    services, _ = _services(trace_ingested=False)
    evidence = recovery_trace(require_attribution=False)(_run(services))
    assert not evidence.observed
    assert TRACE_ID in evidence.summary


@pytest.mark.parametrize(
    ("environment", "enabled"),
    [
        ({}, False),
        ({"E2E_TEMPO_SERVICE": "monitoring/tempo:3200"}, True),
        ({"E2E_TEMPO_URL": "http://localhost:3200"}, True),
    ],
)
def test_trace_evidence_is_opt_in(environment: dict[str, str], enabled: bool) -> None:
    assert e2e_config_from_environment(environment).recovery_trace_enabled is enabled


@pytest.mark.parametrize("service", ["tempo:3200", "monitoring/tempo", "monitoring/tempo:0", "Monitoring/tempo:3200"])
def test_tempo_service_must_name_a_kubernetes_service_port(service: str) -> None:
    with pytest.raises(ValueError, match="E2E_TEMPO_SERVICE"):
        e2e_config_from_environment({"E2E_TEMPO_SERVICE": service})
