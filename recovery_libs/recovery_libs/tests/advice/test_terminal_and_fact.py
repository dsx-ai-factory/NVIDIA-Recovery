# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import UTC, datetime

from recovery_libs.advice.fact import build_fact_issues
from recovery_libs.advice.terminal import build_terminal_advice, map_advice_state
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult, JobExecutionError, NodeEvent
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import AdviceState, Issue, JobTerminalAdvice
from recovery_libs.schema.queue_bad_nodes_isolation.anomaly_isolation import AnomalyIsolation
from recovery_libs.storage.node_events import ErrorAttribution, ErrorAttributionType, ExclusionType
from recovery_libs.tests.advice.helpers import create_anomaly_attribution, random_job_execution_status


def test_map_advice_state_known_values():
    assert map_advice_state(JobTerminalAdvice.JOB_RELEASED) == AdviceState.RESTARTED
    assert map_advice_state(JobTerminalAdvice.USER_CANCELED) == AdviceState.TERMINATED


def test_map_advice_state_unknown_defaults_to_terminated():
    assert map_advice_state("unknown") == AdviceState.TERMINATED


def test_build_terminal_advice_without_status_or_attribution():
    advice = build_terminal_advice(
        job_id="12345",
        job_attempt_index=2,
        terminal_advice=JobTerminalAdvice.USER_CANCELED,
    )

    assert advice.visible is True
    assert advice.job_id == "12345"
    assert advice.restart_count == 2
    assert advice.advice.state == AdviceState.TERMINATED
    assert advice.advice.node_constraints is None


def test_build_terminal_advice_with_attribution_exclusions():
    status = random_job_execution_status(job_id="12345", job_attempt_index=1)
    attribution = create_anomaly_attribution(job_id="12345", node_event_count=2)

    advice = build_terminal_advice(
        job_id="12345",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.JOB_RELEASED,
        status=status,
        anomaly_attribution=attribution,
        exclusion_stickiness_enabled=False,
    )

    assert advice.restart_count == 1
    assert advice.advice.state == AdviceState.RESTARTED
    assert advice.advice.node_constraints is not None
    assert set(advice.advice.node_constraints.exclusions) == {"node_0", "node_1"}


def test_build_terminal_advice_merges_existing_issues():
    existing = build_terminal_advice(
        job_id="12345",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.USER_CANCELED,
    )
    existing.issues = [
        Issue(
            issue_type="CRASH",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            detected_by="Heimdall Watcher",
        )
    ]
    attribution = create_anomaly_attribution(job_id="12345", node_event_count=1)

    advice = build_terminal_advice(
        job_id="12345",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.JOB_RELEASED,
        status=random_job_execution_status(job_id="12345", job_attempt_index=1),
        anomaly_attribution=attribution,
        existing_advice=existing,
        exclusion_stickiness_enabled=False,
    )

    assert advice.issues is not None
    assert len(advice.issues) == 2


def test_build_fact_issues_returns_empty_for_none():
    assert build_fact_issues(None) == []


def test_build_fact_issues_returns_empty_for_anomaly_isolation():
    isolation = AnomalyIsolation(
        job_id="123",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        anomaly_uuid=str(uuid.uuid4()),
        bad_nodes={"node_a": {}},
    )
    assert build_fact_issues(isolation) == []


def test_build_fact_issues_builds_remediation_actions():
    ts = datetime(2026, 3, 5, 0, 47, 38, tzinfo=UTC)
    attribution = AnomalyAttributionResult(
        job_id="test-job",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        anomaly_uuid=str(uuid.uuid4()),
        attribution_type=ErrorAttributionType.FAST,
        node_attributions={
            JobExecutionError.create(
                code=79,
                message="NVRM: Xid 79, GPU has fallen off the bus.",
                signature="XID-79-GPU_HAS_FALLEN_OFF_THE_BUS",
                description="Known HW Cause - Node",
            ): [
                NodeEvent.create(
                    node="dgx-node-01",
                    timestamp=ts,
                    job_id="test-job",
                    job_attempt_index=0,
                    error_attribution=ErrorAttribution.HARDWARE,
                    exclusion_type=ExclusionType.TEMPORARY,
                    error_attribution_type=ErrorAttributionType.FAST,
                ),
            ]
        },
        global_attributions={},
    )

    issues = build_fact_issues(attribution)

    assert len(issues) == 1
    issue = issues[0]
    assert isinstance(issue, Issue)
    assert issue.issue_type == "CRASH"
    assert issue.detected_by == "Heimdall FACT"
    assert issue.remediation_actions is not None
    assert issue.remediation_actions[0].action_type == "Exclude Node"
