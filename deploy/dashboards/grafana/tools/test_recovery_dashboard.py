# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Contract checks for the public recovery trace dashboard."""

import json
from pathlib import Path

DASHBOARD_PATH = Path(__file__).resolve().parents[1] / "heimdall-services-tracing.json"


def _dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text())


def test_recovery_dashboard_uses_operator_selected_tempo_datasource() -> None:
    dashboard = _dashboard()

    assert dashboard["id"] is None
    assert dashboard["uid"] == "nvidia-recovery-traces"
    variables = {variable["name"]: variable for variable in dashboard["templating"]["list"]}
    assert variables["tempo_datasource"]["type"] == "datasource"
    assert variables["tempo_datasource"]["query"] == "tempo"
    assert variables["prometheus_datasource"]["type"] == "datasource"
    assert variables["prometheus_datasource"]["query"] == "prometheus"
    assert "cluster" not in variables
    assert "environment" not in variables
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] in ("${tempo_datasource}", "${prometheus_datasource}")


def test_recovery_outcomes_remain_separate_and_slo_bounded() -> None:
    queries = "\n".join(
        target["query"] for panel in _dashboard()["panels"] for target in panel.get("targets", []) if "query" in target
    )

    assert 'span.recovery.outcome = "applied"' in queries
    assert 'span.recovery.outcome = "skipped"' in queries
    assert 'span.recovery.outcome = "failed"' in queries
    assert 'span.recovery.outcome = "planned"' in queries
    assert "span.recovery.terminal = true" in queries
    assert "span.recovery.terminal = false" in queries
    assert 'span.recovery.operation = "dag_release"' in queries
    assert 'span.recovery.operation = "restart"' in queries
    assert "span.recovery.slo.eligible = false" in queries
    assert "span:status = error" in queries


def test_recovery_dashboard_uses_public_span_and_resource_names() -> None:
    dashboard = _dashboard()
    variables = {variable["name"]: variable for variable in dashboard["templating"]["list"]}

    assert variables["service"]["query"].split(",") == [
        "nvidia-recovery-job-manager",
        "nvidia-recovery-attribution-adapter",
        "nvidia-recovery-slurm-advisor-controller",
    ]
    assert variables["operation"]["query"].split(",") == [
        "job.recovery.trigger",
        "job.recovery.attribution",
        "job.recovery.advice",
        "job.recovery.reconcile",
        "job.recovery.scheduler_action",
        "job.recovery.state_check",
    ]


def test_recovery_dashboard_queries_bounded_public_metrics() -> None:
    expressions = "\n".join(
        target["expr"] for panel in _dashboard()["panels"] for target in panel.get("targets", []) if "expr" in target
    )

    for metric in (
        "nvidia_recovery_sequences_total",
        "nvidia_recovery_reconcile_outcomes_total",
        "nvidia_recovery_scheduler_actions_total",
        "nvidia_recovery_end_to_end_duration_seconds_bucket",
        "nvidia_recovery_trace_context_total",
        "nvidia_recovery_milestone_reports_total",
        "nvidia_recovery_milestones_recorded_total",
    ):
        assert metric in expressions
    for high_cardinality_label in ("job_id", "advice_id", "anomaly_id", "sequence_id"):
        assert high_cardinality_label not in expressions
    scheduler_action_expression = next(
        target["expr"]
        for panel in _dashboard()["panels"]
        for target in panel.get("targets", [])
        if "nvidia_recovery_scheduler_actions_total" in target.get("expr", "")
    )
    assert "record_result" not in scheduler_action_expression
