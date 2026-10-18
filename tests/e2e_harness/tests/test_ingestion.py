# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Negative controls for production file-ingestion qualification."""

from collections.abc import Callable
from dataclasses import replace

import pytest
from e2e_harness.conformance import log_backend_selection
from e2e_harness.ingestion import evaluate_ingestion_records
from e2e_harness.log_backend import LogRecord

START = "2026-01-01T00:00:00Z"
MESSAGES = ("opaque marker plain text", '{"message": "opaque marker", "cluster": "payload-only"}')


def _records(system: bool = False) -> list[LogRecord]:
    labels = {"cluster": "slurm"}
    labels.update(
        {"component": "journal", "node_name": "e2e-a-0"}
        if system
        else {"appName": "slurm-app-log", "job_id": "123", "filename": "/home/log-links/job_123.out"}
    )
    return [LogRecord(labels=labels.copy(), message=body, observed_at=START) for body in MESSAGES]


@pytest.mark.parametrize("system", (False, True))
def test_ingestion_accepts_only_exact_bodies_with_source_identity(system: bool) -> None:
    assert evaluate_ingestion_records(
        _records(system),
        messages=MESSAGES,
        submitted_at=START,
        job_id=None if system else "123",
        node_name="e2e-a-0" if system else None,
    ).observed


@pytest.mark.parametrize("system", (False, True))
def test_ingestion_rejects_a_second_collectors_replayed_records(system: bool) -> None:
    """Two reads of each source line must fail even when their labels agree."""

    records = _records(system)
    records += [replace(record, observed_at="2026-01-01T00:00:01Z") for record in records]
    assert not evaluate_ingestion_records(
        records,
        messages=MESSAGES,
        submitted_at=START,
        job_id=None if system else "123",
        node_name="e2e-a-0" if system else None,
    ).observed


@pytest.mark.parametrize("observed_copies", (1, 2, 3))
def test_ingestion_preserves_legitimate_identical_lines(observed_copies: int) -> None:
    """Identical source lines have multiplicity; neither dedup nor replay is valid."""

    record = _records()[0]
    evidence = evaluate_ingestion_records(
        [record] * observed_copies,
        messages=(record.message, record.message),
        submitted_at=START,
        job_id="123",
    )
    assert evidence.observed is (observed_copies == 2)


@pytest.mark.parametrize("defect", ("missing", "changed-body", "stale", "future", "label-collision", "wrong-job"))
def test_ingestion_rejects_corrupted_application_observations(defect: str) -> None:
    records = _records()
    if defect == "missing":
        records.pop()
    elif defect == "changed-body":
        records[1] = replace(records[1], message="opaque marker")
    elif defect == "stale":
        records[1] = replace(records[1], observed_at="2025-01-01T00:00:00Z")
    elif defect == "future":
        records[1] = replace(records[1], observed_at="2100-01-01T00:00:00Z")
    elif defect == "label-collision":
        records[1].labels["cluster"] = "payload-only"
    elif defect == "wrong-job":
        records[1].labels["job_id"] = "124"
    assert not evaluate_ingestion_records(records, messages=MESSAGES, submitted_at=START, job_id="123").observed


@pytest.mark.parametrize("field", ("job_id", "appName", "workflow_id", "slurm_job_attempt"))
def test_ingestion_rejects_system_fields_extracted_from_message_body(field: str) -> None:
    records = _records(system=True)
    records[1].labels[field] = "payload-only"
    assert not evaluate_ingestion_records(records, messages=MESSAGES, submitted_at=START, node_name="e2e-a-0").observed


def _deployment(backend: str, defect: str = "") -> Callable[..., dict]:
    """A `kubectl get` stand-in serving the deployed readers, collectors, and stores, with one `defect`."""

    other = "victorialogs" if backend == "loki" else "loki"
    adapter_backend = other if defect == "attribution" else backend
    watcher_backend = other if defect == "watcher" else backend
    store = "loki-gateway" if backend == "loki" else "victorialogs"
    writer = other if defect == "writer" else backend
    url = (
        "http://victorialogs.monitoring.svc.cluster.local:9428/insert/loki/api/v1/push?disable_message_parsing=1"
        if writer == "victorialogs"
        else "http://loki-gateway.monitoring.svc.cluster.local/loki/api/v1/push"
    )
    application_url = (
        (
            "http://loki-gateway.monitoring.svc.cluster.local/loki/api/v1/push"
            if backend == "victorialogs"
            else "http://victorialogs.monitoring.svc.cluster.local:9428/insert/loki/api/v1/push?disable_message_parsing=1"
        )
        if defect == "application-writer"
        else url
    )
    services = [store]
    if defect == "opposite-store":
        services.append("victorialogs" if backend == "loki" else "loki-gateway")
    adapter_spec = {
        "containers": [{"name": "adapter", "env": []}],
        "volumes": [{"name": "config", "configMap": {"name": "attribution-adapter-config"}}],
    }
    watcher_spec = {
        "containers": [{"name": "watcher", "env": [{"name": "LOG_QUERY_BACKEND", "value": watcher_backend}]}]
    }

    responses = {
        ("deployments", "-l", "app.kubernetes.io/name=attribution-adapter"): {
            "items": [{"spec": {"template": {"spec": adapter_spec}}}]
        },
        ("configmap", "attribution-adapter-config"): {
            "data": {"settings.env": f"unrelated_setting=preserved\nlog_query_backend={adapter_backend}\n"}
        },
        ("deployments", "-l", "app.kubernetes.io/name=watcher"): {
            "items": [{"spec": {"template": {"spec": watcher_spec}}}]
        },
        ("configmap", "alloy-config"): {
            "data": {"config.alloy": f'loki.write "default" {{ endpoint {{ url = "{url}" }} }}'}
        },
        ("configmap", "alloy-application-config"): {
            "data": {"config.alloy": f'loki.write "default" {{ endpoint {{ url = "{application_url}" }} }}'}
        },
        ("services", "-n", "monitoring"): {"items": [{"metadata": {"name": name}} for name in services]},
    }
    return lambda *arguments: responses[arguments]


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
def test_deployment_qualification_accepts_readers_and_collectors_on_the_selected_store(backend: str) -> None:
    evidence = log_backend_selection(_deployment(backend), log_backend=backend, attribution_mode="available")

    assert evidence.observed, evidence.summary


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
@pytest.mark.parametrize("defect", ("attribution", "watcher", "opposite-store", "writer", "application-writer"))
def test_deployment_qualification_rejects_wrong_reader_writer_or_store(backend: str, defect: str) -> None:
    evidence = log_backend_selection(_deployment(backend, defect), log_backend=backend, attribution_mode="available")

    assert not evidence.observed
    if defect in ("attribution", "watcher"):
        assert {"attribution": "attribution-adapter", "watcher": "watcher"}[defect] in evidence.summary
