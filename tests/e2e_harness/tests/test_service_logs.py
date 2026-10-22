# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exported service logs count only when the store holds each service's record for this job."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime

import httpx
import pytest
from e2e_harness.checks import service_logs_exported
from e2e_harness.recovery_client import (
    ControllerApi,
    GatewayApi,
    JobManagerApi,
    LogsApi,
    NotificationsApi,
    RecoveryClient,
)
from recovery_libs.testing.launch.jobs import Job, JobState
from recovery_libs.testing.scenario import Run

SUBMITTED = datetime(2026, 1, 1, tzinfo=UTC)
NOW_NS = "1767225660000000000"

# What each store holds after the services export through OTLP, in the shape each store reports it. Loki indexes
# `service.name` as the `service_name` label and keeps record attributes as structured metadata; VictoriaLogs keeps
# both as fields under their OTLP names. Job 1234 shares job 123's prefix.
RECORDS = (
    ("job_manager", "Upserting job: job_id=123, attempt_index=0", {}),
    ("job_manager", "Upserting job: job_id=1234, attempt_index=0", {}),
    ("slurm-advisor-controller", "applied advice job release", {"job_id": "123"}),
    ("slurm-advisor-controller", "applied advice job release", {"job_id": "1234"}),
)


def _matches(query: str, backend: str, service: str, message: str, fields: dict[str, str]) -> bool:
    """Evaluate the subset of LogQL or LogsQL that the check sends against one stored record."""

    if backend == "loki":
        selector = re.fullmatch(r'\{service_name="([^"]+)"\} \|= "([^"]*)"((?: \| \w+="[^"]*")*)', query)
        assert selector is not None, query
        wanted = dict(re.findall(r'(\w+)="([^"]*)"', selector.group(3)))
        return selector.group(1) == service and selector.group(2) in message and wanted.items() <= fields.items()
    terms = dict(re.findall(r'([\w.]+):="([^"]*)"', query))
    text = re.search(r'_msg:~"((?:[^"\\]|\\.)*)"', query)
    assert text is not None, query
    pattern = json.loads(f'"{text.group(1)}"')
    stored = {"service.name": service, **fields}
    return terms.items() <= stored.items() and re.search(pattern, message) is not None


def _client(backend: str, records: tuple[tuple[str, str, dict[str, str]], ...]) -> RecoveryClient:
    def reply(request: httpx.Request) -> httpx.Response:
        query = request.url.params["query"]
        found = [record for record in records if _matches(query, backend, *record)]
        if backend == "loki":
            result = [
                {"stream": {"service_name": service, **fields}, "values": [[NOW_NS, message]]}
                for service, message, fields in found
            ]
            return httpx.Response(200, json={"status": "success", "data": {"resultType": "streams", "result": result}})
        rows = [
            json.dumps({"_time": "2026-01-01T00:01:00Z", "_msg": message, "service.name": service, **fields})
            for service, message, fields in found
        ]
        return httpx.Response(200, text="\n".join(rows))

    http = httpx.Client(base_url="http://logs.test", transport=httpx.MockTransport(reply))
    return RecoveryClient(
        JobManagerApi(http),
        NotificationsApi(http),
        GatewayApi(http),
        LogsApi(http, backend=backend),
        ControllerApi(http),
        None,
    )


def _run(services: RecoveryClient, job_id: str = "123") -> Run[RecoveryClient]:
    job = Job(job_id, "job", JobState.RUNNING, ("node-1",), None, 1, SUBMITTED)
    return Run({"job": job}, None, services, submitted={"job": SUBMITTED})


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_both_services_logs_for_the_job_are_evidence(backend: str) -> None:
    assert service_logs_exported()(_run(_client(backend, RECORDS))).observed


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
@pytest.mark.parametrize("silent", ["job_manager", "slurm-advisor-controller"])
def test_a_service_without_exported_logs_is_named(backend: str, silent: str) -> None:
    records = tuple(record for record in RECORDS if record[0] != silent)
    evidence = service_logs_exported()(_run(_client(backend, records)))
    assert not evidence.observed
    assert silent in evidence.summary


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
@pytest.mark.parametrize("service", ["job_manager", "slurm-advisor-controller"])
def test_another_jobs_logs_are_not_evidence(backend: str, service: str) -> None:
    """Job 12's check must not accept `service`'s records for jobs 123 or 1234, whose IDs it prefixes."""

    job_12 = {
        "job_manager": ("job_manager", "Upserting job: job_id=12, attempt_index=0", {}),
        "slurm-advisor-controller": ("slurm-advisor-controller", "applied advice job release", {"job_id": "12"}),
    }
    records = (*RECORDS, *(record for name, record in job_12.items() if name != service))
    evidence = service_logs_exported()(_run(_client(backend, records), job_id="12"))
    assert not evidence.observed
    assert service in evidence.summary
