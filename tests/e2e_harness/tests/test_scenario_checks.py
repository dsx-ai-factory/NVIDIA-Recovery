# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from e2e_harness import audit
from e2e_harness.checks import accounted, restarted, task_checkpoint_audit
from e2e_harness.recovery_client import GatewayApi
from recovery_libs.testing.launch.jobs import Job, JobState
from recovery_libs.testing.scenario import Run

FIRST_SUBMIT = datetime(2026, 1, 1, tzinfo=UTC)
REQUEUED = FIRST_SUBMIT + timedelta(minutes=2)


def run_with(job: Job) -> Run[None]:
    return Run({"job": job}, None, None, submitted={"job": FIRST_SUBMIT})


def successor(state: JobState, started_at: datetime | None) -> Job:
    """The job after one requeue, as scontrol reports it: Slurm resets the submit time to the requeue."""

    return Job("1", "job", state, ("node-1",), None, 1, REQUEUED, started_at=started_at)


@pytest.mark.parametrize(
    ("state", "started_at"),
    [
        (JobState.RUNNING, REQUEUED + timedelta(seconds=5)),
        (JobState.COMPLETED, REQUEUED + timedelta(seconds=5)),
    ],
)
def test_restarted_holds_once_the_successor_has_started(state: JobState, started_at: datetime) -> None:
    assert restarted(times=1)(run_with(successor(state, started_at))).observed


@pytest.mark.parametrize(
    ("state", "started_at"),
    [
        (JobState.CANCELLED, None),
        (JobState.CANCELLED, FIRST_SUBMIT + timedelta(seconds=30)),
    ],
    ids=["never started", "start time of the first attempt"],
)
def test_restarted_does_not_hold_for_a_successor_that_never_started(
    state: JobState, started_at: datetime | None
) -> None:
    assert not restarted(times=1)(run_with(successor(state, started_at))).observed


class Accounting:
    """A launcher whose Slurm accounting reports `latest` as the job's most recent attempt."""

    def __init__(self, latest: Job) -> None:
        self.latest = latest

    def history(self, *, job_id: str) -> Job:
        return self.latest


@pytest.mark.parametrize(
    ("latest", "held"),
    [
        (Job("1", "job", JobState.FAILED, ("node-1",), 1, 1, REQUEUED), True),
        (Job("1", "job", JobState.PENDING, (), None, 2, REQUEUED + timedelta(minutes=1)), False),
    ],
    ids=["failed at the limit", "requeue-held successor past the limit"],
)
def test_accounted_rejects_an_attempt_past_the_limit(latest: Job, held: bool) -> None:
    """Accounting can show a successor that the controller's own view does not show yet."""

    at_limit = Job("1", "job", JobState.FAILED, ("node-1",), 1, 1, REQUEUED)
    run = Run({"job": at_limit}, Accounting(latest), None)

    assert accounted(JobState.FAILED, restarts=1)(run).observed is held


@pytest.mark.parametrize("defect", (None, "sibling", "wrong-attempt", "duplicate"))
def test_task_checkpoint_evidence_rejects_cross_task_attempt_and_replay_defects(defect: str | None) -> None:
    """Qualification must fail even when a service returns HTTP 200 with wrongly associated audit facts."""

    event = {
        "heimdallJobId": audit.heimdall_job_id("7_0", 1),
        "eventProducer": "job-manager",
        "eventType": "checkpoint_associated",
        "eventLevel": "operational",
        "eventProducedAt": 1767225600000000,
        "payload": {"job_id": "7_0", "attempt_index": 1, "checkpoint_kind": "save", "source_event_uuid": "save-1"},
    }
    if defect == "sibling":
        event["payload"]["job_id"] = "7_1"
    if defect == "wrong-attempt":
        event["heimdallJobId"] = audit.heimdall_job_id("7_0", 0)
    items = [event, event] if defect == "duplicate" else [event]
    queries = []

    def respond(request: httpx.Request) -> httpx.Response:
        queries.append(dict(request.url.params))
        return httpx.Response(200, json={"items": items, "cursor": {"nextCursor": ""}})

    with httpx.Client(base_url="http://gateway", transport=httpx.MockTransport(respond)) as http:
        services = SimpleNamespace(gateway=GatewayApi(http))
        job = Job("7_0", "array", JobState.RUNNING, (), None, 1, FIRST_SUBMIT)
        run = Run({"job": job}, None, services)
        if defect is not None:
            with pytest.raises(AssertionError):
                task_checkpoint_audit(saved_attempt=1)(run)
        else:
            assert task_checkpoint_audit(saved_attempt=1)(run).observed
            assert any(query.get("eventType") == "checkpoint_associated" for query in queries)
