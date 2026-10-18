# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from recovery_libs.testing.launch.jobs import Job, JobNotFound, JobSpec, JobState
from recovery_libs.testing.scenario import (
    Evidence,
    Run,
    Scenario,
    StageFailed,
    act,
    consistently,
    eventually,
    on,
    run_scenario,
    submit,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)
WORKLOAD = {"script": "true"}


class FakeLauncher:
    """Keeps jobs in memory; each submission gets the next id and the state the test assigns its name."""

    def __init__(self, states: dict[str, JobState]) -> None:
        self.states = states
        self.jobs: dict[str, Job] = {}
        self.specs: dict[str, JobSpec] = {}
        self.history_records: dict[str, Job] = {}
        self.cancelled: list[str] = []

    def submit(self, spec: JobSpec) -> Job:
        job_id = str(len(self.jobs) + 1)
        self.specs[job_id] = spec
        self.jobs[job_id] = Job(job_id, spec.name, self.states[spec.name], (), None, 0, T0)
        return self.jobs[job_id]

    def get(self, *, job_id: str) -> Job:
        if job_id not in self.jobs:
            raise JobNotFound(job_id)
        return self.jobs[job_id]

    def history(self, *, job_id: str) -> Job:
        if job_id not in self.history_records:
            raise JobNotFound(job_id)
        return self.history_records[job_id]

    def forget(self, job_id: str, *, finished_as: JobState | None = None) -> None:
        """Drop the job like Slurm after MinJobAge, keeping its accounting record when it finished as `finished_as`."""

        job = self.jobs.pop(job_id)
        if finished_as is not None:
            self.history_records[job_id] = replace(job, state=finished_as)

    def cancel(self, *, job_id: str) -> None:
        self.cancelled.append(job_id)
        self.jobs[job_id] = replace(self.jobs[job_id], state=JobState.CANCELLED)


def in_state(state: JobState):
    def check(run: Run[None]) -> Evidence:
        return Evidence(run.job.state is state, f"{run.job.name} is {run.job.state}")

    return check


def test_later_job_is_built_from_earlier_jobs_and_checked_by_role() -> None:
    launcher = FakeLauncher({"parent": JobState.COMPLETED, "child": JobState.RUNNING})
    scenario = Scenario(
        name="parent and child",
        jobs={"parent": JobSpec(name="parent", workload=WORKLOAD)},
        stages=(
            act(
                "submit child",
                submit(
                    "child",
                    lambda run: JobSpec(name="child", workload=WORKLOAD, metadata={"after": run.jobs["parent"].job_id}),
                ),
            ),
            eventually(
                "states", on("parent", in_state(JobState.COMPLETED)), on("child", in_state(JobState.RUNNING)), within=0
            ),
        ),
    )

    result = run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert launcher.specs[result.jobs["child"].job_id].metadata["after"] == result.jobs["parent"].job_id


def test_failing_check_names_the_job_role() -> None:
    launcher = FakeLauncher({"parent": JobState.FAILED})
    scenario = Scenario(
        name="parent",
        jobs={"parent": JobSpec(name="parent", workload=WORKLOAD)},
        stages=(eventually("completed", on("parent", in_state(JobState.COMPLETED)), within=0),),
    )

    with pytest.raises(StageFailed, match="parent: parent is FAILED"):
        run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)


def test_consistently_fails_when_evidence_breaks_within_its_window() -> None:
    """Evidence that holds at first and then breaks, such as a second log reader duplicating lines, must fail."""

    launcher = FakeLauncher({"job": JobState.RUNNING})
    observations = iter((True, False))

    def exact_count(run: Run[None]) -> Evidence:
        exact = next(observations, False)
        return Evidence(exact, "exact count" if exact else "duplicated lines")

    scenario = Scenario(
        name="duplicates",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(consistently("stays exact", exact_count, for_seconds=10),),
    )
    now = [0.0]

    with pytest.raises(StageFailed, match="duplicated lines"):
        run_scenario(
            scenario,
            launcher=launcher,
            services=None,
            clock=lambda: now[0],
            sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )


def test_every_active_job_is_cancelled_when_the_scenario_ends() -> None:
    launcher = FakeLauncher({"done": JobState.COMPLETED, "busy": JobState.RUNNING, "late": JobState.PENDING})
    scenario = Scenario(
        name="cleanup",
        jobs={"done": JobSpec(name="done", workload=WORKLOAD), "busy": JobSpec(name="busy", workload=WORKLOAD)},
        stages=(act("submit late", submit("late", JobSpec(name="late", workload=WORKLOAD))),),
    )

    result = run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert sorted(launcher.cancelled) == sorted([result.jobs["busy"].job_id, result.jobs["late"].job_id])


@pytest.mark.regression
def test_checks_keep_the_first_submit_time_after_the_scheduler_resets_it_on_requeue() -> None:
    launcher = FakeLauncher({"job": JobState.PENDING})
    run = Run({}, launcher, None).launched("job", launcher.submit(JobSpec(name="job", workload=WORKLOAD)))
    launcher.jobs["1"] = replace(launcher.jobs["1"], submitted_at=T0 + timedelta(minutes=3), restart_count=1)

    refreshed = run.refreshed()

    assert refreshed.job.restart_count == 1
    assert refreshed.submitted_at == T0


@pytest.mark.regression
def test_a_job_the_scheduler_forgets_neither_fails_the_scenario_nor_stops_cleanup() -> None:
    """Slurm drops finished jobs after MinJobAge; a long scenario must not fail or leak its active jobs then."""

    launcher = FakeLauncher({"helper": JobState.COMPLETED, "main": JobState.RUNNING})

    def forget_helper(run: Run[None]) -> None:
        launcher.forget(run.jobs["helper"].job_id)

    scenario = Scenario(
        name="forgotten helper",
        jobs={"helper": JobSpec(name="helper", workload=WORKLOAD), "main": JobSpec(name="main", workload=WORKLOAD)},
        stages=(
            act("scheduler forgets helper", forget_helper),
            eventually("main runs", on("main", in_state(JobState.RUNNING)), within=0),
        ),
    )

    result = run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert launcher.cancelled == [result.jobs["main"].job_id]


@pytest.mark.regression
def test_undo_runs_even_when_cleanup_cannot_read_the_jobs() -> None:
    """A scaled-down service must come back even when the scheduler has no record of the scenario's job."""

    launcher = FakeLauncher({"job": JobState.RUNNING})
    undone: list[str] = []

    def scale_down_then_lose_the_job(run: Run[None]) -> Run[None]:
        launcher.forget(run.job.job_id)
        return run.with_cleanup("scale service", lambda: undone.append("scale service"))

    scenario = Scenario(
        name="unreadable at cleanup",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(act("scale down; scheduler loses the job", scale_down_then_lose_the_job),),
    )

    with pytest.raises(ExceptionGroup) as raised:
        run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert undone == ["scale service"]
    assert raised.group_contains(JobNotFound)


@pytest.mark.regression
def test_a_job_forgotten_before_it_was_seen_finishing_is_read_from_history() -> None:
    """A job can finish and age out between polls; its last snapshot is stale, so history decides its state."""

    launcher = FakeLauncher({"job": JobState.RUNNING})

    def finish_and_forget(run: Run[None]) -> None:
        launcher.forget(run.job.job_id, finished_as=JobState.FAILED)

    scenario = Scenario(
        name="forgotten while running",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(
            act("job fails and ages out between polls", finish_and_forget),
            eventually("failed", in_state(JobState.FAILED), within=0),
        ),
    )

    run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert launcher.cancelled == []


def test_one_unreadable_job_does_not_leave_the_others_running() -> None:
    launcher = FakeLauncher({"lost": JobState.RUNNING, "busy": JobState.RUNNING})

    def lose_without_history(run: Run[None]) -> None:
        launcher.forget(run.jobs["lost"].job_id)

    scenario = Scenario(
        name="one unreadable job",
        jobs={"lost": JobSpec(name="lost", workload=WORKLOAD), "busy": JobSpec(name="busy", workload=WORKLOAD)},
        stages=(act("scheduler loses one job", lose_without_history),),
    )

    with pytest.raises(ExceptionGroup) as raised:
        run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert launcher.cancelled == ["2"]
    assert raised.group_contains(JobNotFound)


def test_a_cleanup_failure_does_not_hide_why_the_stage_failed() -> None:
    launcher = FakeLauncher({"job": JobState.RUNNING})

    def cancel_fails(*, job_id: str) -> None:
        raise RuntimeError(f"cancel {job_id} timed out")

    launcher.cancel = cancel_fails
    scenario = Scenario(
        name="stage fails, then cleanup fails",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(eventually("completed", in_state(JobState.COMPLETED), within=0),),
    )

    with pytest.raises(StageFailed, match="job is RUNNING") as raised:
        run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert any("cancel 1 timed out" in note for note in raised.value.__notes__)


def test_cleanup_trusts_the_final_state_a_stage_observed() -> None:
    """A job seen finishing can age out of the scheduler before cleanup; it must not be read as still active."""

    launcher = FakeLauncher({"job": JobState.RUNNING})

    def finish(run: Run[None]) -> None:
        launcher.jobs[run.job.job_id] = replace(run.job, state=JobState.COMPLETED)

    def forget(run: Run[None]) -> None:
        launcher.forget(run.job.job_id)

    scenario = Scenario(
        name="finished, then forgotten",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(
            act("job finishes", finish),
            eventually("completed", in_state(JobState.COMPLETED), within=0),
            act("scheduler forgets the job", forget),
        ),
    )

    run_scenario(scenario, launcher=launcher, services=None, sleep=lambda _: None)

    assert launcher.cancelled == []


def test_a_check_that_raises_counts_as_an_unmet_poll() -> None:
    """A transient error, such as a scheduler command timing out, must not end a stage that later holds."""

    launcher = FakeLauncher({"job": JobState.RUNNING})
    outcomes = iter((TimeoutError("scontrol timed out"), True))

    def flaky(run: Run[None]) -> Evidence:
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return Evidence(outcome, "service answered")

    scenario = Scenario(
        name="transient error",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(eventually("answers", flaky, within=10),),
    )
    now = [0.0]

    result = run_scenario(
        scenario,
        launcher=launcher,
        services=None,
        clock=lambda: now[0],
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
    )

    assert result.stages[0].polls == 2


def test_a_failed_assertion_in_a_check_ends_the_scenario_even_if_a_later_poll_would_hold() -> None:
    """An assertion states a contradiction, such as a job requeued that must not be, so retrying cannot excuse it."""

    launcher = FakeLauncher({"job": JobState.RUNNING})
    outcomes = iter((False, True))

    def contradicted(run: Run[None]) -> Evidence:
        assert next(outcomes), "job was requeued"
        return Evidence(True, "never requeued")

    scenario = Scenario(
        name="contradiction",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(eventually("not requeued", contradicted, within=10),),
    )
    now = [0.0]

    with pytest.raises(AssertionError, match="job was requeued"):
        run_scenario(
            scenario,
            launcher=launcher,
            services=None,
            clock=lambda: now[0],
            sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )

    assert launcher.cancelled == ["1"]


def test_a_check_that_keeps_raising_fails_the_stage_with_the_error() -> None:
    launcher = FakeLauncher({"job": JobState.RUNNING})

    def broken(run: Run[None]) -> Evidence:
        raise TimeoutError("scontrol timed out")

    scenario = Scenario(
        name="persistent error",
        job=JobSpec(name="job", workload=WORKLOAD),
        stages=(eventually("answers", broken, within=0),),
    )
    stages: list[str | None] = []

    with pytest.raises(StageFailed, match="TimeoutError: scontrol timed out"):
        run_scenario(
            scenario,
            launcher=launcher,
            services=None,
            sleep=lambda _: None,
            on_stage=lambda result: stages.append(result.failure),
        )

    assert len(stages) == 1
