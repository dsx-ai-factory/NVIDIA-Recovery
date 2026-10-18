# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Declare a test as its jobs and an ordered list of stages, then run it."""

from __future__ import annotations

import functools
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from opentelemetry import trace

from recovery_libs.testing.http.tracing import TRACER_NAME
from recovery_libs.testing.launch.jobs import Job, JobNotFound, JobSpec
from recovery_libs.testing.launch.launcher import Launcher

_UNSAFE = "__unsafe_action__"

JOB = "job"
"""The role of a single-job scenario's job."""


@dataclass(frozen=True)
class Evidence:
    """Whether a check held, and why."""

    observed: bool
    summary: str


@dataclass(frozen=True)
class Run[Services]:
    """What a check can see: the launched jobs as of this poll, and the services to query.

    `job` and `submitted_at` describe the job in focus, which checks read by default; `on` changes the focus.
    `submitted` keeps each job's first submit time, because schedulers such as Slurm reset it on requeue.
    `notes` holds values that `remember` steps recorded, such as a counter's value before an action.
    `cleanups` holds undo actions, by name, that the scenario runs on the way out unless a step removed them first.
    """

    jobs: Mapping[str, Job]
    launcher: Launcher
    services: Services
    role: str = JOB
    submitted: Mapping[str, datetime] = field(default_factory=dict)
    notes: Mapping[str, object] = field(default_factory=dict)
    cleanups: Mapping[str, Callable[[], None]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        submitted = dict(self.submitted)
        for role, job in self.jobs.items():
            if role not in submitted:
                if job.submitted_at is None:
                    raise ValueError(f"job {job.job_id} has no submit time")
                submitted[role] = job.submitted_at
        object.__setattr__(self, "jobs", MappingProxyType(dict(self.jobs)))
        object.__setattr__(self, "submitted", MappingProxyType(submitted))
        object.__setattr__(self, "notes", MappingProxyType(dict(self.notes)))
        object.__setattr__(self, "cleanups", MappingProxyType(dict(self.cleanups)))

    def with_cleanup(self, name: str, undo: Callable[[], None]) -> Run[Services]:
        return replace(self, cleanups={**self.cleanups, name: undo})

    def without_cleanup(self, name: str) -> Run[Services]:
        return replace(self, cleanups={key: undo for key, undo in self.cleanups.items() if key != name})

    @property
    def job(self) -> Job:
        return self.jobs[self.role]

    @property
    def submitted_at(self) -> datetime:
        return self.submitted[self.role]

    def focused(self, role: str) -> Run[Services]:
        if role not in self.jobs:
            raise KeyError(f"no job with role {role!r}; launched: {sorted(self.jobs)}")
        return replace(self, role=role)

    def launched(self, role: str, job: Job) -> Run[Services]:
        if role in self.jobs:
            raise ValueError(f"a job with role {role!r} was already launched")
        return replace(self, jobs={**self.jobs, role: job})

    def refreshed(self) -> Run[Services]:
        return replace(self, jobs={role: self._current(job) for role, job in self.jobs.items()})

    def _current(self, job: Job) -> Job:
        """The job's latest snapshot. Schedulers forget finished jobs after a while (Slurm's MinJobAge): a forgotten
        job keeps its last snapshot if that was final, and otherwise is read from the scheduler's history."""

        try:
            return self.launcher.get(job_id=job.job_id)
        except JobNotFound:
            return job if job.state.is_final else self.launcher.history(job_id=job.job_id)


class Check[Services](Protocol):
    """A read-only observation of a run."""

    def __call__(self, run: Run[Services]) -> Evidence: ...


class Action[Services](Protocol):
    """A deliberate change to a run, such as cancelling a job; returns the run when it launches one."""

    def __call__(self, run: Run[Services]) -> Run[Services] | None: ...


def unsafe[**Parameters, Services](
    factory: Callable[Parameters, Action[Services]],
) -> Callable[Parameters, Action[Services]]:
    """Mark an action factory whose actions change state other scenarios share, such as a deployed service.

    A scenario that uses one cannot run in parallel with others. Actions that change only the scenario's own jobs
    or files are safe and stay unmarked.
    """

    @functools.wraps(factory)
    def build(*args: Parameters.args, **kwargs: Parameters.kwargs) -> Action[Services]:
        action = factory(*args, **kwargs)
        setattr(action, _UNSAFE, True)
        return action

    return build


def unsafe_steps[Services](scenario: Scenario[Services]) -> tuple[str, ...]:
    """The names of the scenario's steps that apply an `unsafe` action."""

    return tuple(
        step.name
        for step in (*scenario.setup, *scenario.stages)
        if isinstance(step, Act) and getattr(step.action, _UNSAFE, False)
    )


def on[Services](role: str, *checks: Check[Services]) -> Check[Services]:
    """Apply checks to the job launched as `role` instead of the run's default job.

    The evidence names what every check observed once all hold, and only the unmet checks until then.
    """

    def check(run: Run[Services]) -> Evidence:
        evidence = tuple(each(run.focused(role)) for each in checks)
        observed = all(item.observed for item in evidence)
        summary = "; ".join(item.summary for item in evidence if observed or not item.observed)
        return Evidence(observed, f"{role}: {summary}")

    return check


def remember[Services](key: str, read: Callable[[Run[Services]], object]) -> Action[Services]:
    """Record a value read from the run under `key`, for later checks to compare against."""

    def action(run: Run[Services]) -> Run[Services]:
        return replace(run, notes={**run.notes, key: read(run)})

    return action


def submit[Services](
    role: str, spec: JobSpec | Callable[[Run[Services]], JobSpec]
) -> Callable[[Run[Services]], Run[Services]]:
    """Launch another job as `role`; build its spec from the run to refer to jobs already launched."""

    def action(run: Run[Services]) -> Run[Services]:
        built = spec if isinstance(spec, JobSpec) else spec(run)
        return run.launched(role, run.launcher.submit(built))

    return action


class Mode(StrEnum):
    EVENTUALLY = "eventually"
    CONSISTENTLY = "consistently"


@dataclass(frozen=True)
class Stage[Services]:
    """A named step: its checks must hold eventually within, or consistently for, `seconds`.

    Set `known_gap` to the reason this stage is expected to time out, such as a documented product limitation;
    a timeout then raises `KnownGap` instead of `StageFailed`.
    """

    name: str
    checks: tuple[Check[Services], ...]
    mode: Mode
    seconds: float
    known_gap: str | None = None


def eventually[Services](
    name: str, *checks: Check[Services], within: float, known_gap: str | None = None
) -> Stage[Services]:
    """A stage that passes once all checks hold at the same time, before `within` seconds pass."""

    return Stage(name, checks, Mode.EVENTUALLY, within, known_gap)


def consistently[Services](name: str, *checks: Check[Services], for_seconds: float) -> Stage[Services]:
    """A stage that passes only if all checks hold for the whole `for_seconds` window."""

    return Stage(name, checks, Mode.CONSISTENTLY, for_seconds)


@dataclass(frozen=True)
class Act[Services]:
    """A named step that changes the run once, between stages."""

    name: str
    action: Action[Services]


def act[Services](name: str, action: Action[Services]) -> Act[Services]:
    """A step that applies `action` once and moves on."""

    return Act(name, action)


type Step[Services] = Stage[Services] | Act[Services]


@dataclass(frozen=True)
class Scenario[Services]:
    """Jobs to launch at the start, by role, and the steps the run must pass through, in order.

    Give `job` for a single job, launched as role `JOB`, or `jobs` for several. `jobs` launch in order, and an
    entry may be a function of the run so far, so a child job can name the job IDs of parents listed before it.
    `submit` steps launch more jobs later, when the time of submission matters. `setup` acts prepare what the jobs
    need before any launches, such as files they read.
    """

    name: str
    stages: tuple[Step[Services], ...]
    job: JobSpec | None = None
    jobs: Mapping[str, JobSpec | Callable[[Run[Services]], JobSpec]] = field(default_factory=dict)
    setup: tuple[Act[Services], ...] = ()

    def __post_init__(self) -> None:
        if (self.job is None) == (not self.jobs):
            raise ValueError(f"scenario {self.name!r} needs exactly one of job or jobs")
        object.__setattr__(self, "jobs", MappingProxyType({JOB: self.job} if self.job else dict(self.jobs)))


@dataclass(frozen=True)
class StageResult:
    """What a step observed when it finished; `failure` explains why a stage did not hold."""

    name: str
    evidence: tuple[Evidence, ...]
    seconds: float
    polls: int = 0
    failure: str | None = None
    jobs: Mapping[str, str] = field(default_factory=dict)
    """Job ID by role for every job launched so far."""


@dataclass(frozen=True)
class ScenarioResult:
    jobs: Mapping[str, Job]
    stages: tuple[StageResult, ...]


class StageFailed(AssertionError):
    """A stage's checks did not hold."""


class KnownGap(StageFailed):
    """A stage timed out that the scenario expects to fail because of a documented product limitation.

    The pytest plugin reports it as an expected failure (xfail) rather than a test failure.
    """


def run_scenario[Services](
    scenario: Scenario[Services],
    *,
    launcher: Launcher,
    services: Services,
    poll_interval: float = 2.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    on_stage: Callable[[StageResult], None] = lambda _: None,
) -> ScenarioResult:
    """Launch the scenario's jobs, pass the run through every step in order, then cancel any job still active.

    `on_stage` receives each step's result as it finishes, including the stage that fails the scenario.
    """

    __tracebackhide__ = True
    tracer = trace.get_tracer(TRACER_NAME)
    with tracer.start_as_current_span(f"scenario {scenario.name}"):
        run: Run[Services] = Run({}, launcher, services)
        results: list[StageResult] = []
        try:
            for act in scenario.setup:
                run, result = _run_act(act, run, tracer, clock)
                results.append(result)
                on_stage(result)
            for role, spec in scenario.jobs.items():
                run = submit(role, spec)(run)
            for step in scenario.stages:
                if isinstance(step, Stage):
                    run, result = _run_stage(step, run, tracer, poll_interval, clock, sleep)
                else:
                    run, result = _run_act(step, run, tracer, clock)
                results.append(result)
                on_stage(result)
                if result.failure is not None:
                    gap = isinstance(step, Stage) and step.known_gap
                    raise KnownGap(f"{gap}: {result.failure}") if gap else StageFailed(result.failure)
        except BaseException as failure:
            for error in _clean_up(run):
                failure.add_note(f"scenario cleanup also failed: {error!r}")
            raise
        if errors := _clean_up(run):
            raise ExceptionGroup("scenario cleanup failed", errors)
        return ScenarioResult(run.refreshed().jobs, tuple(results))


def _clean_up[Services](run: Run[Services]) -> list[Exception]:
    """Undo outstanding changes newest first, then cancel active jobs; attempt every step and return what failed.

    Undo runs before the jobs are read again, so a stack change such as a scaled-down service is restored even when
    the scheduler cannot be reached. Each job is read and cancelled on its own, so one unreadable job does not leave
    the others running.
    """

    errors: list[Exception] = []
    for undo in reversed(tuple(run.cleanups.values())):
        try:
            undo()
        except Exception as error:  # noqa: BLE001
            errors.append(error)
    for job in run.jobs.values():
        try:
            if not run._current(job).state.is_final:
                run.launcher.cancel(job_id=job.job_id)
        except Exception as error:  # noqa: BLE001
            errors.append(error)
    return errors


def _run_stage[Services](
    stage: Stage[Services],
    run: Run[Services],
    tracer: trace.Tracer,
    poll_interval: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[Run[Services], StageResult]:
    """Poll the stage's checks until it passes or fails, and return the run as last read with the result.

    Each check reads the jobs afresh; the run returned carries the last read, so later steps and cleanup start from
    what the stage saw, such as a job it saw finish before the scheduler forgot it.
    """

    with tracer.start_as_current_span(f"stage {stage.name}", attributes={"stage.mode": stage.mode.value}) as span:
        started = clock()
        deadline = started + stage.seconds
        polls = 0
        evidence: tuple[Evidence, ...] = ()
        failure: str | None = None
        while True:
            polls += 1
            observed = []
            for check in stage.checks:
                run, item = _observe(check, run)
                observed.append(item)
            previous, evidence = evidence, tuple(observed)
            if evidence != previous:
                span.add_event("evidence", {f"check.{index}": _describe(item) for index, item in enumerate(evidence)})
            held = all(item.observed for item in evidence)
            if stage.mode is Mode.EVENTUALLY and held:
                break
            if stage.mode is Mode.CONSISTENTLY and not held:
                failure = _failure(stage, evidence, "stopped holding")
                break
            if clock() >= deadline:
                if stage.mode is Mode.EVENTUALLY:
                    failure = _failure(stage, evidence, f"did not hold within {stage.seconds:g}s")
                break
            sleep(poll_interval)
        span.set_attribute("stage.polls", polls)
        return run, StageResult(stage.name, evidence, clock() - started, polls, failure, _job_ids(run))


def _observe[Services](check: Check[Services], run: Run[Services]) -> tuple[Run[Services], Evidence]:
    """Read the jobs again and apply `check`; a read or check that raises is an unmet poll, not the end of the run.

    Errors such as a scheduler command timing out or a service answering 503 often clear on the next poll; one that
    persists fails the stage at its deadline, with the error as the unmet evidence. A failed assertion is the exception:
    it states a contradiction that a later poll cannot excuse, so it ends the scenario at once.
    """

    try:
        run = run.refreshed()
        return run, check(run)
    except AssertionError:
        raise
    except Exception as error:  # noqa: BLE001
        return run, Evidence(False, f"{type(error).__name__}: {error}")


def _run_act[Services](
    step: Act[Services],
    run: Run[Services],
    tracer: trace.Tracer,
    clock: Callable[[], float],
) -> tuple[Run[Services], StageResult]:
    with tracer.start_as_current_span(f"act {step.name}"):
        started = clock()
        changed = step.action(run.refreshed())
        after = changed or run
        return after, StageResult(step.name, (), clock() - started, jobs=_job_ids(after))


def steps(results: Sequence[StageResult]) -> str:
    """The scenario's steps one line each, with the evidence of a failing step marked met or unmet beneath it."""

    rows = []
    for result in results:
        rows.append(step_line(result))
        if result.failure:
            rows.extend(f"        {'met' if item.observed else 'unmet'}: {item.summary}" for item in result.evidence)
    return "\n".join(rows)


def step_line(result: StageResult) -> str:
    """One finished step: outcome, name, duration, and poll count, in columns that line up across scenarios."""

    return f"{outcome(result):<4}  {result.name:<{_STEP_NAME_WIDTH}}  {result.seconds:7.1f}s  {polls(result)}".rstrip()


_STEP_NAME_WIDTH = 32


def outcome(result: StageResult) -> str:
    """How a step ended: FAIL, PASS for a stage that polled to success, or ACT for an action."""

    return "FAIL" if result.failure else ("PASS" if result.polls else "ACT")


def polls(result: StageResult) -> str:
    """The step's poll count, such as "3 polls", or nothing for an action."""

    return f"{result.polls} poll{'' if result.polls == 1 else 's'}" if result.polls else ""


def _job_ids[Services](run: Run[Services]) -> dict[str, str]:
    return {role: job.job_id for role, job in run.jobs.items()}


def _describe(evidence: Evidence) -> str:
    return f"{'observed' if evidence.observed else 'unmet'}: {evidence.summary}"


def _failure[Services](stage: Stage[Services], evidence: tuple[Evidence, ...], reason: str) -> str:
    unmet = "; ".join(item.summary for item in evidence if not item.observed)
    return f"stage {stage.name!r} {reason}: {unmet}"
