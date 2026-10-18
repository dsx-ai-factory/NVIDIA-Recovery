# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run `@scenario` tests: each returns a Scenario, and the plugin runs it.

A scenario test's parameters are ordinary pytest fixtures. The returned scenario runs on the
`launcher` fixture against the `scenario_services` fixture, which the test suite defines. The test
gets a `scenario(<folder>)` marker named after the folder that contains its module.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, NamedTuple

import pytest

from recovery_libs.testing.scenario import (
    KnownGap,
    Scenario,
    StageResult,
    run_scenario,
    step_line,
    steps,
    unsafe_steps,
)

_SCENARIO_TEST = "__scenario_test__"
_STAGE = "stage "
_UNSAFE = "unsafe steps"
_RUNNER_FIXTURES = ("launcher", "scenario_services")


def scenario[Builder: Callable[..., Scenario[Any]]](builder: Builder) -> Builder:
    """Mark a test that returns the Scenario to run instead of asserting itself."""

    setattr(builder, _SCENARIO_TEST, True)
    return pytest.mark.usefixtures(*_RUNNER_FIXTURES)(builder)


def pytest_itemcollected(item: pytest.Item) -> None:
    if _is_scenario_test(item):
        item.add_marker(pytest.mark.scenario(item.path.parent.name))


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    if not _is_scenario_test(pyfuncitem):
        return None
    # A failure's message and its steps explain it; the runner's frames do not.
    __tracebackhide__ = True
    fixtures: dict[str, Any] = pyfuncitem.funcargs
    parameters = inspect.signature(pyfuncitem.obj).parameters
    built = pyfuncitem.obj(**{name: fixtures[name] for name in parameters})
    results: list[StageResult] = []
    if unsafe := unsafe_steps(built):
        pyfuncitem.user_properties.append((_UNSAFE, ", ".join(unsafe)))
        _warn_unsafe(pyfuncitem.config, built.name, unsafe)

    def record(result: StageResult) -> None:
        results.append(result)
        evidence = "; ".join(item.summary for item in result.evidence)
        pyfuncitem.user_properties.append((f"{_STAGE}{result.name}", f"{result.seconds:.1f}s {evidence}".strip()))
        _report_progress(pyfuncitem.config, built.name, result)

    try:
        run_scenario(built, launcher=fixtures["launcher"], services=fixtures["scenario_services"], on_stage=record)
    except KnownGap as gap:
        pytest.xfail(str(gap))
    finally:
        jobs = "  ".join(f"{role}={job_id}" for role, job_id in (results[-1].jobs if results else {}).items())
        pyfuncitem.add_report_section("call", "scenario steps", f"{built.name}  {jobs}\n{steps(results)}".strip())
    return True


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """Tabulate every scenario test after the run: outcome, time, steps, its slowest step, and where it stopped.

    Each test gets one row. A fixture error happens outside the scenario's call, so a test whose setup or teardown
    failed is an ERROR with the phase and its crash message, even if the scenario itself passed or never ran.
    """

    outcomes = _ScenarioOutcome.collect(terminalreporter.stats)
    if not outcomes:
        return
    terminalreporter.section("scenario summary")
    terminalreporter.write_line(f"{'STATUS':<7} {'TIME':>8}  {'STEPS':>5}  {'SLOWEST STEP':<44} SCENARIO")
    for outcome in outcomes:
        terminalreporter.write_line(outcome.summary_row())
        for name, value in outcome.report.user_properties:
            if name == _UNSAFE:
                terminalreporter.write_line(f"{'':<7} {_unsafe_message(str(value))}", yellow=True)


class _Stage(NamedTuple):
    name: str
    seconds: float


@dataclass(frozen=True)
class _ScenarioOutcome:
    """A scenario test's reports: its call, if the scenario ran, and its first failed setup or teardown."""

    report: pytest.TestReport
    call: pytest.TestReport | None
    error: pytest.TestReport | None

    @classmethod
    def collect(cls, stats: dict[str, list[Any]]) -> list[_ScenarioOutcome]:
        """One outcome per scenario test that ran or errored, from the terminal reporter's stats."""

        calls: dict[str, pytest.TestReport] = {}
        errors: dict[str, pytest.TestReport] = {}
        for reports in stats.values():
            for report in reports:
                if not isinstance(report, pytest.TestReport) or "scenario" not in report.keywords:
                    continue
                if report.when == "call":
                    calls[report.nodeid] = report
                elif report.failed:
                    errors.setdefault(report.nodeid, report)
        return [
            cls(calls.get(nodeid) or errors[nodeid], calls.get(nodeid), errors.get(nodeid))
            for nodeid in dict.fromkeys([*calls, *errors])
        ]

    @property
    def stages(self) -> list[_Stage]:
        return [
            _Stage(name.removeprefix(_STAGE), float(str(value).partition("s")[0]))
            for name, value in self.report.user_properties
            if name.startswith(_STAGE)
        ]

    @property
    def status(self) -> str:
        if self.call is None or self.error is not None:
            return "ERROR"
        return "XFAIL" if hasattr(self.call, "wasxfail") else self.call.outcome.upper()

    def summary_row(self) -> str:
        path, _, test = self.report.nodeid.partition("::")
        return (
            f"{self.status:<7} {self.report.duration:7.1f}s  {len(self.stages):>5}  {self._slowest_stage():<44} "
            f"{PurePosixPath(path).parent.name}::{test}{self._where_it_ended()}"
        )

    def _slowest_stage(self) -> str:
        if not self.stages:
            return "-"
        slowest = max(self.stages, key=lambda stage: stage.seconds)
        return f"{slowest.name} ({slowest.seconds:.1f}s)"

    def _where_it_ended(self) -> str:
        where = ""
        if self.call is not None and not self.call.passed and self.stages:
            where = f"  stopped at: {self.stages[-1].name}"
        if self.error is not None:
            where += f"  error at: {self.error.when}  {_crash_message(self.error)}".rstrip()
        return where


def _crash_message(report: pytest.TestReport) -> str:
    crash = getattr(report.longrepr, "reprcrash", None)
    return crash.message.partition("\n")[0] if crash is not None else ""


def _report_progress(config: pytest.Config, scenario_name: str, result: StageResult) -> None:
    """Print a finished step as it happens; otherwise a long scenario shows nothing until it ends."""

    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return
    reporter.write_line(f"  {scenario_name}  {step_line(result)}")


def _warn_unsafe(config: pytest.Config, scenario_name: str, step_names: tuple[str, ...]) -> None:
    """Flag a scenario that changes state other scenarios share before it starts."""

    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return
    reporter.write_line(f"  {scenario_name}  {_unsafe_message(', '.join(step_names))}", yellow=True)


def _unsafe_message(step_names: str) -> str:
    return f"unsafe to run in parallel: {step_names} change shared state"


def _is_scenario_test(item: pytest.Item) -> bool:
    return isinstance(item, pytest.Function) and getattr(item.obj, _SCENARIO_TEST, False)
