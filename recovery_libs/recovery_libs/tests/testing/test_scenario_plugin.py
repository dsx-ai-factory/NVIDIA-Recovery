# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from xml.etree import ElementTree

import pytest

pytest_plugins = ["pytester"]

CONFTEST = """
from datetime import UTC, datetime

import pytest
from recovery_libs.testing.launch.jobs import Job, JobState


class CompletedLauncher:
    def submit(self, spec):
        return self.get(job_id="1")

    def get(self, *, job_id):
        return Job(job_id, "example", JobState.COMPLETED, (), 0, 0, datetime(2026, 1, 1, tzinfo=UTC))

    def cancel(self, *, job_id):
        raise AssertionError("a finished job must not be cancelled")


@pytest.fixture
def launcher():
    return CompletedLauncher()


@pytest.fixture
def scenario_services():
    return {"healthy": True}


@pytest.fixture
def expected_state():
    return JobState.COMPLETED
"""


def _scenario_test(expected_state: str, known_gap: str | None = None, fixtures: str = "expected_state") -> str:
    return f"""
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Evidence, Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_example({fixtures}):
    def check(run):
        held = run.job.state is {expected_state} and run.services["healthy"]
        return Evidence(held, f"job is {{run.job.state}}")

    return Scenario(
        name="example",
        job=JobSpec(name="example", workload={{"script": "true"}}),
        stages=(eventually("done", check, within=0, known_gap={known_gap!r}),),
    )
"""


def _suite(pytester: pytest.Pytester, expected_state: str, known_gap: str | None = None) -> pytest.Pytester:
    pytester.makeconftest(CONFTEST)
    pytester.mkdir("07-example").joinpath("test_example.py").write_text(_scenario_test(expected_state, known_gap))
    return pytester


def test_returned_scenario_runs_with_test_fixtures_launcher_and_services(pytester: pytest.Pytester) -> None:
    result = _suite(pytester, "expected_state").runpytest("-p", "recovery_libs.testing.scenario_plugin")

    result.assert_outcomes(passed=1)


def test_passing_scenario_reports_what_each_stage_observed(pytester: pytest.Pytester) -> None:
    suite = _suite(pytester, "expected_state")

    result = suite.runpytest("-p", "recovery_libs.testing.scenario_plugin", "--junitxml=report.xml")

    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(
        [
            "  example  PASS  done *s  1 poll",
            "*= scenario summary =*",
            "STATUS * SCENARIO",
            "PASSED * 1  done (0.0s) * 07-example::test_example",
        ]
    )
    properties = ElementTree.parse(suite.path / "report.xml").findall(".//testcase/properties/property")
    assert [(item.get("name"), item.get("value")) for item in properties] == [("stage done", "0.0s job is COMPLETED")]


def test_scenario_whose_fixture_fails_is_listed_in_the_summary(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(
        CONFTEST
        + """

@pytest.fixture
def unreachable_service():
    raise ConnectionRefusedError("[Errno 111] Connection refused")
"""
    )
    test = _scenario_test("expected_state", fixtures="expected_state, unreachable_service")
    pytester.mkdir("07-example").joinpath("test_example.py").write_text(test)

    result = pytester.runpytest("-p", "recovery_libs.testing.scenario_plugin")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(
        [
            "*= scenario summary =*",
            "ERROR * 0  - * 07-example::test_example  error at: setup  ConnectionRefusedError: *Connection refused",
        ]
    )


@pytest.mark.parametrize(
    ("expected_state", "outcomes", "stopped"),
    [
        ("expected_state", {"passed": 1, "errors": 1}, ""),
        ("JobState.FAILED", {"failed": 1, "errors": 1}, "  stopped at: done"),
    ],
)
def test_scenario_whose_teardown_fails_has_one_error_row(
    pytester: pytest.Pytester, expected_state: str, outcomes: dict[str, int], stopped: str
) -> None:
    pytester.makeconftest(
        CONFTEST
        + """

@pytest.fixture
def restored_policy():
    yield
    raise RuntimeError("restore failed")
"""
    )
    test = _scenario_test(expected_state, fixtures="expected_state, restored_policy")
    pytester.mkdir("07-example").joinpath("test_example.py").write_text(test)

    result = pytester.runpytest("-p", "recovery_libs.testing.scenario_plugin")

    result.assert_outcomes(**outcomes)
    summary = result.stdout.str().partition("scenario summary")[2]
    assert summary.count("07-example::test_example") == 1
    result.stdout.fnmatch_lines(
        [f"ERROR * 1  done (*s) * 07-example::test_example{stopped}  error at: teardown  RuntimeError: restore failed"]
    )


def test_failing_stage_fails_the_test(pytester: pytest.Pytester) -> None:
    result = _suite(pytester, "JobState.FAILED").runpytest("-p", "recovery_libs.testing.scenario_plugin")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        ["*scenario steps*", "example  job=1", "FAIL  done *", "        unmet: job is COMPLETED"]
    )


def test_failing_known_gap_stage_is_an_expected_failure(pytester: pytest.Pytester) -> None:
    suite = _suite(pytester, "JobState.FAILED", known_gap="detector misses this fault")

    result = suite.runpytest("-p", "recovery_libs.testing.scenario_plugin", "-rx")

    result.assert_outcomes(xfailed=1)
    result.stdout.fnmatch_lines(["*detector misses this fault*"])


def test_scenario_test_is_marked_with_its_folder(pytester: pytest.Pytester) -> None:
    items = _suite(pytester, "expected_state").inline_genitems("-p", "recovery_libs.testing.scenario_plugin")[0]

    marker = items[0].get_closest_marker("scenario")
    assert marker is not None and marker.args == ("07-example",)
