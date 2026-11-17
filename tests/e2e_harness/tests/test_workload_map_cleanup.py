# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The real live-suite fixture wiring must clean terminal workloads and retain diagnostics."""

import json
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]


def test_live_fixture_cleans_terminal_workload_and_retains_diagnostics(pytester: pytest.Pytester, monkeypatch):
    """A terminal scenario skips runner cancellation but must still execute owned fixture teardown."""
    tests = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", str(tests))
    # Execute the actual suite configuration as a local conftest: loading it only
    # as a plugin would change precisely the fixture precedence this test covers.
    configuration = (tests / "workload_map_live" / "conftest.py").read_text()
    pytester.makeconftest(
        configuration
        + """
from datetime import UTC, datetime
import json
from pathlib import Path

import e2e_kubernetes.conftest as foundation
import recovery_libs.testing.launch.pytest_plugin as generic
from recovery_libs.testing.launch.jobs import Job, JobState


class FinishedLauncher:
    def __init__(self, client=None):
        pass

    def submit(self, spec):
        return self.get(job_id="qualification/owned/original-uid")

    def get(self, *, job_id):
        return Job(job_id, "owned", JobState.FAILED, (), 42, None, datetime(2026, 1, 1, tzinfo=UTC))

    def diagnostics(self, *, job_id):
        return {"job_id": job_id, "native_failure": "retry budget exhausted"}

    def cancel(self, *, job_id):
        Path("cancelled.json").write_text(json.dumps({"job_id": job_id}))


# Both possible fixture choices use the same cluster-free transport. The test
# detects missing teardown, rather than failing by accidentally contacting Kubernetes.
KubernetesJobSetLauncher = FinishedLauncher

@pytest.fixture
def scenario_services():
    return None

foundation.launcher_named = lambda _: FinishedLauncher()
generic.launcher_named = lambda _: FinishedLauncher()
"""
    )
    pytester.makepyfile(
        """
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Evidence, Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario


@scenario
def test_terminal():
    return Scenario(
        name="native failure", job=JobSpec(name="owned", workload={}),
        stages=(eventually("terminal observed", lambda run: Evidence(run.job.state == JobState.FAILED, "native failed"), within=0),),
    )
"""
    )
    result = pytester.runpytest_subprocess(
        "--launcher=kubernetes-jobset", f"--basetemp={pytester.path / 'artifacts'}", "-q"
    )
    result.assert_outcomes(passed=1)
    identity = "qualification/owned/original-uid"
    assert json.loads((pytester.path / "cancelled.json").read_text()) == {"job_id": identity}
    diagnostics = list((pytester.path / "artifacts").rglob("qualification_owned_original-uid.json"))
    assert len(diagnostics) == 1
    assert json.loads(diagnostics[0].read_text()) == {
        "job_id": identity,
        "native_failure": "retry budget exhausted",
    }
