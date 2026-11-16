# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify SDK interpretation and same-object suspend/resume through a real JobSet controller."""

import os
from pathlib import Path

import pytest
from e2e_harness.workload_map import WorkloadMapProbe
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient
from recovery_libs.testing.scenario import Evidence, Run, Scenario, act, consistently, eventually, remember
from recovery_libs.testing.scenario_plugin import scenario


@pytest.fixture
def scenario_services(tmp_path: Path) -> WorkloadMapProbe:
    """Require explicit cluster selection; no provisioning or ambient context fallback."""
    context = os.environ["LAUNCHER_KUBERNETES_JOBSET_CONTEXT"]
    namespace = os.environ["LAUNCHER_KUBERNETES_JOBSET_NAMESPACE"]
    return WorkloadMapProbe(
        Path(os.environ["WORKLOAD_MAP_PROBE"]),
        KubectlJobSetClient(context=context, namespace=namespace),
        tmp_path / "workload-map-observations.json",
    )


def running(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require all expected owned Pods and compare SDK scale with fixture expectations."""
    phase = "resumed" if "old_pods" in run.notes else "running"
    result = run.services.inspect(run.job, phase)
    record = run.job.metadata["workload"]
    assert result["uid"] == record["metadata"]["uid"]
    assert result["name"] == record["metadata"]["name"]
    assert result["namespace"] == record["metadata"]["namespace"]
    assert result["suspend_supported"] is True
    assert result["components"]["replicatedjob"]["workers"]["replicas"] == 2
    pods = run.job.metadata["pods"]
    ready = len(pods) == 2 and all(
        any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))
        for pod in pods
    )
    if run.job.state != JobState.RUNNING or not ready:
        return Evidence(False, "waiting for both independently owned Pods to be running and Ready")
    if "Running" not in result["statuses"]:
        return Evidence(False, "waiting for SDK Running status after Pod readiness")
    if "old_pods" in run.notes:
        assert run.job.job_id == run.notes["identity"]
        assert not ({pod["metadata"]["uid"] for pod in pods} & set(run.notes["old_pods"].values()))
    return Evidence(True, f"{phase}: same workload identity, SDK scale=2, two Ready owned Pods")


def drained(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require an empty owned Pod set, not merely a successful suspend write."""
    result = run.services.inspect(run.job, "suspended")
    record = run.job.metadata["workload"]
    observed = (
        record["spec"].get("suspend") is True
        and not run.job.metadata["pods"]
        and run.services.original_pods_absent(run.notes["old_pods"])
        and "Suspended" in result["statuses"]
    )
    if observed:
        assert run.job.job_id == run.notes["identity"]
    return Evidence(observed, f"suspend={record['spec'].get('suspend')}; owned Pods={len(run.job.metadata['pods'])}")


@pytest.mark.parametrize("iteration", range(2))
@scenario
def test_jobset_suspend_resume(iteration: int) -> Scenario[WorkloadMapProbe]:
    """Repeat and permit concurrent runs to expose ownership or fixture-state leakage."""
    return Scenario(
        name=f"Workload-Map JobSet lifecycle {iteration}",
        job=JobSpec(
            name="workload-map-lifecycle",
            workload={"image": "busybox:1.37.0", "command": ["sh", "-c", "sleep 600"], "replicas": 2},
        ),
        stages=(
            eventually("SDK matches running JobSet", running, within=180),
            act("remember workload identity", remember("identity", lambda run: run.job.job_id)),
            act(
                "remember original Pods",
                remember(
                    "old_pods",
                    lambda run: {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in run.job.metadata["pods"]},
                ),
            ),
            act("suspend through Workload-Map", lambda run: run.services.transition(run.job, "suspend")),
            eventually("all original Pods drained", drained, within=120),
            consistently("workload remains suspended", drained, for_seconds=5),
            act("resume through Workload-Map", lambda run: run.services.transition(run.job, "resume")),
            eventually("new Pods under original UID", running, within=180),
        ),
    )
