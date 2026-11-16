# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify SDK interpretation, instrumentation, and lifecycle through a real JobSet controller."""

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
    return WorkloadMapProbe(
        Path(os.environ["WORKLOAD_MAP_PROBE"]),
        KubectlJobSetClient(
            context=os.environ["LAUNCHER_KUBERNETES_JOBSET_CONTEXT"],
            namespace=os.environ["LAUNCHER_KUBERNETES_JOBSET_NAMESPACE"],
        ),
        tmp_path / "workload-map-observations.json",
    )


def running(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require fixture scale, Pod readiness, and native identity independently of SDK echoes."""
    phase = "resumed" if "old_pods" in run.notes else "running"
    run.services.begin_capability("scale")
    run.services.begin_capability("status")
    result = run.services.inspect(run.job, phase)
    expected = run.notes["expected_pods"]
    assert result["suspend_supported"] is True
    assert not result.get("capability_errors"), result.get("capability_errors")
    assert result["components"]["replicatedjob"]["workers"]["replicas"] == expected
    pods = run.job.metadata["pods"]
    ready = len(pods) == expected and all(
        any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))
        for pod in pods
    )
    if run.job.state != JobState.RUNNING or not ready:
        return Evidence(False, f"waiting for {expected} independently owned Pods to be running and Ready")
    if "Running" not in result["statuses"] or "Suspended" in result["statuses"]:
        return Evidence(False, "waiting for SDK Running without Suspended after Pod readiness")
    run.services.capability("scale", "pass", f"SDK scale and {expected} Ready owned Pods agree")
    if "old_pods" in run.notes:
        assert run.job.metadata["workload"]["metadata"]["uid"] == run.notes["uid"]
        assert not ({pod["metadata"]["uid"] for pod in pods} & set(run.notes["old_pods"].values()))
        marker = run.services.identity["owner"]
        assert all(pod["metadata"]["labels"].get("qualification.nvidia.com/marker") == marker for pod in pods)
        assert all(pod["spec"]["schedulerName"] == run.notes["scheduler"] for pod in pods)
        if run.notes["scheduler"] == "kai-scheduler":
            assert all(pod["metadata"]["labels"].get("kai.scheduler/queue") == run.notes["queue"] for pod in pods)
    return Evidence(
        True,
        f"{phase}: original native UID, SDK scale={expected}, {expected} Ready owned Pods; statuses={result['statuses']}",
    )


def drained(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require no scenario Pods, including new Pods whose intermediate Job has vanished."""
    result = run.services.inspect(run.job, "suspended")
    record = run.job.metadata["workload"]
    assert record["metadata"]["uid"] == run.notes["uid"]
    remaining = run.services.scoped_pods(run.job)
    observed = (
        record["spec"].get("suspend") is True
        and not run.job.metadata["pods"]
        and not remaining
        and run.services.original_pods_absent(run.notes["old_pods"])
        and "Suspended" in result["statuses"]
        and not {"Running", "Initializing"}.intersection(result["statuses"])
    )
    return Evidence(
        observed,
        f"suspend={record['spec'].get('suspend')}; remaining scenario Pods={len(remaining)}; statuses={result['statuses']}",
    )


def record_qualified_capabilities(run: Run[WorkloadMapProbe]) -> None:
    """Mark only capabilities established by all preceding lifecycle stages."""
    run.services.classify_pods(run.job)
    for capability, detail in {
        "scale": f"SDK count and {run.notes['expected_pods']} Ready owned Pods agree",
        "status": "Running and Suspended observed against Pod evidence; full co-matched sets retained",
        "instrumentation": "SDK marker reached every replacement Pod while selected scheduling was preserved",
        "suspend_resume": "original parent UID, complete drain, suspended interval, and new Ready Pod UIDs",
    }.items():
        run.services.capability(capability, "pass", detail)
    if run.notes["scheduler"] == "kai-scheduler":
        run.services.capability(
            "kai", "pass", "selected queue placed replacement Pods; preemption and gang behavior are not tested"
        )


@pytest.mark.parametrize("parallelism,expected_pods", [(1, 2), (3, 6)], ids=["two-pods", "six-pods"])
@scenario
def test_jobset_suspend_resume(parallelism: int, expected_pods: int) -> Scenario[WorkloadMapProbe]:
    """Exercise both replica-only and multiplicative scale with concurrent-safe ownership."""
    scheduler = os.environ.get("WORKLOAD_MAP_SCHEDULER", "default-scheduler")
    queue = os.environ.get("WORKLOAD_MAP_QUEUE")
    if scheduler == "kai-scheduler" and not queue:
        raise ValueError("WORKLOAD_MAP_QUEUE is required for kai-scheduler qualification")
    return Scenario(
        name=f"Workload-Map JobSet lifecycle: 2 x {parallelism}, {scheduler}",
        job=JobSpec(
            name="workload-map-lifecycle",
            workload={
                "image": "busybox:1.37.0",
                "command": ["sh", "-c", "sleep 600"],
                "replicas": 2,
                "parallelism": parallelism,
                "scheduler_name": scheduler,
                **({"queue": queue} if queue else {}),
            },
        ),
        stages=(
            act("pin workload identity and owner", lambda run: run.services.bind(run.job)),
            act("remember selected scheduler", remember("scheduler", lambda _: scheduler)),
            act("remember selected queue", remember("queue", lambda _: queue)),
            act("remember expected Pod count", remember("expected_pods", lambda _: expected_pods)),
            act(
                "remember native workload UID",
                remember("uid", lambda run: run.job.metadata["workload"]["metadata"]["uid"]),
            ),
            eventually("SDK matches running JobSet", running, within=180),
            act("classify independently owned Pods", lambda run: run.services.classify_pods(run.job)),
            act(
                "remember original Pods",
                remember(
                    "old_pods",
                    lambda run: {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in run.job.metadata["pods"]},
                ),
            ),
            act("suspend through Workload-Map", lambda run: run.services.transition(run.job, "suspend")),
            eventually("all scenario Pods drained", drained, within=120),
            consistently("workload remains suspended", drained, for_seconds=5),
            act("instrument drained templates through Workload-Map", lambda run: run.services.instrument(run.job)),
            act("resume through Workload-Map", lambda run: run.services.transition(run.job, "resume")),
            eventually("instrumented new Pods under original UID", running, within=180),
            consistently("observe resumed status and readiness", running, for_seconds=5),
            act("record qualified capabilities", record_qualified_capabilities),
        ),
    )


def native_restarts_exhausted(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require the controller to exhaust its configured restart budget before failing."""
    run.services.begin_capability("retry_exhaustion")
    result = run.services.inspect(run.job, "native_failure")
    record = run.job.metadata["workload"]
    native_failed = any(
        condition.get("type") == "Failed" and condition.get("status") == "True"
        for condition in record.get("status", {}).get("conditions", [])
    )
    if not native_failed:
        return Evidence(
            False,
            f"waiting for terminal failure after native restart; restarts={record.get('status', {}).get('restarts')}",
        )
    assert record["status"]["restarts"] == 1
    assert run.job.state == JobState.FAILED
    assert "Failed" in result["statuses"]
    assert "Completed" not in result["statuses"]
    run.services.capability(
        "retry_exhaustion", "pass", "exit 42 exhausted JobSet maxRestarts=1 with native restarts=1 and SDK Failed"
    )
    return Evidence(True, "native restart budget exhausted; SDK and native terminal Failed agree")


def refuse_terminal_recovery(run: Run[WorkloadMapProbe]) -> None:
    """Require both SDK lifecycle operations to refuse a native terminal workload."""
    run.services.begin_capability("terminal_boundary")
    before = run.job.metadata["workload"]
    for operation in ("suspend", "resume"):
        with pytest.raises(RuntimeError, match="terminal workload recovery"):
            run.services.invoke(operation, before)
    current = run.services.client.get("jobsets.jobset.x-k8s.io", before["metadata"]["name"])
    assert current["metadata"]["uid"] == before["metadata"]["uid"]
    assert current["spec"].get("suspend") == before["spec"].get("suspend")
    run.services.capability(
        "terminal_boundary", "pass", "both SDK lifecycle transformations refused the terminal JobSet"
    )


@scenario
def test_jobset_native_restart_exhaustion() -> Scenario[WorkloadMapProbe]:
    """Exercise native controller retries separately from deliberate suspend/resume."""
    scheduler = os.environ.get("WORKLOAD_MAP_SCHEDULER", "default-scheduler")
    queue = os.environ.get("WORKLOAD_MAP_QUEUE")
    if scheduler == "kai-scheduler" and not queue:
        raise ValueError("WORKLOAD_MAP_QUEUE is required for kai-scheduler qualification")
    return Scenario(
        name=f"Workload-Map JobSet native restart exhaustion: {scheduler}",
        job=JobSpec(
            name="workload-map-native-failure",
            workload={
                "image": "busybox:1.37.0",
                "command": ["sh", "-c", "exit 42"],
                "replicas": 1,
                "max_restarts": 1,
                "scheduler_name": scheduler,
                **({"queue": queue} if queue else {}),
            },
        ),
        stages=(
            act("pin workload identity and owner", lambda run: run.services.bind(run.job)),
            eventually("native restart budget is exhausted", native_restarts_exhausted, within=180),
            act("terminal recovery is refused", refuse_terminal_recovery),
        ),
    )
