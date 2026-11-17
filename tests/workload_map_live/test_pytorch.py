# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify PyTorchJob roles and lifecycle against a real Training Operator."""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from e2e_harness.workload_map import WorkloadMapProbe
from e2e_kubernetes.conftest import DiagnosticLauncher
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.launch.pytorch import KubectlPyTorchClient, KubernetesPyTorchLauncher
from recovery_libs.testing.scenario import Evidence, Run, Scenario, act, consistently, eventually, remember
from recovery_libs.testing.scenario_plugin import scenario


@pytest.fixture
def launcher(pytestconfig: pytest.Config, tmp_path: Path) -> Iterator[DiagnosticLauncher]:
    """Reuse exact-object diagnostics/cleanup with the PyTorch native launcher."""
    if pytestconfig.getoption("launcher") != "kubernetes-pytorch":
        raise pytest.UsageError("PyTorch qualification requires --launcher=kubernetes-pytorch")
    tracked = DiagnosticLauncher(KubernetesPyTorchLauncher.from_environment(), tmp_path)
    yield tracked
    errors = []
    for job_id in tracked.owned:
        try:
            tracked.cancel(job_id=job_id)
        except Exception as error:  # noqa: BLE001 - attempt cleanup for every submitted workload
            errors.append(error)
    if errors:
        raise ExceptionGroup("owned PyTorch workload cleanup failed", errors)


@pytest.fixture
def scenario_services(tmp_path: Path) -> WorkloadMapProbe:
    """Use the suite's explicit cluster selection and the same probe/evidence contract."""
    return WorkloadMapProbe(
        Path(os.environ["WORKLOAD_MAP_PROBE"]),
        KubectlPyTorchClient(
            context=os.environ.get("LAUNCHER_KUBERNETES_PYTORCH_CONTEXT")
            or os.environ["LAUNCHER_KUBERNETES_JOBSET_CONTEXT"],
            namespace=os.environ.get("LAUNCHER_KUBERNETES_PYTORCH_NAMESPACE")
            or os.environ["LAUNCHER_KUBERNETES_JOBSET_NAMESPACE"],
        ),
        tmp_path / "workload-map-observations.json",
    )


def _workload(command: list[str]) -> dict:
    scheduler = os.environ.get("WORKLOAD_MAP_SCHEDULER", "default-scheduler")
    queue = os.environ.get("WORKLOAD_MAP_QUEUE")
    if scheduler == "kai-scheduler" and not queue:
        raise ValueError("WORKLOAD_MAP_QUEUE is required for kai-scheduler qualification")
    return {
        "image": "busybox:1.37.0",
        "command": command,
        "workers": 2,
        "scheduler_name": scheduler,
        **({"queue": queue} if queue else {}),
    }


def running(run: Run[WorkloadMapProbe]) -> Evidence:
    """Compare independent role counts and real Pod readiness with SDK interpretation."""
    run.services.begin_capability("scale")
    run.services.begin_capability("status")
    phase = "resumed" if "old_pods" in run.notes else "running"
    result = run.services.inspect(run.job, phase)
    assert not result.get("capability_errors"), result.get("capability_errors")
    assert result["components"]["master"][""]["replicas"] == 1
    assert result["components"]["worker"][""]["replicas"] == 2
    pods = run.job.metadata["pods"]
    ready = len(pods) == 3 and all(
        any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
        for pod in pods
    )
    if run.job.state != JobState.RUNNING or not ready:
        return Evidence(False, "waiting for one Master and two Worker Pods to become Ready")
    roles = [pod["metadata"]["labels"]["training.kubeflow.org/replica-type"] for pod in pods]
    assert sorted(roles) == ["master", "worker", "worker"]
    if "Running" not in result["statuses"] or "Suspended" in result["statuses"]:
        return Evidence(False, f"waiting for SDK Running without Suspended; statuses={result['statuses']}")
    assert run.job.metadata["workload"]["metadata"]["uid"] == run.notes["uid"]
    scheduler = os.environ.get("WORKLOAD_MAP_SCHEDULER", "default-scheduler")
    assert all(pod["spec"]["schedulerName"] == scheduler for pod in pods)
    if scheduler == "kai-scheduler":
        assert all(
            pod["metadata"]["labels"].get("kai.scheduler/queue") == os.environ["WORKLOAD_MAP_QUEUE"] for pod in pods
        )
    if "old_pods" in run.notes:
        assert not {pod["metadata"]["uid"] for pod in pods}.intersection(run.notes["old_pods"].values())
        assert all(
            pod["metadata"]["labels"].get("qualification.nvidia.com/marker") == run.services.identity["owner"]
            for pod in pods
        )
    if scheduler == "kai-scheduler" and not run.services.kai_scheduled(run.job):
        return Evidence(False, "waiting for KAI Scheduled events for each owned Pod UID")
    return Evidence(True, f"{phase}: native role counts and three Ready Pods agree; statuses={result['statuses']}")


def drained(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require the original parent, SDK suspension and no remaining scenario Pods."""
    result = run.services.inspect(run.job, "suspended")
    record = run.job.metadata["workload"]
    assert record["metadata"]["uid"] == run.notes["uid"]
    remaining = run.services.scoped_pods(run.job)
    observed = (
        record["spec"]["runPolicy"].get("suspend") is True
        and not remaining
        and not run.job.metadata["pods"]
        and run.services.original_pods_absent(run.notes["old_pods"])
        and "Suspended" in result["statuses"]
    )
    return Evidence(observed, f"remaining scenario Pods={len(remaining)}; statuses={result['statuses']}")


def qualified(run: Run[WorkloadMapProbe]) -> None:
    """Mark only observed capabilities; retry exhaustion remains separate."""
    for capability, detail in {
        "pytorchjob_live": "one Master and two Worker roles observed through the selected Training Operator",
        "scale": "SDK role scales agree with independently counted Ready Pods",
        "status": "full normalized status sets retained throughout suspension and readiness",
        "instrumentation": "SDK marker reached all replacement Pods with selected scheduler preserved",
        "suspend_resume": "same PyTorchJob UID, complete drain, suspended interval, and replacement Pod UIDs",
    }.items():
        run.services.capability(capability, "pass", detail)
    if os.environ.get("WORKLOAD_MAP_SCHEDULER") == "kai-scheduler":
        run.services.capability(
            "kai", "pass", "KAI Scheduled events observed for original and replacement Pods; queue label preserved"
        )


@scenario
def test_pytorch_suspend_resume() -> Scenario[WorkloadMapProbe]:
    """Exercise the SDK through a native PyTorchJob controller lifecycle."""
    return Scenario(
        name="Workload-Map PyTorchJob lifecycle",
        job=JobSpec(name="workload-map-pytorch", workload=_workload(["sh", "-c", "sleep 600"])),
        stages=(
            act("pin native identity", lambda run: run.services.bind(run.job)),
            act("remember native UID", remember("uid", lambda run: run.job.metadata["workload"]["metadata"]["uid"])),
            eventually("SDK matches running PyTorchJob", running, within=180),
            act(
                "remember original Pods",
                remember(
                    "old_pods",
                    lambda run: {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in run.job.metadata["pods"]},
                ),
            ),
            act("suspend through Workload-Map", lambda run: run.services.transition(run.job, "suspend")),
            eventually("all PyTorch Pods drained", drained, within=120),
            consistently("PyTorchJob remains suspended", drained, for_seconds=5),
            act("instrument drained role templates", lambda run: run.services.instrument(run.job)),
            act("resume through Workload-Map", lambda run: run.services.transition(run.job, "resume")),
            eventually("instrumented replacement roles under original UID", running, within=180),
            consistently("observe resumed statuses", running, for_seconds=5),
            act("record observed capabilities", qualified),
        ),
    )


def terminal(run: Run[WorkloadMapProbe]) -> Evidence:
    """Observe native failure before testing that recovery transformations are refused."""
    result = run.services.inspect(run.job, "terminal")
    native = run.job.metadata["workload"].get("status", {}).get("conditions", [])
    failed = any(condition.get("type") == "Failed" and condition.get("status") == "True" for condition in native)
    return Evidence(
        failed and "Failed" in result["statuses"], f"native failed={failed}; SDK states={result['statuses']}"
    )


def refuses_terminal_mutation(run: Run[WorkloadMapProbe]) -> None:
    """The probe safety policy must refuse both operations on a terminal workload."""
    run.services.begin_capability("terminal_boundary")
    record = run.job.metadata["workload"]
    assert record["spec"]["runPolicy"]["backoffLimit"] == 0
    assert all(role["restartPolicy"] == "Never" for role in record["spec"]["pytorchReplicaSpecs"].values())
    _refuse_terminal_probe(run)


def _refuse_terminal_probe(run: Run[WorkloadMapProbe]) -> None:
    run.services.begin_capability("terminal_boundary")
    for operation in ("suspend", "resume"):
        # Require the terminal refusal reason; an unrelated subprocess failure is not evidence.
        with pytest.raises(RuntimeError, match="terminal workload"):
            run.services.invoke(operation, run.job.metadata["workload"])
    run.services.capability(
        "terminal_boundary", "pass", "probe safety policy refused suspend/resume on the native terminal PyTorchJob"
    )


@scenario
def test_pytorch_terminal_boundary() -> Scenario[WorkloadMapProbe]:
    """Use native Never/backoffLimit=0 policy to produce an actual terminal workload."""
    return Scenario(
        name="Workload-Map PyTorchJob terminal boundary",
        job=JobSpec(name="workload-map-pytorch-failed", workload=_workload(["sh", "-c", "exit 7"])),
        stages=(
            act("pin native identity", lambda run: run.services.bind(run.job)),
            eventually("native controller marks workload Failed", terminal, within=180),
            act("reject terminal lifecycle operations", refuses_terminal_mutation),
        ),
    )


def native_retry_exhausted(run: Run[WorkloadMapProbe]) -> Evidence:
    """Require actual container restarts and a controller backoff-limit terminal failure."""
    run.services.begin_capability("retry_exhaustion")
    result = run.services.inspect(run.job, "native_retry")
    record = run.job.metadata["workload"]
    assert record["spec"]["runPolicy"]["backoffLimit"] == 1
    assert all(role["restartPolicy"] == "OnFailure" for role in record["spec"]["pytorchReplicaSpecs"].values())
    # Checks receive an immutable Run. Derive the high-water mark from retained
    # evidence, including Pods the controller may already have removed.
    observed_restarts = max(
        (
            sum(container["restart_count"] for pod in row["pods"] for container in pod["containers"])
            for row in run.services.observations
            if row["phase"] == "native_retry"
        ),
        default=0,
    )
    failed = [
        condition
        for condition in record.get("status", {}).get("conditions", [])
        if condition.get("type") == "Failed" and condition.get("status") == "True"
    ]
    if not failed:
        return Evidence(False, f"waiting for native backoff failure; observed container restarts={observed_restarts}")
    assert observed_restarts >= 1
    # The pinned controller uses one Failed reason for deadline and backoff failures;
    # its native message distinguishes the cause, rather than inferring it from status alone.
    assert any("backoff limit" in condition.get("message", "").lower() for condition in failed)
    assert "Failed" in result["statuses"]
    assert "Completed" not in result["statuses"]
    assert run.job.state == JobState.FAILED
    run.services.capability(
        "retry_exhaustion",
        "pass",
        f"OnFailure/backoffLimit=1 exhausted after {observed_restarts} observed container restarts; native and SDK Failed agree",
    )
    return Evidence(True, f"native backoff limit reached after {observed_restarts} observed container restarts")


@scenario
def test_pytorch_native_retry_exhaustion() -> Scenario[WorkloadMapProbe]:
    """Exercise native in-Pod retries separately from Never-policy terminal rejection."""
    return Scenario(
        name="Workload-Map PyTorchJob native retry exhaustion",
        job=JobSpec(
            name="workload-map-pytorch-retries",
            workload={
                **_workload(["sh", "-c", "sleep 2; exit 42"]),
                "restart_policy": "OnFailure",
                "backoff_limit": 1,
            },
        ),
        stages=(
            act("pin native identity", lambda run: run.services.bind(run.job)),
            eventually("native retry budget is exhausted", native_retry_exhausted, within=180),
            act("reject terminal retry recovery", _refuse_terminal_probe),
        ),
    )
