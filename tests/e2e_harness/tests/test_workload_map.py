# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prove live mutation preconditions and retained evidence independently of the SDK."""

import copy
import json
from types import SimpleNamespace

import pytest
from e2e_harness.workload_map import WorkloadMapProbe


class Client:
    """Record writes and model Pods that survive deletion of intermediate Jobs."""

    namespace = "qualification"

    def __init__(self):
        self.writes = []
        self.pod = {"metadata": {"name": "old", "uid": "old-pod", "deletionTimestamp": "now"}}
        self.pods = []
        self.selectors = []

    def command(self, *args, body):
        self.writes.append(body)

    def get(self, resource, name):
        return self.pod

    def list(self, resource, *, selector):
        self.selectors.append(selector)
        return self.pods


class Probe(WorkloadMapProbe):
    """Model SDK output so bridge tests establish independent safety requirements."""

    def invoke(self, operation, record, *, marker=None):
        if operation == "inspect":
            return {"statuses": record.get("mock_statuses", ["Running"])}
        updated = copy.deepcopy(record)
        if operation == "instrument":
            for job in updated["spec"]["replicatedJobs"]:
                job["template"]["spec"]["template"]["metadata"]["labels"]["qualification.nvidia.com/marker"] = marker
            return updated
        updated.setdefault("spec", {})["suspend"] = operation == "suspend"
        return updated


@pytest.fixture
def fixture(tmp_path):
    executable = tmp_path / "probe"
    executable.touch()
    client = Client()
    probe = Probe(executable, client, tmp_path / "report.json")
    record = {
        "metadata": {
            "namespace": "qualification",
            "name": "owned",
            "uid": "original",
            "resourceVersion": "12",
            "labels": {"recovery.nvidia.com/scenario-owner": "run"},
        },
        "spec": {
            "replicatedJobs": [
                {
                    "name": "workers",
                    "replicas": 2,
                    "template": {
                        "spec": {
                            "parallelism": 3,
                            "template": {
                                "metadata": {"labels": {"user": "preserve"}},
                                "spec": {"containers": [{"name": "worker", "image": "fixture"}]},
                            },
                        }
                    },
                }
            ]
        },
    }
    job = SimpleNamespace(
        job_id="qualification/owned/original", metadata={"workload": record, "pods": [], "expected_pods": 6}
    )
    probe.bind(job)
    return probe, client, job


def test_persist_retains_uid_and_resource_version(fixture):
    probe, client, job = fixture
    before = copy.deepcopy(job.metadata["workload"])
    probe.transition(job, "suspend")
    expected = copy.deepcopy(before)
    expected["spec"]["suspend"] = True
    assert client.writes == [expected]
    assert job.metadata["workload"] == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("uid", "replacement"),
        ("namespace", "other"),
        ("resourceVersion", ""),
        ("deletionTimestamp", "now"),
        ("labels", {}),
        ("labels", {"recovery.nvidia.com/scenario-owner": "different-owner"}),
    ],
)
def test_reject_changed_or_unowned_snapshot(fixture, field, value):
    probe, client, job = fixture
    job.metadata["workload"]["metadata"][field] = value
    with pytest.raises(ValueError):
        probe.transition(job, "suspend")
    assert client.writes == []


def test_identity_change_is_reported_before_sdk_invocation(fixture, monkeypatch):
    probe, _, job = fixture
    monkeypatch.setattr(probe, "invoke", lambda *_: pytest.fail("must reject replacement first"))
    job.metadata["workload"]["metadata"]["uid"] = "replacement"
    with pytest.raises(ValueError):
        probe.inspect(job, "resumed")
    assert probe.identity["uid"] == "original"


def test_reject_unexpected_sdk_change(fixture, monkeypatch):
    probe, client, job = fixture
    monkeypatch.setattr(probe, "invoke", lambda *_: {"spec": {"suspend": True}})
    with pytest.raises(AssertionError):
        probe.transition(job, "suspend")
    assert client.writes == []


def test_conflicting_write_is_not_retried_or_hidden(fixture, monkeypatch):
    probe, client, job = fixture
    calls = []

    def conflict(*args, **kwargs):
        calls.append(kwargs["body"])
        raise RuntimeError("Conflict")

    monkeypatch.setattr(client, "command", conflict)
    with pytest.raises(RuntimeError):
        probe.transition(job, "suspend")
    assert len(calls) == 1
    assert calls[0]["metadata"]["resourceVersion"] == "12"


def test_drain_waits_for_terminating_pod_even_without_job_owner(fixture):
    probe, client, _ = fixture
    assert not probe.original_pods_absent({"old": "old-pod"})
    client.pod = None
    assert probe.original_pods_absent({"old": "old-pod"})


def test_drain_query_includes_new_orphan_pods_and_is_scoped(fixture):
    probe, client, job = fixture
    orphan = {"metadata": {"name": "new-orphan", "uid": "new", "deletionTimestamp": "now"}}
    client.pods = [orphan]
    assert probe.scoped_pods(job) == [orphan]
    assert client.selectors == ["recovery.nvidia.com/scenario-owner=run,jobset.sigs.k8s.io/jobset-name=owned"]


def test_observations_retain_status_transitions_and_readiness(fixture, monkeypatch):
    probe, _, job = fixture
    probe.started = 0
    ticks = iter([10.0, 14.0, 17.0])
    monkeypatch.setattr("e2e_harness.workload_map.time.monotonic", lambda: next(ticks))
    job.metadata["expected_pods"] = 1
    job.metadata["pods"] = [
        {"metadata": {"name": "pod", "uid": "pod-uid"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
    ]
    job.metadata["workload"]["mock_statuses"] = ["Initializing", "Running"]
    probe.inspect(job, "resumed")
    probe.inspect(job, "resumed")
    job.metadata["workload"]["mock_statuses"][:] = ["Running"]
    probe.inspect(job, "resumed")
    report = json.loads(probe.report.read_text())
    assert len(report["observations"]) == 3
    assert report["observations"][0]["workload_map"]["statuses"] == ["Initializing", "Running"]
    assert report["observations"][2]["workload_map"]["statuses"] == ["Running"]
    assert all(
        row["observed_at"] and row["all_pods_ready"] and row["workload_uid"] == "original"
        for row in report["observations"]
    )
    assert report["status_timing"]["resumed"] == {
        "ready_first_observed_seconds": 10.0,
        "initializing_last_observed_seconds_after_ready": 4.0,
        "initializing_first_absent_seconds_after_ready": 7.0,
    }


def test_instrumentation_requires_drained_suspension(fixture):
    probe, client, job = fixture
    with pytest.raises(ValueError):
        probe.instrument(job)
    job.metadata["workload"]["spec"]["suspend"] = True
    client.pods = [client.pod]
    with pytest.raises(ValueError):
        probe.instrument(job)
    assert client.writes == []


def test_instrumentation_persists_marked_sdk_templates(fixture):
    probe, client, job = fixture
    job.metadata["workload"]["spec"]["suspend"] = True
    probe.instrument(job)
    submitted = client.writes[0]
    assert submitted["metadata"] == job.metadata["workload"]["metadata"]
    assert submitted["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["metadata"]["labels"] == {
        "user": "preserve",
        "qualification.nvidia.com/marker": "run",
    }
    assert probe.capabilities["instrumentation"]["outcome"] == "not_tested"


def test_instrumentation_rejects_changes_outside_templates(fixture, monkeypatch):
    probe, client, job = fixture
    job.metadata["workload"]["spec"]["suspend"] = True
    invoke = probe.invoke

    def change_scale(*args, **kwargs):
        result = invoke(*args, **kwargs)
        result["spec"]["replicatedJobs"][0]["replicas"] = 9
        return result

    monkeypatch.setattr(probe, "invoke", change_scale)
    with pytest.raises(AssertionError):
        probe.instrument(job)
    assert client.writes == []


def test_failed_scenario_does_not_claim_unexercised_capabilities(fixture):
    probe, _, _ = fixture
    probe.capability("scale", "pass", "six Pods")
    probe.finish("fail")
    report = json.loads(probe.report.read_text())
    assert report["scenario_outcome"] == "fail"
    assert report["capabilities"]["scale"]["outcome"] == "pass"
    assert report["capabilities"]["suspend_resume"]["outcome"] == "not_tested"
    assert report["capabilities"]["kai"]["outcome"] == "not_tested"


def test_failed_attempt_is_distinct_from_capabilities_never_attempted(fixture):
    probe, _, _ = fixture
    probe.begin_capability("instrumentation")
    probe.finish("fail")
    report = json.loads(probe.report.read_text())
    assert report["capabilities"]["instrumentation"]["outcome"] == "fail"
    assert report["capabilities"]["kai"]["outcome"] == "not_tested"


def test_observations_retain_native_restart_budget_and_container_restarts(fixture):
    probe, _, job = fixture
    job.metadata["workload"]["spec"]["failurePolicy"] = {"maxRestarts": 1}
    job.metadata["workload"]["spec"]["replicatedJobs"][0]["template"]["spec"]["backoffLimit"] = 0
    job.metadata["pods"] = [
        {
            "metadata": {"name": "worker", "uid": "pod"},
            "status": {
                "containerStatuses": [
                    {"name": "worker", "restartCount": 2, "lastState": {"terminated": {"exitCode": 42}}}
                ],
            },
        }
    ]
    probe.inspect(job, "native_failure")
    row = probe.observations[0]
    assert row["native_settings"]["failure_policy"] == {"maxRestarts": 1}
    assert row["native_settings"]["replicated_jobs"][0]["backoff_limit"] == 0
    assert row["pods"][0]["containers"][0]["restart_count"] == 2
    assert row["pods"][0]["containers"][0]["last_state"]["terminated"]["exitCode"] == 42


def test_pytorch_transition_changes_only_run_policy(fixture, monkeypatch):
    probe, client, job = fixture
    record = job.metadata["workload"]
    record["apiVersion"] = "kubeflow.org/v1"
    record["kind"] = "PyTorchJob"
    record["spec"] = {"runPolicy": {"cleanPodPolicy": "None"}, "pytorchReplicaSpecs": {}}
    transformed = copy.deepcopy(record)
    transformed["spec"]["runPolicy"]["suspend"] = True
    monkeypatch.setattr(probe, "invoke", lambda *_: transformed)
    probe.transition(job, "suspend")
    assert client.writes == [transformed]
    assert "suspend" not in record["spec"]["runPolicy"]
    assert "suspend" not in client.writes[0]["spec"]


def test_pytorch_instrumentation_validates_every_role_and_scopes_drain(fixture, monkeypatch):
    probe, client, job = fixture
    record = job.metadata["workload"]
    record["apiVersion"] = "kubeflow.org/v1"
    record["kind"] = "PyTorchJob"
    record["spec"] = {
        "runPolicy": {"suspend": True},
        "pytorchReplicaSpecs": {
            role: {
                "replicas": count,
                "template": {
                    "metadata": {"labels": {}},
                    "spec": {"containers": [{"name": "pytorch", "image": "fixture"}]},
                },
            }
            for role, count in (("Master", 1), ("Worker", 2))
        },
    }
    transformed = copy.deepcopy(record)
    for replica in transformed["spec"]["pytorchReplicaSpecs"].values():
        replica["template"]["metadata"]["labels"]["qualification.nvidia.com/marker"] = "run"
    monkeypatch.setattr(probe, "invoke", lambda *_, **__: transformed)
    probe.instrument(job)
    assert client.writes == [transformed]
    assert client.selectors == ["recovery.nvidia.com/scenario-owner=run,training.kubeflow.org/job-name=owned"]
    client.writes.clear()
    del transformed["spec"]["pytorchReplicaSpecs"]["Worker"]["template"]["metadata"]["labels"][
        "qualification.nvidia.com/marker"
    ]
    with pytest.raises(AssertionError):
        probe.instrument(job)
    assert client.writes == []


def test_pytorch_template_validation_is_independent_of_json_key_order(fixture, monkeypatch):
    probe, client, job = fixture
    record = job.metadata["workload"]
    record["kind"] = "PyTorchJob"
    record["spec"] = {
        "runPolicy": {"suspend": True},
        "pytorchReplicaSpecs": {
            role: {
                "replicas": count,
                "template": {
                    "metadata": {"labels": {"role": role}},
                    "spec": {"containers": [{"name": "pytorch", "image": role}]},
                },
            }
            for role, count in (("Worker", 2), ("Master", 1))
        },
    }
    # Go serializes map keys in sorted order, unlike an arbitrary input document.
    transformed = json.loads(json.dumps(record, sort_keys=True))
    for replica in transformed["spec"]["pytorchReplicaSpecs"].values():
        replica["template"]["metadata"]["labels"]["qualification.nvidia.com/marker"] = "run"
    monkeypatch.setattr(probe, "invoke", lambda *_, **__: transformed)
    probe.instrument(job)
    assert client.writes == [transformed]
    assert (
        client.writes[0]["spec"]["pytorchReplicaSpecs"]["Worker"]["template"]["spec"]["containers"][0]["image"]
        == "Worker"
    )


@pytest.mark.parametrize("restart_count", [0, 1])
def test_pytorch_retry_check_uses_history_without_mutating_real_run(fixture, restart_count):
    """Terminal status alone cannot pass retries; immutable checks retain earlier Pod evidence."""
    from dataclasses import replace
    from datetime import UTC, datetime
    from types import MappingProxyType

    from recovery_libs.testing.launch.jobs import Job, JobState
    from recovery_libs.testing.scenario import Run
    from workload_map_live.test_pytorch import native_retry_exhausted

    probe, client, fixture_job = fixture
    record = fixture_job.metadata["workload"]
    record["kind"] = "PyTorchJob"
    record["spec"] = {
        "runPolicy": {"backoffLimit": 1},
        "pytorchReplicaSpecs": {"Worker": {"replicas": 1, "restartPolicy": "OnFailure"}},
    }
    record["mock_statuses"] = ["Running"]
    job = Job(
        job_id=fixture_job.job_id,
        name="owned",
        state=JobState.RUNNING,
        nodes=(),
        exit_code=None,
        restart_count=None,
        submitted_at=datetime.now(UTC),
        metadata={
            "workload": record,
            "expected_pods": 1,
            "pods": [
                {
                    "metadata": {"name": "worker", "uid": "original-pod"},
                    "status": {"containerStatuses": [{"name": "pytorch", "restartCount": restart_count}]},
                }
            ],
        },
    )
    run = Run(jobs={"job": job}, launcher=client, services=probe, notes={"sentinel": "unchanged"})
    assert isinstance(run.notes, MappingProxyType)
    assert not native_retry_exhausted(run).observed
    terminal_record = copy.deepcopy(record)
    terminal_record["status"] = {
        "conditions": [{"type": "Failed", "status": "True", "message": "Job reached specified backoff limit"}]
    }
    terminal_record["mock_statuses"] = ["Failed"]
    # The final poll has no Pods. Passing requires the earlier independently
    # observed restart count, not a writable scratch dictionary on Run.
    terminal_job = replace(
        job, state=JobState.FAILED, metadata={"workload": terminal_record, "expected_pods": 1, "pods": []}
    )
    terminal_run = replace(run, jobs={"job": terminal_job})
    if restart_count:
        assert native_retry_exhausted(terminal_run).observed
        assert probe.capabilities["retry_exhaustion"]["outcome"] == "pass"
    else:
        with pytest.raises(AssertionError):
            native_retry_exhausted(terminal_run)
    assert dict(run.notes) == dict(terminal_run.notes) == {"sentinel": "unchanged"}


@pytest.mark.parametrize(
    "reporter,reason,uid",
    [
        ("default-scheduler", "Scheduled", "pod"),
        ("kai-scheduler", "FailedScheduling", "pod"),
        ("kai-scheduler", "Scheduled", "other-incarnation"),
    ],
)
def test_kai_requires_successful_event_for_exact_pod(fixture, monkeypatch, reporter, reason, uid):
    """Propagated labels, other schedulers, and same-name replacement events cannot pass."""
    probe, client, job = fixture
    job.metadata["pods"] = [{"metadata": {"uid": "pod", "labels": {"kai.scheduler/queue": "queue"}}}]
    event = {"involvedObject": {"uid": uid}, "reason": reason, "reportingComponent": reporter}
    monkeypatch.setattr(client, "events", lambda _: [event], raising=False)
    assert not probe.kai_scheduled(job)
    assert probe.capabilities["kai"]["outcome"] == "not_tested"


def test_kai_retains_evidence_and_requires_every_replacement(fixture, monkeypatch):
    """Each new Pod UID needs its own scheduling evidence; event expiry retains earlier proof."""
    probe, client, job = fixture
    job.metadata["pods"] = [{"metadata": {"uid": uid}} for uid in ("one", "two")]
    events = {
        "one": [{"involvedObject": {"uid": "one"}, "reason": "Scheduled", "source": {"component": "kai-scheduler"}}]
    }
    monkeypatch.setattr(client, "events", lambda uid: events.get(uid, []), raising=False)
    assert not probe.kai_scheduled(job)
    events["two"] = [{"involvedObject": {"uid": "two"}, "reason": "Scheduled", "reportingComponent": "kai-scheduler"}]
    assert probe.kai_scheduled(job)
    events.clear()
    assert probe.kai_scheduled(job)
    job.metadata["pods"] = [{"metadata": {"uid": "replacement"}}]
    assert not probe.kai_scheduled(job)
    assert set(json.loads(probe.report.read_text())["scheduling_events"]) == {"one", "two"}
