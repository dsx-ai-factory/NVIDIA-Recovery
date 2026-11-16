# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""PyTorch manifests, direct Pod ownership, and cleanup preserve native identity."""

from copy import deepcopy

import pytest
from recovery_libs.testing.launch.jobs import JobNotFound, JobSpec, JobState
from recovery_libs.testing.launch.pytorch import KubectlPyTorchClient, KubernetesPyTorchLauncher


class Client:
    """Model namespaced native objects with UID-preconditioned deletion."""

    namespace = "test"

    def __init__(self):
        self.records = {}
        self.pods = []
        self.deleted = []

    def create(self, manifest):
        record = deepcopy(manifest)
        record["metadata"].update(uid=f"uid-{len(self.records)}", creationTimestamp="2026-11-16T00:00:00Z")
        self.records[record["metadata"]["name"]] = record
        return record

    def get(self, resource, name):
        assert resource == "pytorchjobs.kubeflow.org"
        return self.records.get(name)

    def list(self, resource, *, selector):
        assert resource == "pods"
        return self.pods

    def delete(self, name, uid):
        assert self.records[name]["metadata"]["uid"] == uid
        self.deleted.append((name, uid))
        del self.records[name]


def spec(**kwargs):
    return JobSpec(name="roles", workload={"image": "busybox:1.37.0", "command": ["sleep", "600"], **kwargs})


def pod(uid, owner, phase="Running"):
    return {
        "metadata": {"uid": uid, "ownerReferences": [{"uid": owner, "controller": True}]},
        "spec": {"nodeName": "worker"},
        "status": {"phase": phase},
    }


def test_native_roles_and_retry_policy_are_explicit():
    client = Client()
    launcher = KubernetesPyTorchLauncher(client)
    job = launcher.submit(spec(workers=2, scheduler_name="kai-scheduler", queue="selected"))
    record = client.records[job.name]
    assert record["apiVersion"] == "kubeflow.org/v1" and record["kind"] == "PyTorchJob"
    assert record["spec"]["runPolicy"] == {"cleanPodPolicy": "None", "backoffLimit": 0}
    roles = record["spec"]["pytorchReplicaSpecs"]
    assert {role: value["replicas"] for role, value in roles.items()} == {"Master": 1, "Worker": 2}
    for value in roles.values():
        assert value["restartPolicy"] == "Never"
        assert value["template"]["spec"]["schedulerName"] == "kai-scheduler"
        assert value["template"]["metadata"]["labels"]["kai.scheduler/queue"] == "selected"
        assert value["template"]["spec"]["containers"][0]["name"] == "pytorch"
    assert job.metadata["expected_pods"] == 3
    assert job.restart_count is None


def test_running_requires_all_directly_owned_pods():
    client = Client()
    launcher = KubernetesPyTorchLauncher(client)
    job = launcher.submit(spec())
    uid = client.records[job.name]["metadata"]["uid"]
    client.pods = [pod("master", uid), pod("worker-0", uid), pod("foreign", "other")]
    assert launcher.get(job_id=job.job_id).state == JobState.PENDING
    client.pods.append(pod("worker-1", uid, "Pending"))
    assert launcher.get(job_id=job.job_id).state == JobState.PENDING
    client.pods[-1]["status"]["phase"] = "Running"
    observed = launcher.get(job_id=job.job_id)
    assert observed.state == JobState.RUNNING
    assert {p["metadata"]["uid"] for p in observed.metadata["pods"]} == {"master", "worker-0", "worker-1"}
    client.records[job.name]["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
    assert launcher.get(job_id=job.job_id).state == JobState.FAILED


def test_replacement_and_foreign_handles_are_not_deleted():
    client = Client()
    first, second = KubernetesPyTorchLauncher(client), KubernetesPyTorchLauncher(client)
    one, two = first.submit(spec()), second.submit(spec())
    with pytest.raises(ValueError):
        first.cancel(job_id=two.job_id)
    client.records[one.name]["metadata"]["uid"] = "replacement"
    with pytest.raises(JobNotFound):
        first.get(job_id=one.job_id)
    first.cancel(job_id=one.job_id)
    assert client.deleted == []
    second.cancel(job_id=two.job_id)
    assert client.deleted == [(two.name, two.job_id.split("/")[-1])]
    assert second.history(job_id=two.job_id).state == JobState.CANCELLED


def test_create_response_loss_cleans_owned_resource():
    client = Client()
    create = client.create

    def timeout(manifest):
        create(manifest)
        raise TimeoutError("response lost")

    client.create = timeout
    with pytest.raises(TimeoutError):
        KubernetesPyTorchLauncher(client).submit(spec())
    assert client.records == {}
    assert len(client.deleted) == 1


def test_delete_uses_pytorch_api_and_server_uid_precondition(monkeypatch):
    requests = []
    client = KubectlPyTorchClient(context="explicit", namespace="test")
    monkeypatch.setattr(client, "command", lambda *args, body: requests.append((args, body)))
    client.delete("training", "original-uid")
    args, body = requests[0]
    assert args[:3] == ("delete", "--raw", "/apis/kubeflow.org/v1/namespaces/test/pytorchjobs/training")
    assert body["preconditions"] == {"uid": "original-uid"}
    assert body["propagationPolicy"] == "Foreground"


@pytest.mark.parametrize(
    "kwargs", [{"workers": 0}, {"workers": True}, {"scheduler_name": "kai-scheduler"}, {"queue": "bad/value"}]
)
def test_invalid_workload_is_rejected_before_creation(kwargs):
    client = Client()
    with pytest.raises(ValueError):
        KubernetesPyTorchLauncher(client).submit(spec(**kwargs))
    assert client.records == {}


@pytest.mark.parametrize(
    "field,value", [("namespace", "foreign"), ("labels", {"recovery.nvidia.com/scenario-owner": "foreign-owner"})]
)
def test_changed_ownership_is_never_observed_or_deleted(field, value):
    """A matching UID does not authorize an object whose owner token or namespace changed."""
    client = Client()
    launcher = KubernetesPyTorchLauncher(client)
    job = launcher.submit(spec())
    client.records[job.name]["metadata"][field] = value
    with pytest.raises(ValueError):
        launcher.get(job_id=job.job_id)
    with pytest.raises(ValueError):
        launcher.cancel(job_id=job.job_id)
    assert client.deleted == []
    assert job.name in client.records


def test_native_onfailure_restarts_and_backoff_budget_are_preserved():
    """Native retry qualification needs both replica restart policy and controller budget."""
    client = Client()
    job = KubernetesPyTorchLauncher(client).submit(spec(restart_policy="OnFailure", backoff_limit=1))
    record = client.records[job.name]
    assert record["spec"]["runPolicy"] == {"cleanPodPolicy": "None", "backoffLimit": 1}
    assert all(role["restartPolicy"] == "OnFailure" for role in record["spec"]["pytorchReplicaSpecs"].values())


@pytest.mark.parametrize(
    "kwargs", [{"restart_policy": "Always"}, {"backoff_limit": -1}, {"backoff_limit": True}, {"backoff_limit": "1"}]
)
def test_invalid_native_retry_policy_is_rejected_before_creation(kwargs):
    """Malformed retry settings cannot silently use the non-retrying fixture defaults."""
    client = Client()
    with pytest.raises(ValueError):
        KubernetesPyTorchLauncher(client).submit(spec(**kwargs))
    assert client.records == {}
