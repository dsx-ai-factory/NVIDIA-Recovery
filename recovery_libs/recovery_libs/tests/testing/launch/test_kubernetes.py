# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""JobSet readiness and cleanup must respect object identity across concurrent runs."""

from copy import deepcopy

import pytest
from recovery_libs.testing.launch.jobs import JobNotFound, JobSpec, JobState
from recovery_libs.testing.launch.kubernetes import KubernetesJobSetLauncher


class Client:
    namespace = "test"

    def __init__(self):
        self.records = {}
        self.jobs = []
        self.pods = []
        self.deleted = []

    def create(self, manifest):
        record = deepcopy(manifest)
        record["metadata"].update(uid=f"uid-{len(self.records)}", creationTimestamp="2026-11-02T00:00:00Z")
        self.records[record["metadata"]["name"]] = record
        return record

    def get(self, resource, name):
        return self.records.get(name)

    def list(self, resource, *, selector):
        return self.jobs if resource == "jobs" else self.pods

    def delete(self, name, uid):
        assert self.records[name]["metadata"]["uid"] == uid
        self.deleted.append((name, uid))
        del self.records[name]


def spec(replicas=2):
    return JobSpec(name="smoke", workload={"image": "busybox:1.37", "command": ["sleep", "300"], "replicas": replicas})


def child(uid, owner, phase="Running"):
    return {
        "metadata": {"name": uid, "uid": uid, "ownerReferences": [{"uid": owner, "controller": True}]},
        "spec": {"nodeName": "worker"},
        "status": {"phase": phase},
    }


def test_running_requires_all_expected_owned_pods():
    """Foreign or pending Pods cannot satisfy readiness for the submitted workload."""
    client = Client()
    launcher = KubernetesJobSetLauncher(client)
    job = launcher.submit(spec())
    uid = job.job_id.split("/")[-1]
    client.jobs = [child("job-1", uid), child("job-2", uid), child("foreign-job", "other")]
    client.pods = [child("pod-1", "job-1"), child("foreign-pod", "foreign-job")]
    assert launcher.get(job_id=job.job_id).state == JobState.PENDING
    client.pods.append(child("pod-2", "job-2", "Pending"))
    assert launcher.get(job_id=job.job_id).state == JobState.PENDING
    client.pods[-1]["status"]["phase"] = "Running"
    snapshot = launcher.get(job_id=job.job_id)
    assert snapshot.state == JobState.RUNNING
    assert len(snapshot.metadata["pods"]) == 2
    assert snapshot.restart_count is None


def test_same_names_are_isolated_and_cleanup_only_deletes_owned_uid():
    """Concurrent launchers can reuse logical names without crossing cleanup boundaries."""
    client = Client()
    first, second = KubernetesJobSetLauncher(client), KubernetesJobSetLauncher(client)
    one, two = first.submit(spec()), second.submit(spec())
    assert one.job_id != two.job_id
    with pytest.raises(ValueError):
        first.cancel(job_id=two.job_id)
    first.cancel(job_id=one.job_id)
    assert second.get(job_id=two.job_id).state == JobState.PENDING
    assert first.history(job_id=one.job_id).state == JobState.CANCELLED
    first.cancel(job_id=one.job_id)
    assert len(client.deleted) == 1


def test_replacement_is_never_observed_or_deleted_as_original():
    """A replacement UID invalidates the original handle without authorizing deletion."""
    client = Client()
    launcher = KubernetesJobSetLauncher(client)
    job = launcher.submit(spec())
    client.records[job.name]["metadata"]["uid"] = "replacement"
    with pytest.raises(JobNotFound):
        launcher.get(job_id=job.job_id)
    launcher.cancel(job_id=job.job_id)
    assert not client.deleted
    assert client.records[job.name]["metadata"]["uid"] == "replacement"


def test_server_uid_precondition_protects_racing_replacement():
    """Deletion must reject a replacement created after the ownership check."""
    client = Client()
    launcher = KubernetesJobSetLauncher(client)
    job = launcher.submit(spec())
    original_delete = client.delete

    def race(name, uid):
        client.records[name]["metadata"]["uid"] = "replacement"
        original_delete(name, uid)

    client.delete = race
    with pytest.raises(AssertionError):
        launcher.cancel(job_id=job.job_id)
    assert not client.deleted


def test_failed_snapshot_cleans_created_resource():
    """A malformed create response must not leak the newly created workload."""
    client = Client()
    create = client.create

    def malformed(manifest):
        record = create(manifest)
        record["metadata"]["creationTimestamp"] = "invalid"
        return record

    client.create = malformed
    launcher = KubernetesJobSetLauncher(client)
    with pytest.raises(ValueError):
        launcher.submit(spec())
    assert not client.records
    assert len(client.deleted) == 1


def test_create_timeout_recovers_owned_resource_for_cleanup():
    """A lost create response still permits cleanup of the owned server-side resource."""
    client = Client()
    create = client.create

    def timeout(manifest):
        create(manifest)
        raise TimeoutError("response lost")

    client.create = timeout
    with pytest.raises(TimeoutError):
        KubernetesJobSetLauncher(client).submit(spec())
    assert not client.records


def test_cleanup_does_not_require_child_list_access():
    """Workload deletion remains possible when child listing is forbidden."""
    client = Client()
    launcher = KubernetesJobSetLauncher(client)
    job = launcher.submit(spec())

    def forbidden(*args, **kwargs):
        raise RuntimeError("list forbidden")

    client.list = forbidden
    launcher.cancel(job_id=job.job_id)
    assert not client.records


def test_diagnostics_preserves_state_when_events_fail():
    """Unavailable Events do not hide workload state or the diagnostic error."""
    client = Client()
    launcher = KubernetesJobSetLauncher(client)
    job = launcher.submit(spec())

    def forbidden(uid):
        raise RuntimeError("events forbidden")

    client.events = forbidden
    result = launcher.diagnostics(job_id=job.job_id)
    assert result["workload"]["metadata"]["uid"] == job.job_id.split("/")[-1]
    assert result["pods"] == []
    assert result["errors"]


def test_kubectl_delete_sends_uid_precondition(monkeypatch):
    import json
    import subprocess

    from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient

    requests = []

    def execute(args, **kwargs):
        requests.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout="{}", stderr="")

    monkeypatch.setattr(subprocess, "run", execute)
    KubectlJobSetClient(context="kind-test", namespace="owned").delete("workload", "original-uid")
    args, kwargs = requests[0]
    assert args[args.index("--context") + 1] == "kind-test"
    assert args[args.index("--raw") + 1] == "/apis/jobset.x-k8s.io/v1alpha2/namespaces/owned/jobsets/workload"
    assert json.loads(kwargs["input"])["preconditions"] == {"uid": "original-uid"}
    assert json.loads(kwargs["input"])["propagationPolicy"] == "Foreground"
