# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prove live mutation preconditions and drain checks independently of the SDK."""

import copy
from types import SimpleNamespace

import pytest
from e2e_harness.workload_map import WorkloadMapProbe


class Client:
    """Record writes and model a Pod surviving deletion of its intermediate Job."""

    namespace = "qualification"

    def __init__(self):
        self.writes = []
        self.pod = {"metadata": {"uid": "old-pod", "deletionTimestamp": "now"}}

    def command(self, *args, body):
        self.writes.append(body)

    def get(self, resource, name):
        return self.pod


class Probe(WorkloadMapProbe):
    """Model a candidate SDK mutation without invoking a subprocess in safety tests."""

    def invoke(self, operation, record):
        updated = copy.deepcopy(record)
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
        "spec": {},
    }
    job = SimpleNamespace(job_id="qualification/owned/original", metadata={"workload": record})
    return probe, client, job


def test_persist_retains_uid_and_resource_version(fixture):
    probe, client, job = fixture
    probe.transition(job, "suspend")
    assert client.writes == [{**job.metadata["workload"], "spec": {"suspend": True}}]
    assert job.metadata["workload"]["spec"] == {}


@pytest.mark.parametrize(
    "field,value",
    [
        ("uid", "replacement"),
        ("namespace", "other"),
        ("resourceVersion", ""),
        ("deletionTimestamp", "now"),
        ("labels", {}),
    ],
)
def test_reject_changed_or_unowned_snapshot(fixture, field, value):
    probe, client, job = fixture
    job.metadata["workload"]["metadata"][field] = value
    with pytest.raises(ValueError):
        probe.transition(job, "suspend")
    assert client.writes == []


def test_reject_unexpected_sdk_change(fixture, monkeypatch):
    probe, client, job = fixture
    monkeypatch.setattr(probe, "invoke", lambda *_: {"spec": {"suspend": True}})
    with pytest.raises(AssertionError):
        probe.transition(job, "suspend")
    assert client.writes == []


def test_drain_waits_for_terminating_pod_even_without_job_owner(fixture):
    probe, client, _ = fixture
    assert not probe.original_pods_absent({"worker": "old-pod"})
    client.pod = None
    assert probe.original_pods_absent({"worker": "old-pod"})
