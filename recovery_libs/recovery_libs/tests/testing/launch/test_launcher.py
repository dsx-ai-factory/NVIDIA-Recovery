# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from subprocess import CompletedProcess
from typing import Any

import pytest
from recovery_libs.testing.launch.clients import SlurmKubectlClient
from recovery_libs.testing.launch.jobs import JobNotFound, JobSpec
from recovery_libs.testing.launch.launcher import SlurmLauncher, job_from_accounting


class UnreadableAfterSubmit:
    """A Slurm client that accepts a job but then cannot read it back, as when a kubectl exec times out."""

    def __init__(self) -> None:
        self.cancelled: list[str] = []

    def submit(self, description: Mapping[str, Any]) -> str:
        return "7"

    def get(self, *, job_id: str) -> Mapping[str, Any] | None:
        raise TimeoutError("kubectl exec timed out")

    def cancel(self, *, job_id: str) -> None:
        self.cancelled.append(job_id)


def test_a_job_that_cannot_be_read_after_submit_is_cancelled_not_leaked() -> None:
    """The caller never learns the job ID when the read fails, so nothing else could cancel the job later."""

    client = UnreadableAfterSubmit()
    launcher = SlurmLauncher(client)

    with pytest.raises(TimeoutError):
        launcher.submit(JobSpec(name="job", workload={"script": "#!/bin/sh\ntrue\n"}))

    assert client.cancelled == ["7"]


class ArrayClient(UnreadableAfterSubmit):
    """A real array's controller shape: task zero has a raw ID different from the parent."""

    def __init__(self) -> None:
        super().__init__()
        self.descriptions: list[Mapping[str, Any]] = []
        self.selectors: list[str] = []
        self.records = {
            "7_0": self.record(8, 0),
            "7_1": self.record(7, 1),
        }

    @staticmethod
    def record(raw_id: int, task: int | None) -> dict[str, Any]:
        return {
            "job_id": raw_id,
            "array_job_id": 7,
            "array_task_id": {"set": task is not None, "number": task or 0},
            "job_state": ["RUNNING"],
            "submit_time": {"set": True, "number": 1767225600},
        }

    def submit(self, description: Mapping[str, Any]) -> str:
        self.descriptions.append(description)
        return "7"

    def get(self, *, job_id: str) -> Mapping[str, Any] | None:
        self.selectors.append(job_id)
        return self.records.get(job_id)


def test_one_array_submission_returns_canonical_tasks_including_zero_and_raw_parent() -> None:
    client = ArrayClient()
    launcher = SlurmLauncher(client)

    jobs = launcher.submit_array(JobSpec(name="array", workload={"script": "true"}), task_ids=(0, 1))

    assert len(client.descriptions) == 1
    assert client.descriptions[0]["array"] == "0,1"
    assert [job.job_id for job in jobs] == ["7_0", "7_1"]
    assert client.selectors == ["7_0", "7_1"]
    assert client.cancelled == []


def test_unmaterialized_task_does_not_become_task_zero_and_owned_array_is_cleaned_up() -> None:
    client = ArrayClient()
    client.records["7_0"] = client.record(7, None)
    launcher = SlurmLauncher(client)

    with pytest.raises(TimeoutError, match="materialize"):
        launcher.submit_array(JobSpec(name="array", workload={"script": "true"}), task_ids=(0, 1), timeout=0)

    assert client.cancelled == ["7"]


def test_sibling_observation_is_not_accepted_for_an_exact_task_query() -> None:
    client = ArrayClient()
    client.records["7_0"] = client.record(7, 1)

    with pytest.raises(JobNotFound):
        SlurmLauncher(client).get(job_id="7_0")


def test_accounting_normalizes_nested_array_task_zero() -> None:
    job = job_from_accounting(
        {"job_id": 8, "array": {"job_id": 7, "task_id": {"set": True, "number": 0}}, "restart_cnt": 2}
    )
    assert (job.job_id, job.restart_count) == ("7_0", 2)


def test_command_transport_submits_one_array_and_preserves_task_selectors(monkeypatch: pytest.MonkeyPatch) -> None:
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return CompletedProcess(command, 0, b"7\n", b"")

    monkeypatch.setattr("recovery_libs.testing.launch.clients.subprocess.run", run)
    client = SlurmKubectlClient(namespace="slurm", pod="owned-login")
    job_id = client.submit({"name": "array", "script": "true", "current_working_directory": "/tmp", "array": "0,1"})
    client.cancel(job_id="7_0")
    client.release(job_id="7_1")

    assert job_id == "7"
    assert "--array=0,1" in commands[0]
    assert commands[1][-3:] == ("scancel", "--quiet", "7_0")
    assert commands[2][-3:] == ("scontrol", "release", "7_1")
