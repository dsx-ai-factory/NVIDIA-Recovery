# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import httpx
import pytest
from recovery_libs.testing.launch.clients import CommandResult, SlurmHttpClient, SlurmKubectlClient
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.launch.launcher import SlurmLauncher, slurm_job_description


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


@pytest.mark.parametrize("submission", ["command", "http"])
def test_explicit_stdout_and_time_limit_reach_both_submission_interfaces(
    submission: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both launch paths must write the scenario's marker to its declared source file."""

    description = slurm_job_description(
        JobSpec(
            name="stdout-contract",
            workload={"script": "#!/bin/sh\necho marker\n", "standard_output": "/home/logs/run/output"},
            time_limit_minutes=5,
        )
    )
    if submission == "http":

        def submit(request: httpx.Request) -> httpx.Response:
            job = json.loads(request.content)["job"]
            assert job["standard_output"] == "/home/logs/run/output"
            assert job["time_limit"] == {"set": True, "number": 5}
            return httpx.Response(200, json={"job_id": 7})

        with httpx.Client(base_url="https://slurm.example", transport=httpx.MockTransport(submit)) as http:
            assert SlurmHttpClient(http).submit(description) == "7"
    else:
        commands: list[tuple[str, ...]] = []

        def run(
            self: SlurmKubectlClient, command: tuple[str, ...], stdin: bytes | None, timeout: float
        ) -> CommandResult:
            commands.append(tuple(command))
            return CommandResult(tuple(command), 0, "7\n", "")

        monkeypatch.setattr(SlurmKubectlClient, "run", run)
        assert SlurmKubectlClient(namespace="slurm", pod="login").submit(description) == "7"
        assert "--output=/home/logs/run/output" in commands[0]
        assert "--time=5" in commands[0]
