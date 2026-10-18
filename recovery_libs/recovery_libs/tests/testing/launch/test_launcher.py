# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.launch.launcher import SlurmLauncher


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
