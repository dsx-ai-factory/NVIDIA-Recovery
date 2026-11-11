# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scoped fixture control and Kubernetes evidence for the deployed Advice operator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from uuid import uuid4

from recovery_libs.testing.http.tracing import traced
from recovery_libs.testing.launch.jobs import Job
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient
from recovery_libs.testing.scenario import Evidence

RESOURCE = "advices.heimdall.nvidia.com"


class AdviceOperatorServices:
    """Own one fixture partition and observe only its uniquely named Advice resources."""

    def __init__(self, client: KubectlJobSetClient, fixture_url: str, directory: Path) -> None:
        self.client = client
        self.scope = uuid4().hex
        self.endpoint = f"{fixture_url.rstrip('/')}/advice/{self.scope}"
        self.directory = directory
        self.observations: list[dict[str, Any]] = []

    @traced("advice.fixture.publish")
    def publish(self, job: Job, state: str, *, owner: str = "valid") -> None:
        """Replace this scenario's fixture, preserving every other scenario's records."""
        namespace, name, uid = job.job_id.split("/")
        if namespace != self.client.namespace:
            raise ValueError("workload namespace does not match the Advice evidence client")
        metadata = {
            "namespace": namespace,
            "job_api_version": "jobset.x-k8s.io/v1alpha2",
            "job_kind": "JobSet",
            "job_name": name,
            "owner_api_version": "jobset.x-k8s.io/v1alpha2",
            "owner_kind": "JobSet",
            "owner_name": name if owner != "missing" else f"missing-{self.scope}",
            "owner_uid": uid if owner != "wrong-uid" else str(uuid4()),
        }
        advice = {
            "advice_id": self.scope,
            "job_id": name,
            "attempt_index": 0,
            "state": state,
            "scheduler_metadata": metadata,
        }
        self._request("PUT", [advice])

    @traced("advice.fixture.remove")
    def withdraw(self) -> None:
        """Withdraw this scope's source records and reset its observation counter."""
        self._request("PUT", [])

    def _request(self, method: str, payload: object | None = None) -> Any:
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(self.endpoint, data=body, method=method, headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=10) as response:
            content = response.read()
            return json.loads(content) if content else None

    @traced("advice.observe")
    def matches(self, job: Job, state: str) -> Evidence:
        """Require the desired recommendation and the exact workload owner identity."""
        namespace, name, uid = job.job_id.split("/")
        record = self.client.get(RESOURCE, self.scope)
        self.observations.append({"expect": state, "resource": record})
        if record is None:
            return Evidence(False, "Advice has not been created")
        expected_owner = {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": name, "uid": uid}
        spec = record.get("spec", {})
        expected_job = {
            "heimdallId": self.scope,
            "apiVersion": "jobset.x-k8s.io/v1alpha2",
            "kind": "JobSet",
            "name": name,
            "jobId": name,
            "attemptIndex": 0,
        }
        owners = record["metadata"].get("ownerReferences", [])
        actual_job = spec.get("job", {})
        correct = (
            record["metadata"]["namespace"] == namespace
            and len(owners) == 1
            and all(owners[0].get(key) == value for key, value in expected_owner.items())
            and all(actual_job.get(key) == value for key, value in expected_job.items())
            and spec.get("advice", {}).get("state") == state
        )
        return Evidence(correct, f"Advice {self.scope}: {spec}")

    @traced("advice.observe_absent")
    def absent(self) -> Evidence:
        """Observe absence of only this scope's Advice resource."""
        record = self.client.get(RESOURCE, self.scope)
        self.observations.append({"expect": "absent", "resource": record})
        reads = self._request("GET")["reads"]
        return Evidence(
            record is None and reads >= 2, f"Advice {self.scope} absent: {record is None}; source reads: {reads}"
        )

    def close(self) -> None:
        """Save evidence and remove only this scenario's input fixtures."""
        try:
            self.directory.joinpath("advice-evidence.json").write_text(json.dumps(self.observations, indent=2) + "\n")
        finally:
            self._request("DELETE")
