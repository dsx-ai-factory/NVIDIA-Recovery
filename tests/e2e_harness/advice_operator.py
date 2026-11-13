# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scoped fixture control and Kubernetes evidence for the deployed Advice operator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
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
        self.advice_id: str | None = None
        self.watch_version: str | None = None
        self.watch_events: list[dict[str, Any]] = []
        self.watch_error: str | None = None
        self.allowed_owner: tuple[str, str, str, str] | None = None

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
        self.advice_id = str(generate_heimdall_job_id(name, 0))
        advice = {
            "advice_id": self.advice_id,
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
        record = self.client.get(RESOURCE, self.advice_id)
        self.observations.append({"expect": state, "resource": record})
        if record is None:
            return Evidence(False, "Advice has not been created")
        expected_owner = {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": name, "uid": uid}
        spec = record.get("spec", {})
        expected_job = {
            "heimdallId": str(generate_heimdall_job_id(name, 0)),
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
        return Evidence(correct, f"Advice {self.advice_id}: {spec}")

    @traced("advice.observe_absent")
    def absent(self) -> Evidence:
        """Observe absence of only this scope's Advice resource."""
        self._drain_watch()
        record = self.client.get(RESOURCE, self.advice_id)
        self.observations.append({"expect": "absent", "resource": record})
        reads = self._request("GET")["reads"]
        return Evidence(
            record is None and reads >= 2, f"Advice {self.advice_id} absent: {record is None}; source reads: {reads}"
        )

    @traced("advice.watch.begin")
    def guard_owner(self, job: Job) -> None:
        """Capture an atomic LIST cursor before invalidation so GC cannot hide a bad write.

        Each evidence check replays a bounded watch from this cursor to server-side
        timeout. Kubernetes retains events between checks; expiration or any stream
        error fails the scenario instead of silently relisting across an evidence gap.
        """
        _, name, _ = job.job_id.split("/")
        self.advice_id = str(generate_heimdall_job_id(name, 0))
        query = urlencode({"fieldSelector": f"metadata.name={self.advice_id}"})
        path = f"/apis/heimdall.nvidia.com/v1alpha1/namespaces/{self.client.namespace}/advices?{query}"
        # The raw collection preserves the API's resourceVersion even when empty;
        # kubectl's generic List output may discard that synchronization cursor.
        listing = json.loads(self.client.command("get", "--raw", path))
        self.watch_version = listing["metadata"]["resourceVersion"]
        if not self.watch_version:
            raise AssertionError("Advice LIST did not provide a watch resourceVersion")
        items = listing["items"]
        self.allowed_owner = self._owner(items[0]) if items else None
        self.watch_events.append({"type": "BASELINE", "object": listing})
        self._drain_watch()

    @traced("advice.watch.end")
    def finish_owner_guard(self) -> None:
        """Check remaining invalid-owner events before allowing the positive control."""
        self._drain_watch()
        self.watch_version = None

    @staticmethod
    def _owner(record: dict[str, Any]) -> tuple[str, str, str, str] | None:
        owners = record.get("metadata", {}).get("ownerReferences", [])
        if len(owners) != 1:
            return None
        return tuple(owners[0].get(key, "") for key in ("apiVersion", "kind", "name", "uid"))

    def _drain_watch(self) -> None:
        """Consume every retained event and fail closed on errors or forbidden writes."""
        if self.watch_error:
            raise AssertionError(self.watch_error)
        if self.watch_version is None:
            return
        query = urlencode(
            {
                "watch": "true",
                "resourceVersion": self.watch_version,
                "fieldSelector": f"metadata.name={self.advice_id}",
                "timeoutSeconds": 1,
                "allowWatchBookmarks": "true",
            }
        )
        path = f"/apis/heimdall.nvidia.com/v1alpha1/namespaces/{self.client.namespace}/advices?{query}"
        try:
            output = self.client.command("get", "--raw", path)
            events = [json.loads(line) for line in output.splitlines() if line.strip()]
            self.watch_events.extend(events)
            for event in events:
                kind = event["type"]
                record = event["object"]
                if kind == "ERROR":
                    raise AssertionError(f"Advice watch error: {record}")
                if kind not in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK"}:
                    raise AssertionError(f"unknown Advice watch event: {kind}")
                if kind != "BOOKMARK" and record["metadata"]["name"] != self.advice_id:
                    raise AssertionError("Advice watch returned an unexpected resource")
                if kind == "ADDED" or (kind == "MODIFIED" and self._owner(record) != self.allowed_owner):
                    raise AssertionError(f"operator wrote Advice for an invalid owner: {event}")
                self.watch_version = record["metadata"]["resourceVersion"]
                if not self.watch_version:
                    raise AssertionError("Advice watch event has no resourceVersion")
        except Exception as error:
            self.watch_error = f"Advice owner watch failed: {error}"
            raise AssertionError(self.watch_error) from error

    def close(self) -> None:
        """Save polling/watch evidence and remove only this scenario's input fixtures."""
        try:
            evidence = {
                "observations": self.observations,
                "watch_events": self.watch_events,
                "watch_error": self.watch_error,
            }
            self.directory.joinpath("advice-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        finally:
            self._request("DELETE")
