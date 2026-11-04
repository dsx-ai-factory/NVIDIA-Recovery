# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""JobSet workload access, with namespace/UID identity and owned-resource cleanup."""

from __future__ import annotations

import builtins
import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from typing import Any
from uuid import uuid4

from recovery_libs.testing.http.tracing import traced
from recovery_libs.testing.launch.jobs import Job, JobNotFound, JobSpec, JobState

_OWNER = "recovery.nvidia.com/scenario-owner"
_RESOURCE = "jobsets.jobset.x-k8s.io"


class KubectlJobSetClient:
    """Bounded kubectl transport using an explicit context and namespace."""

    def __init__(self, *, context: str, namespace: str, timeout: float = 30) -> None:
        """Bind requests to a validated namespace and explicit cluster context."""
        if not context or not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace):
            raise ValueError("an explicit context and valid namespace are required")
        self.context = context
        self.namespace = namespace
        self.timeout = timeout

    def command(self, *args: str, body: Mapping[str, Any] | None = None) -> str:
        """Execute one bounded request without a shell or ambient context selection."""
        result = subprocess.run(
            [
                "kubectl",
                "--context",
                self.context,
                "--namespace",
                self.namespace,
                f"--request-timeout={self.timeout:g}s",
                *args,
            ],
            input=json.dumps(body) if body is not None else None,
            capture_output=True,
            text=True,
            timeout=self.timeout + 5,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"kubectl {args[0]} failed: {result.stderr.strip()}")
        return result.stdout

    def create(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        """Create a new JobSet, never updating an existing resource."""
        return json.loads(self.command("create", "-f", "-", "-o", "json", body=manifest))

    def get(self, resource: str, name: str) -> dict[str, Any] | None:
        """Read a resource, preserving missing-resource semantics."""
        result = self.command("get", resource, name, "--ignore-not-found", "-o", "json")
        return json.loads(result) if result.strip() else None

    def list(self, resource: str, *, selector: str) -> list[dict[str, Any]]:
        """Read only the resources selected within this client's namespace."""
        return json.loads(self.command("get", resource, "-l", selector, "-o", "json"))["items"]

    def delete(self, name: str, uid: str) -> None:
        """Delete only the exact incarnation returned by create, cascading its children."""
        self.command(
            "delete",
            "--raw",
            f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{self.namespace}/jobsets/{name}",
            "-f",
            "-",
            body={
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": uid},
                "propagationPolicy": "Foreground",
            },
        )

    def events(self, uid: str) -> builtins.list[dict[str, Any]]:
        """Read events about one exact Kubernetes object."""
        return json.loads(self.command("get", "events", "--field-selector", f"involvedObject.uid={uid}", "-o", "json"))[
            "items"
        ]

    def logs(self, pod: str, container: str) -> str:
        """Read bounded recent logs for a workload container."""
        return self.command("logs", pod, "-c", container, "--tail=200", "--limit-bytes=65536")


class KubernetesJobSetLauncher:
    """Submit CPU JobSets and observe their owned Pods through the launcher boundary.

    Each instance has an unguessable ownership label. Handles preserve namespace,
    name and UID; cleanup checks both ownership and incarnation before issuing a
    server-side UID-preconditioned delete. No Kubernetes retry count is translated
    into a Slurm attempt number.
    """

    def __init__(
        self,
        client: KubectlJobSetClient,
        *,
        poll_interval: float = 1,
        cleanup_timeout: float = 60,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = client
        self._owner = uuid4().hex
        self._owned: dict[str, Job] = {}
        self._history: dict[str, Job] = {}
        self._poll_interval = poll_interval
        self._cleanup_timeout = cleanup_timeout
        self._clock = clock
        self._sleep = sleep

    @classmethod
    def from_environment(cls) -> KubernetesJobSetLauncher:
        """Build from explicit LAUNCHER_KUBERNETES_JOBSET context/namespace settings."""
        return cls(
            KubectlJobSetClient(
                context=os.environ.get("LAUNCHER_KUBERNETES_JOBSET_CONTEXT", "kind-nvidia-recovery-kubernetes"),
                namespace=os.environ.get("LAUNCHER_KUBERNETES_JOBSET_NAMESPACE", "recovery-dev"),
            )
        )

    @traced("launcher.submit")
    def submit(self, spec: JobSpec) -> Job:
        """Create a uniquely named workload and return its initial snapshot."""
        workload = dict(spec.workload)
        unknown = workload.keys() - {"image", "command", "replicas"}
        if unknown or not isinstance(workload.get("image"), str) or not workload["image"]:
            raise ValueError("JobSet workload requires image and accepts only command and replicas")
        command = workload.get("command")
        replicas = workload.get("replicas", 1)
        if not isinstance(command, (list, tuple)) or not command or not all(isinstance(x, str) for x in command):
            raise ValueError("JobSet command must be a nonempty sequence of strings")
        if type(replicas) is not int or replicas < 1:
            raise ValueError("JobSet replicas must be a positive integer")
        if spec.time_limit_minutes is not None and spec.time_limit_minutes < 1:
            raise ValueError("JobSet time limit must be positive")
        if not spec.name:
            raise ValueError("JobSet name must not be empty")
        prefix = re.sub(r"[^a-z0-9-]", "-", spec.name.lower()).strip("-")[:25] or "workload"
        name = f"{prefix}-{uuid4().hex[:16]}"
        labels = {_OWNER: self._owner}
        template: dict[str, Any] = {
            "parallelism": 1,
            "completions": 1,
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "schedulerName": "default-scheduler",
                    "containers": [
                        {
                            "name": "worker",
                            "image": workload["image"],
                            "command": list(command),
                            "env": [{"name": k, "value": v} for k, v in spec.environment.items()],
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "32Mi"},
                                "limits": {"cpu": "1", "memory": "128Mi"},
                            },
                        }
                    ],
                },
            },
        }
        if spec.time_limit_minutes is not None:
            template["activeDeadlineSeconds"] = spec.time_limit_minutes * 60
        manifest = {
            "apiVersion": "jobset.x-k8s.io/v1alpha2",
            "kind": "JobSet",
            "metadata": {
                "name": name,
                "namespace": self._client.namespace,
                "labels": labels,
                "annotations": {"recovery.nvidia.com/metadata": json.dumps(dict(spec.metadata))},
            },
            "spec": {"replicatedJobs": [{"name": "workers", "replicas": replicas, "template": {"spec": template}}]},
        }
        try:
            record = self._client.create(manifest)
            job = self._snapshot(record, [])
        except BaseException as error:
            # A timed-out create may have reached the server. Recover by the
            # unique name and ownership label, then delete that exact UID.
            try:
                created = self._client.get(_RESOURCE, name)
                if created and created["metadata"].get("labels", {}).get(_OWNER) == self._owner:
                    self._client.delete(name, created["metadata"]["uid"])
            except Exception as cleanup_error:  # noqa: BLE001
                error.add_note(f"cleanup of possibly created JobSet {name} failed: {cleanup_error!r}")
            raise
        self._owned[job.job_id] = job
        return job

    def _record(self, job_id: str) -> dict[str, Any]:
        """Reject foreign handles and changed ownership before observing a workload."""
        if job_id not in self._owned:
            raise ValueError(f"workload is not owned by this launcher: {job_id}")
        namespace, name, uid = job_id.split("/")
        record = self._client.get(_RESOURCE, name)
        if record is None or record["metadata"]["uid"] != uid:
            raise JobNotFound(job_id)
        metadata = record["metadata"]
        if metadata["namespace"] != namespace or metadata.get("labels", {}).get(_OWNER) != self._owner:
            raise ValueError(f"workload ownership changed: {job_id}")
        return record

    def _children(self, record: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Follow controller UID ownership from this JobSet through Jobs to Pods."""
        uid = record["metadata"]["uid"]
        selector = f"jobset.sigs.k8s.io/jobset-name={record['metadata']['name']}"
        jobs = [job for job in self._client.list("jobs", selector=selector) if _owned_by(job, {uid})]
        job_uids = {job["metadata"]["uid"] for job in jobs}
        pods = [
            pod for pod in self._client.list("pods", selector=f"{_OWNER}={self._owner}") if _owned_by(pod, job_uids)
        ]
        return jobs, pods

    def _snapshot(self, record: dict[str, Any], pods: list[dict[str, Any]]) -> Job:
        """Report running only when every expected owned Pod is running and not deleting."""
        metadata = record["metadata"]
        expected = sum(
            job.get("replicas", 1) * job["template"]["spec"].get("parallelism", 1)
            for job in record["spec"]["replicatedJobs"]
        )
        conditions = {
            item["type"] for item in record.get("status", {}).get("conditions", []) if item.get("status") == "True"
        }
        state = JobState.PENDING
        if "Failed" in conditions:
            state = JobState.FAILED
        elif "Completed" in conditions:
            state = JobState.COMPLETED
        elif len(pods) == expected and all(
            pod.get("status", {}).get("phase") == "Running" and not pod["metadata"].get("deletionTimestamp")
            for pod in pods
        ):
            state = JobState.RUNNING
        return Job(
            job_id=f"{metadata['namespace']}/{metadata['name']}/{metadata['uid']}",
            name=metadata["name"],
            state=state,
            nodes=tuple(sorted({pod["spec"]["nodeName"] for pod in pods if pod["spec"].get("nodeName")})),
            exit_code=None,
            restart_count=None,
            submitted_at=datetime.fromisoformat(metadata["creationTimestamp"]),
            metadata={"workload": record, "pods": pods, "expected_pods": expected},
        )

    @traced("launcher.get")
    def get(self, *, job_id: str) -> Job:
        """Observe only the original JobSet and Pods linked by controller ownership."""
        record = self._record(job_id)
        _, pods = self._children(record)
        return self._snapshot(record, pods)

    def history(self, *, job_id: str) -> Job:
        """Return the last snapshot retained after this launcher cleaned up a workload."""
        if job_id in self._history:
            return self._history[job_id]
        raise JobNotFound(job_id)

    def wait(self, *, job_id: str, until: Callable[[Job], bool], timeout: float) -> Job:
        """Poll this workload under a monotonic deadline."""
        deadline = self._clock() + timeout
        while True:
            job = self.get(job_id=job_id)
            if until(job):
                return job
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise TimeoutError(f"workload {job_id} did not reach expected condition: {job.state}")
            self._sleep(min(self._poll_interval, remaining))

    @traced("launcher.cancel")
    def cancel(self, *, job_id: str) -> None:
        """Delete the owned incarnation and wait only for its foreground cleanup."""
        try:
            record = self._record(job_id)
        except JobNotFound:
            self._history.setdefault(job_id, replace(self._owned[job_id], state=JobState.CANCELLED))
            return
        # Optional child observations must not prevent cleanup of a validated
        # owned resource when list permissions or the API are degraded.
        snapshot = self._owned[job_id]
        self._client.delete(record["metadata"]["name"], record["metadata"]["uid"])
        deadline = self._clock() + self._cleanup_timeout
        while True:
            try:
                self._record(job_id)
            except JobNotFound:
                self._history[job_id] = replace(snapshot, state=JobState.CANCELLED)
                return
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise TimeoutError(f"workload cleanup timed out: {job_id}")
            self._sleep(min(self._poll_interval, remaining))

    def list(self, *, name: str | None = None) -> tuple[Job, ...]:
        """List only workloads submitted by this launcher instance."""
        jobs = []
        for job_id in self._owned:
            try:
                job = self.get(job_id=job_id)
            except JobNotFound:
                continue
            if name is None or job.name == name:
                jobs.append(job)
        return tuple(jobs)

    def diagnostics(self, *, job_id: str) -> dict[str, Any]:
        """Collect owned workload/Pod state, Events and bounded container logs before cleanup."""
        record = self._record(job_id)
        events: builtins.list[dict[str, Any]] = []
        logs: dict[str, str] = {}
        errors: dict[str, str] = {}
        try:
            jobs, pods = self._children(record)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            jobs, pods = [], []
            errors["children"] = str(error)
        for item in [record, *jobs, *pods]:
            uid = item["metadata"]["uid"]
            try:
                events.extend(self._client.events(uid))
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                errors[f"events/{uid}"] = str(error)
        for pod in pods:
            for container in pod["spec"]["containers"]:
                key = f"{pod['metadata']['name']}/{container['name']}"
                try:
                    logs[key] = self._client.logs(pod["metadata"]["name"], container["name"])
                except (RuntimeError, subprocess.TimeoutExpired) as error:
                    logs[key] = str(error)
        return {"workload": record, "jobs": jobs, "pods": pods, "events": events, "logs": logs, "errors": errors}


def _owned_by(record: Mapping[str, Any], uids: set[str]) -> bool:
    """Require a controller owner reference to one of the expected parent UIDs."""
    return any(
        owner.get("uid") in uids and owner.get("controller") is True
        for owner in record["metadata"].get("ownerReferences", [])
    )
