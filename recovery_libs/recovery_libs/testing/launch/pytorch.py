# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Owned CPU PyTorchJobs using the shared Kubernetes launcher lifecycle."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import uuid4

from recovery_libs.testing.http.tracing import traced
from recovery_libs.testing.launch.jobs import Job, JobNotFound, JobSpec, JobState
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient, KubernetesJobSetLauncher, _owned_by

_OWNER = "recovery.nvidia.com/scenario-owner"
_RESOURCE = "pytorchjobs.kubeflow.org"


class KubectlPyTorchClient(KubectlJobSetClient):
    """Reuse bounded transport with the PyTorchJob API's exact-object delete path."""

    def delete(self, name: str, uid: str) -> None:
        """Delete only the original PyTorchJob incarnation and its owned children."""
        self.command(
            "delete",
            "--raw",
            f"/apis/kubeflow.org/v1/namespaces/{self.namespace}/pytorchjobs/{name}",
            "-f",
            "-",
            body={
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": uid},
                "propagationPolicy": "Foreground",
            },
        )


class KubernetesPyTorchLauncher(KubernetesJobSetLauncher):
    """Create PyTorch fixtures while sharing polling, diagnostics and guarded cleanup.

    PyTorchJob owns Pods directly. Only the native manifest, resource lookup,
    child association and state projection differ from the JobSet launcher.
    Native retry counts remain native evidence, not Slurm attempt numbers.
    """

    @classmethod
    def from_environment(cls) -> KubernetesPyTorchLauncher:
        """Use explicit PyTorch selection, or the suite's explicit shared selection."""
        context = (
            os.environ.get("LAUNCHER_KUBERNETES_PYTORCH_CONTEXT") or os.environ["LAUNCHER_KUBERNETES_JOBSET_CONTEXT"]
        )
        namespace = (
            os.environ.get("LAUNCHER_KUBERNETES_PYTORCH_NAMESPACE")
            or os.environ["LAUNCHER_KUBERNETES_JOBSET_NAMESPACE"]
        )
        return cls(KubectlPyTorchClient(context=context, namespace=namespace))

    @traced("launcher.submit")
    def submit(self, spec: JobSpec) -> Job:
        """Create a named Master/Worker fixture with explicit native retry settings."""
        workload = dict(spec.workload)
        allowed = {"image", "command", "workers", "scheduler_name", "queue", "restart_policy", "backoff_limit"}
        if workload.keys() - allowed or not isinstance(workload.get("image"), str) or not workload["image"]:
            raise ValueError(
                "PyTorch workload requires image and accepts command, workers, scheduler_name, queue, restart_policy and backoff_limit"
            )
        command = workload.get("command")
        workers = workload.get("workers", 2)
        scheduler = workload.get("scheduler_name", "default-scheduler")
        queue = workload.get("queue")
        restart_policy = workload.get("restart_policy", "Never")
        backoff_limit = workload.get("backoff_limit", 0)
        if restart_policy not in {"Never", "OnFailure"}:
            raise ValueError("qualification restart_policy must be Never or OnFailure")
        if type(backoff_limit) is not int or backoff_limit < 0:
            raise ValueError("backoff_limit must be a nonnegative integer")
        if not isinstance(command, (list, tuple)) or not command or not all(isinstance(item, str) for item in command):
            raise ValueError("PyTorch command must be a nonempty sequence of strings")
        if type(workers) is not int or workers < 1:
            raise ValueError("PyTorch workers must be a positive integer")
        if scheduler not in {"default-scheduler", "kai-scheduler"}:
            raise ValueError("unsupported qualification scheduler")
        if scheduler == "kai-scheduler" and (not isinstance(queue, str) or not queue):
            raise ValueError("KAI qualification requires an explicit queue")
        if queue is not None and (
            not isinstance(queue, str) or not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", queue)
        ):
            raise ValueError("queue must be a Kubernetes name")
        if not spec.name or (spec.time_limit_minutes is not None and spec.time_limit_minutes < 1):
            raise ValueError("PyTorch fixture requires a name and a positive time limit when specified")
        name = f"{re.sub(r'[^a-z0-9-]', '-', spec.name.lower()).strip('-')[:25] or 'workload'}-{uuid4().hex[:16]}"
        labels = {_OWNER: self._owner}
        if queue:
            labels["kai.scheduler/queue"] = queue

        def replica(count: int) -> dict[str, Any]:
            return {
                "replicas": count,
                "restartPolicy": restart_policy,
                "template": {
                    "metadata": {"labels": dict(labels)},
                    "spec": {
                        "schedulerName": scheduler,
                        "containers": [
                            {
                                "name": "pytorch",
                                "image": workload["image"],
                                "command": list(command),
                                "env": [{"name": key, "value": value} for key, value in spec.environment.items()],
                                "resources": {
                                    "requests": {"cpu": "100m", "memory": "32Mi"},
                                    "limits": {"cpu": "1", "memory": "128Mi"},
                                },
                            }
                        ],
                    },
                },
            }

        run_policy: dict[str, Any] = {"cleanPodPolicy": "None", "backoffLimit": backoff_limit}
        if spec.time_limit_minutes is not None:
            run_policy["activeDeadlineSeconds"] = spec.time_limit_minutes * 60
        manifest = {
            "apiVersion": "kubeflow.org/v1",
            "kind": "PyTorchJob",
            "metadata": {
                "name": name,
                "namespace": self._client.namespace,
                "labels": labels,
                "annotations": {"recovery.nvidia.com/metadata": json.dumps(dict(spec.metadata))},
            },
            "spec": {
                "runPolicy": run_policy,
                "pytorchReplicaSpecs": {"Master": replica(1), "Worker": replica(workers)},
            },
        }
        try:
            record = self._client.create(manifest)
            job = self._snapshot(record, [])
        except BaseException as error:
            try:
                created = self._client.get(_RESOURCE, name)
                if created and created["metadata"].get("labels", {}).get(_OWNER) == self._owner:
                    self._client.delete(name, created["metadata"]["uid"])
            except Exception as cleanup_error:  # noqa: BLE001 - preserve original failure and cleanup evidence
                error.add_note(f"cleanup of possibly created PyTorchJob failed: {cleanup_error!r}")
            raise
        self._owned[job.job_id] = job
        return job

    def _record(self, job_id: str) -> dict[str, Any]:
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
        pods = [
            pod
            for pod in self._client.list("pods", selector=f"{_OWNER}={self._owner}")
            if _owned_by(pod, {record["metadata"]["uid"]})
        ]
        return [], pods

    def _snapshot(self, record: dict[str, Any], pods: list[dict[str, Any]]) -> Job:
        metadata = record["metadata"]
        expected = sum(role.get("replicas", 1) for role in record["spec"]["pytorchReplicaSpecs"].values())
        conditions = {
            item["type"] for item in record.get("status", {}).get("conditions", []) if item.get("status") == "True"
        }
        state = JobState.PENDING
        if "Failed" in conditions:
            state = JobState.FAILED
        elif "Succeeded" in conditions:
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
