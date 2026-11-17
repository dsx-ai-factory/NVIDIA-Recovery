# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the pinned Workload-Map probe against launcher-owned JobSets."""

from __future__ import annotations

import copy
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from recovery_libs.testing.launch.jobs import Job
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient

_OWNER = "recovery.nvidia.com/scenario-owner"
_MARKER = "qualification.nvidia.com/marker"


class WorkloadMapProbe:
    """Bounded JSON transport with pinned ownership and append-only observations."""

    def __init__(self, executable: Path, client: KubectlJobSetClient, report: Path) -> None:
        """Bind the probe to an explicit executable and Kubernetes transport."""
        if not executable.is_file():
            raise ValueError("build the Workload-Map probe before running live qualification")
        self.executable = executable
        self.client = client
        self.report = report
        self.identity: dict[str, str] | None = None
        self.observations: list[dict[str, Any]] = []
        self.scheduling_events: dict[str, list[dict[str, str]]] = {}
        self.status_timing: dict[str, dict[str, Any]] = {}
        self.capabilities = {
            name: {"outcome": "not_tested", "detail": "not exercised in this scenario"}
            for name in (
                "scale",
                "status",
                "instrumentation",
                "suspend_resume",
                "terminal_boundary",
                "pytorchjob_live",
                "kai",
                "oom",
                "eviction",
                "retry_exhaustion",
            )
        }
        self.pending_capabilities: set[str] = set()
        self.started = time.monotonic()
        self.outcome = "not_tested"

    def invoke(self, operation: str, record: dict[str, Any], *, marker: str | None = None) -> dict[str, Any]:
        """Run the actual SDK without giving the subprocess cluster access."""
        args = [str(self.executable), "--operation", operation]
        if marker is not None:
            args.extend(["--marker", marker])
        result = subprocess.run(
            args,
            input=json.dumps(record),
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Workload-Map {operation} failed: {result.stderr.strip()}")
        return json.loads(result.stdout)

    def bind(self, job: Job) -> None:
        """Pin the first launcher-owned UID and owner token for subsequent observations and writes."""
        metadata = job.metadata["workload"]["metadata"]
        identity = {key: metadata[key] for key in ("namespace", "name", "uid")}
        identity["owner"] = metadata.get("labels", {}).get(_OWNER, "")
        if self.identity is not None and identity != self.identity:
            raise ValueError("workload identity or ownership changed")
        if not identity["owner"]:
            raise ValueError("workload has no scenario ownership marker")
        if f"{identity['namespace']}/{identity['name']}/{identity['uid']}" != job.job_id:
            raise ValueError("workload snapshot does not match its launcher handle")
        if identity["namespace"] != self.client.namespace:
            raise ValueError("workload is outside the selected namespace")
        self.identity = identity
        self._write_report()

    def _validate(self, job: Job, *, writing: bool = False) -> dict[str, Any]:
        """Require the remembered incarnation, owner value, and write preconditions."""
        if self.identity is None:
            raise ValueError("bind a launcher-owned workload before using the probe")
        self.bind(job)
        record = job.metadata["workload"]
        metadata = record["metadata"]
        if writing and (not metadata.get("resourceVersion") or metadata.get("deletionTimestamp")):
            raise ValueError("workload is deleting or lacks a concurrency precondition")
        return record

    def inspect(self, job: Job, phase: str) -> dict[str, Any]:
        """Append SDK states, native UID, and Pod readiness at each observation."""
        record = self._validate(job)
        result = self.invoke("inspect", record)
        elapsed = time.monotonic() - self.started
        pods = job.metadata["pods"]
        all_ready = len(pods) == job.metadata["expected_pods"] and all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", [])
            )
            for pod in pods
        )
        self.observations.append(
            copy.deepcopy(
                {
                    "phase": phase,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "elapsed_seconds": elapsed,
                    "workload_uid": record["metadata"]["uid"],
                    "workload_map": result,
                    "native_conditions": record.get("status", {}).get("conditions", []),
                    "native_restarts": record.get("status", {}).get("restarts"),
                    "native_settings": self._native_settings(record),
                    "native_replica_statuses": record.get("status", {}).get("replicaStatuses"),
                    "all_pods_ready": all_ready,
                    "pods": [
                        {
                            "name": pod["metadata"]["name"],
                            "uid": pod["metadata"]["uid"],
                            "phase": pod.get("status", {}).get("phase"),
                            "node": pod.get("spec", {}).get("nodeName"),
                            "conditions": pod.get("status", {}).get("conditions", []),
                            "containers": [
                                {
                                    "name": container["name"],
                                    "restart_count": container.get("restartCount", 0),
                                    "state": container.get("state", {}),
                                    "last_state": container.get("lastState", {}),
                                }
                                for container in pod.get("status", {}).get("containerStatuses", [])
                            ],
                        }
                        for pod in pods
                    ],
                }
            )
        )
        if all_ready:
            timing = self.status_timing.setdefault(phase, {"ready_first_observed_seconds": elapsed})
            if "Initializing" in result["statuses"]:
                timing["initializing_last_observed_seconds_after_ready"] = (
                    elapsed - timing["ready_first_observed_seconds"]
                )
            else:
                timing.setdefault(
                    "initializing_first_absent_seconds_after_ready", elapsed - timing["ready_first_observed_seconds"]
                )
        self._write_report()
        return result

    def kai_scheduled(self, job: Job) -> bool:
        """Require KAI scheduling evidence for each owned Pod, without assuming PodGroup topology.

        Events may arrive after readiness. The caller retries this observation;
        missing events cannot establish scheduler provenance from labels alone.
        """
        self._validate(job)
        self.begin_capability("kai")
        pods = job.metadata["pods"]
        for pod in pods:
            uid = pod["metadata"]["uid"]
            matches = []
            for event in self.client.events(uid):
                reporter = event.get("reportingComponent") or event.get("source", {}).get("component")
                if (
                    event.get("involvedObject", {}).get("uid") == uid
                    and event.get("reason") == "Scheduled"
                    and reporter == "kai-scheduler"
                ):
                    matches.append({"reason": "Scheduled", "reporter": reporter, "pod_uid": uid})
            # Preserve earlier evidence if the API later expires the event.
            if matches:
                self.scheduling_events[uid] = matches
        self._write_report()
        return bool(pods) and all(pod["metadata"]["uid"] in self.scheduling_events for pod in pods)

    def scoped_pods(self, job: Job) -> list[dict[str, Any]]:
        """Find all scenario Pods for this JobSet, including children of vanished Jobs."""
        record = self._validate(job)
        owner = record["metadata"]["labels"][_OWNER]
        job_label = (
            "training.kubeflow.org/job-name" if record.get("kind") == "PyTorchJob" else "jobset.sigs.k8s.io/jobset-name"
        )
        selector = f"{_OWNER}={owner},{job_label}={record['metadata']['name']}"
        return self.client.list("pods", selector=selector)

    def original_pods_absent(self, originals: dict[str, str]) -> bool:
        """Check remembered Pod UIDs even if a controller removed their labels."""
        for name, uid in originals.items():
            pod = self.client.get("pods", name)
            if pod is not None and pod["metadata"]["uid"] == uid:
                return False
        return True

    def transition(self, job: Job, operation: str) -> None:
        """Persist only an SDK suspend-field change, retaining UID and resourceVersion.

        A concurrent update fails with the original API error. The qualification
        must not hide that race by overwriting a newer object or retrying blindly.
        """
        if operation not in {"suspend", "resume"}:
            raise ValueError("only suspend/resume may be persisted")
        record = self._validate(job, writing=True)
        self.begin_capability("suspend_resume")
        updated = self.invoke(operation, record)
        expected = copy.deepcopy(record)
        suspend_owner = expected.setdefault("spec", {})
        if record.get("kind") == "PyTorchJob":
            suspend_owner = suspend_owner.setdefault("runPolicy", {})
        suspend_owner["suspend"] = operation == "suspend"
        if updated != expected:
            raise AssertionError("SDK lifecycle mutation changed fields beyond the native suspend field")
        self.client.command("replace", "-f", "-", "-o", "json", body=updated)

    def instrument(self, job: Job) -> None:
        """Instrument owned, drained templates through the SDK.

        Check the requested marker and the write's scope before persisting it.
        SDK serialization inside the templates is not validated or normalized here.
        """
        record = self._validate(job, writing=True)
        native_policy = record.get("spec", {})
        if record.get("kind") == "PyTorchJob":
            native_policy = native_policy.get("runPolicy", {})
        if native_policy.get("suspend") is not True or self.scoped_pods(job):
            raise ValueError("instrumentation requires a suspended, completely drained workload")
        self.begin_capability("instrumentation")
        marker = self.identity["owner"]
        updated = self.invoke("instrument", record, marker=marker)
        outside_templates = copy.deepcopy(updated)
        old_jobs = self._template_parents(record)
        new_jobs = self._template_parents(outside_templates)
        if len(old_jobs) != len(new_jobs):
            raise AssertionError("instrumentation changed component count")
        for original, candidate in zip(old_jobs, new_jobs, strict=True):
            template = candidate["template"]
            if template.get("metadata", {}).get("labels", {}).get(_MARKER) != marker:
                raise AssertionError("instrumentation did not label every native Pod template")
            candidate["template"] = original["template"]
        if outside_templates != record:
            raise AssertionError("instrumentation changed fields outside Pod templates")
        self.client.command("replace", "-f", "-", "-o", "json", body=updated)

    @staticmethod
    def _native_settings(record: dict[str, Any]) -> dict[str, Any]:
        """Retain controller retry controls without copying container configuration."""
        spec = record.get("spec", {})
        if record.get("kind") == "PyTorchJob":
            return {
                "run_policy": spec.get("runPolicy", {}),
                "replicas": {
                    role: {"replicas": replica.get("replicas"), "restart_policy": replica.get("restartPolicy")}
                    for role, replica in spec.get("pytorchReplicaSpecs", {}).items()
                },
            }
        return {
            "failure_policy": spec.get("failurePolicy", {}),
            "replicated_jobs": [
                {
                    "name": job["name"],
                    "replicas": job.get("replicas"),
                    "parallelism": job["template"]["spec"].get("parallelism"),
                    "completions": job["template"]["spec"].get("completions"),
                    "backoff_limit": job["template"]["spec"].get("backoffLimit"),
                    "restart_policy": job["template"]["spec"]["template"]["spec"].get("restartPolicy"),
                }
                for job in spec.get("replicatedJobs", [])
            ],
        }

    @staticmethod
    def _template_parents(record: dict[str, Any]) -> list[dict[str, Any]]:
        """Locate native templates independently of SDK extraction for write validation."""
        if record.get("kind") == "PyTorchJob":
            replicas = record["spec"]["pytorchReplicaSpecs"]
            return [replicas[role] for role in sorted(replicas)]
        return [job["template"]["spec"] for job in record["spec"]["replicatedJobs"]]

    def begin_capability(self, name: str) -> None:
        """Track attempted checks so a failed scenario does not label them untested."""
        self.pending_capabilities.add(name)

    def capability(self, name: str, outcome: str, detail: str) -> None:
        """Record a capability verdict separately from overall pytest success."""
        if outcome not in {"pass", "fail", "unsupported", "not_tested"}:
            raise ValueError("invalid capability outcome")
        self.pending_capabilities.discard(name)
        self.capabilities[name] = {"outcome": outcome, "detail": detail}
        self._write_report()

    def finish(self, outcome: str) -> None:
        """Record pytest's scenario outcome; pytest/JUnit also reports cleanup failures."""
        self.outcome = outcome
        if outcome == "fail":
            for name in sorted(self.pending_capabilities):
                self.capabilities[name] = {
                    "outcome": "fail",
                    "detail": "scenario failed before this attempted capability was established; see JUnit and observations",
                }
        self._write_report()

    def _write_report(self) -> None:
        """Persist accumulated evidence without discarding earlier observations."""
        self.report.write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "scenario_outcome": self.outcome,
                    "workload_identity": self.identity,
                    "capabilities": self.capabilities,
                    "status_timing": self.status_timing,
                    "observations": self.observations,
                    "scheduling_events": self.scheduling_events,
                },
                indent=2,
            )
            + "\n"
        )
