# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the pinned Workload-Map probe against launcher-owned JobSets."""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path
from typing import Any

from recovery_libs.testing.launch.jobs import Job
from recovery_libs.testing.launch.kubernetes import KubectlJobSetClient


class WorkloadMapProbe:
    """Bounded JSON transport to a cluster-free probe, with per-scenario evidence."""

    def __init__(self, executable: Path, client: KubectlJobSetClient, report: Path) -> None:
        """Bind the probe to an explicit executable and Kubernetes transport."""
        if not executable.is_file():
            raise ValueError("build the Workload-Map probe before running live qualification")
        self.executable = executable
        self.client = client
        self.report = report
        self.observations: dict[str, Any] = {}

    def invoke(self, operation: str, record: dict[str, Any]) -> dict[str, Any]:
        """Run the actual SDK without giving the subprocess cluster access."""
        result = subprocess.run(
            [str(self.executable), "--operation", operation],
            input=json.dumps(record),
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Workload-Map {operation} failed: {result.stderr.strip()}")
        return json.loads(result.stdout)

    def inspect(self, job: Job, phase: str) -> dict[str, Any]:
        """Record normalized output alongside independently observed Pod identities."""
        record = job.metadata["workload"]
        result = self.invoke("inspect", record)
        self.observations[phase] = {
            "workload_map": result,
            "native_conditions": record.get("status", {}).get("conditions", []),
            "native_restarts": record.get("status", {}).get("restarts"),
            "pods": [
                {
                    "uid": pod["metadata"]["uid"],
                    "phase": pod.get("status", {}).get("phase"),
                    "node": pod.get("spec", {}).get("nodeName"),
                }
                for pod in job.metadata["pods"]
            ],
        }
        self.report.write_text(json.dumps(self.observations, indent=2) + "\n")
        return result

    def original_pods_absent(self, originals: dict[str, str]) -> bool:
        """Check old Pod identities even after their intermediate Job owner disappears."""
        for name, uid in originals.items():
            pod = self.client.get("pods", name)
            if pod is not None and pod["metadata"]["uid"] == uid:
                return False
        return True

    def transition(self, job: Job, operation: str) -> None:
        """Persist an SDK suspend/resume change with exact-object optimistic concurrency.

        The scenario supplies a fresh snapshot through its owned launcher. Validate
        its handle, then retain UID and resourceVersion in a replace request so a
        concurrent update or same-name replacement cannot be overwritten. No retry
        hides a race; the scenario fails with the original API response.
        """
        if operation not in {"suspend", "resume"}:
            raise ValueError("only suspend/resume may be persisted")
        record = job.metadata["workload"]
        metadata = record["metadata"]
        identity = f"{metadata['namespace']}/{metadata['name']}/{metadata['uid']}"
        if identity != job.job_id or metadata["namespace"] != self.client.namespace:
            raise ValueError("workload snapshot does not match its scoped launcher handle")
        if not metadata.get("labels", {}).get("recovery.nvidia.com/scenario-owner"):
            raise ValueError("workload has no scenario ownership marker")
        if not metadata.get("resourceVersion") or metadata.get("deletionTimestamp"):
            raise ValueError("workload is deleting or lacks a concurrency precondition")
        updated = self.invoke(operation, record)
        expected = copy.deepcopy(record)
        expected.setdefault("spec", {})["suspend"] = operation == "suspend"
        if updated != expected:
            raise AssertionError("SDK lifecycle mutation changed fields beyond spec.suspend")
        self.client.command("replace", "-f", "-", "-o", "json", body=updated)
