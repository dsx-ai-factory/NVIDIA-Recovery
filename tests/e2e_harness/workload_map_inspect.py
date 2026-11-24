# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only observations of one workload; this command never qualifies its lifecycle."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_RESOURCES = {"jobset": "jobsets.jobset.x-k8s.io", "pytorchjob": "pytorchjobs.kubeflow.org"}
_ACTIVE_CHECKS = ("lifecycle", "instrumentation", "pod_association", "failure_recovery")


class ObservationUnavailable(Exception):
    """A bounded read or local probe could not produce usable evidence."""


def _json_command(command: list[str], *, body: dict[str, Any] | None = None) -> dict[str, Any] | None:
    # Never publish raw stderr: an API/probe error can include input manifest data.
    try:
        result = subprocess.run(
            command,
            input=json.dumps(body) if body is not None else None,
            text=True,
            capture_output=True,
            timeout=35,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ObservationUnavailable("command unavailable or timed out") from error
    if result.returncode:
        raise ObservationUnavailable(
            "command failed; check credentials, RBAC, resource availability, and probe version"
        )
    if not result.stdout.strip():
        return None
    try:
        value = json.loads(result.stdout)
    except ValueError as error:
        raise ObservationUnavailable("command did not return JSON") from error
    if not isinstance(value, dict):
        raise ObservationUnavailable("command did not return a JSON object")
    return value


def _owned_by(record: dict[str, Any], uids: set[str]) -> bool:
    return any(
        owner.get("controller") is True and owner.get("uid") in uids
        for owner in record.get("metadata", {}).get("ownerReferences", [])
    )


def inspect_cluster(args: argparse.Namespace) -> dict[str, Any]:
    """Collect independent reads and SDK extraction while retaining prerequisite failures.

    Every Kubernetes invocation is an explicit get or version operation against
    the selected kubeconfig/context. Failure of optional controller observation
    does not discard workload extraction, and no active capability can pass here.
    """
    report: dict[str, Any] = {
        "schema_version": 1,
        "mode": "read_only",
        "observed_at": datetime.now(UTC).isoformat(),
        "context": args.context,
        "namespace": args.namespace,
        "resource": args.resource,
        "name": args.name,
        "checks": {
            name: {"outcome": "not_tested", "reason": "requires explicit active qualification"}
            for name in _ACTIVE_CHECKS
        },
    }
    checks = report["checks"]
    prefix = ["kubectl", "--kubeconfig", str(args.kubeconfig), "--context", args.context, "--request-timeout=30s"]

    def get(resource: str, namespace: str, name: str | None = None) -> dict[str, Any] | None:
        command = [*prefix, "--namespace", namespace, "get", resource]
        if name is not None:
            command.extend([name, "--ignore-not-found"])
        return _json_command([*command, "-o", "json"])

    try:
        version = _json_command([*prefix, "version", "-o", "json"])
        server = (version or {}).get("serverVersion", {})
        if not server.get("gitVersion"):
            raise ObservationUnavailable("server version not returned")
        report["kubernetes_version"] = {key: server[key] for key in ("gitVersion", "platform") if key in server}
        checks["cluster_access"] = {"outcome": "pass", "reason": "server version read successfully"}
    except ObservationUnavailable as error:
        checks["cluster_access"] = {"outcome": "inconclusive", "reason": str(error)}

    try:
        workload = get(_RESOURCES[args.resource], args.namespace, args.name)
        if workload is None:
            checks["workload_access"] = {"outcome": "unsupported", "reason": "selected workload does not exist"}
            checks["sdk_extraction"] = {"outcome": "not_tested", "reason": "no workload to inspect"}
        else:
            metadata = workload.get("metadata", {})
            report["input_identity"] = {
                "api_version": workload.get("apiVersion"),
                "kind": workload.get("kind"),
                "name": metadata.get("name"),
                "namespace": metadata.get("namespace"),
                "uid": metadata.get("uid"),
                "resource_version": metadata.get("resourceVersion"),
            }
            checks["workload_access"] = {"outcome": "pass", "reason": "selected workload read successfully"}
            try:
                result = _json_command([str(args.probe), "--operation", "inspect"], body=workload)
                required = {"sdk_version", "definition_name", "definition_sha256", "statuses", "components"}
                if result is None or not required.issubset(result):
                    raise ObservationUnavailable("probe returned an incomplete inspection report")
                report["workload_map"] = {
                    key: result[key]
                    for key in (
                        "sdk_version",
                        "definition_name",
                        "definition_sha256",
                        "statuses",
                        "components",
                        "suspend_supported",
                        "capability_errors",
                    )
                    if key in result
                }
                errors = result.get("capability_errors", {})
                checks["sdk_extraction"] = {
                    "outcome": "fail" if errors else "pass",
                    "reason": "one or more SDK capabilities failed"
                    if errors
                    else "SDK extracted observations; correctness is not live-qualified",
                }
                for capability in errors:
                    checks[f"sdk:{capability}"] = {
                        "outcome": "fail",
                        "reason": "SDK extraction error; see capability_errors",
                    }
                checks["suspend_mapping"] = {
                    "outcome": "pass" if result.get("suspend_supported") else "unsupported",
                    "reason": "definition mapping presence only; no lifecycle operation attempted",
                }
            except ObservationUnavailable as error:
                checks["sdk_extraction"] = {"outcome": "inconclusive", "reason": str(error)}
    except ObservationUnavailable as error:
        checks["workload_access"] = {"outcome": "inconclusive", "reason": str(error)}
        checks["sdk_extraction"] = {"outcome": "not_tested", "reason": "workload could not be read"}

    if not args.controller_deployment:
        checks["controller"] = {"outcome": "not_tested", "reason": "no controller deployment selected"}
    else:
        try:
            deployment = get("deployments.apps", args.controller_namespace, args.controller_deployment)
            if deployment is None:
                checks["controller"] = {
                    "outcome": "unsupported",
                    "reason": "selected controller deployment does not exist",
                }
            else:
                status = deployment.get("status", {})
                desired = deployment.get("spec", {}).get("replicas", 1)
                available = (
                    desired > 0
                    and status.get("replicas", 0) == desired
                    and status.get("updatedReplicas", 0) == desired
                    and status.get("availableReplicas", 0) >= desired
                    and status.get("observedGeneration", 0) >= deployment["metadata"].get("generation", 1)
                )
                checks["controller"] = {
                    "outcome": "pass" if available else "fail",
                    "reason": "deployment availability observed; reconciliation behavior not tested",
                }
                report["controller"] = {
                    "namespace": args.controller_namespace,
                    "deployment": args.controller_deployment,
                    "uid": deployment["metadata"]["uid"],
                    "images": [
                        container["image"]
                        for container in deployment.get("spec", {})
                        .get("template", {})
                        .get("spec", {})
                        .get("containers", [])
                    ],
                }
                # Follow Deployment -> ReplicaSet -> Pod UIDs; matching labels alone
                # could attribute unrelated Pods' image digests to this controller.
                replicasets = get("replicasets.apps", args.controller_namespace) or {}
                uids = {
                    item["metadata"]["uid"]
                    for item in replicasets.get("items", [])
                    if _owned_by(item, {deployment["metadata"]["uid"]})
                }
                pods = get("pods", args.controller_namespace) or {}
                report["controller"]["image_ids"] = sorted(
                    {
                        container["imageID"]
                        for pod in pods.get("items", [])
                        if _owned_by(pod, uids)
                        for container in pod.get("status", {}).get("containerStatuses", [])
                        if container.get("imageID")
                    }
                )
                checks["controller_provenance"] = {
                    "outcome": "pass" if report["controller"]["image_ids"] else "inconclusive",
                    "reason": "observed image IDs from controller-owned Pods",
                }
        except ObservationUnavailable as error:
            checks.setdefault("controller", {"outcome": "inconclusive", "reason": str(error)})
            checks["controller_provenance"] = {"outcome": "inconclusive", "reason": str(error)}
    return report


def main(argv: list[str] | None = None) -> int:
    """Write a JSON report; nonzero means an attempted check did not pass."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--resource", choices=tuple(_RESOURCES), required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--controller-namespace")
    parser.add_argument("--controller-deployment")
    args = parser.parse_args(argv)
    if bool(args.controller_namespace) != bool(args.controller_deployment):
        parser.error("select both --controller-namespace and --controller-deployment")
    for key in ("context", "namespace", "name", "controller_namespace", "controller_deployment"):
        value = getattr(args, key)
        if value is not None and (not value.strip() or value.startswith("-")):
            parser.error(f"--{key.replace('_', '-')} must be a nonempty value, not a flag")
    if not args.probe.is_file() or not args.kubeconfig.is_file():
        parser.error("--probe and --kubeconfig must point to existing files")
    report = inspect_cluster(args)
    print(json.dumps(report, indent=2))
    return int(any(check["outcome"] in {"fail", "unsupported", "inconclusive"} for check in report["checks"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
