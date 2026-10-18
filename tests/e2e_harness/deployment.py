# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deployment-conformance oracles kept separate from workload scenarios."""

from __future__ import annotations

import json

from e2e_harness.recovery import CausalEvidence


def evaluate_collector_deployment(pods_text: str, service_account_text: str, deployment_text: str) -> CausalEvidence:
    """Check runtime collector ownership, durable positions, and restricted mounts."""
    try:
        pod_items = json.loads(pods_text)["items"]
        service_account = json.loads(service_account_text)
        deployment = json.loads(deployment_text)
        if len(pod_items) != 3:
            return CausalEvidence(False, f"Alloy pods={len(pod_items)}, want two syslog and one application collector")
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        return CausalEvidence(False, f"invalid Alloy runtime manifest: {error}")

    failures: list[str] = []
    if service_account.get("automountServiceAccountToken") is not False:
        failures.append("service-account token automount is not disabled")
    deployment_spec = deployment.get("spec", {})
    if (
        deployment.get("metadata", {}).get("name") != "alloy-application"
        or deployment_spec.get("replicas") != 1
        or deployment_spec.get("strategy", {}).get("type") != "Recreate"
    ):
        failures.append("application collector must be a single-replica Recreate Deployment")

    observed_nodes: set[str] = set()
    instances = {"alloy": 0, "alloy-application": 0}
    for item in pod_items:
        try:
            metadata = item["metadata"]
            instance = metadata["labels"]["app.kubernetes.io/instance"]
            pod_spec = item["spec"]
            node = pod_spec["nodeName"]
            containers = pod_spec["containers"]
            volumes = {volume["name"]: volume for volume in pod_spec["volumes"]}
            mounts = {mount["name"]: mount for mount in containers[0]["volumeMounts"]}
        except (IndexError, KeyError, TypeError) as error:
            failures.append(f"invalid Alloy pod manifest: {error}")
            continue
        if not isinstance(node, str) or not node:
            failures.append(f"invalid Alloy pod nodeName: {node!r}")
            continue
        prefix = f"instance={instance}, node={node}: "
        if instance not in instances:
            failures.append(prefix + "unexpected collector instance")
            continue
        instances[instance] += 1
        is_syslog = instance == "alloy"
        if is_syslog:
            observed_nodes.add(node)
        expected_owner = "DaemonSet" if is_syslog else "ReplicaSet"
        if not any(
            owner.get("kind") == expected_owner and owner.get("controller") is True
            for owner in metadata.get("ownerReferences", [])
        ):
            failures.append(prefix + f"missing {expected_owner} controller owner")
        pod_security = pod_spec.get("securityContext", {})
        container_security = containers[0].get("securityContext", {}) if len(containers) == 1 else {}
        if pod_spec.get("serviceAccountName") != "alloy":
            failures.append(prefix + f"serviceAccountName={pod_spec.get('serviceAccountName')!r}")
        if len(containers) != 1 or containers[0].get("name") != "alloy":
            failures.append(prefix + f"containers={[container.get('name') for container in containers]!r}")
        if (
            pod_security.get("runAsNonRoot") is not True
            or pod_security.get("runAsUser") != 401
            or pod_security.get("runAsGroup") != 401
            or pod_security.get("fsGroup") != 401
        ):
            failures.append(prefix + f"podSecurityContext={pod_security!r}")
        if (
            container_security.get("allowPrivilegeEscalation") is not False
            or container_security.get("readOnlyRootFilesystem") is not True
            or container_security.get("capabilities", {}).get("drop") != ["ALL"]
        ):
            failures.append(prefix + f"containerSecurityContext={container_security!r}")

        expected_claims = (
            {}
            if is_syslog
            else {
                "log-links": "slurm-log-links",
                "log-sources": "slurm-job-logs",
                "positions": "alloy-application-positions",
            }
        )
        for name, claim_name in expected_claims.items():
            actual = volumes.get(name, {}).get("persistentVolumeClaim", {}).get("claimName")
            if actual != claim_name:
                failures.append(prefix + f"{name} claim={actual!r}")
        if "configMap" not in volumes.get("config", {}):
            failures.append(prefix + "config volume is not a ConfigMap")
        if is_syslog:
            for name, source in (
                ("node-syslog", {"path": "/var/log/syslog", "type": "FileOrCreate"}),
                ("positions", {"path": "/var/lib/alloy-system-positions", "type": "Directory"}),
            ):
                actual = volumes.get(name, {}).get("hostPath", {})
                if actual != source:
                    failures.append(prefix + f"{name} source={actual!r}")
        unexpected_claims = [
            name
            for name, volume in volumes.items()
            if "persistentVolumeClaim" in volume and name not in expected_claims
        ]
        if unexpected_claims:
            failures.append(prefix + f"unexpected PVC volumes={unexpected_claims!r}")
        allowed_host_paths = {"node-syslog", "positions"} if is_syslog else set()
        unexpected_host_paths = [
            name for name, volume in volumes.items() if "hostPath" in volume and name not in allowed_host_paths
        ]
        if unexpected_host_paths:
            failures.append(prefix + f"unexpected hostPath volumes={unexpected_host_paths!r}")

        source_mounts = (
            (("node-syslog", "/var/log/syslog"),)
            if is_syslog
            else (("log-links", "/home/log-links"), ("log-sources", "/home/logs"))
        )
        for name, path in source_mounts:
            mount = mounts.get(name, {})
            if mount.get("mountPath") != path or mount.get("readOnly") is not True:
                failures.append(prefix + f"{name} mount={mount!r}")
        positions = mounts.get("positions", {})
        if positions.get("mountPath") != "/var/lib/alloy-positions" or positions.get("readOnly", False) is not False:
            failures.append(prefix + f"positions mount={positions!r}")

    if instances != {"alloy": 2, "alloy-application": 1}:
        failures.append(f"collector instances={instances!r}, want two syslog and one application collector")
    if (
        len(observed_nodes) != 2
        or not any(node.endswith("-worker") for node in observed_nodes)
        or not any(node.endswith("-worker2") for node in observed_nodes)
    ):
        failures.append(f"collector nodes={sorted(observed_nodes)!r}, want distinct Kind worker and worker2 targets")

    summary = "Alloy collector deployment conforms" if not failures else "; ".join(failures)
    return CausalEvidence(not failures, summary)


def evaluate_shared_log_permissions(stat_text: str) -> CausalEvidence:
    """Validate the runtime source-directory mode and collector group."""
    actual = stat_text.strip()
    expected = "2770 0:401"
    if actual != expected:
        return CausalEvidence(False, f"shared log permissions={actual!r}, want {expected!r}")
    return CausalEvidence(True, "shared log directory enforces root:401 setgid access")
