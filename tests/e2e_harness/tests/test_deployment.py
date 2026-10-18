# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cluster-free red checks for deployment-conformance oracles."""

from __future__ import annotations

import json

import pytest
from e2e_harness.deployment import evaluate_collector_deployment, evaluate_shared_log_permissions


def _pod(instance: str, node_name: str) -> dict:
    is_syslog = instance == "alloy"
    volumes = (
        [
            {"name": "node-syslog", "hostPath": {"path": "/var/log/syslog", "type": "FileOrCreate"}},
            {"name": "positions", "hostPath": {"path": "/var/lib/alloy-system-positions", "type": "Directory"}},
        ]
        if is_syslog
        else [
            {"name": "log-links", "persistentVolumeClaim": {"claimName": "slurm-log-links"}},
            {"name": "log-sources", "persistentVolumeClaim": {"claimName": "slurm-job-logs"}},
            {"name": "positions", "persistentVolumeClaim": {"claimName": "alloy-application-positions"}},
        ]
    )
    source_mounts = (
        [{"name": "node-syslog", "mountPath": "/var/log/syslog", "readOnly": True}]
        if is_syslog
        else [
            {"name": "log-links", "mountPath": "/home/log-links", "readOnly": True},
            {"name": "log-sources", "mountPath": "/home/logs", "readOnly": True},
        ]
    )
    return {
        "metadata": {
            "labels": {"app.kubernetes.io/name": "alloy", "app.kubernetes.io/instance": instance},
            "ownerReferences": [{"kind": "DaemonSet" if is_syslog else "ReplicaSet", "controller": True}],
        },
        "spec": {
            "nodeName": node_name,
            "serviceAccountName": "alloy",
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 401,
                "runAsGroup": 401,
                "fsGroup": 401,
            },
            "containers": [
                {
                    "name": "alloy",
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [
                        {"name": "config", "mountPath": "/etc/alloy"},
                        {"name": "positions", "mountPath": "/var/lib/alloy-positions"},
                        *source_mounts,
                    ],
                }
            ],
            "volumes": [{"name": "config", "configMap": {"name": f"{instance}-config"}}, *volumes],
        },
    }


def _pods() -> dict:
    return {
        "items": [
            _pod("alloy", "heimdall-e2e-worker"),
            _pod("alloy", "heimdall-e2e-worker2"),
            _pod("alloy-application", "heimdall-e2e-worker"),
        ]
    }


def _deployment() -> dict:
    return {
        "metadata": {"name": "alloy-application"},
        "spec": {"replicas": 1, "strategy": {"type": "Recreate"}},
    }


def test_collector_claim_and_sandbox_contract_is_observed() -> None:
    evidence = evaluate_collector_deployment(
        json.dumps(_pods()),
        json.dumps({"automountServiceAccountToken": False}),
        json.dumps(_deployment()),
    )

    assert evidence.observed, evidence.summary


@pytest.mark.parametrize(
    "mutation",
    (
        "application-host-path",
        "syslog-shared-claim",
        "writable-source",
        "writable-syslog",
        "broad-syslog",
        "volatile-syslog-positions",
        "volatile-application-positions",
        "token",
        "wrong-group",
        "same-node",
        "invalid-node",
        "duplicate-application",
        "missing-syslog-worker",
        "unknown-instance",
        "wrong-controller",
        "application-replicas",
        "application-rolling-update",
    ),
)
def test_collector_conformance_goes_red_for_mutations(mutation: str) -> None:
    pods = _pods()
    service_account = {"automountServiceAccountToken": False}
    deployment = _deployment()
    syslog = pods["items"][0]["spec"]
    application = pods["items"][2]["spec"]
    if mutation == "application-host-path":
        application["volumes"][1] = {"name": "log-links", "hostPath": {"path": "/tmp/links"}}
    elif mutation == "syslog-shared-claim":
        syslog["volumes"].append(application["volumes"][1])
    elif mutation == "writable-source":
        application["containers"][0]["volumeMounts"][3]["readOnly"] = False
    elif mutation == "writable-syslog":
        syslog["containers"][0]["volumeMounts"][2]["readOnly"] = False
    elif mutation == "broad-syslog":
        syslog["volumes"][1]["hostPath"]["path"] = "/var/log"
    elif mutation == "volatile-syslog-positions":
        syslog["volumes"][2] = {"name": "positions", "emptyDir": {}}
    elif mutation == "volatile-application-positions":
        application["volumes"][3] = {"name": "positions", "emptyDir": {}}
    elif mutation == "token":
        service_account["automountServiceAccountToken"] = True
    elif mutation == "wrong-group":
        syslog["securityContext"]["fsGroup"] = 999
    elif mutation == "same-node":
        pods["items"][1]["spec"]["nodeName"] = "heimdall-e2e-worker"
    elif mutation == "invalid-node":
        syslog["nodeName"] = 7
    elif mutation == "duplicate-application":
        pods["items"].append(_pod("alloy-application", "heimdall-e2e-worker2"))
    elif mutation == "missing-syslog-worker":
        pods["items"][1] = _pod("alloy-application", "heimdall-e2e-worker2")
    elif mutation == "unknown-instance":
        pods["items"][2]["metadata"]["labels"]["app.kubernetes.io/instance"] = "unexpected"
    elif mutation == "wrong-controller":
        pods["items"][0]["metadata"]["ownerReferences"][0]["kind"] = "ReplicaSet"
    elif mutation == "application-replicas":
        deployment["spec"]["replicas"] = 2
    else:
        deployment["spec"]["strategy"]["type"] = "RollingUpdate"

    evidence = evaluate_collector_deployment(json.dumps(pods), json.dumps(service_account), json.dumps(deployment))

    assert not evidence.observed


@pytest.mark.parametrize("mutation", ("1777 0:0", "2770 0:0", "750 0:401"))
def test_shared_log_permission_contract_goes_red_for_mutations(mutation: str) -> None:
    assert evaluate_shared_log_permissions("2770 0:401\n").observed
    assert not evaluate_shared_log_permissions(mutation).observed
