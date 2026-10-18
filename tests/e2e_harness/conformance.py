# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check that the deployed log collection matches what the scenarios rely on, before any scenario runs.

Reads SLURM_NS (default `slurm`), E2E_LOG_BACKEND, E2E_ATTRIBUTION_MODE, and KUBECONFIG. Exits 1 when any check
fails.
"""

from __future__ import annotations

import functools
import json
import os
import subprocess
import sys
from collections.abc import Callable
from typing import Any

from e2e_harness.deployment import evaluate_collector_deployment, evaluate_shared_log_permissions
from e2e_harness.recovery import CausalEvidence

LOGIN_POD = "app.kubernetes.io/component=login"


def _kubectl(namespace: str, *arguments: str) -> str:
    result = subprocess.run(
        ("kubectl", "--namespace", namespace, *arguments), capture_output=True, text=True, timeout=60, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(arguments)} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def collector_deployment(namespace: str) -> CausalEvidence:
    return evaluate_collector_deployment(
        _kubectl(namespace, "get", "pods", "--selector", "app.kubernetes.io/name=alloy", "--output", "json"),
        _kubectl(namespace, "get", "serviceaccount", "alloy", "--output", "json"),
        _kubectl(namespace, "get", "deployment", "alloy-application", "--output", "json"),
    )


def shared_log_permissions(namespace: str) -> CausalEvidence:
    login = _kubectl(
        namespace, "get", "pods", "--selector", LOGIN_POD, "--output", "jsonpath={.items[0].metadata.name}"
    ).strip()
    if not login:
        return CausalEvidence(False, "Slurm login pod was not found")
    return evaluate_shared_log_permissions(
        _kubectl(namespace, "exec", login, "--", "stat", "-c", "%a %u:%g", "/home/logs")
    )


def log_backend_selection(get: Callable[..., Any], *, log_backend: str, attribution_mode: str) -> CausalEvidence:
    """Reject a harness selector that differs from the deployed writer or readers.

    `get(*arguments)` returns the parsed JSON of `kubectl get *arguments -o json` in the Slurm namespace.
    """

    failures: list[str] = []
    names = {"watcher"}
    if attribution_mode == "available":
        names.add("attribution-adapter")
    for name in sorted(names):
        deployments = get("deployments", "-l", f"app.kubernetes.io/name={name}")["items"]
        if not deployments:
            failures.append(f"no deployed {name} reader")
        for deployment in deployments:
            pod_spec = deployment["spec"]["template"]["spec"]
            selectors = [
                entry.get("value")
                for container in pod_spec["containers"]
                for entry in container.get("env", [])
                if entry["name"] == "LOG_QUERY_BACKEND"
            ]
            if name == "attribution-adapter":
                # The adapter reads dotenv configuration from a mounted
                # ConfigMap; Watcher uses its explicit environment selector.
                config_name = next(
                    volume["configMap"]["name"] for volume in pod_spec["volumes"] if volume["name"] == "config"
                )
                settings = get("configmap", config_name)["data"]["settings.env"]
                selectors += [
                    line.partition("=")[2].strip()
                    for line in settings.splitlines()
                    if line.startswith("log_query_backend=")
                ]
            if selectors != [log_backend]:
                failures.append(f"{name}: deployed selector {selectors!r}")

    config_maps = ("alloy-config", "alloy-application-config")
    collectors = [get("configmap", name)["data"]["config.alloy"] for name in config_maps]
    services = {item["metadata"]["name"] for item in get("services", "-n", "monitoring")["items"]}
    if log_backend == "victorialogs":
        if "victorialogs" not in services or any(name == "loki" or name.startswith("loki-") for name in services):
            failures.append(f"log stores {sorted(services)}, want victorialogs alone")
        for alloy in collectors:
            if (
                "victorialogs.monitoring.svc" not in alloy
                or "/insert/loki/api/v1/push?disable_message_parsing=1" not in alloy
                or "loki-gateway" in alloy
            ):
                failures.append("a collector does not write to VictoriaLogs alone")
    else:
        if "loki-gateway" not in services or "victorialogs" in services:
            failures.append(f"log stores {sorted(services)}, want loki-gateway alone")
        for alloy in collectors:
            if "loki-gateway.monitoring.svc" not in alloy or "victorialogs.monitoring.svc" in alloy:
                failures.append("a collector does not write to Loki alone")
    if failures:
        return CausalEvidence(False, "; ".join(failures))
    return CausalEvidence(True, f"readers and collectors use {log_backend}")


def main() -> int:
    namespace = os.environ.get("SLURM_NS") or "slurm"
    selection = functools.partial(
        log_backend_selection,
        lambda *arguments: json.loads(_kubectl(namespace, "get", *arguments, "-o", "json")),
        log_backend=os.environ.get("E2E_LOG_BACKEND") or "loki",
        attribution_mode=os.environ.get("E2E_ATTRIBUTION_MODE") or "available",
    )
    failed = False
    for name, check in (
        ("collector deployment", functools.partial(collector_deployment, namespace)),
        ("shared log permissions", functools.partial(shared_log_permissions, namespace)),
        ("log backend selection", selection),
    ):
        try:
            evidence = check()
        except RuntimeError as error:
            evidence = CausalEvidence(False, str(error))
        print(f"{'PASS' if evidence.observed else 'FAIL'}  {name}: {evidence.summary}")
        failed |= not evidence.observed
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
