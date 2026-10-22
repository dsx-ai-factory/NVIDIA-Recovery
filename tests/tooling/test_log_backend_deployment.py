# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cluster-free contracts for local log-store selection and native queries."""

from __future__ import annotations

import json
import os
import re
import runpy
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml

ROOT = Path(__file__).parents[2]
E2E = ROOT / "scripts/ci/e2e"


@pytest.mark.parametrize("backend", [None, "loki", "victorialogs"])
def test_environment_exports_selected_backend(backend: str | None) -> None:
    env = {key: value for key, value in os.environ.items() if key != "E2E_LOG_BACKEND"}
    if backend is not None:
        env["E2E_LOG_BACKEND"] = backend
    result = subprocess.run(
        ["bash", "-c", 'source "$1"; bash -c \'echo "$E2E_LOG_BACKEND"\'', "test", str(E2E / "env.sh")],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.strip() == (backend or "loki")


@pytest.mark.parametrize("backend", ["", "victoria", "LOKI"])
def test_invalid_backend_fails_before_cluster_preparation(backend: str) -> None:
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/tilt/cluster.sh"), "up", "echo", "prepared"],
        env={**os.environ, "E2E_LOG_BACKEND": backend},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "invalid E2E_LOG_BACKEND=" in result.stderr
    assert "prepared" not in result.stdout


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
@pytest.mark.parametrize("source", ["alloy-config.river", "alloy-application-config.river"])
def test_alloy_renders_selected_destination_only(tmp_path: Path, backend: str, source: str) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text('#!/bin/sh\nif [ "$1" = apply ]; then cat >/dev/null; fi\n')
    kubectl.chmod(0o755)
    destination = tmp_path / "alloy.river"
    subprocess.run(
        [
            str(ROOT / "deploy/tilt/apply-alloy-config.sh"),
            str(E2E / "services/values" / source),
            str(destination),
            backend,
            source.removesuffix(".river"),
        ],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        check=True,
    )
    rendered = destination.read_text()
    assert "__LOG_PUSH_URL__" not in rendered
    if backend == "victorialogs":
        assert "http://victorialogs.monitoring.svc:9428/insert/loki/api/v1/push?disable_message_parsing=1" in rendered
        assert "loki-gateway" not in rendered
    else:
        assert "http://loki-gateway.monitoring.svc/loki/api/v1/push" in rendered
        assert "victorialogs" not in rendered


def _provider_queries() -> tuple[dict, dict]:
    loki = yaml.safe_load((ROOT / "deploy/helm/watcher/values.yaml").read_text())
    victoria = yaml.safe_load((E2E / "values/stack-local-victorialogs.yaml").read_text())
    return (
        loki["global"]["log_query"]["providers"]["loki"]["queries"]["watcher"]["workflows"],
        victoria["global"]["log_query"]["providers"]["victorialogs"]["queries"]["watcher"]["workflows"],
    )


def _matches_native_queries(queries: list, line: str, backend: str) -> bool:
    for query in queries:
        if isinstance(query, dict):
            query = next(iter(query.values()))
        if backend == "loki":
            clauses = re.findall(r'\|([=~])\s+(["`])(.*?)\2', query)
            results = [pattern in line if op == "=" else re.search(pattern, line) for op, _, pattern in clauses]
        else:
            patterns = re.findall(r'_msg:~("(?:\\.|[^"\\])*")', query)
            results = [re.search(json.loads(pattern), line) for pattern in patterns]
        if results and all(results):
            return True
    return False


def test_native_variants_cover_every_default_workflow_and_state() -> None:
    loki, victoria = _provider_queries()
    assert set(victoria) == set(loki)
    for workflow, queries in loki.items():
        native = victoria[workflow]["query"]
        assert len(native) == len(queries["query"])
        for original, selected in zip(queries["query"], native, strict=True):
            if isinstance(original, dict):
                assert selected.keys() == original.keys()
                selected = next(iter(selected.values()))
            assert selected.startswith("_msg:~")
            assert "|=" not in selected
    assert victoria["noprogress"]["nooutput_validation_query"] == 'job_id:="{job_id}"'


@pytest.mark.parametrize(
    ("workflow", "message"),
    [
        ("straggler", "throughput per GPU (TFLOP/s/GPU): 22.0"),
        ("noprogress", "before the start of training step"),
        ("hangpushrecv", "pushrecv: expected something"),
        ("hang_app", "Disk quota exceeded"),
        ("crash", "RuntimeError: something failed"),
        ("crash", "CUDA error: uncorrectable ECC error"),
        ("chkpt_load", "Loaded checkpoint from disk"),
        ("chkpt_save", "Saved checkpoint to disk"),
        ("synthetic_application_log_events", "heartbeat_application_logs"),
    ],
)
def test_native_filter_semantics_preserve_positive_and_negative_markers(workflow: str, message: str) -> None:
    loki, victoria = _provider_queries()

    for sample, expected in [(message, True), ("ordinary healthy progress output", False)]:
        assert _matches_native_queries(loki[workflow]["query"], sample, "loki") == expected
        assert _matches_native_queries(victoria[workflow]["query"], sample, "victorialogs") == expected


def test_victorialogs_deployment_is_disposable_and_ready_before_collection() -> None:
    manifest = list(yaml.safe_load_all((ROOT / "deploy/tilt/services/victorialogs.yaml").read_text()))
    deployment = next(item for item in manifest if item["kind"] == "Deployment")
    spec = deployment["spec"]["template"]["spec"]
    container = spec["containers"][0]
    assert container["image"] == "victoriametrics/victoria-logs:v1.52.0"
    assert "-retentionPeriod=1d" in container["args"]
    assert container["readinessProbe"]["httpGet"]["path"] == "/health"
    assert spec["volumes"] == [{"name": "data", "emptyDir": {}}]
    service = next(item for item in manifest if item["kind"] == "Service")
    assert service["metadata"]["namespace"] == "monitoring"
    assert service["spec"]["ports"][0]["port"] == 9428


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
@pytest.mark.parametrize("attribution", ["unavailable", "available"])
@pytest.mark.parametrize("release", ["nvidia-recovery", "recovery-qualification", "notifications-e2e"])
def test_tilt_graph_selects_one_store_and_applies_effective_backend_last(
    backend: str, attribution: str, release: str
) -> None:
    from types import SimpleNamespace

    calls: list[tuple[str, tuple, dict]] = []

    def record(name: str):
        def stub(*args, **kwargs):
            calls.append((name, args, kwargs))
            return ""

        return stub

    environment = {
        "E2E_LOG_BACKEND": backend,
        "E2E_ATTRIBUTION_MODE": attribution,
        "HELM_RELEASE": release,
        "DEPLOY_INFRA": "1",
        "DEPLOY_STACK": "1",
    }
    # Capture graph declarations without invoking builds, source fetches, Helm,
    # or kubectl. The production Tiltfile is also valid Python for this subset.
    symbols = {
        name: record(name)
        for name in [
            "load",
            "fail",
            "update_settings",
            "local",
            "local_resource",
            "helm_repo",
            "helm_resource",
            "k8s_yaml",
            "k8s_resource",
            "watch_file",
            "custom_build",
            "docker_build",
            "helm",
        ]
    }
    symbols["os"] = SimpleNamespace(environ=environment)
    symbols["read_file"] = lambda path: (ROOT / path).read_text()
    symbols["blob"] = lambda text: text
    runpy.run_path(str(ROOT / "Tiltfile"), init_globals=symbols)
    assert not any(name == "fail" for name, _, _ in calls)
    resources = {
        (args[0] if args else kwargs["workload"]): kwargs
        for name, args, kwargs in calls
        if name in {"helm_resource", "k8s_resource"}
    }
    assert backend in resources
    assert ("victorialogs" if backend == "loki" else "loki") not in resources
    assert backend in resources["alloy"]["resource_deps"]
    values = next(
        kwargs["values"] for name, args, kwargs in calls if name == "helm" and args[0] == "deploy/helm/nvidia-recovery"
    )
    effective_backend = None
    for value_file in values:
        contents = yaml.safe_load((ROOT / value_file).read_text())
        effective_backend = contents.get("global", {}).get("log_query", {}).get("backend", effective_backend)
    assert effective_backend == backend
    attribution_file = "deploy/tilt/services/attribution/stack-values.yaml"
    assert (attribution_file in values) == (attribution == "available")
    stack = next(kwargs for name, args, kwargs in calls if name == "helm" and args[0] == "deploy/helm/nvidia-recovery")
    settings = dict(setting.split("=", 1) for setting in stack["set"])
    notifications = {
        "nvidia-recovery": "nvidia-recovery-notifications",
        "recovery-qualification": "recovery-qualification-notifications",
        "notifications-e2e": "notifications-e2e",
    }[release]
    assert stack["name"] == release
    for key in (
        "job-manager.event_bus.host",
        "watcher.config.datasources.rabbitmq.publisher.host",
        "anomaly-detection-isolator.event_bus.host",
    ):
        assert settings[key] == f"{release}-rabbitmq"
    assert settings["api-gateway.audit.endpoint"] == f"http://{notifications}:8080"
    assert settings["dashboard.app.apiGatewayURL"] == f"http://{release}-api-gateway:8080"
    advisor = next(
        kwargs for name, args, kwargs in calls if name == "helm" and args[0] == "deploy/helm/slurm-advisor-controller"
    )
    assert f"controller.heimdallEndpoint={release}-api-gateway-internal:9090" in advisor["set"]
    assert resources[f"{release}-dashboard"]["port_forwards"] == ["3001:3001"]
    assert notifications in resources
    dependencies = resources["slurm-advisor-controller"]["resource_deps"]
    assert notifications in dependencies
    assert (f"{release}-attribution-service" in dependencies) == (attribution == "available")

    # Services export logs and traces to the local collector, which must deliver logs to the store the suite reads.
    assert "scripts/ci/e2e/values/otlp.yaml" in values
    assert "scripts/ci/e2e/values/otlp.yaml" in advisor["values"]
    assert "otel-collector" in resources[f"{release}-job-manager"]["resource_deps"]
    collector = next(
        document
        for name, args, _ in calls
        if name == "k8s_yaml" and "kind: ConfigMap" in args[0]
        for document in yaml.safe_load_all(args[0])
        if document and document["kind"] == "ConfigMap" and document["metadata"]["name"] == "otel-collector"
    )
    exporters = yaml.safe_load(collector["data"]["config.yaml"])["exporters"]
    store = "loki-gateway" if backend == "loki" else "victorialogs"
    assert urlsplit(exporters["otlphttp/logs"]["logs_endpoint"]).hostname == f"{store}.monitoring.svc.cluster.local"

    readiness = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s\\n%s" "$E2E_STACK_DEPLOYMENTS" "$E2E_STACK_STATEFULSETS"',
            "test",
            str(E2E / "env.sh"),
        ],
        env={"PATH": os.environ["PATH"], **environment},
        text=True,
        capture_output=True,
        check=True,
    )
    assert set(readiness.stdout.split()) <= set(resources)


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_nooutput_fixture_arms_training_loop_for_selected_backend(backend: str) -> None:
    fixture = (ROOT / "tests/e2e/scenarios/31-no-output-hang/job_output.txt").read_text().splitlines()
    provider = dict(zip(("loki", "victorialogs"), _provider_queries(), strict=True))[backend]
    filters = [query["TRAINING_LOOP"] for query in provider["noprogress"]["query"] if "TRAINING_LOOP" in query]
    values = yaml.safe_load((ROOT / "deploy/helm/watcher/values.yaml").read_text())
    workflow = next(item for item in values["config"]["anomaly_workflow_definitions"] if item["name"] == "noprogress")
    cache_patterns = [
        entry["TRAINING_LOOP"] for entry in workflow["attributes"]["query"]["regex"] if "TRAINING_LOOP" in entry
    ]

    assert any(
        _matches_native_queries(filters, line, backend) and any(re.search(pattern, line) for pattern in cache_patterns)
        for line in fixture
    ), f"no-output fixture never enters TRAINING_LOOP through {backend} collection and the production cache parser"


@pytest.mark.parametrize(
    "configmap,workload",
    (("alloy-config", "daemonset.apps/alloy"), ("alloy-application-config", "deployment.apps/alloy-application")),
)
def test_updated_pipeline_restarts_and_waits_for_its_existing_owner(
    tmp_path: Path, configmap: str, workload: str
) -> None:
    """External ConfigMaps must reach the running collector without a reloader."""

    calls = tmp_path / "calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$CALLS"\n'
        'case "$1" in\n'
        '  get) printf "%s\\n" "$WORKLOAD" ;;\n'
        "  apply) cat >/dev/null ;;\n"
        "esac\n"
    )
    kubectl.chmod(0o755)
    subprocess.run(
        [
            str(ROOT / "deploy/tilt/apply-alloy-config.sh"),
            str(E2E / "services/values/alloy-config.river"),
            str(tmp_path / "alloy.river"),
            "loki",
            configmap,
        ],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "CALLS": str(calls), "WORKLOAD": workload},
        check=True,
    )
    commands = [line.split() for line in calls.read_text().splitlines()]
    lookup = next(command for command in commands if command[0] == "get")
    assert f"app.kubernetes.io/instance={configmap.removesuffix('-config')}" in lookup
    rollouts = [command[:3] for command in commands if command[0] == "rollout"]
    assert rollouts == [["rollout", "restart", workload], ["rollout", "status", workload]]
    assert max(index for index, command in enumerate(commands) if command[0] == "apply") < next(
        index for index, command in enumerate(commands) if command[:2] == ["rollout", "restart"]
    )


def test_pipeline_update_fails_when_existing_owner_lookup_fails(tmp_path: Path) -> None:
    """An API/RBAC failure must not report a successful live config update."""

    kubectl = tmp_path / "kubectl"
    kubectl.write_text('#!/bin/sh\ncase "$1" in get) exit 23 ;; apply) cat >/dev/null ;; esac\n')
    kubectl.chmod(0o755)
    result = subprocess.run(
        [
            str(ROOT / "deploy/tilt/apply-alloy-config.sh"),
            str(E2E / "services/values/alloy-config.river"),
            str(tmp_path / "alloy.river"),
        ],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        check=False,
    )
    assert result.returncode == 23


@pytest.mark.parametrize(
    "context,owner", (("kind-qualified", "qualified"), ("external", ""), ("kind-qualified", "other"))
)
def test_existing_cluster_positions_bootstrap_requires_kind_ownership(tmp_path: Path, context: str, owner: str) -> None:
    """Reuse repairs owned Kind nodes, while external or mismatched nodes are untouched."""

    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.touch()
    calls = tmp_path / "docker-calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        '#!/bin/sh\ncase "$1" in config) printf "%s\\n" "$CONTEXT" ;; get) echo qualified-worker ;; esac\n'
    )
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALLS"\nif [ "$1" = inspect ]; then printf "%s\\n" "$OWNER"; fi\n'
    )
    kubectl.chmod(0o755)
    docker.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", 'source "$1"; cluster_up', "test", str(E2E / "cluster.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "KUBECONFIG": str(kubeconfig),
            "TOOLS_DIR": str(tmp_path),
            "DEPLOY_KIND_CLUSTER": "",
            "DEPLOY_INFRA": "1",
            "CLUSTER_NAME": "qualified",
            "CONTEXT": context,
            "OWNER": owner,
            "CALLS": str(calls),
        },
        text=True,
        capture_output=True,
        check=False,
    )
    commands = calls.read_text().splitlines() if calls.exists() else []
    mutations = [command for command in commands if command.startswith("exec ")]
    if context == "kind-qualified" and owner == "qualified":
        assert result.returncode == 0, result.stderr
        assert len(mutations) == 1
        assert "qualified-worker install -d -o 401 -g 401 -m 0750 /var/lib/alloy-system-positions" in mutations[0]
    elif context == "external":
        assert result.returncode == 0, result.stderr
        assert not commands
        assert "sudo install -d" in result.stdout + result.stderr
    else:
        assert result.returncode != 0
        assert not mutations


@pytest.mark.parametrize("workers,lookup_status", [("", 0), (" \n", 0), ("qualified-worker", 23)])
@pytest.mark.parametrize("conditional", [False, True])
def test_positions_bootstrap_rejects_missing_or_failed_worker_lookup(
    tmp_path: Path, workers: str, lookup_status: int, conditional: bool
) -> None:
    """Failed discovery cannot report success or touch Docker, even without errexit."""

    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.touch()
    calls = tmp_path / "docker-calls"
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        'case "$1" in\n'
        "  config) echo kind-qualified ;;\n"
        '  get) printf "%s" "$WORKERS"; exit "$LOOKUP_STATUS" ;;\n'
        "esac\n"
    )
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALLS"\necho qualified\n')
    kubectl.chmod(0o755)
    docker.chmod(0o755)
    # Bash suppresses errexit throughout functions invoked as conditions.
    invocation = "if cluster_up; then exit 0; else exit 1; fi" if conditional else "cluster_up"
    result = subprocess.run(
        ["bash", "-c", 'source "$1"; ' + invocation, "test", str(E2E / "cluster.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "KUBECONFIG": str(kubeconfig),
            "TOOLS_DIR": str(tmp_path),
            "DEPLOY_KIND_CLUSTER": "",
            "DEPLOY_INFRA": "1",
            "CLUSTER_NAME": "qualified",
            "WORKERS": workers,
            "LOOKUP_STATUS": str(lookup_status),
            "CALLS": str(calls),
        },
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert not calls.exists(), "worker discovery must succeed before any Docker calls"
