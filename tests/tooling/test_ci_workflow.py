# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for E2E scenario ownership and credential boundaries in GitHub CI."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parents[2]
SCENARIO_ROOT = REPO_ROOT / "tests" / "e2e" / "scenarios"
GITHUB_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "e2e-base.yml"
ATTRIBUTION_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "e2e-attribution.yml"
TILTFILE = REPO_ROOT / "Tiltfile"
E2E_ENV = REPO_ROOT / "scripts" / "ci" / "e2e" / "env.sh"
E2E_ENTRYPOINT = REPO_ROOT / "scripts" / "ci" / "e2e" / "e2e.sh"
STACK_VALUES = REPO_ROOT / "scripts" / "ci" / "e2e" / "values" / "stack-local.yaml"
ATTRIBUTION_VALUES = REPO_ROOT / "deploy" / "tilt" / "services" / "attribution" / "stack-values.yaml"
ROOT_PROJECT = REPO_ROOT / "pyproject.toml"
IMAGE_BUILDER = REPO_ROOT / "scripts" / "build-images.sh"
ATTRIBUTION_SERVICE_BUILDER = REPO_ROOT / "deploy" / "tilt" / "build-attribution-service.sh"
LOCAL_ATTRIBUTION_SERVICE_DOCKERFILE = REPO_ROOT / "deploy" / "tilt" / "Dockerfile.attribution-service"
ATTRIBUTION_SOURCE_URL = "git@github.com:dsx-ai-factory/attribution.git"
ATTRIBUTION_SOURCE_REF = "7d6dff168bc282f471828fc8468e02de0df17480"
ATTRIBUTION_SCENARIOS = {
    "03-attribution-modes",
    "10-simple-segfault",
    "55-checkpoint-stop-guardrail",
    "57-system-requeue-limit",
    "02-log-ingestion",
    "20-segfault-template",
    "40-fault-inject-1",
    "41-fault-inject-2",
    "42-fault-inject-xid-79",
}
DAG_COMMIT_SCENARIOS = {
    "35-dag-parent-termination",
    "36-dag-release",
    "39-dag-late-registration",
    "44-dag-workflow-name-scope",
    "45-dag-multi-parent-abort",
    "46-dag-stopped-parent",
}
DAG_NIGHTLY_SCENARIOS = {
    "37-dag-parent-recovery",
    "38-dag-parent-requeue-limit",
    "47-dag-initial-watchdog",
    "48-dag-retry-watchdog",
    "49-dag-diamond",
    "50-dag-observe-mode",
    "51-dag-restart-resilience",
    "52-dag-api-outage",
    "53-dag-terminal-precedence",
    "54-dag-idempotence",
}
QUALIFICATION_SCENARIOS = (
    {
        "29-dag-metadata-ingress",
        "31-no-output-hang",
        "32-requeue-limit-guardrail",
        "33-scheduler-cancellation",
        "34-scheduler-timeout",
    }
    | DAG_COMMIT_SCENARIOS
    | DAG_NIGHTLY_SCENARIOS
)


def _job_body(workflow: str, job_name: str) -> str:
    match = re.search(
        rf"(?ms)^(?P<indent> *){re.escape(job_name)}:\n"
        rf"(?P<body>.*?)(?=^(?P=indent)[\w.-]+:\n|\Z)",
        workflow,
    )
    assert match is not None, f"missing {job_name} job"
    return match.group("body")


def _scenario_allowlist(job_body: str) -> set[str]:
    match = re.search(r'^\s+E2E_SCENARIOS: "(?P<scenarios>[^"]+)"$', job_body, re.MULTILINE)
    assert match is not None, "missing E2E_SCENARIOS allowlist"
    return set(match.group("scenarios").split())


def _scenario_tests() -> dict[str, Path]:
    return {path.parent.name: path for path in SCENARIO_ROOT.glob("[0-9][0-9]-*/test_*.py")}


def test_ci_scenario_names_resolve_to_collected_scenario_directories() -> None:
    workflow = ATTRIBUTION_WORKFLOW.read_text()
    configured = _scenario_allowlist(_job_body(workflow, "e2e-attribution"))
    discovered = set(_scenario_tests())

    assert configured <= discovered
    assert QUALIFICATION_SCENARIOS <= discovered


def test_dag_scenarios_have_explicit_commit_and_nightly_tiers() -> None:
    assert DAG_COMMIT_SCENARIOS.isdisjoint(DAG_NIGHTLY_SCENARIOS)
    scenario_tests = _scenario_tests()

    for scenario in DAG_COMMIT_SCENARIOS:
        source = scenario_tests[scenario].read_text()
        assert "@pytest.mark.nightly" not in source, scenario

    for scenario in DAG_NIGHTLY_SCENARIOS:
        source = scenario_tests[scenario].read_text()
        test_count = source.count("\ndef test_")
        assert source.count("@pytest.mark.nightly") == test_count, scenario


def test_attribution_lane_runs_conditional_scenarios_from_pinned_source() -> None:
    job = _job_body(ATTRIBUTION_WORKFLOW.read_text(), "e2e-attribution")

    assert _scenario_allowlist(job) == ATTRIBUTION_SCENARIOS
    assert "E2E_ATTRIBUTION_MODE: available" in job
    assert ATTRIBUTION_SOURCE_URL in job
    assert ATTRIBUTION_SOURCE_REF in job


def test_e2e_surfaces_do_not_publish_legacy_provider_name() -> None:
    public_e2e_surfaces = (
        GITHUB_WORKFLOW,
        TILTFILE,
        E2E_ENV,
        E2E_ENTRYPOINT,
    )

    for path in public_e2e_surfaces:
        text = path.read_text().lower()
        assert "e2e_fact" not in text, path
        assert "real-fact" not in text, path
        assert "fact_image" not in text, path


def test_attribution_adapter_uses_the_pinned_provider_build() -> None:
    tiltfile = TILTFILE.read_text()
    image_builder = IMAGE_BUILDER.read_text()
    service_builder = ATTRIBUTION_SERVICE_BUILDER.read_text()
    stack_values = STACK_VALUES.read_text()
    attribution_values = ATTRIBUTION_VALUES.read_text()

    assert '"attribution_adapter",' in ROOT_PROJECT.read_text()
    assert "  attribution-adapter\n" in image_builder
    assert "  attribution-service\n" in image_builder
    assert 'services=("${DEFAULT_SERVICES[@]}")' in image_builder
    assert 'DOCKERFILE="$ATTRIBUTION_SOURCE_DIR/service/service-app/Dockerfile"' in image_builder
    available_graph = tiltfile.split("if attribution_mode == 'available':", maxsplit=1)[1]
    assert "custom_build(\n            'attribution-adapter'," in available_graph
    assert "attribution-client=" in image_builder
    assert 'dockerfile="$source_dir/service/service-app/Dockerfile"' in service_builder
    assert '--file "$dockerfile"' in service_builder
    assert '"$source_dir"' in service_builder
    assert "--build-context" not in service_builder
    assert not LOCAL_ATTRIBUTION_SERVICE_DOCKERFILE.exists()
    assert "Dockerfile.attribution-service" not in tiltfile
    dockerfile = REPO_ROOT / "attribution_adapter" / "Dockerfile"
    assert "third_party/fact/fact-client" not in dockerfile.read_text()
    assert "fact-client" not in (REPO_ROOT / "attribution_adapter" / "pyproject.toml").read_text()
    assert "attribution-adapter:" not in stack_values
    assert re.search(r"(?m)^attribution-adapter:\n  enabled: true$", attribution_values)
    assert "attribution_enabled: true" in attribution_values


@pytest.mark.parametrize("mode", [None, "available", "unavailable"])
def test_e2e_attribution_default_and_explicit_modes(mode) -> None:
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("E2E_", "_HEIMDALL_"))}
    if mode is not None:
        environment["E2E_ATTRIBUTION_MODE"] = mode
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s\\n%s" "$E2E_ATTRIBUTION_MODE" "$E2E_STACK_DEPLOYMENTS"',
            "test",
            str(E2E_ENV),
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    selected, deployments = result.stdout.splitlines()
    assert selected == (mode or "available")
    assert ("nvidia-recovery-attribution-service" in deployments) == (selected == "available")
    assert ("nvidia-recovery-attribution-adapter" in deployments) == (selected == "available")


def test_default_e2e_job_does_not_receive_or_fetch_attribution_source() -> None:
    job = _job_body(GITHUB_WORKFLOW.read_text(), "e2e")

    assert "ATTRIBUTION_SOURCE_DEPLOY_KEY" not in job
    assert "E2E_ATTRIBUTION_MODE: available" not in job


def test_public_workflows_do_not_pass_private_provider_credentials() -> None:
    for name in ("e2e-base.yml", "e2e-review.yml", "e2e-main.yml", "nightly.yml", "tag-release.yml"):
        workflow = (GITHUB_WORKFLOW.parent / name).read_text()
        assert "ATTRIBUTION_SOURCE_DEPLOY_KEY" not in workflow, name
        assert "secrets: inherit" not in workflow, name
    assert "E2E_ATTRIBUTION_MODE: unavailable" in GITHUB_WORKFLOW.read_text()


@pytest.mark.parametrize(
    ("workflow_path", "job_name", "report_lane"),
    (
        (GITHUB_WORKFLOW, "e2e", "e2e"),
        (ATTRIBUTION_WORKFLOW, "e2e-attribution", "e2e-attribution"),
    ),
)
def test_ci_lanes_run_each_log_backend_with_distinct_artifacts(
    workflow_path: Path, job_name: str, report_lane: str
) -> None:
    job = _job_body(workflow_path.read_text(), job_name)

    assert "log_backend: loki" in job
    assert "log_backend: victorialogs" in job
    assert "E2E_LOG_BACKEND: ${{ matrix.log_backend }}" in job
    assert f"reports/{report_lane}/${{{{ matrix.profile }}}}-${{{{ matrix.log_backend }}}}/job_state/" in job
    assert "${{ matrix.profile }}-${{ matrix.log_backend }}-job_state" in job
    assert "bash scripts/ci/e2e/e2e.sh" in job


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
@pytest.mark.parametrize("attribution_report", (False, True))
def test_pytest_adapter_exports_backend_and_keeps_report_variants_separate(
    tmp_path: Path, backend: str, attribution_report: bool
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "kubernetes-service-auth-smoke.sh").write_text("exit 0\n")
    binary = scripts / "uv"
    binary.write_text('#!/bin/sh\nprintf "%s\\n" "$E2E_LOG_BACKEND" "$VICTORIALOGS_URL" "$@" > "$CAPTURE"\n')
    binary.chmod(0o755)
    lane = "e2e-attribution" if attribution_report else "e2e"
    report = f"reports/{lane}/advisor-act-{backend}/job_state/junit.xml"
    capture = tmp_path / "pytest-args"
    env = {key: value for key, value in os.environ.items() if not key.startswith(("E2E_", "_HEIMDALL_", "REPO_ROOT"))}
    env.update(
        {
            "REPO_ROOT": str(tmp_path),
            "PATH": f"{scripts}:{os.environ['PATH']}",
            "E2E_PROFILE": "advisor-act",
            "E2E_LOG_BACKEND": backend,
            "VICTORIALOGS_URL": "http://victorialogs.monitoring.svc.cluster.local:9428",
            "RUN_TESTS": "1",
            "CAPTURE": str(capture),
        }
    )
    if attribution_report:
        env["E2E_XUNIT_PATH"] = report
    subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; SCRIPT_DIR="$2"; run_tests',
            "test-runner",
            str(REPO_ROOT / "scripts/ci/e2e/run-tests.sh"),
            str(scripts),
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    arguments = capture.read_text().splitlines()
    assert arguments[:2] == [backend, env["VICTORIALOGS_URL"]]
    assert arguments[arguments.index("--junitxml") + 1] == str(tmp_path / report)
    assert (tmp_path / report).parent.is_dir()


@pytest.mark.parametrize(
    ("workflow_path", "job_name"),
    ((GITHUB_WORKFLOW, "e2e"), (ATTRIBUTION_WORKFLOW, "e2e-attribution")),
)
@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
def test_ci_cluster_names_fit_kubernetes_node_label_limit(workflow_path: Path, job_name: str, backend: str) -> None:
    job = _job_body(workflow_path.read_text(), job_name)
    cluster = next(
        line.split("CLUSTER_NAME:", 1)[1].strip().strip('"') for line in job.splitlines() if "CLUSTER_NAME:" in line
    )
    suffix = "act-loki" if backend == "loki" else "act-vl"
    substitutions = {
        "${{ github.run_id }}": "123456789012",
        "${{ matrix.cluster_suffix }}": suffix,
    }
    for variable, value in substitutions.items():
        cluster = cluster.replace(variable, value)
    node_name = f"{cluster}-control-plane"
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]*[a-z0-9]", node_name), node_name
    assert len(node_name) <= 63, node_name


def test_private_qualification_is_explicit_and_environment_protected() -> None:
    workflow = ATTRIBUTION_WORKFLOW.read_text()
    assert "  workflow_dispatch:" in workflow
    assert "  push:" not in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "environment: attribution-qualification" in workflow
    assert "continue-on-error" not in workflow


def test_github_retains_helm_validation_and_go_race_checks() -> None:
    workflow = (GITHUB_WORKFLOW.parent / "pull-request-and-main.yml").read_text()
    assert "run: make test-go-race" in _job_body(workflow, "go-test")
    chart_job = _job_body(workflow, "helm-lint")
    for command in (
        "helm dependency build deploy/helm/nvidia-recovery",
        "deploy/helm/scripts/check-charts.sh deploy/helm/*/Chart.yaml",
    ):
        assert command in chart_job


@pytest.mark.parametrize(
    "runner_arch,engine_arch,accepted",
    (("x86_64", "amd64", True), ("aarch64", "amd64", False), ("x86_64", "aarch64", False)),
)
def test_private_qualification_rejects_emulated_architecture(
    tmp_path: Path, runner_arch: str, engine_arch: str, accepted: bool
) -> None:
    """A cross-built provider must not be counted as native amd64 qualification."""

    workflow = yaml.safe_load(ATTRIBUTION_WORKFLOW.read_text())
    script = next(
        step["run"] for step in workflow["jobs"]["e2e-attribution"]["steps"] if step["name"] == "Docker preflight"
    )
    for name, source in {
        "uname": '#!/bin/sh\nprintf "%s\\n" "$RUNNER_ARCH"\n',
        "docker": '#!/bin/sh\nif [ "$1" = info ]; then printf "%s\\n" "$ENGINE_ARCH"; fi\n',
    }.items():
        executable = tmp_path / name
        executable.write_text(source)
        executable.chmod(0o755)
    result = subprocess.run(
        ["bash", "-e", "-c", script],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "RUNNER_ARCH": runner_arch,
            "ENGINE_ARCH": engine_arch,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is accepted, result.stderr
