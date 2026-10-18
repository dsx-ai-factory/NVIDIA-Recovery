# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise default and degraded image selection through the consumer build command."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
IMAGE_BUILDER = REPO_ROOT / "scripts" / "build-images.sh"
PUBLIC_SERVICES = {
    "anomaly-detection-isolator",
    "api-gateway",
    "dashboard",
    "job-manager",
    "notifications",
    "watcher",
    "slurm-advisor-controller",
    "slurm-watchdog",
}


@pytest.fixture
def build_environment(tmp_path: Path) -> dict[str, str]:
    """Replace external executables and reject any attempt to fetch source."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    command = (
        f"#!{sys.executable}\n"
        + """\
import json
import os
import sys
from pathlib import Path

name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["BUILD_COMMAND_LOG"], "a") as log:
    log.write(json.dumps([name, *args]) + "\\n")
if name == "git" and os.environ.get("BUILD_PROVIDER_AVAILABLE") == "1":
    if args[-2:] == ["rev-parse", "HEAD"]:
        print(os.environ["ATTRIBUTION_SOURCE_REF"])
    elif args[-3:] == ["config", "--get", "remote.origin.url"]:
        print(os.environ["ATTRIBUTION_SOURCE_URL"])
    elif args[-2:] != ["status", "--porcelain"]:
        sys.exit("unexpected source mutation")
    sys.exit(0)
if name != "docker":
    sys.exit("unexpected source access")
if args == ["buildx", "version"]:
    print("github.com/docker/buildx test")
elif args == ["buildx", "inspect", "--bootstrap"]:
    print("Driver: docker-container\\nPlatforms: linux/amd64, linux/arm64")
elif args[:3] == ["buildx", "imagetools", "inspect"]:
    print("Platform: linux/amd64\\nPlatform: linux/arm64")
elif args[:2] != ["buildx", "build"]:
    sys.exit("unexpected Docker command")
"""
    )
    for executable in ("docker", "git"):
        path = bin_dir / executable
        path.write_text(command)
        path.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "BUILD_COMMAND_LOG": str(tmp_path / "commands.jsonl"),
        "IMAGE_PREFIX": "registry.example.com/consumer/nvidia-recovery",
        "IMAGE_TAG": "public-test",
        "PLATFORMS": "linux/amd64",
        "ATTRIBUTION_ENABLED": "false",
        "E2E_ATTRIBUTION_MODE": "unavailable",
        "IMAGE_SOURCE": "https://example.com/source",
        "IMAGE_REVISION": "a" * 40,
        # Poison optional configuration so an accidental default fetch fails
        # before network access. Explicit opt-in must reach the real guard.
        "ATTRIBUTION_SOURCE_URL": "https://example.invalid/optional-provider.git",
        "ATTRIBUTION_SOURCE_REF": "not-a-commit",
        "ATTRIBUTION_SOURCE_DIR": str(tmp_path / "provider-checkout"),
        "ATTRIBUTION_SOURCE_DEPLOY_KEY": "",
        "ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE": "",
    }


def _build(environment: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(IMAGE_BUILDER), *args],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _commands(environment: dict[str, str]) -> list[list[str]]:
    log = Path(environment["BUILD_COMMAND_LOG"])
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


@pytest.mark.parametrize("mode", ("--load", "--publish"))
def test_explicit_degraded_build_selects_services_without_provider_access(
    build_environment: dict[str, str], mode: str
) -> None:
    """Explicit OFF builds do not read provider source or emit provider images."""
    result = _build(build_environment, mode)

    assert result.returncode == 0, result.stderr
    commands = _commands(build_environment)
    assert all(command[0] == "docker" for command in commands)
    builds = [command for command in commands if command[:3] == ["docker", "buildx", "build"]]
    images = [command[command.index("--tag") + 1] for command in builds]
    expected = {f"{build_environment['IMAGE_PREFIX']}/{service}:public-test" for service in PUBLIC_SERVICES}
    assert len(builds) == len(PUBLIC_SERVICES)
    assert set(images) == expected
    assert all("" not in command for command in builds)
    assert not Path(build_environment["ATTRIBUTION_SOURCE_DIR"]).exists()
    if mode == "--load":
        assert all("--load" in command and "--push" not in command for command in builds)
    else:
        assert all("--push" in command and "--load" not in command for command in builds)
        assert all(command[command.index("--platform") + 1] == "linux/amd64" for command in builds)
        inspected = [
            command[-1] for command in commands if command[:4] == ["docker", "buildx", "imagetools", "inspect"]
        ]
        assert set(inspected) == expected


@pytest.mark.parametrize("service", ("attribution-adapter", "attribution-service"))
def test_attribution_build_requires_source_preparation(build_environment: dict[str, str], service: str) -> None:
    """Explicit selection reaches the provider guard before building an image."""
    result = _build({**build_environment, "ATTRIBUTION_ENABLED": "true"}, "--load", service)

    assert result.returncode == 2
    assert "SOURCE_COMMIT must be a full lowercase Git commit SHA" in result.stderr
    assert not any(command[:3] == ["docker", "buildx", "build"] for command in _commands(build_environment))


@pytest.mark.parametrize("mode", ("--load", "--publish"))
def test_invalid_service_is_rejected_before_any_docker_command(build_environment: dict[str, str], mode: str) -> None:
    """Validate the whole selection before an earlier valid service can build."""
    result = _build(build_environment, mode, "watcher", "unknown-service")

    assert result.returncode == 1
    assert "unsupported service 'unknown-service'" in result.stderr
    assert _commands(build_environment) == []


def test_list_keeps_optional_attribution_services_discoverable(build_environment: dict[str, str]) -> None:
    """Listing the supported selection requires neither Docker nor source access."""
    result = _build(build_environment, "--list")

    assert result.returncode == 0, result.stderr
    assert PUBLIC_SERVICES | {"attribution-adapter", "attribution-service"} == set(result.stdout.splitlines())
    assert _commands(build_environment) == []


@pytest.mark.parametrize("service", ["api-gateway", "slurm-advisor-controller", "slurm-watchdog"])
def test_builder_uses_maintained_dockerfile_defaults(build_environment, service):
    """Public wrappers must not override the runtime or Slurm donor pin."""
    environment = {
        **build_environment,
        "SLINKY_SLURM_IMAGE": "",
        "SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE": "",
        "SLURM_WATCHDOG_BASE_IMAGE": "",
    }
    result = _build(environment, "--load", service)
    assert result.returncode == 0, result.stderr
    build = next(c for c in _commands(environment) if c[:3] == ["docker", "buildx", "build"])
    build_args = [build[i + 1] for i, arg in enumerate(build) if arg == "--build-arg"]
    if service == "api-gateway":
        assert not any(arg.startswith("GO_BASEIMAGE=") for arg in build_args)
    else:
        argument = "SLURM_IMAGE" if service == "slurm-advisor-controller" else "BASE_IMAGE"
        dockerfile = Path(build[build.index("--file") + 1]).read_text()
        default = re.search(rf"^ARG {argument}=(.+)$", dockerfile, re.MULTILINE).group(1)
        assert f"{argument}={default}" in build_args


@pytest.mark.parametrize("tag", ["", "25.05-ubuntu24.04", "custom-tag"])
def test_e2e_donor_defaults_preserve_custom_tags(build_environment, tag):
    """Evaluate configuration only; never launch a scheduler or cluster."""
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s" "$E2E_SLINKY_SACKD_IMAGE"',
            "fixture",
            str(REPO_ROOT / "scripts/ci/e2e/env.sh"),
        ],
        env={
            **build_environment,
            "REPO_ROOT": str(REPO_ROOT),
            "E2E_PROFILE": "advisor-observe",
            "E2E_SLINKY_SLURM_TAG": tag,
            "E2E_SLINKY_SACKD_IMAGE": "",
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    if tag == "custom-tag":
        assert result.stdout == "ghcr.io/slinkyproject/sackd:custom-tag"
    else:
        default = re.search(
            r"^ARG SLURM_IMAGE=(.+)$", (REPO_ROOT / "slurm-advisor-controller/Dockerfile").read_text(), re.MULTILINE
        ).group(1)
        assert result.stdout == default
    assert _commands(build_environment) == []


@pytest.mark.parametrize("mode", ("--load", "--publish"))
def test_default_build_fails_before_building_when_provider_source_is_missing(build_environment, mode):
    """Default ON cannot silently emit an incomplete set of stack images."""
    environment = {key: value for key, value in build_environment.items() if key != "ATTRIBUTION_ENABLED"}
    result = _build(environment, mode)
    assert result.returncode != 0
    assert not any(command[:3] == ["docker", "buildx", "build"] for command in _commands(environment))


def test_unqualified_attribution_platform_is_rejected_before_building(build_environment):
    environment = {**build_environment, "ATTRIBUTION_ENABLED": "true", "PLATFORMS": "linux/arm64"}
    result = _build(environment, "--publish")
    assert result.returncode != 0
    assert _commands(environment) == []


@pytest.mark.parametrize("mode", ("--load", "--publish"))
@pytest.mark.parametrize("entrypoint", ("script", "make"))
def test_default_build_includes_provider_and_adapter(build_environment, mode, entrypoint):
    """A validated provider checkout feeds both images in the default build."""
    environment = {
        key: value for key, value in build_environment.items() if key not in {"ATTRIBUTION_ENABLED", "PLATFORMS"}
    }
    environment.update(BUILD_PROVIDER_AVAILABLE="1", ATTRIBUTION_SOURCE_REF="a" * 40)
    source = Path(environment["ATTRIBUTION_SOURCE_DIR"])
    for directory in (
        "service/attribution-lib",
        "service/service-app/nvidia_attribution_client/nvidia_attribution_client",
    ):
        (source / directory).mkdir(parents=True)
    for filename in (
        "LICENSE",
        "service/service-app/Dockerfile",
        "service/service-app/pyproject.toml",
        "service/service-app/uv.lock",
        "service/service-app/nvidia_attribution_client/pyproject.toml",
    ):
        (source / filename).touch()
    if entrypoint == "make":
        result = subprocess.run(
            ["make", "build-local" if mode == "--load" else "build-publish"],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    else:
        result = _build(environment, mode)
    assert result.returncode == 0, result.stderr
    builds = [command for command in _commands(environment) if command[:3] == ["docker", "buildx", "build"]]
    images = {command[command.index("--tag") + 1] for command in builds}
    services = PUBLIC_SERVICES | {"attribution-adapter", "attribution-service"}
    assert images == {f"{environment['IMAGE_PREFIX']}/{service}:public-test" for service in services}
    provider_builds = [command for command in builds if "/attribution-" in command[command.index("--tag") + 1]]
    assert all(command[command.index("--platform") + 1] == "linux/amd64" for command in provider_builds)
    adapter = next(command for command in provider_builds if "--build-context" in command)
    assert (
        adapter[adapter.index("--build-context") + 1]
        == f"attribution-client={source}/service/service-app/nvidia_attribution_client"
    )
