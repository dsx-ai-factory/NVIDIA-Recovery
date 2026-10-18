# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Build source and Helm assets from one committed revision without publishing."""

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path


def run(*args, **kwargs):
    """Run a required local tool, propagating failure."""
    return subprocess.check_output(args, **kwargs)


def digest(path):
    """Return the SHA-256 of an artifact."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalize(raw, destination):
    """Remove clock, ownership and entry-order variance while retaining file modes."""
    with (
        tarfile.open(fileobj=io.BytesIO(raw), mode="r:*") as source,
        destination.open("wb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as target,
    ):
        for member in sorted(source.getmembers(), key=lambda item: item.name):
            member.mtime = member.uid = member.gid = 0
            member.uname = member.gname = ""
            member.pax_headers = {}
            target.addfile(member, source.extractfile(member) if member.isfile() else None)


def inspect_archive(path, prefix, required):
    """Reject excluded payloads and require release/license material in an archive."""
    with tarfile.open(path) as archive:
        names = {item.name for item in archive.getmembers()}
        for name in names:
            parts = Path(name).parts
            if Path(name).is_absolute() or ".." in parts or not (name == prefix or name.startswith(prefix + "/")):
                raise ValueError(f"unsafe or unexpected archive path: {name}")
            # Distribute the provider Helm chart, not a checkout of its source/client.
            if {"advice-operator", "attribution-source", "attribution-client"} & set(parts) or (
                "attribution-service" in parts
                and parts[1:4] != ("deploy", "helm", "attribution-service")
                and parts[1:3] != ("charts", "attribution-service")
            ):
                raise ValueError(f"excluded release payload: {name}")
        for name in required:
            if f"{prefix}/{name}" not in names:
                raise ValueError(f"{path.name}: missing {name}")


def check_chart_version(content, version):
    """Require literal source and packaged chart metadata to identify this release."""
    fields = [
        line.split(":", 1) for line in content.decode().splitlines() if line.startswith(("version:", "appVersion:"))
    ]
    metadata = {key: value.strip().strip("\"'") for key, value in fields}
    if len(fields) != 2 or metadata != {"version": version, "appVersion": f"v{version}"}:
        raise ValueError("chart version/appVersion must match release version")


def verify(directory, expected_sha=None, expected_version=None):
    """Verify exact asset checksums, source identity, and packaged release material."""
    manifest = directory / "SHA256SUMS"
    listed = set()
    for line in manifest.read_text().splitlines():
        checksum, name = line.split("  ", 1)
        if Path(name).name != name or name in listed:
            raise ValueError(f"invalid checksum entry: {name}")
        listed.add(name)
        if digest(directory / name) != checksum:
            raise ValueError(f"checksum mismatch: {name}")
    actual = {p.name for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS"}
    if listed != actual:
        raise ValueError("checksum manifest must cover every asset")
    metadata = json.loads((directory / "release.json").read_text())
    version = metadata["version"]
    if expected_sha and metadata["source_sha"] != expected_sha:
        raise ValueError("release source SHA does not match checkout")
    if expected_version and version != expected_version.removeprefix("v"):
        raise ValueError("release version does not match tag")
    inspect_archive(
        directory / f"nvidia-recovery-{version}-source.tar.gz",
        f"nvidia-recovery-{version}",
        [
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "RELEASE.md",
            f"docs/releases/v{version}.md",
            "deploy/helm/nvidia-recovery/Chart.lock",
        ],
    )
    inspect_archive(
        directory / f"nvidia-recovery-{version}.tgz",
        "nvidia-recovery",
        [
            "Chart.yaml",
            "Chart.lock",
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "licenses/bitnami-charts.LICENSE",
            "licenses/grafana-charts.LICENSE",
            "licenses/alloy.LICENSE",
            "licenses/opentelemetry-charts.LICENSE",
            "licenses/minio.LICENSE",
            "licenses/minio.NOTICE",
        ],
    )
    if (directory / "version").read_text().strip() != version:
        raise ValueError("chart version file does not match release")
    source = directory / f"nvidia-recovery-{version}-source.tar.gz"
    chart = directory / f"nvidia-recovery-{version}.tgz"
    with tarfile.open(source) as source_archive, tarfile.open(chart) as chart_archive:
        check_chart_version(
            source_archive.extractfile(f"nvidia-recovery-{version}/deploy/helm/nvidia-recovery/Chart.yaml").read(),
            version,
        )
        check_chart_version(chart_archive.extractfile("nvidia-recovery/Chart.yaml").read(), version)
        if (
            source_archive.extractfile(f"nvidia-recovery-{version}/deploy/helm/nvidia-recovery/Chart.lock").read()
            != chart_archive.extractfile("nvidia-recovery/Chart.lock").read()
        ):
            raise ValueError("packaged chart lock differs from source")
    if metadata["source_sha256"] != digest(source) or metadata["chart_sha256"] != digest(chart):
        raise ValueError("release metadata digest differs from archive")
    return metadata


def package(version, destination, ref):
    """Archive one commit, package its locked chart in isolation, then check those bytes."""
    version = version.removeprefix("v")
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?", version):
        raise ValueError("version must be SemVer without build metadata")
    sha = run("git", "rev-parse", "--verify", f"{ref}^{{commit}}", text=True).strip()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise ValueError("output directory must be empty")
    with tempfile.TemporaryDirectory(prefix="nvidia-recovery-release-") as temporary:
        work = Path(temporary)
        prefix = f"nvidia-recovery-{version}"
        source = destination / f"{prefix}-source.tar.gz"
        normalize(run("git", "archive", "--format=tar", f"--prefix={prefix}/", sha), source)
        inspect_archive(source, prefix, ["LICENSE", "RELEASE.md", f"docs/releases/v{version}.md"])
        with tarfile.open(source) as archive:
            archive.extractall(work, filter="data")
        root = work / prefix
        check_chart_version((root / "deploy/helm/nvidia-recovery/Chart.yaml").read_bytes(), version)
        env = os.environ.copy()
        # Do not read the operator's Helm repository list or registry credentials.
        env.update(
            HELM_REPOSITORY_CONFIG=str(work / "repositories.yaml"),
            HELM_REPOSITORY_CACHE=str(work / "repository"),
            HELM_REGISTRY_CONFIG=str(work / "registry.json"),
            HELM_PLUGINS=str(work / "plugins"),
            HELM_CHART_DIR=str(root / "deploy/helm/nvidia-recovery"),
            HELM_OUTPUT_DIR=str(work / "chart"),
        )
        run("sh", "scripts/ci/helm-release.sh", "package", version, cwd=root, env=env)
        chart = destination / f"nvidia-recovery-{version}.tgz"
        normalize((work / "chart" / chart.name).read_bytes(), chart)
        # Test the normalized archive that will be distributed, never a repack at publication.
        values = str(root / "deploy/helm/scripts/fixtures/umbrella-provider-values.yaml")
        run("helm", "lint", str(chart), "--values", values, env=env)
        run("helm", "template", "release-check", str(chart), "--values", values, env=env)
        (destination / "version").write_text(version + "\n")
        (destination / "RELEASE_NOTES.md").write_bytes((root / f"docs/releases/v{version}.md").read_bytes())
        metadata = {
            "version": version,
            "source_sha": sha,
            "helm": run("helm", "version", "--short", text=True).strip(),
            "chart_sha256": digest(chart),
            "source_sha256": digest(source),
            "qualification": "archive inspection and local Helm lint/template; no runtime qualification",
            "secret_scanning_and_removal": "explicitly skipped",
        }
        (destination / "release.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        (destination / "SHA256SUMS").write_text(
            "".join(f"{digest(path)}  {path.name}\n" for path in sorted(destination.iterdir()))
        )
    verify(destination, sha, version)
    print(f"Validated source/chart assets for {sha} in {destination}")


def main():
    """Parse the packaging or offline verification command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("package")
    build.add_argument("version")
    build.add_argument("directory", type=Path)
    build.add_argument("--ref", default="HEAD")
    check = commands.add_parser("verify")
    check.add_argument("directory", type=Path)
    check.add_argument("--sha")
    check.add_argument("--version")
    args = parser.parse_args()
    if args.command == "package":
        package(args.version, args.directory.resolve(), args.ref)
    else:
        verify(args.directory, args.sha, args.version)
        print("Release checksums and archive contents verified")


if __name__ == "__main__":
    main()
