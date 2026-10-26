# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression checks for release reproducibility, exclusions and artifact handoff."""

import io
import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

from package import check_chart_version, digest, inspect_archive, normalize, verify


def archive_bytes(files, timestamp=123):
    """Create an archive with controlled metadata for release regression cases."""
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, content in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            member.mtime = timestamp
            member.uid = timestamp
            member.mode = 0o755 if name.endswith(".sh") else 0o644
            archive.addfile(member, io.BytesIO(content))
    return output.getvalue()


class PackageTests(unittest.TestCase):
    """Exercise corruption and excluded-payload failures through archive interfaces."""

    def setUp(self):
        """Allocate a private directory per case."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_normalization_is_reproducible_and_preserves_content_and_mode(self):
        """Input timestamps/order must not affect distributed bytes or executability."""
        first, second = self.root / "one.tgz", self.root / "two.tgz"
        files = {"chart/z.sh": b"echo test", "chart/a": b"license"}
        normalize(archive_bytes(files), first)
        normalize(archive_bytes(dict(reversed(list(files.items()))), 456), second)
        self.assertEqual(first.read_bytes(), second.read_bytes())
        with tarfile.open(first) as archive:
            self.assertEqual(archive.getmember("chart/z.sh").mode, 0o755)
            self.assertEqual(archive.extractfile("chart/a").read(), b"license")

    def test_missing_license_is_rejected(self):
        """A chart with valid metadata still requires retained license material."""
        archive = self.root / "chart.tgz"
        normalize(archive_bytes({"chart/Chart.yaml": b"version: 1.0.0"}), archive)
        with self.assertRaisesRegex(ValueError, "missing LICENSE"):
            inspect_archive(archive, "chart", ["Chart.yaml", "LICENSE"])

    def test_excluded_source_and_nested_chart_payloads_are_rejected(self):
        """Disabled components must not be shipped as archive payloads."""
        for name in (
            "chart/advice-operator/main.go",
            "chart/charts/advice-operator/Chart.yaml",
            "chart/deploy/helm/advice-operator/Chart.yaml",
            "chart/attribution-source/client.py",
            "chart/attribution-service/main.py",
            "chart/attribution-client/client.py",
            "chart/../escape",
        ):
            with self.subTest(name=name):
                archive = self.root / "chart.tgz"
                normalize(archive_bytes({name: b"excluded"}), archive)
                with self.assertRaises(ValueError):
                    inspect_archive(archive, "chart", [])

    def test_source_archive_omits_vendored_operator_and_retains_supported_components(self):
        """Vendored source stays out of releases without dropping supported payloads."""
        files = {
            "source/advice-operator": b"",
            "source/advice-operator/go.mod": b"module example/advice-operator",
            "source/deploy/helm/advice-operator/Chart.yaml": b"name: advice-operator",
            "source/api-gateway/go.mod": b"module example/api-gateway",
            "source/deploy/helm/nvidia-recovery/Chart.yaml": b"name: nvidia-recovery",
        }
        archive = self.root / "source.tgz"
        normalize(
            archive_bytes(files),
            archive,
            excluded_roots=("advice-operator", "deploy/helm/advice-operator"),
        )
        with tarfile.open(archive) as result:
            self.assertEqual(
                set(result.getnames()),
                {
                    "source/api-gateway/go.mod",
                    "source/deploy/helm/nvidia-recovery/Chart.yaml",
                },
            )
        inspect_archive(archive, "source", ["api-gateway/go.mod"])

    def test_attribution_helm_chart_is_distributed(self):
        """The default-on provider chart belongs in source and packaged charts."""
        for name in (
            "chart/deploy/helm/attribution-service",
            "chart/deploy/helm/attribution-service/templates/deployment.yaml",
            "chart/charts/attribution-service",
            "chart/charts/attribution-service/templates/deployment.yaml",
        ):
            with self.subTest(name=name):
                archive = self.root / "chart.tgz"
                normalize(archive_bytes({name: b"provider chart"}), archive)
                inspect_archive(archive, "chart", [name.removeprefix("chart/")])

    def make_release(self):
        """Build a complete handoff fixture using real tar/gzip assets."""
        source_files = [
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "RELEASE.md",
            "docs/releases/v0.2.2.md",
            "deploy/helm/nvidia-recovery/Chart.lock",
        ]
        chart_files = [
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
        ]
        metadata = b'version: 0.2.2\nappVersion: "v0.2.2"\n'
        source = {f"nvidia-recovery-0.2.2/{name}": b"source" for name in source_files}
        source["nvidia-recovery-0.2.2/deploy/helm/nvidia-recovery/Chart.yaml"] = metadata
        source["nvidia-recovery-0.2.2/deploy/helm/nvidia-recovery/Chart.lock"] = b"lock"
        chart = {f"nvidia-recovery/{name}": b"chart" for name in chart_files}
        chart["nvidia-recovery/Chart.yaml"] = metadata
        chart["nvidia-recovery/Chart.lock"] = b"lock"
        normalize(archive_bytes(source), self.root / "nvidia-recovery-0.2.2-source.tar.gz")
        normalize(archive_bytes(chart), self.root / "nvidia-recovery-0.2.2.tgz")
        (self.root / "release.json").write_text(
            json.dumps(
                {
                    "version": "0.2.2",
                    "source_sha": "a" * 40,
                    "source_sha256": digest(self.root / "nvidia-recovery-0.2.2-source.tar.gz"),
                    "chart_sha256": digest(self.root / "nvidia-recovery-0.2.2.tgz"),
                }
            )
        )
        (self.root / "version").write_text("0.2.2\n")
        self.write_checksums()

    def write_checksums(self):
        """Update the fixture checksum manifest."""
        (self.root / "SHA256SUMS").write_text(
            "".join(
                f"{digest(path)}  {path.name}\n" for path in sorted(self.root.iterdir()) if path.name != "SHA256SUMS"
            )
        )

    def test_exact_handoff_verifies(self):
        """The producer's complete assets pass consumer validation."""
        self.make_release()
        self.assertEqual(verify(self.root, "a" * 40, "v0.2.2")["version"], "0.2.2")

    def test_modified_chart_fails_before_publication(self):
        """Changing an archive after qualification invalidates the handoff."""
        self.make_release()
        with (self.root / "nvidia-recovery-0.2.2.tgz").open("ab") as archive:
            archive.write(b"changed")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            verify(self.root)

    def test_wrong_commit_or_tag_is_rejected(self):
        """Assets from another commit or version cannot satisfy this release."""
        self.make_release()
        with self.assertRaisesRegex(ValueError, "source SHA"):
            verify(self.root, "b" * 40)
        with self.assertRaisesRegex(ValueError, "version does not match tag"):
            verify(self.root, expected_version="v0.3.0")

    def test_chart_version_and_app_version_must_match_release(self):
        """Tags cannot silently override committed chart metadata."""
        for content in (b"version: 0.2.1\nappVersion: v0.2.2\n", b"version: 0.2.2\nappVersion: v0.0.1\n"):
            with self.assertRaisesRegex(ValueError, "version/appVersion"):
                check_chart_version(content, "0.2.2")

    def test_wrong_packaged_version_rejected_even_with_updated_checksums(self):
        """Consumer inspection checks packaged chart metadata, not just file names."""
        self.make_release()
        path = self.root / "nvidia-recovery-0.2.2.tgz"
        with tarfile.open(path) as archive:
            files = {m.name: archive.extractfile(m).read() for m in archive.getmembers()}
        files["nvidia-recovery/Chart.yaml"] = b"version: 0.2.2\nappVersion: v0.0.1\n"
        normalize(archive_bytes(files), path)
        self.write_checksums()
        with self.assertRaisesRegex(ValueError, "version/appVersion"):
            verify(self.root)

    def test_public_ci_requires_success_for_the_exact_commit(self):
        """Missing, running, failed, or different-commit CI cannot permit publication."""
        repository = Path(__file__).resolve().parents[2]
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
        gh = self.root / "gh"
        gh.write_text('#!/bin/sh\nprintf "%s\\n" "$@" >> "$CI_ARGUMENTS"\nprintf "%s" "$CI_RESULT"\n')
        gh.chmod(0o755)
        env = os.environ.copy()
        env.update(
            PATH=f"{self.root}:{env['PATH']}",
            GITHUB_REPOSITORY="example/recovery",
            CI_ARGUMENTS=str(self.root / "arguments"),
        )
        for result, success in (
            (f"{sha} completed success", True),
            (f"{sha} completed failure", False),
            (f"{sha} in_progress ", False),
            ("", False),
            (f"{'b' * 40} completed success", False),
        ):
            with self.subTest(result=result):
                env["CI_RESULT"] = result
                check = subprocess.run(
                    ["sh", "scripts/release/check-public-ci.sh"],
                    cwd=repository,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(check.returncode == 0, success, check.stderr)
        arguments = (self.root / "arguments").read_text()
        for expected in (f"head_sha={sha}", "branch=main", "event=push", "per_page=1"):
            self.assertIn(expected, arguments)
        self.assertNotIn("status=success", arguments)
        self.assertIn("workflows/docs.yml/runs", arguments)
        self.assertIn("workflows/pull-request-and-main.yml/runs", arguments)

    def test_unlisted_asset_and_unsafe_checksum_path_are_rejected(self):
        """The manifest must enumerate all assets and cannot escape its directory."""
        self.make_release()
        (self.root / "unexpected").write_text("extra")
        with self.assertRaisesRegex(ValueError, "every asset"):
            verify(self.root)
        (self.root / "SHA256SUMS").write_text("0" * 64 + "  ../outside\n")
        with self.assertRaisesRegex(ValueError, "invalid checksum entry"):
            verify(self.root)


if __name__ == "__main__":
    unittest.main()
