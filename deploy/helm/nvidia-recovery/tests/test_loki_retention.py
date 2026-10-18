# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify bundled log storage survives StatefulSet deletion and scale-down.

After building chart dependencies, run with the project's Python environment:
uv run --locked python deploy/helm/nvidia-recovery/tests/test_loki_retention.py
"""

import subprocess
import unittest
from pathlib import Path

import yaml


class LokiRetentionTest(unittest.TestCase):
    """Test the rendered storage lifecycle, including chart dependency defaults."""

    def test_log_volume_is_retained(self):
        """Uninstall/scale-down must not automatically delete filesystem logs."""
        chart = Path(__file__).resolve().parents[1]
        provider_values = chart.parent / "scripts/fixtures/log-query-provider-values.yaml"
        rendered = subprocess.check_output(
            [
                "helm",
                "template",
                "retention-test",
                str(chart),
                "--values",
                str(provider_values),
                "--set",
                "global.loki.bundled.enabled=true",
            ],
            text=True,
        )
        loki = next(
            resource
            for resource in yaml.safe_load_all(rendered)
            if resource["kind"] == "StatefulSet"
            and resource["metadata"]["labels"].get("app.kubernetes.io/component") == "single-binary"
        )
        claims = loki["spec"]["volumeClaimTemplates"]
        self.assertTrue(claims, "Filesystem logs must use persistent storage")
        policy = loki["spec"].get("persistentVolumeClaimRetentionPolicy", {})
        self.assertEqual(policy.get("whenDeleted", "Retain"), "Retain")
        self.assertEqual(policy.get("whenScaled", "Retain"), "Retain")


if __name__ == "__main__":
    unittest.main()
