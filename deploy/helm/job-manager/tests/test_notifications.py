# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check rendered Notifications routing with Helm and the existing PyYAML dependency.

Run: uv run --locked python deploy/helm/job-manager/tests/test_notifications.py
"""

import json
import subprocess
import unittest
from pathlib import Path

import yaml

CHARTS = Path(__file__).resolve().parents[2]


def render(chart, release, **values):
    """Render schema-validated resources, preserving structured configuration."""
    command = ["helm", "template", release, str(CHARTS / chart)]
    for key, value in values.items():
        command += ["--set-json", f"{key}={json.dumps(value)}"]
    return list(yaml.safe_load_all(subprocess.check_output(command, text=True)))


def service_config(release="custom", **values):
    """Read Job Manager's effective configuration from its rendered ConfigMap."""
    resources = render("job-manager", release, **values)
    config = next(
        item["data"]["job_manager_config.yaml"]
        for item in resources
        if item["kind"] == "ConfigMap" and "job_manager_config.yaml" in item["data"]
    )
    return yaml.safe_load(config)["service"]


class NotificationsRoutingTest(unittest.TestCase):
    """Ensure default delivery resolves the actual Service and overrides survive."""

    def test_default_targets_same_release_service(self):
        """Arbitrary names, embedded chart names, and truncation still resolve."""
        for release in ("heimdall", "customer-test", "notifications-test", "x" * 53):
            with self.subTest(release=release):
                service = next(item for item in render("notifications", release) if item["kind"] == "Service")
                url = f"http://{service['metadata']['name']}:{service['spec']['ports'][0]['port']}"
                config = service_config(release)
                self.assertEqual(config["notification_service_url"], url)
                self.assertEqual(config["audit_trail"]["destination"], url + "/v1/audit/events")

    def test_explicit_notification_url(self):
        """External notification URLs also supply the default audit receiver."""
        url = "https://notifications.example.com/base/"
        config = service_config(**{"service_config.notification_service_url": url})
        self.assertEqual(config["notification_service_url"], url)
        self.assertEqual(config["audit_trail"]["destination"], url + "v1/audit/events")

    def test_audit_overrides(self):
        """Separate receivers and explicit logging-only mode override discovery."""
        for destination in ("https://audit.example.com/events", ""):
            with self.subTest(destination=destination):
                config = service_config(**{"service_config.audit_trail.destination": destination})
                if destination:
                    self.assertEqual(config["audit_trail"]["destination"], destination)
                else:
                    self.assertNotIn("destination", config["audit_trail"])


if __name__ == "__main__":
    unittest.main()
