# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Render Attribution contracts: python -m unittest discover -s deploy/helm/nvidia-recovery/tests.

Requires Helm, PyYAML, and current dependencies in nvidia-recovery/charts (helm dependency build).
"""

import subprocess
import unittest
from pathlib import Path
from urllib.parse import urlparse

import yaml

CHART = Path(__file__).resolve().parents[1]
PROVIDER_VALUES = CHART.parent / "scripts/fixtures/log-query-provider-values.yaml"


def render(values, release="recovery"):
    """Render real chart resources with explicit log connections and test overrides."""
    return subprocess.run(
        ["helm", "template", release, str(CHART), "--values", str(PROVIDER_VALUES), "--values", "-"],
        input=yaml.safe_dump(values),
        capture_output=True,
        text=True,
        check=False,
    )


class AttributionChartTest(unittest.TestCase):
    """Prove deployable service wiring and rejection of missing Attribution dependencies."""

    def resources(self, values, release="recovery"):
        """Return successfully rendered Kubernetes objects."""
        result = render(values, release)
        self.assertEqual(result.returncode, 0, result.stderr)
        return [item for item in yaml.safe_load_all(result.stdout) if item]

    def settings(self, resources, component):
        """Read the rendered application configuration delivered to a component."""
        config = next(
            item
            for item in resources
            if item["kind"] == "ConfigMap" and item["metadata"]["labels"]["app.kubernetes.io/name"] == component
        )
        return {
            key: value.strip('"')
            for line in config["data"]["settings.env"].splitlines()
            for key, separator, value in [line.partition("=")]
            if separator
        }

    def test_default_chain_resolves_to_same_release_services(self):
        """Each consumer reaches its rendered Service, including long and overlapping names."""
        for release in (
            "recovery",
            "prod-attribution-service",
            "prod-attribution-adapter",
            "attribution-adapter-attribution-service",
            "r" * 53,
        ):
            with self.subTest(release=release):
                resources = self.resources({"anomaly-detection-isolator": {"enabled": True}}, release)
                services = {
                    item["metadata"]["labels"]["app.kubernetes.io/name"]: item
                    for item in resources
                    if item["kind"] == "Service"
                }
                names = [item["metadata"]["name"] for item in services.values()]
                self.assertEqual(len(names), len(set(names)), "Service names must not collide")
                for component, key, target in (
                    ("anomaly-detection-isolator", "attribution_service_url", "attribution-adapter"),
                    ("attribution-adapter", "fact_url", "attribution-service"),
                ):
                    url = urlparse(self.settings(resources, component)[key])
                    service = services[target]
                    self.assertEqual(url.hostname, service["metadata"]["name"])
                    self.assertEqual(url.port, service["spec"]["ports"][0]["port"])
                for component in ("attribution-service", "attribution-adapter"):
                    workload = next(
                        item
                        for item in resources
                        if item["kind"] == "Deployment"
                        and item["metadata"]["labels"]["app.kubernetes.io/name"] == component
                    )
                    pod = workload["spec"]["template"]
                    self.assertEqual(pod["spec"]["nodeSelector"]["kubernetes.io/arch"], "amd64")
                    for key, value in services[component]["spec"]["selector"].items():
                        self.assertEqual(pod["metadata"]["labels"][key], value)
                self.assertEqual(self.settings(resources, "anomaly-detection-isolator")["attribution_enabled"], "true")

    def test_explicit_degraded_mode(self):
        """Explicit disablement removes provider and adapter while retaining degraded ADI."""
        resources = self.resources(
            {
                "attribution-service": {"enabled": False},
                "attribution-adapter": {"enabled": False},
                "anomaly-detection-isolator": {"enabled": True, "attribution_enabled": False},
            }
        )
        workloads = [
            item["metadata"]["labels"]["app.kubernetes.io/name"] for item in resources if item["kind"] == "Deployment"
        ]
        self.assertNotIn("attribution-service", workloads)
        self.assertNotIn("attribution-adapter", workloads)
        self.assertEqual(self.settings(resources, "anomaly-detection-isolator")["attribution_enabled"], "false")

    def test_disabled_isolator_does_not_require_adapter(self):
        """Absent workloads need no provider even when integration defaults to enabled."""
        for isolator in ({}, {"enabled": False}):
            with self.subTest(isolator=isolator):
                resources = self.resources(
                    {
                        "anomaly-detection-isolator": isolator,
                        "attribution-adapter": {"enabled": False},
                        "attribution-service": {"enabled": False},
                    }
                )
                workloads = {
                    item["metadata"]["labels"]["app.kubernetes.io/name"]
                    for item in resources
                    if item["kind"] == "Deployment"
                }
                self.assertTrue(
                    workloads.isdisjoint({"anomaly-detection-isolator", "attribution-adapter", "attribution-service"})
                )

    def test_explicit_external_provider(self):
        """A supplied provider endpoint permits omitting the bundled provider."""
        endpoint = "https://provider.example.test:8443"
        resources = self.resources(
            {
                "attribution-service": {"enabled": False},
                "attribution-adapter": {"fact": {"url": endpoint}},
            }
        )
        self.assertEqual(self.settings(resources, "attribution-adapter")["fact_url"], endpoint)
        self.assertFalse(
            any(
                item["kind"] == "Deployment"
                and item["metadata"]["labels"]["app.kubernetes.io/name"] == "attribution-service"
                for item in resources
            )
        )

    def test_missing_dependencies_and_empty_urls_fail(self):
        """Incomplete enabled chains cannot install while silently disabling diagnosis."""
        cases = (
            ({"attribution-service": {"enabled": False}}, "attribution-adapter.fact.url"),
            (
                {"anomaly-detection-isolator": {"enabled": True}, "attribution-adapter": {"enabled": False}},
                "attribution_service_url",
            ),
            ({"attribution-adapter": {"fact": {"url": ""}}}, "fact.url"),
            (
                {"anomaly-detection-isolator": {"enabled": True, "attribution_service_url": ""}},
                "attribution_service_url",
            ),
        )
        for values, key in cases:
            with self.subTest(values=values):
                result = render(values)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(key, result.stderr)


if __name__ == "__main__":
    unittest.main()
