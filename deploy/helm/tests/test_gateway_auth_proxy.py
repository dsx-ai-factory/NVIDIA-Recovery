# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify trusted proxy routing with Helm and PyYAML.

Run: uv run --locked python deploy/helm/tests/test_gateway_auth_proxy.py
"""

import json
import subprocess
import unittest
from pathlib import Path

import yaml

CHART = Path(__file__).resolve().parents[1] / "api-gateway"


def render(*, check=True, **values):
    """Render the gateway chart with the supplied value overrides."""
    command = ["helm", "template", "test-api-gateway", str(CHART)]
    for key, value in values.items():
        command.extend(["--set-json", f"{key}={json.dumps(value)}"])
    return subprocess.run(command, capture_output=True, text=True, check=check)


class TrustedProxyTest(unittest.TestCase):
    """Check public listener isolation and the preserved internal route."""

    def test_routes_public_traffic_to_proxy_and_loads_oidc_client_id(self):
        rendered = render(
            **{
                "auth.trustHeaderBasedIdentity": True,
                "auth.oidc.enabled": True,
                "auth.oidc.clientIdSecretName": "example-oidc",
                "auth.oidc.clientIdSecretKey": "client-id",
                "externalService.targetPorts": {"rest": 4180, "grpc": 4181},
            }
        )
        resources = {(r["kind"], r["metadata"]["name"]): r for r in yaml.safe_load_all(rendered.stdout) if r}
        pod = resources["Deployment", "test-api-gateway"]["spec"]["template"]["spec"]
        env = {entry["name"]: entry for entry in pod["containers"][0]["env"]}
        self.assertEqual(env["HEIMDALL_TRUST_HEADER_BASED_IDENTITY"]["value"], "true")
        self.assertEqual(env["HEIMDALL_HTTP_SERVER_ENDPOINT"]["value"], "127.0.0.1:8080")
        self.assertEqual(env["HEIMDALL_GRPC_SERVER_ENDPOINT"]["value"], "127.0.0.1:9090")
        self.assertNotIn("HEIMDALL_INTERNAL_GRPC_SERVER_ENDPOINT", env)
        self.assertEqual(
            env["HEIMDALL_OIDC_CLIENT_ID"]["valueFrom"]["secretKeyRef"],
            {"name": "example-oidc", "key": "client-id"},
        )
        public_ports = resources["Service", "test-api-gateway"]["spec"]["ports"]
        internal_ports = resources["Service", "test-api-gateway-internal"]["spec"]["ports"]
        self.assertEqual({p["name"]: p["targetPort"] for p in public_ports}, {"rest": 4180, "grpc": 4181})
        self.assertEqual({p["name"]: p["targetPort"] for p in internal_ports}["grpc"], "grpc-internal")

    def test_rejects_public_service_targeting_gateway_listeners(self):
        for target in ("", "rest-internal", 8081):
            with self.subTest(target=target):
                result = render(
                    check=False,
                    **{
                        "auth.trustHeaderBasedIdentity": True,
                        "externalService.targetPorts": {"rest": target, "grpc": 4181},
                    },
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("externalService.targetPorts.rest", result.stderr)


if __name__ == "__main__":
    unittest.main()
