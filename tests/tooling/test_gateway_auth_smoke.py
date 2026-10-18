# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the in-Pod HTTP probe against a real local HTTP server."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SMOKE = Path(__file__).parents[2] / "scripts/ci/e2e/kubernetes-service-auth-smoke.sh"


@pytest.mark.parametrize(
    ("missing_status", "projected_status", "succeeds"),
    ((401, 200, True), (200, 200, False), (401, 401, False)),
)
def test_gateway_auth_smoke_checks_both_http_auth_outcomes(
    tmp_path: Path, missing_status: int, projected_status: int, succeeds: bool
) -> None:
    """Reject both an unprotected route and a route rejecting a valid bearer."""
    token = "synthetic-projected-token"
    token_path = tmp_path / "service-account-token"
    token_path.write_text(token)
    requests: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            """Record the probe's actual wire request and respond by identity."""
            authorization = self.headers.get("Authorization")
            requests.append((self.path, authorization))
            status = projected_status if authorization == f"Bearer {token}" else missing_status
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *args: object) -> None:
            """Keep synthetic credentials out of test diagnostics."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "args = sys.argv[1:]\n"
        "if args[0] == 'exec':\n"
        "    command = args[args.index('--') + 1:]\n"
        "    command = command[command.index('bash'):]\n"
        "    first_argument = command.index('--') + 1\n"
        "    assert command[first_argument] == 'nvidia-recovery-api-gateway-internal', command[first_argument]\n"
        f"    command[first_argument:first_argument + 3] = ['127.0.0.1', '{server.server_port}', {str(token_path)!r}]\n"
        "    os.execv('/bin/bash', command)\n"
        "elif args[0] == 'rollout':\n"
        "    pass\n"
        "elif args[0] == 'auth':\n"
        "    print('yes')\n"
        "elif args[0] == 'logs':\n"
        '    print(\'{"msg":"controller sync completed"}\')\n'
        "elif args[:2] == ['get', 'deployment']:\n"
        "    if 'serviceAccountName' in args[-1]:\n"
        "        print('nvidia-recovery-api-gateway-tokenreviewer')\n"
        "    else:\n"
        "        print('HEIMDALL_KUBERNETES_AUTH_ENABLED=true')\n"
        "        print('HEIMDALL_KUBERNETES_AUTH_AUDIENCE=nvidia-recovery-api-gateway')\n"
        "        print('HEIMDALL_KUBERNETES_AUTH_EXPECTED_SERVICE_ACCOUNT=system:serviceaccount:slurm:slurm-advisor-controller')\n"
        "elif args[:2] == ['get', 'pod']:\n"
        "    if args[-1] == 'yaml':\n"
        "        print('serviceAccountToken:\\n  audience: nvidia-recovery-api-gateway\\n  expirationSeconds: 3600')\n"
        "    elif 'serviceAccountName' in args[-1]:\n"
        "        print('slurm-advisor-controller')\n"
        "    else:\n"
        "        print('controller-pod')\n"
        "else:\n"
        "    sys.exit('unexpected kubectl arguments: ' + repr(args))\n"
    )
    kubectl.chmod(0o755)
    try:
        result = subprocess.run(
            ["bash", str(SMOKE)],
            env={
                **os.environ,
                "PATH": f"{tmp_path}:{os.environ['PATH']}",
                "E2E_PROFILE": "advisor-observe",
                "E2E_ATTRIBUTION_MODE": "unavailable",
                "E2E_KUBERNETES_SERVICE_AUTH_SMOKE": "1",
                "E2E_KUBERNETES_SERVICE_AUTH_TIMEOUT": "5",
                "HELM_RELEASE": "nvidia-recovery",
                "SLURM_ADVISOR_RELEASE": "slurm-advisor-controller",
            },
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    output = result.stdout + result.stderr
    assert (result.returncode == 0) is succeeds, output
    expected = [("/v1/audit/events?limit=1", None)]
    if missing_status == 401:
        expected.append(("/v1/audit/events?limit=1", f"Bearer {token}"))
    assert requests == expected
    assert token not in output
