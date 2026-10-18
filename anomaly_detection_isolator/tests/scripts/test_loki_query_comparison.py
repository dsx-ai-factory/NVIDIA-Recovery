# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check that comparisons require explicit targets before fetching credentials."""

import contextlib
import importlib.util
import io
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "comparison", Path(__file__).resolve().parents[2] / "scripts" / "loki_query_comparison.py"
)
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class ExplicitConfigurationTests(unittest.TestCase):
    """Exercise CLI validation without contacting Kubernetes or Loki."""

    def setUp(self):
        self.target = [
            "--kubeconfig",
            "/tmp/test-kubeconfig",
            "--context",
            "test-context",
            "--loki-url",
            "https://loki.example.com",
            "--query1",
            '{app="test"}',
            "--start-time",
            "2026-01-01T00:00:00+00:00",
            "--end-time",
            "2026-01-01T01:00:00+00:00",
        ]
        self.token = ["--secret-name", "test-token", "--secret-token-key", "token"]
        self.certs = [
            "--cert-secret",
            "test-cert",
            "--cert-secret-key",
            "tls.crt",
            "--key-secret",
            "test-key",
            "--key-secret-key",
            "tls.key",
            "--ca-cert-secret",
            "test-ca",
            "--ca-cert-secret-key",
            "ca.crt",
        ]

    def assert_rejected(self, args):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            comparison.parse_args(args)
        self.assertEqual(raised.exception.code, 2)

    def test_missing_target_fails_before_credentials_are_fetched(self):
        with patch.object(comparison, "SimpleKubectl") as kubectl, patch("sys.argv", ["comparison"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                comparison.main()
            kubectl.assert_not_called()

    def test_insecure_endpoint_is_rejected_before_fetching_credentials(self):
        for endpoint in ("http://loki.example.com", "https://user:password@loki.example.com"):
            args = self.target + self.token + ["--query2", '{app="test"}', "--loki-url", endpoint]
            with self.subTest(endpoint=endpoint), patch.object(comparison, "SimpleKubectl") as kubectl:
                with (
                    patch("sys.argv", ["comparison", *args]),
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    comparison.main()
                kubectl.assert_not_called()

    def test_empty_shell_variable_cannot_select_an_implicit_context(self):
        self.assert_rejected(self.target + self.token + ["--query2", '{app="test"}', "--context", ""])

    def test_bearer_target_is_preserved_without_certificate_defaults(self):
        args = comparison.parse_args(self.target + self.token + ["--query2", '{app="test"} |= "error"'])
        self.assertEqual(args.context, "test-context")
        self.assertEqual(args.loki_url, "https://loki.example.com")
        self.assertIsNone(args.cert_secret)
        self.assertIsNone(args.x_scope_org_id)

    def test_direction_comparison_needs_only_first_query(self):
        args = comparison.parse_args(self.target + self.certs + ["--compare-directions", "--use-adi-client"])
        self.assertIsNone(args.query2)
        self.assertEqual(args.cert_secret_key, "tls.crt")
        self.assertIsNone(args.secret_name)

    def test_missing_or_ambiguous_configuration_is_rejected(self):
        for args in (
            self.target + self.token,
            self.target + ["--query2", '{app="test"}'],
            self.target + self.certs[:-2] + ["--query2", '{app="test"}'],
            self.target + self.token + self.certs + ["--query2", '{app="test"}'],
        ):
            with self.subTest(args=args):
                self.assert_rejected(args)


if __name__ == "__main__":
    unittest.main()
