# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from attribution_adapter.metrics_fetcher import (
    ADIMetricsFetcher,
)
from recovery_libs.utils.retry import exponential_backoff_retry


# Create a patched version of the retry decorator with max_retries=1 for testing
def patched_retry_decorator(*args, **kwargs):
    kwargs["max_retries"] = 1
    return exponential_backoff_retry(*args, **kwargs)


# Patch the retry decorator for all tests
@patch("recovery_libs.utils.retry.exponential_backoff_retry", patched_retry_decorator)
class TestADIMetricsFetcher(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock Settings object
        self.mock_settings = MagicMock()
        self.mock_settings.prometheus_url = "http://prometheus.example.com"
        self.mock_settings.prometheus_cluster = "test-cluster"
        self.mock_settings.prometheus_tenants = ["tenant1", "tenant2"]
        self.mock_settings.prometheus_checks_enabled = True
        self.mock_settings.prometheus_hostname_label = "hostname"

        # Create the metrics fetcher with the mock settings
        self.prom_token = "test-token"
        self.metrics_fetcher = ADIMetricsFetcher(self.mock_settings, self.prom_token)

        # Add missing attributes that are used in the class but not defined in the code
        self.metrics_fetcher.prometheus_ep = "http://prometheus.example.com"
        self.metrics_fetcher.logger = MagicMock()
        self.metrics_fetcher.timeout = 30
        self.metrics_fetcher.rs = MagicMock()

    def test_initialization(self):
        """Test that the ADIMetricsFetcher initializes correctly."""
        self.assertEqual(self.metrics_fetcher.settings, self.mock_settings)
        self.assertEqual(self.metrics_fetcher.prometheus_url, "http://prometheus.example.com")
        self.assertEqual(self.metrics_fetcher.prometheus_cluster, "test-cluster")
        self.assertEqual(self.metrics_fetcher.prometheus_tenants, ["tenant1", "tenant2"])
        self.assertEqual(self.metrics_fetcher.prometheus_checks_enabled, True)
        self.assertEqual(self.metrics_fetcher.prometheus_hostname_label, "hostname")
        self.assertEqual(self.metrics_fetcher.qepr, "http://prometheus.example.com/api/v1/query_range")

    @patch("attribution_adapter.metrics_fetcher.ADIMetricsFetcher._get_or_post")
    def test_get_metrics_success(self, mock_get_or_post):
        """Test successful metrics retrieval."""
        # Setup mock response
        mock_response = {"status": "success", "data": {"result": [{"values": [[1609459200, "42"]]}]}}
        mock_get_or_post.return_value = mock_response

        # Call the method
        start_time = datetime(2021, 1, 1)
        end_time = datetime(2021, 1, 2)
        query = "test_query"

        result = self.metrics_fetcher.get_metrics(query, start_time, end_time)

        # Verify the result
        self.assertEqual(result, {"result": [{"values": [[1609459200, "42"]]}]})

        # Verify the method was called with correct parameters
        mock_get_or_post.assert_called_once()
        call_args = mock_get_or_post.call_args[1]
        self.assertEqual(call_args["ep"], "http://prometheus.example.com")
        self.assertEqual(call_args["filter"], {"query": "test_query"})

    @patch("attribution_adapter.metrics_fetcher.ADIMetricsFetcher._get_or_post")
    def test_get_metrics_failure_status(self, mock_get_or_post):
        """Test metrics retrieval with failure status."""
        # Setup mock response with error
        mock_response = {"status": "error", "error": "Test error message"}
        mock_get_or_post.return_value = mock_response

        # Call the method
        start_time = datetime(2021, 1, 1)
        end_time = datetime(2021, 1, 2)
        query = "test_query"

        result = self.metrics_fetcher.get_metrics(query, start_time, end_time)

        # Verify the result is None
        self.assertIsNone(result)

        # Verify logger was called with error message
        self.metrics_fetcher.logger.critical.assert_called_with("Prometheus Query Failed [Test error message]")

    @patch.object(ADIMetricsFetcher, "_get_or_post")
    def test_get_metrics_exception(self, mock_get_or_post):
        """Test metrics retrieval with exception."""
        # Setup mock to raise an exception
        mock_get_or_post.side_effect = Exception("Connection error")

        # Call the method
        start_time = datetime(2021, 1, 1)
        end_time = datetime(2021, 1, 2)
        query = "test_query"

        result = self.metrics_fetcher.get_metrics(query, start_time, end_time)

        # Verify the result is None
        self.assertIsNone(result)

        # Verify logger was called with error message
        self.metrics_fetcher.logger.critical.assert_called_with(
            "Cannot contact Prometheus endpoint", exc_info=True, extra={"error": "Connection error"}
        )

    def test_get_or_post_get_success(self):
        """Test successful GET request in _get_or_post."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success", "data": "test_data"}
        self.metrics_fetcher.rs.get.return_value = mock_response

        # Call the method
        result = self.metrics_fetcher._get_or_post("http://test.com")

        # Verify the result
        self.assertEqual(result, {"status": "success", "data": "test_data"})

        # Verify the method was called with correct parameters
        self.metrics_fetcher.rs.get.assert_called_once_with(url="http://test.com", timeout=30)

    def test_get_or_post_post_success(self):
        """Test successful POST request in _get_or_post."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success", "data": "test_data"}
        self.metrics_fetcher.rs.post.return_value = mock_response

        # Call the method
        filter_data = {"query": "test_query"}
        result = self.metrics_fetcher._get_or_post("http://test.com", filter_data)

        # Verify the result
        self.assertEqual(result, {"status": "success", "data": "test_data"})

        # Verify the method was called with correct parameters
        self.metrics_fetcher.rs.post.assert_called_once_with(url="http://test.com", timeout=30, data=filter_data)


if __name__ == "__main__":
    unittest.main()
