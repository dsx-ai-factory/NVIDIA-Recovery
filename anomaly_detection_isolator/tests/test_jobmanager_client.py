# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import Mock, patch

import requests
from anomaly_detection_isolator.job_manager_client import JobManagerClient
from anomaly_detection_isolator.settings.settings import Settings
from pydantic import ValidationError
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus


class TestJobManagerClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_settings = Mock(spec=Settings)
        self.mock_settings.jobmanager_host = "http://jobmanager.example.com"
        self.mock_settings.jobmanager_status_path = "/jobs/status/"
        self.mock_settings.jobmanager_http_timeout_seconds = 30

        # Sample response data matching SlurmJobExecutionStatus fields
        self.sample_response = {
            "job_id": "123",
            "workflow_id": "test-workflow",
            "job_attempt_index": 1,
            "state": "COMPLETED",
            "state_reason": "Job completed successfully",
            "submit_time": "2024-01-01T00:00:00Z",
            "start_time": "2024-01-01T00:01:00Z",
            "end_time": "2024-01-01T00:02:00Z",
            "cluster": "cluster1",
            "partition": "partition1",
            "reservation": "reservation1",
            # Add other required fields based on SlurmJobExecutionStatus model
        }

    def test_init_with_port(self):
        """Test client initialization with port specified."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)
        expected_url = "http://jobmanager.example.com:8080"
        self.assertEqual(client.base_url, expected_url)

    def test_init_without_port(self):
        """Test client initialization without port specified."""
        self.mock_settings.jobmanager_port = None
        client = JobManagerClient(self.mock_settings)
        expected_url = "http://jobmanager.example.com"
        self.assertEqual(client.base_url, expected_url)

    @patch("requests.get")
    def test_get_job_status_basic(self, mock_get):
        """Test basic job status retrieval without attempt index."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = self.sample_response
        mock_get.return_value = mock_response

        result = client.get_job_status("tag1", 123)

        # Verify the request
        expected_url = "http://jobmanager.example.com:8080/jobs/status/123"
        mock_get.assert_called_once_with(expected_url, params={}, timeout=30)

        # Verify the response is parsed into SlurmJobExecutionStatus
        self.assertIsInstance(result, SlurmJobExecutionStatus)
        self.assertEqual(result.job_id, "123")
        self.assertEqual(result.state, "COMPLETED")
        self.assertEqual(result.state_reason, "Job completed successfully")

    @patch("requests.get")
    def test_get_job_status_with_attempt(self, mock_get):
        """Test job status retrieval with attempt index."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = self.sample_response
        mock_get.return_value = mock_response

        result = client.get_job_status("tag1", 123, job_attempt_index=2)

        # Verify the request
        expected_url = "http://jobmanager.example.com:8080/jobs/status/123"
        mock_get.assert_called_once_with(expected_url, params={"job_attempt_index": 2}, timeout=30)

        # Verify the response is parsed into SlurmJobExecutionStatus
        self.assertIsInstance(result, SlurmJobExecutionStatus)

    @patch("requests.get")
    def test_get_job_status_with_string_job_id(self, mock_get):
        """Test job status retrieval with a RunAI-style string job id."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        mock_response = Mock()
        mock_response.json.return_value = {**self.sample_response, "job_id": "qa-amcharg-xid149pri-0402-142414"}
        mock_get.return_value = mock_response

        result = client.get_job_status("tag1", "qa-amcharg-xid149pri-0402-142414")

        expected_url = "http://jobmanager.example.com:8080/jobs/status/qa-amcharg-xid149pri-0402-142414"
        mock_get.assert_called_once_with(expected_url, params={}, timeout=30)
        self.assertIsInstance(result, SlurmJobExecutionStatus)
        self.assertEqual(result.job_id, "qa-amcharg-xid149pri-0402-142414")

    @patch("requests.get")
    def test_get_job_status_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        # Setup mock to raise an HTTP error
        mock_get.side_effect = requests.exceptions.HTTPError("404 Client Error")

        jm_response = client.get_job_status("tag1", 123)
        self.assertIsNone(jm_response)

    @patch("requests.get")
    def test_get_job_status_connection_error(self, mock_get):
        """Test handling of connection errors."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        # Setup mock to raise a connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

        jm_response = client.get_job_status("tag1", 123)
        self.assertIsNone(jm_response)

    @patch("requests.get")
    def test_get_job_status_passes_timeout(self, mock_get):
        """Test that timeout is passed to requests.get()."""
        self.mock_settings.jobmanager_http_timeout_seconds = 30
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)
        mock_response = Mock()
        mock_response.json.return_value = self.sample_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        client.get_job_status("tag1", 123)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs.get("timeout"), 30)

    @patch("requests.get")
    def test_get_job_status_invalid_response(self, mock_get):
        """Test handling of invalid response data."""
        self.mock_settings.jobmanager_port = 8080
        client = JobManagerClient(self.mock_settings)

        # Setup mock with invalid response data
        mock_response = Mock()
        mock_response.json.return_value = {"invalid": "data"}
        mock_get.return_value = mock_response

        with self.assertRaises(ValidationError):
            client.get_job_status("tag1", 123)


if __name__ == "__main__":
    unittest.main()
