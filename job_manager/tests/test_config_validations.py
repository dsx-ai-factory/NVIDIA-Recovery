# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from job_manager.main import validate_service_config
from test_utils import create_basic_service_config


class TestConfigValidations(unittest.TestCase):
    def setUp(self):
        self.config = create_basic_service_config()

    def test_validate_service_config(self):
        # If all validations pass, the function should return None, otherwise a RuntimeError will be raised.
        self.assertIsNone(validate_service_config(config=self.config))

    def test_legacy_refresh_flag_requires_no_queue(self):
        """Old service overrides cannot require the retired refresh route."""
        self.config["service"]["on_demand_job_update_request_enabled"] = True
        self.assertIsNone(validate_service_config(config=self.config))

    def test_legacy_relay_flag_requires_no_queue(self):
        """Obsolete relay settings cannot require a retired publisher route."""
        self.config["service"]["job_anomaly_message_relay_enabled"] = True
        self.assertIsNone(validate_service_config(config=self.config))

    def test_validate_audit_trail_config(self):
        self.config["service"]["audit_trail"] = {
            "destination": "http://heimdall-notifications:8080/v1/audit/events",
            "max_batch_size": 100,
            "max_buffer_size": 1000,
            "flush_interval_seconds": 30,
        }
        self.assertIsNone(validate_service_config(config=self.config))

    def test_invalid_audit_trail_batch_size(self):
        self.config["service"]["audit_trail"] = {"max_batch_size": 0}
        with self.assertRaisesRegex(RuntimeError, "Invalid audit trail configuration"):
            validate_service_config(config=self.config)

    def test_invalid_audit_trail_destination(self):
        self.config["service"]["audit_trail"] = {"destination": "notifications"}
        with self.assertRaisesRegex(RuntimeError, "Invalid audit trail configuration"):
            validate_service_config(config=self.config)

    def test_invalid_job_state_based_auto_resume_config_missing_field(self):
        self.config["service"]["job_state_based_auto_resume_time_window_secs"] = None
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)
        self.config["service"]["job_state_based_auto_resume_time_window_secs"] = 900

        self.config["service"]["job_state_based_auto_resume_threshold_secs"] = None
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)

    def test_invalid_job_state_based_auto_resume_config_non_postive_value(self):
        self.config["service"]["job_state_based_auto_resume_time_window_secs"] = -100
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)
        self.config["service"]["job_state_based_auto_resume_time_window_secs"] = 900

        self.config["service"]["job_state_based_auto_resume_threshold_secs"] = -100
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)

    def test_invalid_job_state_based_auto_resume_config_invalid_values(self):
        # Time window is set to 200 which is smaller than the thershold (300).
        self.config["service"]["job_state_based_auto_resume_time_window_secs"] = 200
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)

    def test_invalid_guardrail_rules_config_missing_field(self):
        self.config["service"]["guardrail_rules"] = None
        with self.assertRaises(RuntimeError):
            validate_service_config(config=self.config)

    def test_valid_dag_support_config(self):
        """DAG support persists Advice and needs no dedicated event-bus route."""
        self.config["service"]["dag_support"]["enabled"] = True
        self.assertIsNone(validate_service_config(config=self.config))
