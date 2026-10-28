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

    def test_advice_management_requires_delayed_attribution_retry(self):
        """Late attribution must not be consumed without a working retry delay."""
        self.config["service"]["advice_management_enabled"] = True

        with self.assertRaisesRegex(RuntimeError, "must be greater than 0 when Advice management is enabled"):
            validate_service_config(config=self.config)

    def test_advice_management_uses_consumer_retry_delay_default(self):
        """Omitting the delay uses the same safe default as consumer creation."""
        self.config["service"]["advice_management_enabled"] = True
        self.config["service"]["event_bus"].pop("anomaly_queue_delivery_retry_delay_secs")

        self.assertIsNone(validate_service_config(config=self.config))

    def test_disabled_advice_management_does_not_require_retry_configuration(self):
        """Observe-only configurations need no attribution retry topology."""
        event_bus = self.config["service"]["event_bus"]
        event_bus.pop("anomaly_queue_delivery_retry_delay_secs")
        event_bus.pop("anomaly_dead_letter_routing_key")
        event_bus.pop("anomaly_dead_letter_redelivery_routing_key")

        self.assertIsNone(validate_service_config(config=self.config))

    def test_delayed_attribution_retry_requires_both_routing_keys(self):
        """A positive retry delay is usable only when both queue bindings exist."""
        event_bus = self.config["service"]["event_bus"]
        event_bus["anomaly_queue_delivery_retry_delay_secs"] = 30

        for missing_key in (
            "anomaly_dead_letter_routing_key",
            "anomaly_dead_letter_redelivery_routing_key",
        ):
            with self.subTest(missing_key=missing_key):
                configured_value = event_bus.pop(missing_key)
                with self.assertRaisesRegex(RuntimeError, "must be specified"):
                    validate_service_config(config=self.config)
                event_bus[missing_key] = configured_value

    def test_delayed_attribution_retry_requires_distinct_routing_keys(self):
        """One key cannot safely address both the timer and consumer queues."""
        event_bus = self.config["service"]["event_bus"]
        event_bus["anomaly_queue_delivery_retry_delay_secs"] = 30
        event_bus["anomaly_dead_letter_redelivery_routing_key"] = event_bus["anomaly_dead_letter_routing_key"]

        with self.assertRaisesRegex(RuntimeError, "must be different"):
            validate_service_config(config=self.config)

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
