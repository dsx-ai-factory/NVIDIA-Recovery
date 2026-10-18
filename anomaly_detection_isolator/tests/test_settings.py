# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os

import pytest
from anomaly_detection_isolator.settings.settings import Settings


@pytest.fixture
def env_vars():
    """Fixture to set up environment variables for testing."""
    # Store original environment
    original_env = dict(os.environ)

    # Clear all environment variables first
    os.environ.clear()

    # Remove any existing CONFIG_FILE_PATH
    original_env.pop("CONFIG_FILE_PATH", None)

    # Set required environment variables
    os.environ.update(
        {
            "root_path": "/test/path",
            "exchange_name": "test-exchange",
            "routing_key": "test-routing",
            "queue_name": "test-queue",
            "rabbitmq_user": "test-user",
            "rabbitmq_password": "test-password",
            "rabbitmq_url": "test-rabbitmq",
            "rabbitmq_port": "5672",
            "rabbitmq_vhost": "test-vhost",
            "attribution_request_exchange": "test-jobkill-exchange",
            "attribution_request_queue": "test-jobkill-queue",
            "isolator_completed_exchange": "test-isolator-exchange",
            "slow_signals_exchange": "test-slow-exchange",
            "slow_signals_queue": "test-slow-queue",
            "epilogue_started_wait_seconds": "30",
            "epilog_query_duration_minutes": "5",
            "slow_signals_delay_seconds": "60",
            "num_worker_threads": "4",
            "jobmanager_host": "test-jobmanager",
            "jobmanager_port": "8080",
            "jobmanager_status_path": "/status",
            "max_isolated_nodes": "5",
        }
    )

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


def test_settings_from_env_vars(env_vars):
    """Test that Settings class correctly loads values from environment variables."""
    settings = Settings()

    # Test required fields
    assert settings.root_path == "/test/path"

    # Test RabbitMQ settings
    assert settings.exchange_name == "test-exchange"
    assert settings.routing_key == "test-routing"
    assert settings.epilog_query_duration_minutes == 5
    assert settings.queue_name == "test-queue"
    assert settings.rabbitmq_user == "test-user"
    assert settings.rabbitmq_password.get_secret_value() == "test-password"
    assert settings.rabbitmq_url == "test-rabbitmq"
    assert settings.rabbitmq_port == 5672
    assert settings.rabbitmq_vhost == "test-vhost"

    # Test queue and exchange settings
    assert settings.attribution_request_exchange == "test-jobkill-exchange"
    assert settings.attribution_request_queue == "test-jobkill-queue"
    assert settings.isolator_completed_exchange == "test-isolator-exchange"
    assert settings.slow_signals_exchange == "test-slow-exchange"
    assert settings.slow_signals_queue == "test-slow-queue"

    # Test numeric settings
    assert settings.epilogue_started_wait_seconds == 30
    assert settings.slow_signals_delay_seconds == 60
    assert settings.num_worker_threads == 4
    assert settings.jobmanager_port == 8080
    assert settings.max_isolated_nodes == 5


def test_settings_default_values(env_vars):
    """Test that Settings class has correct default values."""
    settings = Settings()
    # Attribution service defaults
    assert settings.attribution_enabled is True
    assert settings.attribution_service_url is None
    assert settings.attribution_fast_http_timeout_seconds == 90
    assert settings.attribution_slow_http_timeout_seconds == 180
    assert settings.audit_trail.destination is None
    assert settings.audit_trail.max_batch_size == 100
    assert settings.audit_trail.max_buffer_size == 1000
    assert settings.audit_trail.flush_interval_seconds == 30


def test_settings_optional_fields(env_vars):
    """Test that optional fields can be None."""
    settings = Settings()
    assert settings.fact_non_isolated_errors is None
    assert settings.fact_isolated_errors is None
    assert settings.fact_ambiguous_errors is None
    assert settings.attribution_service_url is None


def test_settings_invalid_port(env_vars):
    """Test that invalid port number raises validation error."""
    os.environ["rabbitmq_port"] = "invalid"
    with pytest.raises(ValueError):
        Settings()


def test_http_timeout_settings_defaults(env_vars):
    """Verify HTTP timeout settings have correct defaults."""
    settings = Settings()
    assert settings.jobmanager_http_timeout_seconds == 30


def test_attribution_settings(env_vars):
    """Test that attribution service fields load from the environment."""
    os.environ.update(
        {
            "attribution_enabled": "true",
            "attribution_service_url": "http://attribution:8000",
            "attribution_fast_http_timeout_seconds": "120",
            "attribution_slow_http_timeout_seconds": "240",
        }
    )
    settings = Settings()
    assert settings.attribution_enabled is True
    assert settings.attribution_service_url == "http://attribution:8000"
    assert settings.attribution_fast_http_timeout_seconds == 120
    assert settings.attribution_slow_http_timeout_seconds == 240


def test_audit_trail_settings(env_vars):
    """Test that audit trail fields load from the environment."""
    os.environ["audit_trail"] = json.dumps(
        {
            "destination": "http://heimdall-notifications:8080/v1/audit/events",
            "max_batch_size": 25,
            "max_buffer_size": 250,
            "flush_interval_seconds": 5,
        }
    )

    settings = Settings()

    assert str(settings.audit_trail.destination) == "http://heimdall-notifications:8080/v1/audit/events"
    assert settings.audit_trail.max_batch_size == 25
    assert settings.audit_trail.max_buffer_size == 250
    assert settings.audit_trail.flush_interval_seconds == 5


def test_settings_fact_fields(env_vars):
    """Test that fact-related exclusion error fields are handled correctly."""
    # Test with None values (default)
    settings = Settings()
    assert settings.fact_non_isolated_errors is None
    assert settings.fact_isolated_errors is None
    assert settings.fact_ambiguous_errors is None
    assert settings.fact_non_isolated_errors_env is None
    assert settings.fact_isolated_errors_env is None
    assert settings.fact_ambiguous_errors_env is None

    # Test with actual values
    os.environ.update(
        {
            "fact_non_isolated_errors": '["error1", "error2"]',
            "fact_isolated_errors": '["error3", "error4"]',
            "fact_ambiguous_errors": '["error5", "error6"]',
            "fact_non_isolated_errors_env": '["error7", "error8"]',
            "fact_isolated_errors_env": '["error9", "error10"]',
            "fact_ambiguous_errors_env": '["error11", "error12"]',
        }
    )

    settings = Settings()
    assert settings.fact_non_isolated_errors == ["error1", "error2"]
    assert settings.fact_isolated_errors == ["error3", "error4"]
    assert settings.fact_ambiguous_errors == ["error5", "error6"]
    assert settings.fact_non_isolated_errors_env == ["error7", "error8"]
    assert settings.fact_isolated_errors_env == ["error9", "error10"]
    assert settings.fact_ambiguous_errors_env == ["error11", "error12"]


@pytest.mark.parametrize(
    ("enabled", "url", "valid"),
    [
        (True, None, False),
        (True, "", False),
        (True, "ftp://adapter", False),
        (True, "http://adapter:8000", True),
        (False, None, True),
    ],
)
def test_enabled_attribution_requires_valid_endpoint_at_startup(env_vars, enabled, url, valid):
    """Missing enabled provider configuration must fail before queue consumption."""
    from anomaly_detection_isolator.main import validate_settings

    settings = Settings(
        attribution_enabled=enabled,
        attribution_service_url=url,
        fact_isolated_errors=["XID_31"],
    )
    assert validate_settings(settings) is valid
