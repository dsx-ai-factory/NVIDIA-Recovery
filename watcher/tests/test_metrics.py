# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
from typing import Any, ClassVar
from unittest.mock import patch

import pytest
from prometheus_client import CollectorRegistry, generate_latest
from watcher.lib.utils import metrics
from watcher.lib.utils.metrics import emit_app_info_metrics, measure_duration


def test_neutral_log_query_metrics_export_neutral_series():
    """Provider-independent collectors must not export Loki-named families."""
    registry = CollectorRegistry()
    for name, collector in vars(metrics).items():
        if name.startswith(("LOG_QUERY_", "WORKFLOW_LOG_QUERY_")):
            registry.register(collector)

    metrics.LOG_QUERY_POLLER_BACKLOG.labels(query="crash").set(1)
    exposition = generate_latest(registry).decode()
    # Provider label values may still legitimately contain "loki".
    assert all("loki" not in family.name for family in registry.collect())
    assert 'watcher_log_query_poller_backlog_seconds{query="crash"} 1.0' in exposition
    assert "watcher_workflow_log_query_failures_total" in exposition
    assert "watcher_downstream_log_query_records_received_total" in exposition


## Fake Gauge classes to intercept metric creation and updates
##
class FakeGaugeChild:
    def __init__(self):
        self.value = None

    def set(self, value):
        self.value = value


class FakeGauge:
    """A fake Gauge class that mimics the prometheus_client Gauge API."""

    instances: ClassVar[list[Any]] = []  # This will hold every FakeGauge instance created

    def __init__(self, name, documentation, labelnames):
        self.name = name
        self.documentation = documentation
        self.labelnames = labelnames
        self.labels_calls = {}  # Will map a tuple of label items to a FakeGaugeChild instance.
        FakeGauge.instances.append(self)

    def labels(self, **labels):
        # Sort the labels items to have a consistent key.
        key = tuple(sorted(labels.items()))
        if key not in self.labels_calls:
            self.labels_calls[key] = FakeGaugeChild()
        return self.labels_calls[key]


## Fake Histogram classes to intercept metric creation and updates
##
class FakeHistogram:
    """A fake Histogram class that mimics the prometheus_client Histogram API."""

    instances: ClassVar[list[Any]] = []  # This will hold every FakeHistogram instance created

    def __init__(self, name, documentation, labelnames=None):
        self.name = name
        self.documentation = documentation
        self.labelnames = labelnames or []
        self.observations = []  # Will store all observed values
        FakeHistogram.instances.append(self)

    def observe(self, value):
        """Record an observation."""
        self.observations.append(value)

    def labels(self, **labels):
        # For simplicity, return self since we're not testing label functionality
        return self


## Fixtures
##
@pytest.fixture(autouse=True)
def reset_fake_instances():
    """
    Ensure FakeGauge and FakeHistogram instances are cleared before each test.
    """
    FakeGauge.instances.clear()
    FakeHistogram.instances.clear()


@pytest.fixture(autouse=True)
def patch_prometheus_client(monkeypatch):
    """
    Monkeypatch the Gauge and Histogram in the watcher.lib.utils.metrics module.
    """
    monkeypatch.setattr("watcher.lib.utils.metrics.Gauge", FakeGauge)
    monkeypatch.setattr("watcher.lib.utils.metrics.Histogram", FakeHistogram)


## Tests
##
def test_emit_app_info_metrics_single_env_set(monkeypatch, caplog):
    """
    When a single environment variable is set, the metric
    should be emitted with the label formatted as 'envvar_lowercased_value' and set to 1.
    """
    env_var = "VERSION"
    env_value = "1.0.0-alpha147"
    # Set the environment variable.
    monkeypatch.setenv(env_var, env_value)

    with caplog.at_level(logging.INFO):
        emit_app_info_metrics(env_var)

    # There should be one FakeGauge instance.
    assert len(FakeGauge.instances) == 1
    gauge = FakeGauge.instances[0]
    # Expected label: "version_1.0.0-alpha147"
    expected_label = {"name": f"{env_var.lower()}_{env_value}"}
    expected_key = tuple(sorted(expected_label.items()))
    # Verify that the FakeGauge was used to emit a metric with the expected label.
    assert expected_key in gauge.labels_calls, "Expected label not found in gauge labels_calls"
    gauge_child = gauge.labels_calls[expected_key]
    assert gauge_child.value == 1, "Gauge value was not set to 1"

    # Verify the log message.
    expected_log = f"[emit_app_info_metrics] Emitting app info metric: app_info/{env_var.lower()}_{env_value}"
    assert any(expected_log in record.message for record in caplog.records), "Expected log message not found"


def test_emit_app_info_metrics_single_env_not_set(monkeypatch, caplog):
    """
    When a single environment variable is not set, no metric should be emitted,
    and no log message should mention the variable.
    """
    env_var = "LOG_LEVEL"
    # Ensure the environment variable is not set.
    monkeypatch.delenv(env_var, raising=False)

    with caplog.at_level(logging.INFO):
        emit_app_info_metrics(env_var)

    # A FakeGauge instance is still created but no call to .labels() should be made.
    assert len(FakeGauge.instances) == 1
    gauge = FakeGauge.instances[0]
    assert gauge.labels_calls == {}, "No metric should be emitted if the env var is not set"

    # Ensure no log message mentioning the env var is present.
    for record in caplog.records:
        assert env_var.lower() not in record.message.lower()


def test_emit_app_info_metrics_multiple_env(monkeypatch, caplog):
    """
    When a list of environment variables is provided, and only some are set,
    metrics should be emitted only for those that are set.
    """
    env_var_set = "LOG_LEVEL"
    env_value_set = "debug"
    env_var_not_set = "NON_EXISTENT"
    monkeypatch.setenv(env_var_set, env_value_set)
    monkeypatch.delenv(env_var_not_set, raising=False)

    env_vars = [env_var_set, env_var_not_set]

    with caplog.at_level(logging.INFO):
        emit_app_info_metrics(env_vars)

    # Check that one FakeGauge instance was created.
    assert len(FakeGauge.instances) == 1
    gauge = FakeGauge.instances[0]

    # Only the set env var should result in a label call.
    expected_label = {"name": f"{env_var_set.lower()}_{env_value_set}"}
    expected_key = tuple(sorted(expected_label.items()))
    assert expected_key in gauge.labels_calls, "Expected label for set env var not found"
    assert gauge.labels_calls[expected_key].value == 1, "Gauge value for set env var not set to 1"

    # There should be exactly one label call.
    assert len(gauge.labels_calls) == 1


## Duration Metrics Tests
##
def test_measure_duration_decorator_sync_function():
    """
    Test that the measure_duration decorator correctly measures and records
    the duration of a synchronous function.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_metric", "Test metric")

    # Mock time.perf_counter to return specific values
    with patch("watcher.lib.utils.metrics.time.perf_counter", side_effect=[1.0, 1.5]):

        @measure_duration(fake_metric)
        def test_function():
            return "test_result"

        result = test_function()

    # Verify the function returned the expected result
    assert result == "test_result"

    # Verify that a duration was recorded
    assert len(fake_metric.observations) == 1

    # Verify the exact duration (1.5 - 1.0 = 0.5)
    recorded_duration = fake_metric.observations[0]
    assert recorded_duration == 0.5


def test_measure_duration_decorator_async_function():
    """
    Test that the measure_duration decorator correctly measures and records
    the duration of an asynchronous function.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_async_metric", "Test async metric")

    # Mock time.perf_counter to return specific values
    with patch("watcher.lib.utils.metrics.time.perf_counter", side_effect=[2.0, 3.2]):

        @measure_duration(fake_metric)
        async def test_async_function():
            return "async_test_result"

        # Run the async function
        result = asyncio.run(test_async_function())

    # Verify the function returned the expected result
    assert result == "async_test_result"

    # Verify that a duration was recorded
    assert len(fake_metric.observations) == 1

    # Verify the exact duration (3.2 - 2.0 = 1.2)
    recorded_duration = fake_metric.observations[0]
    assert abs(recorded_duration - 1.2) < 1e-10


def test_measure_duration_decorator_function_with_exception():
    """
    Test that the measure_duration decorator records duration even when
    the decorated function raises an exception.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_exception_metric", "Test exception metric")

    # Mock time.perf_counter to return specific values
    with patch("watcher.lib.utils.metrics.time.perf_counter", side_effect=[5.0, 5.75]):

        @measure_duration(fake_metric)
        def test_function_with_exception():
            raise ValueError("Test exception")

        # Verify the exception is propagated
        with pytest.raises(ValueError, match="Test exception"):
            test_function_with_exception()

    # Verify that a duration was still recorded
    assert len(fake_metric.observations) == 1

    # Verify the exact duration (5.75 - 5.0 = 0.75)
    recorded_duration = fake_metric.observations[0]
    assert recorded_duration == 0.75


def test_measure_duration_decorator_async_function_with_exception():
    """
    Test that the measure_duration decorator records duration even when
    the decorated async function raises an exception.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_async_exception_metric", "Test async exception metric")

    # Mock time.perf_counter to return specific values
    with patch("watcher.lib.utils.metrics.time.perf_counter", side_effect=[10.0, 10.25]):

        @measure_duration(fake_metric)
        async def test_async_function_with_exception():
            raise ValueError("Test async exception")

        # Verify the exception is propagated
        with pytest.raises(ValueError, match="Test async exception"):
            asyncio.run(test_async_function_with_exception())

    # Verify that a duration was still recorded
    assert len(fake_metric.observations) == 1

    # Verify the exact duration (10.25 - 10.0 = 0.25)
    recorded_duration = fake_metric.observations[0]
    assert recorded_duration == 0.25


def test_measure_duration_decorator_with_function_arguments():
    """
    Test that the measure_duration decorator works correctly with functions
    that have arguments and return values.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_args_metric", "Test args metric")

    # Mock time.perf_counter to return specific values
    with patch("watcher.lib.utils.metrics.time.perf_counter", side_effect=[0.0, 0.123]):

        @measure_duration(fake_metric)
        def test_function_with_args(x, y, z=None):
            return x + y + (z or 0)

        result = test_function_with_args(1, 2, z=3)

    # Verify the function returned the expected result
    assert result == 6

    # Verify that a duration was recorded
    assert len(fake_metric.observations) == 1

    # Verify the exact duration (0.123 - 0.0 = 0.123)
    recorded_duration = fake_metric.observations[0]
    assert recorded_duration == 0.123


def test_measure_duration_decorator_multiple_calls():
    """
    Test that the measure_duration decorator correctly records multiple
    function calls to the same metric.
    """
    # Create a fake histogram metric
    fake_metric = FakeHistogram("test_multiple_calls_metric", "Test multiple calls metric")

    # Mock time.perf_counter to return specific values for multiple calls
    # Each call needs two values (start, end)
    with patch(
        "watcher.lib.utils.metrics.time.perf_counter",
        side_effect=[
            1.0,
            1.1,  # First call: 0.1 duration
            2.0,
            2.3,  # Second call: 0.3 duration
            3.0,
            3.5,  # Third call: 0.5 duration
        ],
    ):

        @measure_duration(fake_metric)
        def test_function():
            return "result"

        # Call the function multiple times
        for _ in range(3):
            result = test_function()
            assert result == "result"

    # Verify that three durations were recorded
    assert len(fake_metric.observations) == 3

    # Verify the exact durations
    assert abs(fake_metric.observations[0] - 0.1) < 1e-10
    assert abs(fake_metric.observations[1] - 0.3) < 1e-10
    assert abs(fake_metric.observations[2] - 0.5) < 1e-10
