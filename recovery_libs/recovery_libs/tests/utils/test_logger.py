# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import logging
import time
from unittest.mock import Mock, patch

import pytest
from recovery_libs.utils.logger import (
    NO_SPAM_LOGGER_INTERVAL_SECONDS,
    NO_SPAM_LOGGER_MAX_CACHE_SIZE,
    ConsoleFormatter,
    FastJSONFormatter,
    NoSpamLogger,
    _check_endpoint_availability,
    get_logger,
)
from recovery_libs.utils.types import OTLPProtocol


@pytest.fixture
def log_record():
    """Create a standard LogRecord for testing."""

    def _make_record(msg="Test message", **kwargs):
        return logging.LogRecord(
            name=kwargs.get("name", "test"),
            level=kwargs.get("level", logging.INFO),
            pathname=kwargs.get("pathname", "test.py"),
            lineno=kwargs.get("lineno", 10),
            msg=msg,
            args=kwargs.get("args", ()),
            exc_info=kwargs.get("exc_info", None),
        )

    return _make_record


class TestGetLogger:
    """Test suite for get_logger function."""

    def test_get_logger_default_configuration(self, clean_logger):
        """Test logger with default config and handler management."""
        # Test default configuration
        test_logger = get_logger("test")
        assert test_logger.name == "test"
        assert len(test_logger.handlers) >= 1
        assert not test_logger.propagate

        # Test console handler exists and is configured
        console_handlers = [h for h in test_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) >= 1
        assert console_handlers[0].formatter is not None

        # Test handlers don't duplicate on multiple calls
        initial_count = len(test_logger.handlers)
        test_logger2 = get_logger("test")
        assert len(test_logger2.handlers) == initial_count

    @pytest.mark.parametrize(
        "level",
        [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR],
        ids=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    def test_get_logger_with_different_levels(self, level: int, clean_logger):
        """Test logger with different log levels."""
        assert get_logger("test", level=level).level == level

    def test_get_logger_with_custom_name(self, clean_logger):
        """Test logger with custom name."""
        assert get_logger(name="test_app").name == "test_app"

    def test_get_logger_with_custom_library_levels(self, clean_logger):
        """Test logger with custom third-party library log levels."""
        get_logger("test", custom_library_level={"httpx": logging.WARNING, "urllib3": logging.ERROR})
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.ERROR

    def test_get_logger_uses_text_formatting_for_console(self, clean_logger):
        """Test logger always uses traditional text formatting for console output."""
        test_logger = get_logger("test")
        console_handlers = [h for h in test_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) >= 1
        assert type(console_handlers[0].formatter).__name__ == "ConsoleFormatter"


class TestEndpointAvailability:
    """Test suite for OTLP endpoint availability checking."""

    @pytest.mark.parametrize(
        "endpoint,protocol",
        [
            ("http://localhost:4318", OTLPProtocol.HTTP),
            ("localhost:4317", OTLPProtocol.GRPC),
            ("http://example.com:4318/v1/logs", OTLPProtocol.HTTP),
        ],
        ids=["HTTP-with-scheme", "gRPC-bare", "HTTP-with-path"],
    )
    def test_endpoint_availability_check_success(self, endpoint, protocol):
        """Test endpoint availability check succeeds when connection is possible."""
        with patch("recovery_libs.utils.logger.socket.create_connection") as mock_conn:
            mock_conn.return_value.__enter__ = Mock()
            mock_conn.return_value.__exit__ = Mock()
            assert _check_endpoint_availability(endpoint, protocol) is True

    @pytest.mark.parametrize(
        "exception",
        [ConnectionRefusedError, OSError, TimeoutError],
        ids=["connection-refused", "os-error", "timeout"],
    )
    def test_endpoint_availability_check_failure(self, exception):
        """Test endpoint availability check fails on connection errors."""
        with patch("recovery_libs.utils.logger.socket.create_connection", side_effect=exception):
            assert _check_endpoint_availability("localhost:4317", OTLPProtocol.GRPC) is False


class TestOTLPIntegration:
    """Test suite for OTLP handler integration."""

    @pytest.mark.parametrize(
        "protocol,endpoint,expected_protocol",
        [
            ("http", "http://localhost:4318", OTLPProtocol.HTTP),
            ("grpc", "localhost:4317", OTLPProtocol.GRPC),
        ],
        ids=["HTTP", "gRPC"],
    )
    def test_otlp_handler_created_with_valid_config(
        self, protocol, endpoint, expected_protocol, clean_logger, mock_otlp_handler, monkeypatch
    ):
        """Test OTLP handler created when enable_otlp=True, all env vars set, and endpoint reachable."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", protocol)
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "test-token")

        with patch("recovery_libs.utils.logger._check_endpoint_availability", return_value=True):
            get_logger("test", enable_otlp=True)
            mock_otlp_handler.assert_called_once_with(
                endpoint=endpoint,
                protocol=expected_protocol,
                token="test-token",
                service_name="test",
            )

    @pytest.mark.parametrize(
        "protocol,endpoint,token",
        [
            (None, None, None),  # No config
            ("http", "http://localhost:4318", None),  # Missing token
            ("http", None, "token"),  # Missing endpoint
            (None, "http://localhost:4318", "token"),  # Missing protocol
            ("invalid", "http://localhost:4318", "token"),  # Invalid protocol
        ],
        ids=["no-config", "no-token", "no-endpoint", "no-protocol", "invalid-protocol"],
    )
    def test_otlp_handler_not_created_with_incomplete_config(
        self, protocol, endpoint, token, clean_logger, mock_otlp_handler, monkeypatch
    ):
        """Test OTLP handler not created when any required env var is missing or invalid."""
        if protocol:
            monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", protocol)
        if endpoint:
            monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        if token:
            monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", token)

        with patch("recovery_libs.utils.logger._check_endpoint_availability", return_value=True):
            get_logger("test", enable_otlp=True)
            mock_otlp_handler.assert_not_called()

    def test_otlp_handler_not_created_when_endpoint_unreachable(self, clean_logger, mock_otlp_handler, monkeypatch):
        """Test OTLP handler not created when endpoint is unreachable after retries."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "test-token")

        with (
            patch("recovery_libs.utils.logger._check_endpoint_availability", return_value=False),
            patch("recovery_libs.utils.logger.threading.Event.wait"),  # Speed up test
        ):
            test_logger = get_logger("test", enable_otlp=True)
            mock_otlp_handler.assert_not_called()
            assert test_logger is not None and len(test_logger.handlers) == 1  # Only console handler

    def test_otlp_handler_not_created_when_enable_otlp_false(self, clean_logger, mock_otlp_handler, monkeypatch):
        """Test OTLP handler is not created when enable_otlp=False."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "test-token")

        # Even with all OTLP env vars set, enable_otlp=False should skip OTLP handler
        test_logger = get_logger("test_lib_logger", enable_otlp=False)
        mock_otlp_handler.assert_not_called()
        assert test_logger is not None
        assert len(test_logger.handlers) == 1  # Only console handler
        assert isinstance(test_logger.handlers[0], logging.StreamHandler)

    def test_logger_continues_on_otlp_handler_error(self, clean_logger, mock_otlp_handler, monkeypatch):
        """Test logger creation continues when OTLP handler fails."""
        mock_otlp_handler.side_effect = Exception("OTLP connection failed")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "test-token")

        with patch("recovery_libs.utils.logger._check_endpoint_availability", return_value=True):
            test_logger = get_logger("test", enable_otlp=True)
            assert test_logger is not None and len(test_logger.handlers) == 1  # Only console handler

    def test_otlp_handler_not_recreated_on_second_call(self, clean_logger, monkeypatch):
        """Test OTLP handler is NOT recreated when get_logger is called multiple times (idempotent)."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TOKEN", "test-token")

        with (
            patch("recovery_libs.utils.logger._check_endpoint_availability", return_value=True),
            patch("recovery_libs.utils.logger.OTLPHandler") as mock_handler,
        ):
            # Configure mock handler instance with required attributes
            mock_instance = Mock()
            mock_instance.__class__.__name__ = "OTLPHandler"
            mock_instance.level = logging.INFO
            mock_instance.protocol.value = "http"
            mock_instance.endpoint = "http://localhost:4318"

            mock_handler.return_value = mock_instance

            # First logger creation with enable_otlp=True
            logger1 = get_logger("test", enable_otlp=True)
            assert len(logger1.handlers) == 2  # console + OTLP
            assert mock_handler.call_count == 1

            # Second logger creation (should NOT create new OTLP handler - idempotent)
            logger2 = get_logger("test", enable_otlp=True)
            assert logger1 is logger2  # Same logger object
            assert len(logger2.handlers) == 2  # Still has same handlers
            assert mock_handler.call_count == 1  # Handler was NOT recreated

            # Handler should NOT be closed (still in use)
            mock_instance.close.assert_not_called()

    def test_lazy_otlp_handler_not_recreated(self, clean_logger):
        """Test LazyOTLPHandler is NOT recreated when already present (idempotent)."""
        # Manually create a mock LazyOTLPHandler and add it to the logger
        mock_lazy_handler = Mock()
        mock_lazy_handler.__class__.__name__ = "LazyOTLPHandler"
        mock_lazy_handler.level = logging.INFO

        # Get a basic logger and manually add the lazy handler
        test_logger = logging.getLogger("test_lazy")
        test_logger.addHandler(mock_lazy_handler)

        # Call get_logger which should recognize existing LazyOTLPHandler and NOT recreate
        result_logger = get_logger("test_lazy")

        # Verify the LazyOTLPHandler was NOT closed (still in use)
        mock_lazy_handler.close.assert_not_called()
        # Logger should still have the same handler
        assert mock_lazy_handler in result_logger.handlers


class TestLoggerUsage:
    """Test suite for actual logger usage."""

    @pytest.mark.parametrize(
        "log_level,message,level_name",
        [
            (logging.INFO, "Test info", "INFO"),
            (logging.ERROR, "Test error", "ERROR"),
        ],
        ids=["info", "error"],
    )
    def test_logger_messages(self, log_level, message, level_name, clean_logger, caplog):
        """Test logging messages at different levels."""
        test_logger = get_logger("test")
        test_logger.propagate = True

        with caplog.at_level(log_level, logger="test"):
            test_logger.log(log_level, message)

        assert message in caplog.text
        assert level_name in caplog.text

    def test_logger_level_filtering(self, clean_logger):
        """Test log level filtering."""
        # Test INFO logger filters DEBUG messages
        info_logger = get_logger("test", level=logging.INFO)
        assert info_logger.isEnabledFor(logging.INFO)
        assert not info_logger.isEnabledFor(logging.DEBUG)

        # Test DEBUG logger allows DEBUG messages
        debug_logger = get_logger("test2", level=logging.DEBUG)
        assert debug_logger.isEnabledFor(logging.DEBUG)

    def test_logger_with_extra_fields(self, clean_logger, capsys):
        """Test logging with extra contextual fields."""
        test_logger = get_logger("test")
        test_logger.info("User action", extra={"user_id": 123, "action": "login"})

        # Capture stdout where the console handler logs
        captured = capsys.readouterr()
        assert "User action" in captured.out
        # Extra fields should be included in the console output
        assert "user_id=123" in captured.out
        assert "action=login" in captured.out


class TestConsoleFormatter:
    """Test suite for ConsoleFormatter."""

    def test_format_without_extra_fields(self, log_record):
        formatter = ConsoleFormatter("%(levelname)s - %(message)s")
        formatted = formatter.format(log_record())
        assert "INFO - Test message" in formatted
        assert "|" not in formatted

    def test_format_with_extra_fields(self, log_record):
        formatter = ConsoleFormatter("%(levelname)s - %(message)s")
        record = log_record("User action")
        record.user_id = 123
        record.action = "login"
        formatted = formatter.format(record)
        assert "INFO - User action" in formatted
        assert "user_id=123" in formatted and "action=login" in formatted
        assert "|" in formatted


class TestFastJSONFormatter:
    """Test suite for FastJSONFormatter."""

    def test_format_without_extra_fields(self, log_record):
        formatter = FastJSONFormatter()
        formatted = formatter.format(log_record())
        assert '"level":"INFO"' in formatted
        assert '"message":"Test message"' in formatted
        assert '"module":"test"' in formatted

    @pytest.mark.parametrize(
        "field_name,field_value,expected",
        [
            ("user_id", "abc123", '"user_id":"abc123"'),
            ("count", 42, '"count":42'),
            ("duration", 1.5, '"duration":1.5'),
            ("success", True, '"success":true'),
            ("failed", False, '"failed":false'),
            ("optional", None, '"optional":null'),
        ],
    )
    def test_format_with_extra_field_types(self, log_record, field_name, field_value, expected):
        formatter = FastJSONFormatter()
        record = log_record()
        setattr(record, field_name, field_value)
        formatted = formatter.format(record)
        assert expected in formatted

    def test_format_escapes_special_characters(self, log_record):
        formatter = FastJSONFormatter()
        record = log_record('Message with "quotes"')
        record.text = 'Value with "quotes"'
        formatted = formatter.format(record)
        assert r"Message with \"quotes\"" in formatted
        assert r"Value with \"quotes\"" in formatted


@pytest.fixture
def nospam_logger():
    """Create NoSpamLogger with mock for testing."""
    return NoSpamLogger(Mock(), min_interval_seconds=1.0)


class TestNoSpamLogger:
    """Test suite for NoSpamLogger."""

    @pytest.mark.parametrize(
        "interval,cache_size",
        [
            (NO_SPAM_LOGGER_INTERVAL_SECONDS, NO_SPAM_LOGGER_MAX_CACHE_SIZE),
            (60.0, 1000),
        ],
        ids=["defaults", "custom"],
    )
    def test_initialization(self, interval, cache_size):
        nospam = NoSpamLogger(Mock(), max_cache_size=cache_size, min_interval_seconds=interval)
        assert nospam.last_logged.currsize == 0
        assert nospam.last_logged.ttl == interval
        assert nospam.last_logged.maxsize == cache_size

    def test_interval_filtering(self, nospam_logger):
        assert nospam_logger._should_log(logging.INFO, "Test") is True
        nospam_logger._log(logging.INFO, "Test")
        assert nospam_logger._should_log(logging.INFO, "Test") is False
        time.sleep(1.1)
        assert nospam_logger._should_log(logging.INFO, "Test") is True

    @pytest.mark.parametrize(
        "level,method",
        [
            (logging.DEBUG, "debug"),
            (logging.INFO, "info"),
            (logging.WARNING, "warning"),
            (logging.ERROR, "error"),
            (logging.CRITICAL, "critical"),
        ],
    )
    def test_log_methods(self, nospam_logger, level, method):
        getattr(nospam_logger, method)("Test message")
        nospam_logger.logger.log.assert_called_with(level, "Test message")

    def test_log_with_args_and_kwargs(self, nospam_logger):
        nospam_logger.info("Test %s %d", "arg1", 42, extra="value")
        nospam_logger.logger.log.assert_called_once_with(logging.INFO, "Test %s %d", "arg1", 42, extra="value")

    def test_unique_messages_and_caching(self, nospam_logger):
        messages = ["Message 1", "Message 2", "Message 3"]
        for message in messages:
            nospam_logger.info(message)
        assert nospam_logger.logger.log.call_count == len(messages)
        for message in messages:
            assert (logging.INFO, message) in nospam_logger.last_logged

    def test_high_volume_messages(self):
        nospam = NoSpamLogger(Mock(), min_interval_seconds=100.0)
        for i in range(100):
            nospam.info(f"Unique {i}")
            nospam.info("Non-unique %d", i)
        assert nospam.logger.log.call_count == 101
        assert len(nospam.last_logged) == 101

    @pytest.mark.parametrize("message", ["", "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"])
    def test_message_edge_cases(self, nospam_logger, message):
        nospam_logger.info(message)
        nospam_logger.info(message)
        nospam_logger.logger.log.assert_called_once_with(logging.INFO, message)

    @pytest.mark.parametrize(
        "param,value",
        [
            ("min_interval_seconds", 0.0),
            ("min_interval_seconds", -1.0),
            ("max_cache_size", 0),
            ("max_cache_size", -1),
        ],
    )
    def test_invalid_parameters(self, param, value):
        with pytest.raises(ValueError):
            NoSpamLogger(Mock(), **{param: value})

    def test_cache_ttl_expiration(self):
        nospam = NoSpamLogger(Mock(), min_interval_seconds=0.1)
        nospam.info("Test")
        assert nospam.last_logged.currsize == 1
        time.sleep(1.2)
        assert nospam.last_logged.currsize == 0

    def test_cache_size_limit(self):
        nospam = NoSpamLogger(Mock(), max_cache_size=10, min_interval_seconds=1)
        for i in range(11):
            nospam.info(f"Message {i}")
        assert nospam.last_logged.currsize == 10
