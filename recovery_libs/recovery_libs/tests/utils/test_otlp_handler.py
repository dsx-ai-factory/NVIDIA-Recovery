# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Unit tests for the OTLP handler module."""

import logging
from unittest.mock import MagicMock, Mock

import pytest
from recovery_libs.utils.otlp_handler import OTLPHandler
from recovery_libs.utils.types import OTLPProtocol


@pytest.fixture
def log_record():
    """Create a test log record."""
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )


class TestOTLPHandlerInitialization:
    """Test suite for OTLPHandler initialization."""

    @pytest.mark.parametrize(
        "protocol,endpoint",
        [
            (OTLPProtocol.HTTP, "http://localhost:4318"),
            (OTLPProtocol.HTTP, "https://otel.example.com:4318"),
            (OTLPProtocol.GRPC, "localhost:4317"),
            (OTLPProtocol.GRPC, "otel.example.com:4317"),
        ],
        ids=["HTTP-localhost", "HTTP-remote", "gRPC-localhost", "gRPC-remote"],
    )
    def test_otlp_handler_initialization_valid_configs(
        self,
        protocol: OTLPProtocol,
        endpoint: str,
        mock_http_exporter: Mock,
        mock_grpc_exporter: Mock,
        mock_logger_provider: Mock,
    ):
        """Test handler initialization with valid configurations.

        Args:
            protocol: OTLP protocol enum.
            endpoint: OTLP endpoint URL.
            mock_http_exporter: Mocked HTTP exporter.
            mock_grpc_exporter: Mocked gRPC exporter.
            mock_logger_provider: Mocked logger provider.
        """
        handler = OTLPHandler(endpoint=endpoint, protocol=protocol)

        assert handler.endpoint == endpoint
        assert handler.protocol == protocol
        assert handler.token is None
        assert handler._logger_provider is not None

    @pytest.mark.parametrize(
        "protocol,expected_exporter",
        [
            (OTLPProtocol.HTTP, "http"),
            (OTLPProtocol.GRPC, "grpc"),
        ],
        ids=["HTTP", "gRPC"],
    )
    def test_otlp_handler_protocol_selection(
        self, protocol, expected_exporter, mock_http_exporter, mock_grpc_exporter, mock_logger_provider
    ):
        """Test handler selects correct exporter for protocol."""
        endpoint = "http://localhost:4318" if protocol == OTLPProtocol.HTTP else "localhost:4317"
        OTLPHandler(endpoint=endpoint, protocol=protocol)

        if expected_exporter == "http":
            mock_http_exporter.assert_called_once_with(endpoint=endpoint)
            mock_grpc_exporter.assert_not_called()
        else:
            mock_grpc_exporter.assert_called_once_with(endpoint=endpoint)
            mock_http_exporter.assert_not_called()

    @pytest.mark.parametrize(
        "invalid_protocol",
        ["tcp", 123, None, {"protocol": "http"}],
        ids=["string", "int", "None", "dict"],
    )
    def test_otlp_handler_rejects_invalid_protocol_type(
        self,
        invalid_protocol,
        mock_logger_provider: Mock,
    ):
        """Test handler rejects invalid protocol types.

        Args:
            invalid_protocol: Invalid protocol value.
            mock_logger_provider: Mocked logger provider.
        """
        with pytest.raises(ValueError) as exc_info:
            OTLPHandler(endpoint="http://localhost:4318", protocol=invalid_protocol)

        assert "Invalid protocol type" in str(exc_info.value)

    def test_otlp_handler_exporter_initialization_failure(
        self,
        mock_http_exporter: Mock,
        mock_logger_provider: Mock,
    ):
        """Test handler raises RuntimeError when exporter fails to initialize."""
        mock_http_exporter.side_effect = Exception("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)

        assert "Failed to initialize" in str(exc_info.value)
        assert "Connection refused" in str(exc_info.value)

    @pytest.mark.parametrize(
        "protocol,token,expected_call",
        [
            (
                OTLPProtocol.HTTP,
                "test-token",
                {"endpoint": "http://localhost:4318", "headers": {"Authorization": "Bearer test-token"}},
            ),
            (OTLPProtocol.HTTP, None, {"endpoint": "http://localhost:4318"}),
            (
                OTLPProtocol.GRPC,
                "test-token",
                {"endpoint": "localhost:4317", "headers": (("authorization", "Bearer test-token"),)},
            ),
            (OTLPProtocol.GRPC, None, {"endpoint": "localhost:4317"}),
        ],
        ids=["HTTP-with-token", "HTTP-no-token", "gRPC-with-token", "gRPC-no-token"],
    )
    def test_otlp_handler_token_authentication(
        self, protocol, token, expected_call, mock_http_exporter, mock_grpc_exporter, mock_logger_provider
    ):
        """Test token authentication for both HTTP and gRPC protocols."""
        endpoint = expected_call["endpoint"]
        handler = OTLPHandler(endpoint=endpoint, protocol=protocol, token=token)

        assert handler.token == token
        exporter = mock_http_exporter if protocol == OTLPProtocol.HTTP else mock_grpc_exporter
        exporter.assert_called_once_with(**expected_call)


class TestOTLPHandlerEmit:
    """Test suite for OTLPHandler emit functionality."""

    def test_emit_log_record(self, mock_http_exporter, mock_logger_provider, log_record):
        """Test emitting a log record."""
        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler._otlp_handler = Mock()

        handler.emit(log_record)
        handler._otlp_handler.emit.assert_called_once_with(log_record)

    def test_emit_enriches_record(self, mock_http_exporter, mock_logger_provider):
        """Test emit enriches record with timestamp and severity."""
        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler._otlp_handler = Mock()

        record = logging.LogRecord("test", logging.WARNING, "test.py", 10, "Warning", (), None)
        handler.emit(record)

        assert hasattr(record, "timestamp_ms")
        assert isinstance(record.timestamp_ms, int)
        assert record.timestamp_ms > 0
        assert hasattr(record, "severity_text")
        assert record.severity_text == "WARNING"

    def test_emit_handles_non_string_messages(self, mock_http_exporter, mock_logger_provider):
        """Test emit converts non-string messages to strings."""
        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler._otlp_handler = Mock()

        record = logging.LogRecord("test", logging.INFO, "test.py", 10, {"key": "value"}, (), None)
        handler.emit(record)

        assert isinstance(record.msg, str)

    def test_emit_handles_errors_gracefully(self, mock_http_exporter, mock_logger_provider, log_record):
        """Test emit doesn't crash on errors."""
        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler._otlp_handler = Mock()
        handler._otlp_handler.emit.side_effect = Exception("Network error")

        handler.emit(log_record)  # Should not raise exception


class TestOTLPHandlerLifecycle:
    """Test suite for OTLPHandler lifecycle methods."""

    def test_flush_calls_force_flush(self, mock_http_exporter, mock_logger_provider):
        """Test flush calls force_flush on the shared provider."""
        mock_provider = MagicMock()
        mock_logger_provider.return_value = mock_provider

        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler.flush()

        mock_provider.force_flush.assert_called_once()

    def test_flush_handles_errors(self, mock_http_exporter, mock_logger_provider):
        """Test flush handles errors gracefully."""
        mock_provider = MagicMock()
        mock_provider.force_flush.side_effect = Exception("Operation failed")
        mock_logger_provider.return_value = mock_provider

        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler.flush()  # Should not raise exception

    def test_close_flushes_but_does_not_shutdown(self, mock_http_exporter, mock_logger_provider):
        """Test close flushes but does NOT shutdown the shared provider."""
        mock_provider = MagicMock()
        mock_logger_provider.return_value = mock_provider

        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler.close()

        # Close should flush but NOT shutdown the shared provider
        mock_provider.force_flush.assert_called_once()
        mock_provider.shutdown.assert_not_called()

    def test_close_handles_errors(self, mock_http_exporter, mock_logger_provider):
        """Test close handles errors gracefully."""
        mock_provider = MagicMock()
        mock_provider.force_flush.side_effect = Exception("Operation failed")
        mock_logger_provider.return_value = mock_provider

        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler.close()  # Should not raise exception


class TestOTLPHandlerIntegration:
    """Integration tests for OTLPHandler with logging system."""

    def test_handler_integration(self, mock_http_exporter, mock_logger_provider):
        """Test handler integrates with logging system and respects log levels."""
        handler = OTLPHandler(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP)
        handler._otlp_handler = Mock()
        handler.setLevel(logging.WARNING)

        test_logger = logging.getLogger("test_integration")
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)

        # INFO message should be filtered by handler level
        test_logger.info("Should not reach handler")
        handler._otlp_handler.emit.assert_not_called()

        # WARNING message should reach handler
        test_logger.warning("Should reach handler")
        assert handler._otlp_handler.emit.call_count >= 1
