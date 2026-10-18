# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""OpenTelemetry Protocol (OTLP) logging handler.

This module provides a custom logging handler that sends log records
to an OTLP endpoint using either HTTP or gRPC protocol.

IMPORTANT: This module uses a shared singleton LoggerProvider to avoid
conflicts when multiple loggers use OTLP handlers. All handlers share
the same gRPC/HTTP connection to the OTLP endpoint.
"""

import atexit
import contextlib
import logging
import os
import threading
import time

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter as GRPCLogExporter,
)
from opentelemetry.exporter.otlp.proto.http._log_exporter import (
    OTLPLogExporter as HTTPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.resources import Resource

from recovery_libs.utils.types import OTLPProtocol

logger = logging.getLogger(__name__)

# Shared singleton LoggerProvider to avoid multiple gRPC/HTTP connections
# conflicting with each other
_shared_logger_provider: LoggerProvider | None = None
_shared_provider_lock = threading.Lock()
_shared_provider_config: dict | None = None  # Stores config of the shared provider


def _get_shared_logger_provider(
    endpoint: str,
    protocol: OTLPProtocol,
    token: str | None,
    service_name: str,
) -> LoggerProvider:
    """Get or create the shared singleton LoggerProvider.

    The first caller's configuration is used. Subsequent calls with different
    configurations will log a warning but use the existing provider.

    Args:
        endpoint: OTLP endpoint URL.
        protocol: Protocol to use (HTTP or gRPC).
        token: Optional authentication token.
        service_name: Name of the service for resource attributes.

    Returns:
        The shared LoggerProvider instance.
    """
    global _shared_logger_provider, _shared_provider_config

    with _shared_provider_lock:
        if _shared_logger_provider is None:
            # First caller initializes the shared provider
            resource = Resource.create(
                {
                    "service.name": service_name,
                    "service.version": "1.0.0",
                }
            )
            _shared_logger_provider = LoggerProvider(resource=resource)

            # Create appropriate exporter based on protocol
            exporter: LogRecordExporter
            if protocol == OTLPProtocol.HTTP:
                if token:
                    exporter = HTTPLogExporter(endpoint=endpoint, headers={"Authorization": f"Bearer {token}"})
                else:
                    exporter = HTTPLogExporter(endpoint=endpoint)
            elif token:
                exporter = GRPCLogExporter(endpoint=endpoint, headers=(("authorization", f"Bearer {token}"),))
            else:
                exporter = GRPCLogExporter(endpoint=endpoint)

            _shared_logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

            # Store configuration for comparison
            _shared_provider_config = {
                "endpoint": endpoint,
                "protocol": protocol,
                "service_name": service_name,
            }

            # Register cleanup on interpreter shutdown
            atexit.register(_shutdown_shared_provider)

            logger.debug(
                f"Initialized shared OTLP LoggerProvider: endpoint={endpoint}, "
                f"protocol={protocol.value}, service={service_name}"
            )
        else:
            # Warn if configuration differs from the existing provider
            if _shared_provider_config and (
                _shared_provider_config["endpoint"] != endpoint or _shared_provider_config["protocol"] != protocol
            ):
                logger.warning(
                    f"OTLP handler requested with different config than existing provider. "
                    f"Using existing: endpoint={_shared_provider_config['endpoint']}, "
                    f"protocol={_shared_provider_config['protocol'].value}. "
                    f"Requested: endpoint={endpoint}, protocol={protocol.value}"
                )

        return _shared_logger_provider


def _shutdown_shared_provider() -> None:
    """Shutdown the shared LoggerProvider on interpreter exit."""
    global _shared_logger_provider
    if _shared_logger_provider is not None:
        with contextlib.suppress(Exception):
            _shared_logger_provider.force_flush(timeout_millis=5000)
            _shared_logger_provider.shutdown()
        _shared_logger_provider = None


def _reset_shared_provider() -> None:
    """Reset the shared LoggerProvider for testing purposes.

    WARNING: Only use this in tests! This will invalidate any existing
    OTLPHandler instances that were using the previous provider.
    """
    global _shared_logger_provider, _shared_provider_config
    with _shared_provider_lock:
        _shared_logger_provider = None
        _shared_provider_config = None


class OTLPHandler(logging.Handler):
    """Custom logging handler for sending logs via OpenTelemetry Protocol (OTLP).

    This handler integrates with OpenTelemetry's logging infrastructure to
    send structured log data to a configured OTLP endpoint. Supports both
    HTTP and gRPC protocols with optional token-based authentication.

    IMPORTANT: All OTLPHandler instances share a singleton LoggerProvider
    to avoid conflicts from multiple gRPC connections. The first handler
    to be created initializes the shared provider with its configuration.

    Attributes:
        endpoint: The OTLP endpoint URL.
        protocol: The protocol to use ('http' or 'grpc').
        token: Optional authentication token.

    Example:
        >>> handler = OTLPHandler(
        ...     endpoint="http://localhost:4318",
        ...     protocol="http"
        ... )
        >>> logger = logging.getLogger("myapp")
        >>> logger.addHandler(handler)
        >>> logger.info("Test message")
    """

    def __init__(
        self,
        endpoint: str,
        protocol: OTLPProtocol = OTLPProtocol.GRPC,
        token: str | None = None,
        service_name: str | None = None,
    ):
        """Initialize the OTLP logging handler.

        Args:
            endpoint: OTLP endpoint URL (e.g., "http://localhost:4318" or
                "localhost:4317" for gRPC).
            protocol: Protocol to use for communication. Either OTLPProtocol.HTTP
                or OTLPProtocol.GRPC. Defaults to OTLPProtocol.GRPC.
            token: Optional authentication token. If provided, it will be sent
                as a Bearer token in the Authorization header for HTTP, or as
                metadata for gRPC.
            service_name: Name of the service sending logs. If not provided,
                attempts to detect from environment variable OTEL_SERVICE_NAME,
                otherwise defaults to "heimdall".

        Raises:
            ValueError: If protocol is not a valid OTLPProtocol.
            RuntimeError: If the OTLP exporter fails to initialize.
        """
        super().__init__()

        if not isinstance(protocol, OTLPProtocol):
            raise ValueError(f"Invalid protocol type: {type(protocol).__name__}. Must be OTLPProtocol enum.")  # noqa: TRY004

        self.endpoint = endpoint
        self.protocol = protocol
        self.token = token

        # Determine service name from parameter, environment variable, or default
        if service_name is None:
            service_name = os.environ.get("OTEL_SERVICE_NAME", "heimdall")

        self._service_name = service_name

        # Get the shared LoggerProvider (creates it if first call)
        try:
            self._logger_provider = _get_shared_logger_provider(
                endpoint=endpoint,
                protocol=protocol,
                token=token,
                service_name=service_name,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize OTLP exporter: {e}") from e

        # Create OpenTelemetry logging handler using the shared provider
        self._otlp_handler = LoggingHandler(logger_provider=self._logger_provider)

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to the OTLP endpoint.

        Args:
            record: The LogRecord to be logged.
        """
        try:
            # Add custom attributes to the record
            self._enrich_record(record)

            # Let OpenTelemetry handler process the record
            self._otlp_handler.emit(record)
        except Exception:  # noqa: BLE001
            # Don't let logging failures crash the application
            self.handleError(record)

    def _enrich_record(self, record: logging.LogRecord) -> None:
        """Enrich log record with additional contextual information.

        Args:
            record: The LogRecord to enrich.
        """
        record_dict = record.__dict__

        # Add timestamp in milliseconds using time_ns()
        if "timestamp_ms" not in record_dict:
            record.timestamp_ms = int(time.time_ns() // 1_000_000)

        # Add severity text for better filtering
        if "severity_text" not in record_dict:
            record.severity_text = record.levelname

        # Ensure message is a string
        if type(record.msg) is not str:
            record.msg = str(record.msg)

    def flush(self) -> None:
        """Flush any buffered log records.

        Forces immediate export of all pending log records.
        Useful for ensuring logs are sent before application shutdown.
        """
        try:
            self._logger_provider.force_flush(timeout_millis=30000)
        except Exception as e:
            logger.exception("Failed to flush OTLP logs", extra={"error": str(e)})

    def close(self) -> None:
        """Close the handler.

        Note: Does NOT shutdown the shared LoggerProvider, as other handlers
        may still be using it. The provider is shut down on interpreter exit.
        """
        try:
            # Just flush, don't shutdown the shared provider
            self.flush()
        except Exception as e:
            logger.exception("Error closing OTLP handler", extra={"error": str(e)})
        finally:
            super().close()
