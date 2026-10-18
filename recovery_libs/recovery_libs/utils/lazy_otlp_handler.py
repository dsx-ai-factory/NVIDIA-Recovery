# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Lazy-loading OTLP handler with automatic retry and self-healing.

This module provides a logging handler that gracefully handles OTLP endpoint
unavailability by retrying connections in the background with exponential backoff.
"""

import atexit
import contextlib
import logging
import socket
import threading
from urllib.parse import urlparse

from recovery_libs.utils.constants import OTLP_GRPC_DEFAULT_PORT, OTLP_HTTP_DEFAULT_PORT
from recovery_libs.utils.otlp_handler import OTLPHandler
from recovery_libs.utils.types import OTLPProtocol

logger = logging.getLogger(__name__)


def _check_endpoint_reachable(endpoint: str, protocol: OTLPProtocol, timeout: float = 5.0) -> bool:
    """Check if the OTLP endpoint is reachable via TCP socket connection.

    This performs a TCP connection check to verify the endpoint is accessible
    before attempting to create the full OTLP exporter pipeline. This avoids
    false positives from the OpenTelemetry SDK's asynchronous batch processing
    which doesn't raise exceptions on connection failures.

    Args:
        endpoint: OTLP endpoint URL (e.g., "http://localhost:4317" or "localhost:4317").
        protocol: The protocol being used (affects default port selection).
        timeout: Connection timeout in seconds. Defaults to 5.0.

    Returns:
        True if the endpoint is reachable, False otherwise.
    """
    try:
        parsed = urlparse(endpoint)

        if parsed.hostname:
            host = parsed.hostname
            port = parsed.port
        else:
            # Handle bare host:port format (common for gRPC)
            if ":" in endpoint:
                parts = endpoint.rsplit(":", 1)
                host = parts[0]
                port = int(parts[1]) if len(parts) > 1 else None
            else:
                host = endpoint
                port = None

        # Default ports based on protocol
        if port is None:
            port = OTLP_GRPC_DEFAULT_PORT if protocol == OTLPProtocol.GRPC else OTLP_HTTP_DEFAULT_PORT

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
            return True
        finally:
            sock.close()

    except (TimeoutError, OSError, ValueError, TypeError) as e:
        logger.warning("[OTLPHandler] Endpoint reachability check failed: %s", e)
        return False


class LazyOTLPHandler(logging.Handler):
    """OTLP handler that retries connection in background until successful.

    This handler provides graceful degradation when OTLP endpoints are unavailable:
    - Application starts immediately without blocking
    - Background thread retries connection with exponential backoff
    - Automatically activates when connection succeeds
    - Self-heals if connection is lost and later restored
    - Gracefully shuts down on application exit

    Attributes:
        _endpoint: OTLP endpoint URL.
        _protocol: Protocol to use (HTTP or gRPC).
        _token: Optional authentication token.
        _service_name: Name of the service sending logs.
        _initial_retry_interval: Initial retry interval in seconds (default: 60).
        _max_retry_interval: Maximum retry interval in seconds (default: 300).
        _active_handler: The actual OTLPHandler instance once connected.
        _handler_lock: Thread lock for write operations only.
        _stop_retrying: Event to signal retry thread shutdown.
        _retry_thread: Background thread that handles connection retries.
        _current_retry_interval: Current backoff interval.
    """

    def __init__(
        self,
        endpoint: str,
        protocol: OTLPProtocol = OTLPProtocol.GRPC,
        token: str | None = None,
        service_name: str | None = None,
        initial_retry_interval: int = 60,
        max_retry_interval: int = 300,
    ):
        """Initialize the lazy OTLP handler.

        Args:
            endpoint: OTLP endpoint URL.
            protocol: Protocol to use (HTTP or gRPC). Defaults to gRPC.
            token: Optional authentication token.
            service_name: Name of the service sending logs.
            initial_retry_interval: Initial retry interval in seconds. Defaults to 60.
            max_retry_interval: Maximum retry interval in seconds. Defaults to 300.
        """
        super().__init__()

        self._endpoint = endpoint
        self._protocol = protocol
        self._token = token
        self._service_name = service_name
        self._initial_retry_interval = initial_retry_interval
        self._max_retry_interval = max_retry_interval

        # Connection state
        self._active_handler: OTLPHandler | None = None
        self._handler_lock = threading.RLock()  # Only for write operations
        self._stop_retrying = threading.Event()
        self._current_retry_interval = initial_retry_interval

        # Start background thread to establish connection
        self._retry_thread = threading.Thread(target=self._retry_connection_loop, name="OTLPRetryThread", daemon=True)
        self._retry_thread.start()

        # Register cleanup on application exit
        atexit.register(self._cleanup_safe)

    def _retry_connection_loop(self) -> None:
        """Background thread that retries OTLP connection with exponential backoff.

        Continuously attempts to establish connection until successful or shutdown.
        Uses exponential backoff: 60s, 120s, 240s, 300s (max).
        """
        attempt = 0

        while not self._stop_retrying.is_set():
            # Only retry if not already connected
            if self._active_handler is not None:
                break

            attempt += 1

            # Attempt connection validation
            if self._test_and_activate_connection():
                logger.info(
                    "[OTLPHandler] Successfully connected to OTLP endpoint %s after %d attempt(s)",
                    self._endpoint,
                    attempt,
                )
                break

            # Connection failed, apply exponential backoff
            logger.debug(
                "[OTLPHandler] OTLP connection attempt %d failed. Retrying in %ds",
                attempt,
                self._current_retry_interval,
            )

            # Wait for retry interval or shutdown signal
            self._stop_retrying.wait(self._current_retry_interval)

            # Increase backoff interval (exponential) up to max
            self._current_retry_interval = min(self._current_retry_interval * 2, self._max_retry_interval)

    def _test_and_activate_connection(self) -> bool:
        """Test OTLP connection by verifying endpoint reachability.

        Performs a TCP connection check to verify the endpoint is accessible,
        then creates the handler. This avoids false positives from the OpenTelemetry
        SDK's asynchronous batch processing which doesn't raise exceptions on
        connection failures.

        Returns:
            True if connection successful and handler activated, False otherwise.
        """
        try:
            # Verify endpoint is reachable via TCP connection
            if not _check_endpoint_reachable(self._endpoint, self._protocol, timeout=5.0):
                logger.info(
                    "[OTLPHandler] Endpoint %s not reachable, skipping handler creation",
                    self._endpoint,
                )
                return False

            # Create handler (validates configuration)
            handler = OTLPHandler(
                endpoint=self._endpoint,
                protocol=self._protocol,
                token=self._token,
                service_name=self._service_name,
            )

            # Activate this handler
            with self._handler_lock:
                self._active_handler = handler

            return True

        except Exception as e:
            logger.info("[OTLPHandler] Connection validation failed", exc_info=True, extra={"error": str(e)})
            return False

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to OTLP endpoint if connected.

        If the handler is not yet connected, the log is silently dropped.
        If the connection fails during emit, the handler is reset for retry.

        Args:
            record: The LogRecord to emit.
        """
        # Fast path: atomic read, no lock
        handler = self._active_handler
        if handler is None:
            return  # No handler, return immediately

        try:
            handler.emit(record)
        except Exception:  # noqa: BLE001
            # Slow path: handle connection failure (rare)
            self._handle_emit_failure(record)

    def _handle_emit_failure(self, record: logging.LogRecord) -> None:
        """Handle emit failure by resetting connection (slow path, rare).

        Args:
            record: The LogRecord that failed to emit.
        """
        with self._handler_lock:
            # Double-check pattern: handler might have been reset by another thread
            if self._active_handler is None:
                return

            logger.debug("[OTLPHandler] Emit failed, resetting connection")

            # Close failed handler
            with contextlib.suppress(Exception):
                self._active_handler.close()

            # Reset for retry
            self._active_handler = None
            self._current_retry_interval = self._initial_retry_interval

            # Restart retry thread if needed
            self._restart_retry_thread()

            # Don't crash the application
            self.handleError(record)

    def _restart_retry_thread(self) -> None:
        """Restart retry thread if not running (must be called within lock)."""
        if not self._retry_thread.is_alive() and not self._stop_retrying.is_set():
            self._retry_thread = threading.Thread(
                target=self._retry_connection_loop, name="OTLPRetryThread", daemon=True
            )
            self._retry_thread.start()

    def flush(self) -> None:
        """Flush any buffered log records.

        Forces immediate export of all pending log records if handler is connected.
        """
        # Atomic read, no lock needed
        handler = self._active_handler
        if handler is not None:
            try:
                handler.flush()
            except Exception as e:
                logger.exception("[OTLPHandler] Failed to flush logs", extra={"error": str(e)})

    def close(self) -> None:
        """Close the handler and release resources.

        Stops the retry thread, flushes pending logs, and shuts down cleanly.
        This is called automatically via atexit registration.
        """
        self._cleanup()
        super().close()

    def _cleanup(self) -> None:
        """Internal cleanup method for graceful shutdown."""
        # Signal retry thread to stop
        self._stop_retrying.set()

        # Wait for retry thread to finish (with timeout)
        if self._retry_thread.is_alive():
            self._retry_thread.join(timeout=5.0)

        # Close active handler if present
        with self._handler_lock:
            if self._active_handler is not None:
                try:
                    self._active_handler.flush()
                    self._active_handler.close()
                    logger.debug("[OTLPHandler] Gracefully shut down OTLP handler")
                except Exception as e:
                    logger.exception("[OTLPHandler] Error during cleanup", extra={"error": str(e)})
                finally:
                    self._active_handler = None

    def _cleanup_safe(self) -> None:
        """Cleanup method safe to call during interpreter shutdown.

        Avoids threading operations that may fail during shutdown.
        """
        # Signal retry thread to stop (doesn't create new threads)
        self._stop_retrying.set()

        # Close active handler if present (avoid waiting on threads)
        with contextlib.suppress(Exception), self._handler_lock:
            if self._active_handler is not None:
                try:
                    # Try to flush but don't fail if threading doesn't work
                    self._active_handler.flush()
                except RuntimeError:
                    # Ignore "can't create new thread at interpreter shutdown"
                    pass
                except Exception as e:
                    logger.exception("[OTLPHandler] Error during shutdown flush", extra={"error": str(e)})

                try:
                    self._active_handler.close()
                except RuntimeError:
                    pass
                except Exception as e:
                    logger.exception("[OTLPHandler] Error during shutdown close", extra={"error": str(e)})

                self._active_handler = None

    @property
    def is_connected(self) -> bool:
        """Check if OTLP handler is currently connected.

        Returns:
            True if connected and ready to send logs, False otherwise.
        """
        # Atomic read, no lock needed
        return self._active_handler is not None
